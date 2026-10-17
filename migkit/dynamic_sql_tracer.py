"""Trace dynamic SQL and external application dependencies (backlog RCH21, row 629).

Extends AST scanning with bounded configuration-aware constant propagation
and sandbox/runtime traces where approved. Tracks queries from ORM mappings,
prepared statements, generated SQL, report definitions, external jobs, and
opaque/encrypted modules as unresolved or discovered dependencies.

Never treats heuristic absence as proof of no use. Parameterized SQL, generated
identifiers, and encrypted/unavailable code are reflected in readiness coverage;
migration waves cannot cut over a dependency whose consumer still points at
the old shape.

Pure UNIX style, zero emojis/glyphs, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
import os
import re
import time
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union


# Care badges
CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class DependencySourceKind(str, Enum):
    AST_PYTHON = "ast_python"
    ORM_MAPPING = "orm_mapping"
    PREPARED_STATEMENT = "prepared_statement"
    GENERATED_SQL = "generated_sql"
    REPORT_DEFINITION = "report_definition"
    EXTERNAL_JOB = "external_job"
    ENCRYPTED_OPAQUE_CODE = "encrypted_opaque_code"


class ResolutionState(str, Enum):
    RESOLVED_STATIC = "resolved_static"
    RESOLVED_CONSTANT_PROPAGATION = "resolved_constant_propagation"
    RESOLVED_APPROVED_TRACE = "resolved_approved_trace"
    PARTIALLY_RESOLVED = "partially_resolved"
    UNRESOLVED_DYNAMIC = "unresolved_dynamic"
    UNAVAILABLE_SOURCE = "unavailable_source"


@dataclasses.dataclass
class CallSite:
    """Location of a query construction or database call."""
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    routine_or_function: Optional[str] = None
    code_snippet: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "line_number": self.line_number,
            "routine_or_function": self.routine_or_function,
            "code_snippet": self.code_snippet,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CallSite:
        return cls(
            file_path=data.get("file_path"),
            line_number=data.get("line_number"),
            routine_or_function=data.get("routine_or_function"),
            code_snippet=data.get("code_snippet", ""),
        )


@dataclasses.dataclass
class RuntimeTrace:
    """Concrete execution trace observed in sandbox, staging or audit log."""
    trace_id: str
    call_site: Optional[CallSite] = None
    concrete_sql: str = ""
    accessed_tables: List[str] = dataclasses.field(default_factory=list)
    accessed_columns: List[str] = dataclasses.field(default_factory=list)
    parameters: Dict[str, Any] = dataclasses.field(default_factory=dict)
    approved: bool = False
    approved_by: Optional[str] = None
    captured_at: float = 0.0
    host: str = "127.0.0.1"
    environment: str = "sandbox"
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "call_site": self.call_site.to_dict() if self.call_site else None,
            "concrete_sql": self.concrete_sql,
            "accessed_tables": list(self.accessed_tables),
            "accessed_columns": list(self.accessed_columns),
            "parameters": dict(self.parameters),
            "approved": self.approved,
            "approved_by": self.approved_by,
            "captured_at": self.captured_at,
            "host": self.host,
            "environment": self.environment,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RuntimeTrace:
        cs = CallSite.from_dict(data["call_site"]) if data.get("call_site") else None
        return cls(
            trace_id=data["trace_id"],
            call_site=cs,
            concrete_sql=data.get("concrete_sql", ""),
            accessed_tables=list(data.get("accessed_tables", [])),
            accessed_columns=list(data.get("accessed_columns", [])),
            parameters=dict(data.get("parameters", {})),
            approved=bool(data.get("approved", False)),
            approved_by=data.get("approved_by"),
            captured_at=float(data.get("captured_at", 0.0)),
            host=data.get("host", "127.0.0.1"),
            environment=data.get("environment", "sandbox"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclasses.dataclass
class TableShape:
    """Schema shape definition for an entity."""
    table_name: str
    schema_name: str = "public"
    columns: Dict[str, str] = dataclasses.field(default_factory=dict)
    shape_version: str = "v1"
    is_deprecated: bool = False
    deprecated_replacement: Optional[str] = None

    @property
    def canonical_name(self) -> str:
        if self.schema_name and self.schema_name != "public":
            return f"{self.schema_name}.{self.table_name}"
        return self.table_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "columns": dict(self.columns),
            "shape_version": self.shape_version,
            "is_deprecated": self.is_deprecated,
            "deprecated_replacement": self.deprecated_replacement,
            "canonical_name": self.canonical_name,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TableShape:
        return cls(
            table_name=data["table_name"],
            schema_name=data.get("schema_name", "public"),
            columns=dict(data.get("columns", {})),
            shape_version=data.get("shape_version", "v1"),
            is_deprecated=bool(data.get("is_deprecated", False)),
            deprecated_replacement=data.get("deprecated_replacement"),
        )


class SchemaContract:
    """Tracks pre-migration (current) vs post-migration (target) shapes."""

    def __init__(self) -> None:
        self.current_shapes: Dict[str, TableShape] = {}
        self.target_shapes: Dict[str, TableShape] = {}
        self.renamed_tables: Dict[str, str] = {}
        self.renamed_columns: Dict[str, Dict[str, str]] = {}
        self.dropped_columns: Dict[str, Set[str]] = {}

    def add_current_shape(self, shape: TableShape) -> None:
        self.current_shapes[shape.canonical_name] = shape
        self.current_shapes[shape.table_name] = shape

    def add_target_shape(self, shape: TableShape) -> None:
        self.target_shapes[shape.canonical_name] = shape
        self.target_shapes[shape.table_name] = shape

    def record_table_rename(self, old_name: str, new_name: str) -> None:
        self.renamed_tables[old_name] = new_name

    def record_column_rename(self, table: str, old_col: str, new_col: str) -> None:
        if table not in self.renamed_columns:
            self.renamed_columns[table] = {}
        self.renamed_columns[table][old_col] = new_col

    def record_dropped_column(self, table: str, col_name: str) -> None:
        if table not in self.dropped_columns:
            self.dropped_columns[table] = set()
        self.dropped_columns[table].add(col_name)

    def evaluate_consumer_compatibility(
        self,
        accessed_tables: Sequence[str],
        accessed_columns: Sequence[str],
        raw_query: str = "",
    ) -> Tuple[bool, List[str]]:
        """Returns (is_pointing_at_old_shape, reasons)."""
        reasons: List[str] = []

        for tbl in accessed_tables:
            # Check if table was renamed in target schema
            if tbl in self.renamed_tables:
                new_tbl = self.renamed_tables[tbl]
                reasons.append(
                    f"consumer references pre-migration table name '{tbl}'"
                    f" (renamed to '{new_tbl}')"
                )

            # Check if table is marked deprecated
            curr = self.current_shapes.get(tbl)
            if curr and curr.is_deprecated:
                reasons.append(
                    f"consumer references deprecated table '{tbl}'"
                    f" (replacement: {curr.deprecated_replacement or 'none'})"
                )

            # Check dropped columns
            dropped = self.dropped_columns.get(tbl, set())
            for col in accessed_columns:
                col_bare = col.split(".")[-1]
                if col_bare in dropped or col in dropped:
                    reasons.append(
                        f"consumer references dropped column '{col}'"
                        f" on table '{tbl}'"
                    )

            # Check renamed columns
            renamed = self.renamed_columns.get(tbl, {})
            for col in accessed_columns:
                col_bare = col.split(".")[-1]
                if col_bare in renamed:
                    reasons.append(
                        f"consumer references old column name '{col}' on table '{tbl}'"
                        f" (renamed to '{renamed[col_bare]}')"
                    )

        # Also inspect raw_query for dropped column or old table names if columns empty
        if not accessed_columns and raw_query:
            query_upper = raw_query.upper()
            for tbl, dropped_set in self.dropped_columns.items():
                if tbl in accessed_tables:
                    for d_col in dropped_set:
                        pattern = r"\b" + re.escape(d_col.upper()) + r"\b"
                        if re.search(pattern, query_upper):
                            reasons.append(
                                f"consumer query text contains dropped column '{d_col}'"
                                f" on table '{tbl}'"
                            )

        return (len(reasons) > 0, reasons)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_shapes": {k: v.to_dict() for k, v in self.current_shapes.items()},
            "target_shapes": {k: v.to_dict() for k, v in self.target_shapes.items()},
            "renamed_tables": dict(self.renamed_tables),
            "renamed_columns": {k: dict(v) for k, v in self.renamed_columns.items()},
            "dropped_columns": {k: sorted(list(v)) for k, v in self.dropped_columns.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchemaContract:
        sc = cls()
        for k, v in data.get("current_shapes", {}).items():
            sc.current_shapes[k] = TableShape.from_dict(v)
        for k, v in data.get("target_shapes", {}).items():
            sc.target_shapes[k] = TableShape.from_dict(v)
        sc.renamed_tables = dict(data.get("renamed_tables", {}))
        for k, v in data.get("renamed_columns", {}).items():
            sc.renamed_columns[k] = dict(v)
        for k, v in data.get("dropped_columns", {}).items():
            sc.dropped_columns[k] = set(v)
        return sc


@dataclasses.dataclass
class AppDependency:
    """Discovered application query or external dependency."""
    dependency_id: str
    source_kind: DependencySourceKind
    consumer_name: str
    call_site: Optional[CallSite] = None
    raw_query_or_expression: str = ""
    resolved_sql: Optional[str] = None
    accessed_tables: List[str] = dataclasses.field(default_factory=list)
    accessed_columns: List[str] = dataclasses.field(default_factory=list)
    resolution_state: ResolutionState = ResolutionState.UNRESOLVED_DYNAMIC
    blocking_identifiers: List[str] = dataclasses.field(default_factory=list)
    is_parameterized: bool = False
    parameter_markers: List[str] = dataclasses.field(default_factory=list)
    associated_trace_id: Optional[str] = None
    points_at_old_shape: bool = False
    old_shape_reasons: List[str] = dataclasses.field(default_factory=list)
    badge: CareBadge = CareBadge.ACTION
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dependency_id": self.dependency_id,
            "source_kind": self.source_kind.value,
            "consumer_name": self.consumer_name,
            "call_site": self.call_site.to_dict() if self.call_site else None,
            "raw_query_or_expression": self.raw_query_or_expression,
            "resolved_sql": self.resolved_sql,
            "accessed_tables": list(self.accessed_tables),
            "accessed_columns": list(self.accessed_columns),
            "resolution_state": self.resolution_state.value,
            "blocking_identifiers": list(self.blocking_identifiers),
            "is_parameterized": self.is_parameterized,
            "parameter_markers": list(self.parameter_markers),
            "associated_trace_id": self.associated_trace_id,
            "points_at_old_shape": self.points_at_old_shape,
            "old_shape_reasons": list(self.old_shape_reasons),
            "badge": self.badge.value,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AppDependency:
        cs = CallSite.from_dict(data["call_site"]) if data.get("call_site") else None
        return cls(
            dependency_id=data["dependency_id"],
            source_kind=DependencySourceKind(data["source_kind"]),
            consumer_name=data["consumer_name"],
            call_site=cs,
            raw_query_or_expression=data.get("raw_query_or_expression", ""),
            resolved_sql=data.get("resolved_sql"),
            accessed_tables=list(data.get("accessed_tables", [])),
            accessed_columns=list(data.get("accessed_columns", [])),
            resolution_state=ResolutionState(data.get("resolution_state", ResolutionState.UNRESOLVED_DYNAMIC.value)),
            blocking_identifiers=list(data.get("blocking_identifiers", [])),
            is_parameterized=bool(data.get("is_parameterized", False)),
            parameter_markers=list(data.get("parameter_markers", [])),
            associated_trace_id=data.get("associated_trace_id"),
            points_at_old_shape=bool(data.get("points_at_old_shape", False)),
            old_shape_reasons=list(data.get("old_shape_reasons", [])),
            badge=CareBadge(data.get("badge", CareBadge.ACTION.value)),
            notes=data.get("notes", ""),
        )


@dataclasses.dataclass
class MigrationWave:
    """A migration wave containing target tables to cut over together."""
    wave_id: str
    wave_name: str
    target_tables: Set[str]
    prerequisite_wave_ids: List[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "wave_id": self.wave_id,
            "wave_name": self.wave_name,
            "target_tables": sorted(list(self.target_tables)),
            "prerequisite_wave_ids": list(self.prerequisite_wave_ids),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> MigrationWave:
        return cls(
            wave_id=data["wave_id"],
            wave_name=data["wave_name"],
            target_tables=set(data.get("target_tables", [])),
            prerequisite_wave_ids=list(data.get("prerequisite_wave_ids", [])),
        )


@dataclasses.dataclass
class CutoverGateVerdict:
    """Verdict on whether a migration wave can safely cut over."""
    wave_id: str
    can_cutover: bool
    badge: CareBadge
    blocking_dependencies: List[str]
    reasons: List[str]
    total_consumers_checked: int
    old_shape_consumer_count: int
    unresolved_dynamic_count: int
    opaque_code_count: int

    def format_unix_report(self) -> str:
        lines = [
            f"=== MIGRATION WAVE CUTOVER GATE [{self.wave_id}] ===",
            f"STATUS       : {self.badge.value}",
            f"CAN CUTOVER  : {'YES' if self.can_cutover else 'NO'}",
            f"TOTAL CONSUMERS CHECKED     : {self.total_consumers_checked}",
            f"CONSUMERS ON OLD SHAPE      : {self.old_shape_consumer_count}",
            f"UNRESOLVED DYNAMIC QUERIES  : {self.unresolved_dynamic_count}",
            f"OPAQUE / UNAVAILABLE MODULES: {self.opaque_code_count}",
        ]
        if self.blocking_dependencies:
            lines.append("BLOCKING DEPENDENCY IDS:")
            for dep_id in self.blocking_dependencies:
                lines.append(f"  - {dep_id}")
        if self.reasons:
            lines.append("REASONS:")
            for r in self.reasons:
                lines.append(f"  * {r}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "wave_id": self.wave_id,
            "can_cutover": self.can_cutover,
            "badge": self.badge.value,
            "blocking_dependencies": list(self.blocking_dependencies),
            "reasons": list(self.reasons),
            "total_consumers_checked": self.total_consumers_checked,
            "old_shape_consumer_count": self.old_shape_consumer_count,
            "unresolved_dynamic_count": self.unresolved_dynamic_count,
            "opaque_code_count": self.opaque_code_count,
        }


@dataclasses.dataclass
class ReadinessCoverage:
    """Aggregate readiness assessment across all tracked dependencies."""
    total_dependencies: int
    resolved_count: int
    partially_resolved_count: int
    unresolved_count: int
    opaque_code_count: int
    approved_trace_count: int
    unapproved_trace_count: int
    old_shape_consumer_count: int
    coverage_pct: float
    badge: CareBadge
    blockers: List[str]

    def format_unix_report(self) -> str:
        lines = [
            "=== APPLICATION DEPENDENCY READINESS COVERAGE ===",
            f"STATUS BADGE : {self.badge.value}",
            f"COVERAGE PCT : {self.coverage_pct:.2f}%",
            f"TOTAL DEPENDENCIES DISCOVERED: {self.total_dependencies}",
            f"  FULLY RESOLVED             : {self.resolved_count}",
            f"  PARTIALLY RESOLVED         : {self.partially_resolved_count}",
            f"  UNRESOLVED DYNAMIC QUERIES : {self.unresolved_count}",
            f"  OPAQUE / UNAVAILABLE CODE  : {self.opaque_code_count}",
            f"  APPROVED RUNTIME TRACES    : {self.approved_trace_count}",
            f"  UNAPPROVED RUNTIME TRACES  : {self.unapproved_trace_count}",
            f"  CONSUMERS ON OLD SHAPE     : {self.old_shape_consumer_count}",
        ]
        if self.blockers:
            lines.append("BLOCKERS:")
            for b in self.blockers:
                lines.append(f"  [ACTION] {b}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_dependencies": self.total_dependencies,
            "resolved_count": self.resolved_count,
            "partially_resolved_count": self.partially_resolved_count,
            "unresolved_count": self.unresolved_count,
            "opaque_code_count": self.opaque_code_count,
            "approved_trace_count": self.approved_trace_count,
            "unapproved_trace_count": self.unapproved_trace_count,
            "old_shape_consumer_count": self.old_shape_consumer_count,
            "coverage_pct": self.coverage_pct,
            "badge": self.badge.value,
            "blockers": list(self.blockers),
        }


def extract_sql_tables(sql_text: str) -> List[str]:
    """Extract table references from SQL statement via regex patterns."""
    tables: Set[str] = set()
    cleaned = re.sub(r"--[^\n]*", " ", sql_text)
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)

    patterns = [
        r"\bFROM\s+([a-zA-Z0-9_\.]+)",
        r"\bJOIN\s+([a-zA-Z0-9_\.]+)",
        r"\bINTO\s+([a-zA-Z0-9_\.]+)",
        r"\bUPDATE\s+([a-zA-Z0-9_\.]+)",
        r"\bTABLE\s+([a-zA-Z0-9_\.]+)",
        r"\bTRUNCATE\s+TABLE\s+([a-zA-Z0-9_\.]+)",
    ]

    for p in patterns:
        for m in re.finditer(p, cleaned, flags=re.IGNORECASE):
            raw_tbl = m.group(1).strip("`\"'[]")
            if raw_tbl.upper() not in {"SELECT", "SET", "VALUES", "WHERE", "JOIN", "DUAL"}:
                tables.add(raw_tbl)

    return sorted(list(tables))


def extract_sql_columns(sql_text: str) -> List[str]:
    """Extract referenced column names from simple SELECT/WHERE/SET clauses."""
    columns: Set[str] = set()
    cleaned = re.sub(r"--[^\n]*", " ", sql_text)
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)

    # SELECT col1, col2 FROM ...
    m_sel = re.search(r"\bSELECT\s+(.+?)\s+\bFROM\b", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if m_sel:
        col_list = m_sel.group(1).split(",")
        for c in col_list:
            c = c.strip()
            # remove aliases AS ...
            c = re.split(r"\s+AS\s+", c, flags=re.IGNORECASE)[0].strip()
            c = c.strip("`\"'[]")
            if c and not c.startswith("*") and not "(" in c:
                columns.add(c)

    # WHERE col = ... or col IN ...
    for m in re.finditer(r"\bWHERE\s+([a-zA-Z0-9_\.]+)", cleaned, flags=re.IGNORECASE):
        c = m.group(1).strip("`\"'[]")
        columns.add(c)

    for m in re.finditer(r"\bAND\s+([a-zA-Z0-9_\.]+)", cleaned, flags=re.IGNORECASE):
        c = m.group(1).strip("`\"'[]")
        columns.add(c)

    return sorted(list(columns))


def extract_parameter_markers(sql_text: str) -> List[str]:
    """Detect parameter markers in SQL (? or $1 or :name or %s)."""
    markers: List[str] = []
    # Positional $1, $2
    markers.extend(re.findall(r"\$\d+", sql_text))
    # Named :name
    named = re.findall(r":[a-zA-Z_][a-zA-Z0-9_]*", sql_text)
    markers.extend(named)
    # Question marks ?
    q_count = len(re.findall(r"\?", sql_text))
    if q_count > 0:
        markers.extend(["?"] * q_count)
    # Python %s or %(name)s
    py_named = re.findall(r"%\([a-zA-Z0-9_]+\)s", sql_text)
    markers.extend(py_named)
    if not py_named:
        py_s = len(re.findall(r"%s", sql_text))
        if py_s > 0:
            markers.extend(["%s"] * py_s)
    return markers


class BoundedConstantPropagator:
    """Evaluates Python AST expressions with bounded depth and configuration scope."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        max_depth: int = 10,
        max_steps: int = 100,
    ) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self.max_depth: int = max_depth
        self.max_steps: int = max_steps
        self.scope: Dict[str, Any] = dict(self.config)

    def set_variable(self, name: str, value: Any) -> None:
        self.scope[name] = value

    def evaluate_node(
        self,
        node: ast.AST,
        current_depth: int = 0,
        step_counter: Optional[List[int]] = None,
    ) -> Tuple[Optional[Any], bool, List[str]]:
        """Evaluates an AST node.

        Returns: (resolved_value, is_dynamic, blocking_identifiers)
        """
        if step_counter is None:
            step_counter = [0]
        step_counter[0] += 1

        if current_depth > self.max_depth or step_counter[0] > self.max_steps:
            return (None, True, ["DEPTH_OR_STEP_LIMIT_EXCEEDED"])

        # Literals
        if isinstance(node, ast.Constant):
            return (node.value, False, [])

        # Identifiers
        if isinstance(node, ast.Name):
            var_name = node.id
            if var_name in self.scope:
                val = self.scope[var_name]
                return (val, False, [])
            return (None, True, [var_name])

        # Attribute lookup (e.g. Config.TABLE_NAME)
        if isinstance(node, ast.Attribute):
            val, is_dyn, blockers = self.evaluate_node(node.value, current_depth + 1, step_counter)
            if not is_dyn and val is not None:
                attr_name = node.attr
                if isinstance(val, dict) and attr_name in val:
                    return (val[attr_name], False, [])
                if hasattr(val, attr_name):
                    return (getattr(val, attr_name), False, [])
            attr_full = f"{blockers[0] if blockers else 'obj'}.{node.attr}"
            return (None, True, [attr_full])

        # Subscript (e.g. cfg["tenant_table"])
        if isinstance(node, ast.Subscript):
            val, val_dyn, val_block = self.evaluate_node(node.value, current_depth + 1, step_counter)
            slice_node = node.slice
            # Python < 3.9 wrapped slice in ast.Index
            if hasattr(ast, "Index") and isinstance(slice_node, ast.Index):
                slice_node = slice_node.value
            key_val, key_dyn, key_block = self.evaluate_node(slice_node, current_depth + 1, step_counter)

            if not val_dyn and not key_dyn and isinstance(val, dict) and key_val in val:
                return (val[key_val], False, [])
            blockers = list(val_block) + list(key_block)
            return (None, True, blockers or ["DYNAMIC_SUBSCRIPT"])

        # String Concatenation (+) or Modulo formatting (%)
        if isinstance(node, ast.BinOp):
            left_val, left_dyn, left_blk = self.evaluate_node(node.left, current_depth + 1, step_counter)
            right_val, right_dyn, right_blk = self.evaluate_node(node.right, current_depth + 1, step_counter)

            if isinstance(node.op, ast.Add):
                if not left_dyn and not right_dyn and isinstance(left_val, str) and isinstance(right_val, str):
                    return (left_val + right_val, False, [])
                # Partially dynamic concatenation: retain known strings
                partial = ""
                if isinstance(left_val, str):
                    partial += left_val
                if isinstance(right_val, str):
                    partial += right_val
                return (partial if partial else None, True, left_blk + right_blk)

            if isinstance(node.op, ast.Mod):
                if not left_dyn and not right_dyn and isinstance(left_val, str):
                    try:
                        return (left_val % right_val, False, [])
                    except Exception:
                        pass
                return (left_val if isinstance(left_val, str) else None, True, left_blk + right_blk)

        # F-strings (JoinedStr)
        if isinstance(node, ast.JoinedStr):
            parts: List[str] = []
            is_any_dyn = False
            all_blockers: List[str] = []
            for elt in node.values:
                if isinstance(elt, ast.FormattedValue):
                    val, dyn, blk = self.evaluate_node(elt.value, current_depth + 1, step_counter)
                    if dyn or val is None:
                        is_any_dyn = True
                        all_blockers.extend(blk or ["FSTRING_EXPRESSION"])
                        parts.append("{DYNAMIC}")
                    else:
                        parts.append(str(val))
                elif isinstance(elt, ast.Constant):
                    parts.append(str(elt.value))
                else:
                    val, dyn, blk = self.evaluate_node(elt, current_depth + 1, step_counter)
                    if dyn or val is None:
                        is_any_dyn = True
                        all_blockers.extend(blk)
                        parts.append("{DYNAMIC}")
                    else:
                        parts.append(str(val))

            resolved = "".join(parts)
            return (resolved, is_any_dyn, all_blockers)

        # Calls (e.g. "str".format(...), "".join(...), str(...))
        if isinstance(node, ast.Call):
            # Check for str.format()
            if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
                base_val, base_dyn, base_blk = self.evaluate_node(node.func.value, current_depth + 1, step_counter)
                args_res: List[Any] = []
                args_dyn = False
                all_blk = list(base_blk)

                for a in node.args:
                    av, ad, ab = self.evaluate_node(a, current_depth + 1, step_counter)
                    all_blk.extend(ab)
                    if ad:
                        args_dyn = True
                        args_res.append("{PARAM}")
                    else:
                        args_res.append(av)

                kwargs_res: Dict[str, Any] = {}
                for kw in node.keywords:
                    kv, kd, kb = self.evaluate_node(kw.value, current_depth + 1, step_counter)
                    all_blk.extend(kb)
                    if kd:
                        args_dyn = True
                        kwargs_res[kw.arg or ""] = "{PARAM}"
                    else:
                        kwargs_res[kw.arg or ""] = kv

                if not base_dyn and not args_dyn and isinstance(base_val, str):
                    try:
                        return (base_val.format(*args_res, **kwargs_res), False, [])
                    except Exception:
                        pass
                return (base_val if isinstance(base_val, str) else None, True, all_blk)

            # Check for "".join(...)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "join":
                sep_val, sep_dyn, sep_blk = self.evaluate_node(node.func.value, current_depth + 1, step_counter)
                if not sep_dyn and isinstance(sep_val, str) and node.args:
                    arg_node = node.args[0]
                    if isinstance(arg_node, (ast.List, ast.Tuple)):
                        elts_res = []
                        elts_dyn = False
                        elts_blk = list(sep_blk)
                        for e in arg_node.elts:
                            ev, ed, eb = self.evaluate_node(e, current_depth + 1, step_counter)
                            elts_blk.extend(eb)
                            if ed:
                                elts_dyn = True
                                elts_res.append("{DYNAMIC}")
                            else:
                                elts_res.append(str(ev))
                        if not elts_dyn:
                            return (sep_val.join(elts_res), False, [])
                        return (sep_val.join(elts_res), True, elts_blk)

            # Other calls are dynamic expressions
            call_name = getattr(node.func, "id", getattr(node.func, "attr", "function_call"))
            return (None, True, [f"CALL:{call_name}"])

        # Unknown node kind
        return (None, True, [f"UNSUPPORTED_AST:{type(node).__name__}"])


class DynamicSqlASTScanner:
    """Scans Python source code using AST visitors and bounded constant propagation."""

    DB_CALL_NAMES = {
        "execute",
        "executemany",
        "query",
        "raw",
        "select",
        "run_query",
        "cursor_execute",
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None, max_depth: int = 10) -> None:
        self.propagator = BoundedConstantPropagator(config=config, max_depth=max_depth)

    def scan_code(
        self,
        code_text: str,
        file_path: str = "app.py",
        consumer_name: str = "app",
    ) -> List[AppDependency]:
        """Scans code for database queries and dynamic SQL constructions."""
        dependencies: List[AppDependency] = []
        try:
            tree = ast.parse(code_text, filename=file_path)
        except SyntaxError as e:
            # If code cannot be parsed as Python, record an opaque/unavailable dependency
            dep = AppDependency(
                dependency_id=f"dep_parse_err_{int(time.time() * 1000)}",
                source_kind=DependencySourceKind.ENCRYPTED_OPAQUE_CODE,
                consumer_name=consumer_name,
                call_site=CallSite(file_path=file_path, line_number=e.lineno or 1),
                raw_query_or_expression=code_text[:200],
                resolution_state=ResolutionState.UNAVAILABLE_SOURCE,
                blocking_identifiers=["SYNTAX_PARSE_ERROR"],
                badge=CareBadge.ACTION,
                notes=f"AST parsing failed: {e.msg}",
            )
            return [dep]

        # Scan for variable assignments of constant strings to track in scope
        for stmt in ast.walk(tree):
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        val, is_dyn, _ = self.propagator.evaluate_node(stmt.value)
                        if not is_dyn and val is not None:
                            self.propagator.set_variable(target.id, val)

        # Walk AST to find calls to execute / query methods
        dep_index = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = None
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr

                if func_name and func_name in self.DB_CALL_NAMES and node.args:
                    dep_index += 1
                    arg_node = node.args[0]
                    resolved_val, is_dyn, blockers = self.propagator.evaluate_node(arg_node)

                    # Extract line number and snippet
                    lineno = getattr(node, "lineno", None)
                    call_site = CallSite(
                        file_path=file_path,
                        line_number=lineno,
                        routine_or_function=func_name,
                        code_snippet=ast.unparse(node) if hasattr(ast, "unparse") else "",
                    )

                    param_markers = []
                    is_param = False
                    if len(node.args) > 1 or node.keywords:
                        is_param = True

                    if isinstance(resolved_val, str):
                        param_markers.extend(extract_parameter_markers(resolved_val))
                        if param_markers:
                            is_param = True
                        accessed_tables = extract_sql_tables(resolved_val)
                        accessed_columns = extract_sql_columns(resolved_val)
                    else:
                        accessed_tables = []
                        accessed_columns = []

                    if not is_dyn and resolved_val:
                        res_state = ResolutionState.RESOLVED_CONSTANT_PROPAGATION
                        badge = CareBadge.PASS
                    elif accessed_tables and is_dyn:
                        res_state = ResolutionState.PARTIALLY_RESOLVED
                        badge = CareBadge.REVIEW
                    else:
                        res_state = ResolutionState.UNRESOLVED_DYNAMIC
                        badge = CareBadge.ACTION

                    dep = AppDependency(
                        dependency_id=f"dep_{file_path}_{lineno}_{dep_index}",
                        source_kind=DependencySourceKind.AST_PYTHON,
                        consumer_name=consumer_name,
                        call_site=call_site,
                        raw_query_or_expression=ast.unparse(arg_node) if hasattr(ast, "unparse") else str(arg_node),
                        resolved_sql=resolved_val if isinstance(resolved_val, str) else None,
                        accessed_tables=accessed_tables,
                        accessed_columns=accessed_columns,
                        resolution_state=res_state,
                        blocking_identifiers=blockers,
                        is_parameterized=is_param,
                        parameter_markers=param_markers,
                        badge=badge,
                    )
                    dependencies.append(dep)

        return dependencies


class ORMMappingParser:
    """Extracts tables, columns, and relations from ORM definitions (SQLAlchemy, Django, JPA)."""

    @staticmethod
    def parse_python_models(code_text: str, file_path: str = "models.py", consumer_name: str = "orm") -> List[AppDependency]:
        deps: List[AppDependency] = []
        try:
            tree = ast.parse(code_text, filename=file_path)
        except Exception:
            return []

        dep_idx = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                table_name = None
                columns: List[str] = []
                lineno = getattr(node, "lineno", None)

                # Inspect class body
                for item in node.body:
                    # SQLAlchemy __tablename__ = 'users'
                    if isinstance(item, ast.Assign):
                        for tgt in item.targets:
                            if isinstance(tgt, ast.Name) and tgt.id == "__tablename__":
                                if isinstance(item.value, ast.Constant):
                                    table_name = str(item.value.value)
                            # Column attributes
                            elif isinstance(tgt, ast.Name):
                                if isinstance(item.value, ast.Call):
                                    c_name = getattr(item.value.func, "id", getattr(item.value.func, "attr", ""))
                                    if "Column" in c_name or "Field" in c_name:
                                        columns.append(tgt.id)

                    # Django class Meta: db_table = 'custom_table'
                    if isinstance(item, ast.ClassDef) and item.name == "Meta":
                        for meta_item in item.body:
                            if isinstance(meta_item, ast.Assign):
                                for tgt in meta_item.targets:
                                    if isinstance(tgt, ast.Name) and tgt.id == "db_table":
                                        if isinstance(meta_item.value, ast.Constant):
                                            table_name = str(meta_item.value.value)

                if table_name:
                    dep_idx += 1
                    dep = AppDependency(
                        dependency_id=f"orm_{node.name}_{dep_idx}",
                        source_kind=DependencySourceKind.ORM_MAPPING,
                        consumer_name=consumer_name,
                        call_site=CallSite(file_path=file_path, line_number=lineno, routine_or_function=node.name),
                        raw_query_or_expression=f"Model {node.name} -> {table_name}",
                        resolved_sql=f"SELECT {', '.join(columns) if columns else '*'} FROM {table_name}",
                        accessed_tables=[table_name],
                        accessed_columns=columns,
                        resolution_state=ResolutionState.RESOLVED_STATIC,
                        badge=CareBadge.PASS,
                        notes=f"ORM mapping for entity {node.name}",
                    )
                    deps.append(dep)

        return deps

    @staticmethod
    def parse_jpa_annotations(code_text: str, file_path: str = "Entity.java", consumer_name: str = "jpa") -> List[AppDependency]:
        """Parses JPA / Hibernate @Table(name="...") and @Column annotations."""
        deps: List[AppDependency] = []
        table_matches = re.finditer(r'@Table\s*\(\s*name\s*=\s*["\']([^"\']+)["\']', code_text)
        for idx, tm in enumerate(table_matches, 1):
            table_name = tm.group(1)
            # Find columns
            cols = [cm.group(1) for cm in re.finditer(r'@Column\s*\(\s*name\s*=\s*["\']([^"\']+)["\']', code_text)]
            dep = AppDependency(
                dependency_id=f"jpa_{table_name}_{idx}",
                source_kind=DependencySourceKind.ORM_MAPPING,
                consumer_name=consumer_name,
                call_site=CallSite(file_path=file_path, line_number=1),
                raw_query_or_expression=f"@Table(name=\"{table_name}\")",
                resolved_sql=f"SELECT {', '.join(cols) if cols else '*'} FROM {table_name}",
                accessed_tables=[table_name],
                accessed_columns=cols,
                resolution_state=ResolutionState.RESOLVED_STATIC,
                badge=CareBadge.PASS,
                notes="JPA entity mapping",
            )
            deps.append(dep)
        return deps


class ReportDefinitionParser:
    """Parses BI and report definitions (Metabase cards, Tableau/LookML YAML/JSON)."""

    @staticmethod
    def parse_report_config(config_data: Union[str, Dict[str, Any]], consumer_name: str = "bi_report") -> List[AppDependency]:
        deps: List[AppDependency] = []
        if isinstance(config_data, str):
            try:
                data = json.loads(config_data)
            except Exception:
                # Text with embedded SQL query
                sql_matches = re.findall(r"(SELECT\s+.+?\s+FROM\s+[a-zA-Z0-9_\.]+.*?);", config_data, re.IGNORECASE | re.DOTALL)
                for idx, sql in enumerate(sql_matches, 1):
                    tbls = extract_sql_tables(sql)
                    cols = extract_sql_columns(sql)
                    markers = extract_parameter_markers(sql)
                    deps.append(AppDependency(
                        dependency_id=f"report_sql_{idx}",
                        source_kind=DependencySourceKind.REPORT_DEFINITION,
                        consumer_name=consumer_name,
                        raw_query_or_expression=sql.strip(),
                        resolved_sql=sql.strip(),
                        accessed_tables=tbls,
                        accessed_columns=cols,
                        resolution_state=ResolutionState.RESOLVED_STATIC,
                        is_parameterized=len(markers) > 0,
                        parameter_markers=markers,
                        badge=CareBadge.PASS,
                    ))
                return deps
        else:
            data = config_data

        reports = data if isinstance(data, list) else [data]
        for idx, rep in enumerate(reports, 1):
            rep_id = rep.get("report_id") or rep.get("id") or f"report_{idx}"
            query_sql = rep.get("sql") or rep.get("query")
            dataset = rep.get("dataset") or rep.get("table")

            tables = extract_sql_tables(query_sql) if query_sql else ([dataset] if dataset else [])
            cols = extract_sql_columns(query_sql) if query_sql else list(rep.get("fields", []))
            markers = extract_parameter_markers(query_sql) if query_sql else []

            deps.append(AppDependency(
                dependency_id=f"rep_{rep_id}",
                source_kind=DependencySourceKind.REPORT_DEFINITION,
                consumer_name=consumer_name,
                raw_query_or_expression=query_sql or f"Dataset: {dataset}",
                resolved_sql=query_sql,
                accessed_tables=tables,
                accessed_columns=cols,
                resolution_state=ResolutionState.RESOLVED_STATIC if tables else ResolutionState.UNRESOLVED_DYNAMIC,
                is_parameterized=len(markers) > 0,
                parameter_markers=markers,
                badge=CareBadge.PASS if tables else CareBadge.ACTION,
                notes=f"Report definition {rep_id}",
            ))

        return deps


class DynamicSqlTracerRegistry:
    """Central coordinator for AST scanning, runtime traces, and cutover readiness gating."""

    def __init__(self, schema_contract: Optional[SchemaContract] = None) -> None:
        self.dependencies: Dict[str, AppDependency] = {}
        self.traces: Dict[str, RuntimeTrace] = {}
        self.schema_contract: SchemaContract = schema_contract or SchemaContract()
        self.waves: Dict[str, MigrationWave] = {}
        self.config: Dict[str, Any] = {}
        self._lease_lock_file: Optional[Path] = None

    def register_config(self, config_dict: Dict[str, Any]) -> None:
        self.config.update(config_dict)

    def set_schema_contract(self, contract: SchemaContract) -> None:
        self.schema_contract = contract
        self._recheck_all_consumers_against_schema()

    def add_dependency(self, dep: AppDependency) -> None:
        self._check_dependency_shape(dep)
        self.dependencies[dep.dependency_id] = dep

    def register_python_code(self, code_text: str, file_path: str = "app.py", consumer_name: str = "app") -> List[AppDependency]:
        scanner = DynamicSqlASTScanner(config=self.config)
        deps = scanner.scan_code(code_text, file_path=file_path, consumer_name=consumer_name)
        for d in deps:
            self.add_dependency(d)
        return deps

    def register_orm_models(self, code_text: str, file_path: str = "models.py", consumer_name: str = "orm") -> List[AppDependency]:
        deps = ORMMappingParser.parse_python_models(code_text, file_path=file_path, consumer_name=consumer_name)
        for d in deps:
            self.add_dependency(d)
        return deps

    def register_prepared_statement(
        self,
        stmt_name: str,
        sql_template: str,
        consumer_name: str = "prepared_stmt",
        file_path: Optional[str] = None,
        line_number: Optional[int] = None,
    ) -> AppDependency:
        tables = extract_sql_tables(sql_template)
        columns = extract_sql_columns(sql_template)
        markers = extract_parameter_markers(sql_template)

        dep = AppDependency(
            dependency_id=f"prep_{stmt_name}",
            source_kind=DependencySourceKind.PREPARED_STATEMENT,
            consumer_name=consumer_name,
            call_site=CallSite(file_path=file_path, line_number=line_number, routine_or_function=stmt_name),
            raw_query_or_expression=sql_template,
            resolved_sql=sql_template,
            accessed_tables=tables,
            accessed_columns=columns,
            resolution_state=ResolutionState.RESOLVED_STATIC if tables else ResolutionState.UNRESOLVED_DYNAMIC,
            is_parameterized=True,
            parameter_markers=markers,
            badge=CareBadge.PASS if tables else CareBadge.ACTION,
            notes=f"Prepared statement {stmt_name}",
        )
        self.add_dependency(dep)
        return dep

    def register_generated_sql(
        self,
        generator_name: str,
        builder_expression: str,
        target_tables: Sequence[str],
        target_columns: Sequence[str] = (),
        is_dynamic: bool = True,
        consumer_name: str = "query_builder",
    ) -> AppDependency:
        state = ResolutionState.PARTIALLY_RESOLVED if is_dynamic else ResolutionState.RESOLVED_STATIC
        badge = CareBadge.REVIEW if is_dynamic else CareBadge.PASS

        dep = AppDependency(
            dependency_id=f"gen_{generator_name}",
            source_kind=DependencySourceKind.GENERATED_SQL,
            consumer_name=consumer_name,
            raw_query_or_expression=builder_expression,
            resolved_sql=f"DYNAMIC_BUILDER({', '.join(target_tables)})",
            accessed_tables=list(target_tables),
            accessed_columns=list(target_columns),
            resolution_state=state,
            badge=badge,
            notes=f"Generated SQL builder {generator_name}",
        )
        self.add_dependency(dep)
        return dep

    def register_report(self, config_data: Union[str, Dict[str, Any]], consumer_name: str = "bi_report") -> List[AppDependency]:
        deps = ReportDefinitionParser.parse_report_config(config_data, consumer_name=consumer_name)
        for d in deps:
            self.add_dependency(d)
        return deps

    def register_external_job(
        self,
        job_name: str,
        job_schedule: str,
        sql_or_commands: str,
        target_tables: Sequence[str] = (),
        consumer_name: str = "external_job",
    ) -> AppDependency:
        tables = list(target_tables) or extract_sql_tables(sql_or_commands)
        cols = extract_sql_columns(sql_or_commands)
        dep = AppDependency(
            dependency_id=f"job_{job_name}",
            source_kind=DependencySourceKind.EXTERNAL_JOB,
            consumer_name=consumer_name,
            raw_query_or_expression=sql_or_commands,
            resolved_sql=sql_or_commands if tables else None,
            accessed_tables=tables,
            accessed_columns=cols,
            resolution_state=ResolutionState.RESOLVED_STATIC if tables else ResolutionState.UNRESOLVED_DYNAMIC,
            badge=CareBadge.PASS if tables else CareBadge.ACTION,
            notes=f"External job {job_name} ({job_schedule})",
        )
        self.add_dependency(dep)
        return dep

    def register_opaque_or_encrypted_code(
        self,
        module_identifier: str,
        reason: str = "encrypted or unavailable source code",
        suspected_tables: Sequence[str] = (),
        consumer_name: str = "opaque_module",
    ) -> AppDependency:
        """Never treat heuristic absence as proof of no use."""
        dep = AppDependency(
            dependency_id=f"opaque_{module_identifier}",
            source_kind=DependencySourceKind.ENCRYPTED_OPAQUE_CODE,
            consumer_name=consumer_name,
            raw_query_or_expression="<ENCRYPTED_OR_UNAVAILABLE_BINARY>",
            resolved_sql=None,
            accessed_tables=list(suspected_tables),
            resolution_state=ResolutionState.UNAVAILABLE_SOURCE,
            blocking_identifiers=["SOURCE_CODE_UNAVAILABLE"],
            badge=CareBadge.ACTION,
            notes=f"Opaque code: {reason}. Heuristic absence is not proof of no use.",
        )
        self.add_dependency(dep)
        return dep

    def ingest_runtime_trace(self, trace: RuntimeTrace) -> None:
        """Ingests a runtime trace from sandbox or staging execution."""
        self.traces[trace.trace_id] = trace
        # Match trace with dependencies
        for dep in self.dependencies.values():
            matched = False
            # Match by call site if available
            if dep.call_site and trace.call_site:
                if (
                    dep.call_site.file_path == trace.call_site.file_path
                    and dep.call_site.line_number == trace.call_site.line_number
                ):
                    matched = True

            # Match by table overlap on dynamic unresolved dependencies
            if not matched and dep.resolution_state in {
                ResolutionState.UNRESOLVED_DYNAMIC,
                ResolutionState.PARTIALLY_RESOLVED,
                ResolutionState.UNAVAILABLE_SOURCE,
            }:
                if set(dep.accessed_tables).intersection(set(trace.accessed_tables)):
                    matched = True

            if matched:
                dep.associated_trace_id = trace.trace_id
                if trace.approved:
                    dep.resolution_state = ResolutionState.RESOLVED_APPROVED_TRACE
                    dep.resolved_sql = trace.concrete_sql
                    for tbl in trace.accessed_tables:
                        if tbl not in dep.accessed_tables:
                            dep.accessed_tables.append(tbl)
                    for col in trace.accessed_columns:
                        if col not in dep.accessed_columns:
                            dep.accessed_columns.append(col)
                    # Re-check shape
                    self._check_dependency_shape(dep)
                    if not dep.points_at_old_shape:
                        dep.badge = CareBadge.PASS
                else:
                    dep.resolution_state = ResolutionState.PARTIALLY_RESOLVED
                    dep.badge = CareBadge.REVIEW
                    dep.notes = f"Associated with unapproved runtime trace {trace.trace_id}"

    def approve_runtime_trace(self, trace_id: str, approved_by: str = "admin") -> bool:
        """Approves a runtime trace, upgrading matched dependencies."""
        if trace_id not in self.traces:
            return False
        trace = self.traces[trace_id]
        trace.approved = True
        trace.approved_by = approved_by

        for dep in self.dependencies.values():
            if dep.associated_trace_id == trace_id:
                dep.resolution_state = ResolutionState.RESOLVED_APPROVED_TRACE
                dep.resolved_sql = trace.concrete_sql
                for tbl in trace.accessed_tables:
                    if tbl not in dep.accessed_tables:
                        dep.accessed_tables.append(tbl)
                for col in trace.accessed_columns:
                    if col not in dep.accessed_columns:
                        dep.accessed_columns.append(col)
                self._check_dependency_shape(dep)
                if not dep.points_at_old_shape:
                    dep.badge = CareBadge.PASS

        return True

    def register_migration_wave(self, wave: MigrationWave) -> None:
        self.waves[wave.wave_id] = wave

    def _check_dependency_shape(self, dep: AppDependency) -> None:
        is_old, reasons = self.schema_contract.evaluate_consumer_compatibility(
            dep.accessed_tables,
            dep.accessed_columns,
            dep.raw_query_or_expression,
        )
        dep.points_at_old_shape = is_old
        dep.old_shape_reasons = reasons
        if is_old:
            dep.badge = CareBadge.ACTION

    def _recheck_all_consumers_against_schema(self) -> None:
        for dep in self.dependencies.values():
            self._check_dependency_shape(dep)

    def compute_readiness_coverage(self) -> ReadinessCoverage:
        """Calculates global readiness coverage across all dependencies."""
        total = len(self.dependencies)
        resolved = 0
        partially = 0
        unresolved = 0
        opaque = 0
        old_shape = 0
        blockers: List[str] = []

        for dep in self.dependencies.values():
            if dep.points_at_old_shape:
                old_shape += 1
                blockers.append(
                    f"Consumer '{dep.consumer_name}' ({dep.dependency_id}) points at old shape: "
                    + "; ".join(dep.old_shape_reasons)
                )

            if dep.resolution_state in {
                ResolutionState.RESOLVED_STATIC,
                ResolutionState.RESOLVED_CONSTANT_PROPAGATION,
                ResolutionState.RESOLVED_APPROVED_TRACE,
            }:
                resolved += 1
            elif dep.resolution_state == ResolutionState.PARTIALLY_RESOLVED:
                partially += 1
            elif dep.resolution_state == ResolutionState.UNAVAILABLE_SOURCE:
                opaque += 1
                blockers.append(
                    f"Opaque/encrypted dependency '{dep.dependency_id}' has unavailable source"
                )
            else:
                unresolved += 1
                blockers.append(
                    f"Unresolved dynamic query in '{dep.consumer_name}' ({dep.dependency_id})"
                )

        approved_traces = sum(1 for t in self.traces.values() if t.approved)
        unapproved_traces = len(self.traces) - approved_traces

        pct = (resolved / total * 100.0) if total > 0 else 100.0

        if old_shape > 0 or opaque > 0 or unresolved > 0:
            badge = CareBadge.ACTION
        elif partially > 0 or unapproved_traces > 0:
            badge = CareBadge.REVIEW
        elif pct >= 100.0:
            badge = CareBadge.PASS
        else:
            badge = CareBadge.BENIGN

        return ReadinessCoverage(
            total_dependencies=total,
            resolved_count=resolved,
            partially_resolved_count=partially,
            unresolved_count=unresolved,
            opaque_code_count=opaque,
            approved_trace_count=approved_traces,
            unapproved_trace_count=unapproved_traces,
            old_shape_consumer_count=old_shape,
            coverage_pct=pct,
            badge=badge,
            blockers=blockers,
        )

    def validate_cutover_gate(self, wave: MigrationWave) -> CutoverGateVerdict:
        """Migration waves cannot cut over a dependency whose consumer still points at the old shape."""
        blocking_deps: List[str] = []
        reasons: List[str] = []
        total_checked = 0
        old_shape_count = 0
        unresolved_count = 0
        opaque_count = 0

        target_tables = set(wave.target_tables)

        for dep_id, dep in self.dependencies.items():
            # Check if this dependency interacts with any table in the wave
            touches_wave = False
            if set(dep.accessed_tables).intersection(target_tables):
                touches_wave = True
            elif dep.resolution_state == ResolutionState.UNAVAILABLE_SOURCE:
                # Opaque code might touch anything in the database: cannot cutover blindly
                touches_wave = True
            elif dep.resolution_state == ResolutionState.UNRESOLVED_DYNAMIC and not dep.accessed_tables:
                # Unknown dynamic SQL without table targets is a risk to any wave
                touches_wave = True

            if not touches_wave:
                continue

            total_checked += 1

            # Rule 1: Consumer still points at old shape
            if dep.points_at_old_shape:
                old_shape_count += 1
                blocking_deps.append(dep_id)
                reasons.append(
                    f"Consumer '{dep.consumer_name}' ({dep_id}) still points at old schema shape: "
                    + "; ".join(dep.old_shape_reasons)
                )

            # Rule 2: Unresolved dynamic SQL without approved runtime trace
            elif dep.resolution_state == ResolutionState.UNRESOLVED_DYNAMIC:
                unresolved_count += 1
                blocking_deps.append(dep_id)
                reasons.append(
                    f"Dependency '{dep_id}' contains unresolved dynamic SQL touching wave tables "
                    f"without approved runtime trace"
                )

            # Rule 3: Opaque/encrypted code without approved runtime trace
            elif dep.resolution_state == ResolutionState.UNAVAILABLE_SOURCE:
                opaque_count += 1
                blocking_deps.append(dep_id)
                reasons.append(
                    f"Dependency '{dep_id}' is opaque/encrypted code without approved runtime verification; "
                    f"heuristic absence is not proof of no use"
                )

        can_cutover = len(blocking_deps) == 0
        badge = CareBadge.PASS if can_cutover else CareBadge.ACTION

        return CutoverGateVerdict(
            wave_id=wave.wave_id,
            can_cutover=can_cutover,
            badge=badge,
            blocking_dependencies=blocking_deps,
            reasons=reasons,
            total_consumers_checked=total_checked,
            old_shape_consumer_count=old_shape_count,
            unresolved_dynamic_count=unresolved_count,
            opaque_code_count=opaque_count,
        )

    def save_checkpoint(self, path: Union[str, Path]) -> str:
        """Saves registry state atomically with SHA256 checksum."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp_p = p.with_suffix(".tmp")

        data = {
            "version": "1.0",
            "saved_at": time.time(),
            "config": self.config,
            "dependencies": {k: v.to_dict() for k, v in self.dependencies.items()},
            "traces": {k: v.to_dict() for k, v in self.traces.items()},
            "schema_contract": self.schema_contract.to_dict(),
            "waves": {k: v.to_dict() for k, v in self.waves.items()},
        }
        encoded = json.dumps(data, indent=2, sort_keys=True)
        checksum = hashlib.sha256(encoded.encode("ascii")).hexdigest()

        wrapped = {
            "checksum": checksum,
            "payload": data,
        }
        with open(tmp_p, "w", encoding="ascii") as f:
            json.dump(wrapped, f, indent=2)
        tmp_p.replace(p)
        return checksum

    @classmethod
    def load_checkpoint(cls, path: Union[str, Path]) -> DynamicSqlTracerRegistry:
        """Loads registry state from checkpoint with checksum verification."""
        p = Path(path)
        with open(p, "r", encoding="ascii") as f:
            wrapped = json.load(f)

        data = wrapped["payload"]
        checksum = wrapped.get("checksum")
        if checksum:
            computed = hashlib.sha256(json.dumps(data, indent=2, sort_keys=True).encode("ascii")).hexdigest()
            if computed != checksum:
                raise ValueError("Checkpoint corrupted: checksum mismatch")

        sc = SchemaContract.from_dict(data.get("schema_contract", {}))
        registry = cls(schema_contract=sc)
        registry.config = dict(data.get("config", {}))

        for k, v in data.get("dependencies", {}).items():
            registry.dependencies[k] = AppDependency.from_dict(v)
        for k, v in data.get("traces", {}).items():
            registry.traces[k] = RuntimeTrace.from_dict(v)
        for k, v in data.get("waves", {}).items():
            registry.waves[k] = MigrationWave.from_dict(v)

        return registry

    def acquire_lease(self, lock_dir: Union[str, Path], timeout_sec: float = 30.0) -> bool:
        """Acquires active verification lease lock (.run.lock)."""
        ld = Path(lock_dir)
        ld.mkdir(parents=True, exist_ok=True)
        lock_file = ld / ".run.lock"

        now = time.time()
        if lock_file.exists():
            try:
                with open(lock_file, "r", encoding="ascii") as f:
                    lock_info = json.load(f)
                ts = float(lock_info.get("timestamp", 0.0))
                pid = int(lock_info.get("pid", -1))
                if (now - ts) < timeout_sec:
                    # Still active lease
                    return False
            except Exception:
                pass

        # Write new lease
        lease_data = {
            "pid": os.getpid(),
            "timestamp": now,
            "host": "127.0.0.1",
        }
        tmp_lock = lock_file.with_suffix(".tmp")
        with open(tmp_lock, "w", encoding="ascii") as f:
            json.dump(lease_data, f)
        tmp_lock.replace(lock_file)
        self._lease_lock_file = lock_file
        return True

    def release_lease(self) -> None:
        """Releases the active verification lease lock."""
        if self._lease_lock_file and self._lease_lock_file.exists():
            try:
                self._lease_lock_file.unlink()
            except Exception:
                pass
            self._lease_lock_file = None
