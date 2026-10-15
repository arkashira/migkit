"""Schema version discovery from application migration history tables (backlog 26.versions).
Discovers, extracts, compares and validates schema versions across Flyway, Liquibase,
Alembic, Django, Rails, Entity Framework, and schemachange.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class MigrationRecord:
    tool: str
    version: str
    name: str = ""
    applied_at: Optional[str] = None
    checksum: Optional[str] = None
    success: bool = True
    execution_order: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def identifier(self) -> str:
        return f"{self.version}:{self.name}" if self.name else self.version


@dataclass
class VersionComparisonResult:
    tool: str
    status: str
    source_head: Optional[str] = None
    target_head: Optional[str] = None
    source_count: int = 0
    target_count: int = 0
    source_only: List[MigrationRecord] = field(default_factory=list)
    target_only: List[MigrationRecord] = field(default_factory=list)
    checksum_mismatches: List[Tuple[str, str, str]] = field(default_factory=list)
    detail: str = ""


class ApplicationMigrationTool:
    FLYWAY = "Flyway"
    LIQUIBASE = "Liquibase"
    ALEMBIC = "Alembic"
    DJANGO = "Django"
    RAILS = "Rails"
    ENTITY_FRAMEWORK = "Entity Framework"
    SCHEMACHANGE = "schemachange"


TOOL_SPECS: Dict[str, Dict[str, Any]] = {
    ApplicationMigrationTool.FLYWAY: {
        "tables": ("flyway_schema_history", "schema_version"),
        "version_col": "version",
        "name_col": "description",
        "applied_at_col": "installed_on",
        "checksum_col": "checksum",
        "order_col": "installed_rank",
        "success_filter": lambda r: r.get("success") in (1, True, "1", "true", "TRUE"),
    },
    ApplicationMigrationTool.LIQUIBASE: {
        "tables": ("databasechangelog",),
        "version_col": "id",
        "name_col": "filename",
        "applied_at_col": "dateexecuted",
        "checksum_col": "md5sum",
        "order_col": "orderexecuted",
        "success_filter": lambda r: str(r.get("exectype", "")).upper() in ("EXECUTED", "RERUN", "MARK_RAN", ""),
    },
    ApplicationMigrationTool.ALEMBIC: {
        "tables": ("alembic_version",),
        "version_col": "version_num",
        "name_col": "",
        "applied_at_col": "",
        "checksum_col": "",
        "order_col": "",
        "success_filter": lambda r: True,
    },
    ApplicationMigrationTool.DJANGO: {
        "tables": ("django_migrations",),
        "version_col": "name",
        "name_col": "app",
        "applied_at_col": "applied",
        "checksum_col": "",
        "order_col": "id",
        "success_filter": lambda r: True,
    },
    ApplicationMigrationTool.RAILS: {
        "tables": ("schema_migrations",),
        "version_col": "version",
        "name_col": "",
        "applied_at_col": "",
        "checksum_col": "",
        "order_col": "",
        "success_filter": lambda r: True,
    },
    ApplicationMigrationTool.ENTITY_FRAMEWORK: {
        "tables": ("__efmigrationshistory", "__EFMigrationsHistory"),
        "version_col": "MigrationId",
        "name_col": "ProductVersion",
        "applied_at_col": "",
        "checksum_col": "",
        "order_col": "",
        "success_filter": lambda r: True,
    },
    ApplicationMigrationTool.SCHEMACHANGE: {
        "tables": ("schemachange_history", "change_history"),
        "version_col": "version",
        "name_col": "script_name",
        "applied_at_col": "installed_on",
        "checksum_col": "checksum",
        "order_col": "",
        "success_filter": lambda r: str(r.get("status", "")).upper() in ("SUCCESS", "1", "TRUE", ""),
    },
}

MIGKIT_OWN_OBJECT_PATTERN = re.compile(
    r"^(?:_)?(?:migkit|mk)_.*|^#sql-.*|^_vt_.*|^repack\..*",
    re.IGNORECASE,
)


class SchemaVersionDiscovery:
    """Discovery and comparison of application schema migration versions."""

    @classmethod
    def discover_tools(cls, table_names: Iterable[str]) -> Dict[str, str]:
        normalized = {t.lower(): t for t in table_names}
        discovered = {}
        for tool, spec in TOOL_SPECS.items():
            for t_candidate in spec["tables"]:
                if t_candidate.lower() in normalized:
                    discovered[tool] = normalized[t_candidate.lower()]
                    break
        return discovered

    @classmethod
    def extract_records(cls, tool: str, rows: List[Dict[str, Any]]) -> List[MigrationRecord]:
        spec = TOOL_SPECS.get(tool)
        if not spec:
            return []

        version_col = spec["version_col"]
        name_col = spec.get("name_col", "")
        applied_at_col = spec.get("applied_at_col", "")
        checksum_col = spec.get("checksum_col", "")
        order_col = spec.get("order_col", "")
        success_filter = spec.get("success_filter", lambda r: True)

        records = []
        for r in rows:
            if not success_filter(r):
                continue
            v = str(r.get(version_col, "")).strip()
            if not v:
                continue

            name = str(r.get(name_col, "")).strip() if name_col else ""
            applied_at = str(r.get(applied_at_col, "")).strip() if applied_at_col and r.get(applied_at_col) else None
            checksum = str(r.get(checksum_col, "")).strip() if checksum_col and r.get(checksum_col) else None
            
            order_val = None
            if order_col and r.get(order_col) is not None:
                try:
                    order_val = int(r[order_col])
                except (ValueError, TypeError):
                    pass

            records.append(
                MigrationRecord(
                    tool=tool,
                    version=v,
                    name=name,
                    applied_at=applied_at,
                    checksum=checksum,
                    success=True,
                    execution_order=order_val,
                    metadata=dict(r),
                )
            )

        if any(rec.execution_order is not None for rec in records):
            records.sort(key=lambda rec: (rec.execution_order or 0, rec.version))
        else:
            records.sort(key=lambda rec: rec.version)

        return records

    @classmethod
    def get_head_version(cls, records: List[MigrationRecord]) -> Optional[str]:
        if not records:
            return None
        return records[-1].version

    @classmethod
    def compare_tool_versions(
        cls,
        tool: str,
        source_records: List[MigrationRecord],
        target_records: List[MigrationRecord],
    ) -> VersionComparisonResult:
        src_map = {rec.version: rec for rec in source_records}
        dst_map = {rec.version: rec for rec in target_records}

        src_versions = set(src_map.keys())
        dst_versions = set(dst_map.keys())

        only_src = [src_map[v] for v in sorted(src_versions - dst_versions)]
        only_dst = [dst_map[v] for v in sorted(dst_versions - src_versions)]

        mismatches = []
        for common_v in sorted(src_versions & dst_versions):
            s_rec = src_map[common_v]
            d_rec = dst_map[common_v]
            if s_rec.checksum and d_rec.checksum and s_rec.checksum != d_rec.checksum:
                mismatches.append((common_v, s_rec.checksum, d_rec.checksum))

        src_head = cls.get_head_version(source_records)
        dst_head = cls.get_head_version(target_records)

        if not only_src and not only_dst and not mismatches:
            status = "ok"
            detail = f"{tool}: synchronized on version {src_head} ({len(source_records)} migrations applied on both sides)"
        else:
            status = "diff"
            issues = []
            if only_src:
                issues.append(f"{len(only_src)} migration(s) on source only (pending on target, latest: {only_src[-1].version})")
            if only_dst:
                issues.append(f"{len(only_dst)} migration(s) on target only (errant migrations, latest: {only_dst[-1].version})")
            if mismatches:
                issues.append(f"{len(mismatches)} checksum mismatch(es) on common versions: {[m[0] for m in mismatches]}")
            detail = f"{tool}: " + "; ".join(issues)

        return VersionComparisonResult(
            tool=tool,
            status=status,
            source_head=src_head,
            target_head=dst_head,
            source_count=len(source_records),
            target_count=len(target_records),
            source_only=only_src,
            target_only=only_dst,
            checksum_mismatches=mismatches,
            detail=detail,
        )

    @classmethod
    def verify_cutover_gate(
        cls,
        source_records_by_tool: Dict[str, List[MigrationRecord]],
        target_records_by_tool: Dict[str, List[MigrationRecord]],
        allow_target_ahead: bool = False,
    ) -> Tuple[bool, List[str]]:
        reasons = []
        all_tools = sorted(set(source_records_by_tool.keys()) | set(target_records_by_tool.keys()))

        if not all_tools:
            return True, ["No application migration history tables detected; neutral pass"]

        for tool in all_tools:
            s_recs = source_records_by_tool.get(tool, [])
            t_recs = target_records_by_tool.get(tool, [])

            if not s_recs and t_recs:
                reasons.append(f"{tool}: history table present on target only")
                continue
            if s_recs and not t_recs:
                reasons.append(f"{tool}: history table present on source only; target lacks all {len(s_recs)} migrations")
                continue

            cmp_res = cls.compare_tool_versions(tool, s_recs, t_recs)
            if cmp_res.status != "ok":
                if cmp_res.source_only:
                    reasons.append(
                        f"{tool}: target lacks {len(cmp_res.source_only)} migration(s) applied on source "
                        f"(head on source: {cmp_res.source_head}, head on target: {cmp_res.target_head})"
                    )
                if cmp_res.checksum_mismatches:
                    reasons.append(
                        f"{tool}: checksum mismatch on {len(cmp_res.checksum_mismatches)} migration(s) between source and target"
                    )
                if cmp_res.target_only and not allow_target_ahead:
                    reasons.append(
                        f"{tool}: target has {len(cmp_res.target_only)} migration(s) not applied on source "
                        f"(e.g. {cmp_res.target_only[-1].version})"
                    )

        ready = len(reasons) == 0
        return ready, reasons

    @classmethod
    def identify_migkit_internal_objects(
        cls,
        all_tables: Iterable[str],
        tool_tables: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, str]]:
        known_tool_tables = set(t.lower() for t in (tool_tables or []))
        internal_objects = []

        for tbl in all_tables:
            tbl_clean = tbl.lower()
            if tbl_clean in known_tool_tables:
                continue
            if MIGKIT_OWN_OBJECT_PATTERN.match(tbl):
                internal_objects.append({
                    "table": tbl,
                    "kind": "migkit_internal_object",
                    "explanation": "Internal migkit schema artifact; untracked by and unknown to application migration tool",
                })
        return internal_objects


def discover_and_compare(
    src_tables: Iterable[str],
    dst_tables: Iterable[str],
    src_row_fetcher: Callable[[str], List[Dict[str, Any]]],
    dst_row_fetcher: Callable[[str], List[Dict[str, Any]]],
) -> List[VersionComparisonResult]:
    src_tools = SchemaVersionDiscovery.discover_tools(src_tables)
    dst_tools = SchemaVersionDiscovery.discover_tools(dst_tables)
    all_tools = sorted(set(src_tools.keys()) | set(dst_tools.keys()))

    results = []
    for tool in all_tools:
        s_tbl = src_tools.get(tool)
        d_tbl = dst_tools.get(tool)

        if s_tbl is None or d_tbl is None:
            missing_side = "target" if d_tbl is None else "source"
            results.append(
                VersionComparisonResult(
                    tool=tool,
                    status="missing_table",
                    detail=f"{tool} history table is missing on {missing_side}",
                )
            )
            continue

        s_rows = src_row_fetcher(s_tbl)
        d_rows = dst_row_fetcher(d_tbl)

        s_records = SchemaVersionDiscovery.extract_records(tool, s_rows)
        d_records = SchemaVersionDiscovery.extract_records(tool, d_rows)

        results.append(SchemaVersionDiscovery.compare_tool_versions(tool, s_records, d_records))

    return results
