import re
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


def _parse_version(v: str) -> Tuple[int, ...]:
    nums = re.findall(r"\d+", str(v))
    return tuple(int(x) for x in nums) if nums else (0,)


@dataclass
class CallSite:
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    routine_name: Optional[str] = None
    statement: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "line_number": self.line_number,
            "routine_name": self.routine_name,
            "statement": self.statement,
        }


@dataclass
class UnknownDynamicLineage:
    lineage_id: str
    routine_name: str
    blocking_identifiers: List[str]
    call_sites: List[CallSite]
    required_runtime_evidence: List[str]
    construct_kind: str
    reason: str
    resolved: bool = False
    resolved_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "lineage_id": self.lineage_id,
            "routine_name": self.routine_name,
            "blocking_identifiers": list(self.blocking_identifiers),
            "call_sites": [cs.to_dict() for cs in self.call_sites],
            "required_runtime_evidence": list(self.required_runtime_evidence),
            "construct_kind": self.construct_kind,
            "reason": self.reason,
            "resolved": self.resolved,
            "resolved_by": self.resolved_by,
        }


@dataclass
class SyntaxReference:
    caller: str
    target_type: str
    target_name: str
    call_site: Optional[CallSite] = None
    is_dynamic: bool = False
    dynamic_detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "caller": self.caller,
            "target_type": self.target_type,
            "target_name": self.target_name,
            "call_site": self.call_site.to_dict() if self.call_site else None,
            "is_dynamic": self.is_dynamic,
            "dynamic_detail": self.dynamic_detail,
        }


@dataclass
class RuntimeTrace:
    trace_id: str
    caller: str
    concrete_sql: str
    accessed_entities: List[str]
    approved: bool = False
    captured_at: float = 0.0
    call_site: Optional[CallSite] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "caller": self.caller,
            "concrete_sql": self.concrete_sql,
            "accessed_entities": list(self.accessed_entities),
            "approved": self.approved,
            "captured_at": self.captured_at,
            "call_site": self.call_site.to_dict() if self.call_site else None,
            "metadata": dict(self.metadata),
        }


@dataclass
class VersionedCatalogue:
    engine: str = "postgres"
    version: str = "15.0"
    tables: Set[str] = field(default_factory=set)
    views: Set[str] = field(default_factory=set)
    routines: Set[str] = field(default_factory=set)
    supported_extensions: Dict[str, str] = field(default_factory=dict)
    unavailable_dependencies: Set[str] = field(default_factory=set)

    def has_entity(self, name: str) -> bool:
        low = name.lower()
        all_entities = {e.lower() for e in (self.tables | self.views | self.routines)}
        if low in all_entities:
            return True
        unqualified = low.split(".")[-1]
        return any(e.split(".")[-1] == unqualified for e in all_entities)

    def has_extension(self, name: str, min_version: Optional[str] = None) -> bool:
        ext_low = name.lower()
        matched_key = next((k for k in self.supported_extensions if k.lower() == ext_low), None)
        if not matched_key:
            return False
        if min_version is None:
            return True
        supported_ver = self.supported_extensions[matched_key]
        return _parse_version(supported_ver) >= _parse_version(min_version)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "version": self.version,
            "tables": sorted(self.tables),
            "views": sorted(self.views),
            "routines": sorted(self.routines),
            "supported_extensions": dict(self.supported_extensions),
            "unavailable_dependencies": sorted(self.unavailable_dependencies),
        }


@dataclass
class AssessmentCoverage:
    routine_name: str
    total_dependencies: int
    resolved_dependencies: int
    unavailable_dependencies: int
    unknown_dynamic_count: int
    coverage_pct: float
    is_fully_assessed: bool
    blockers: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "routine_name": self.routine_name,
            "total_dependencies": self.total_dependencies,
            "resolved_dependencies": self.resolved_dependencies,
            "unavailable_dependencies": self.unavailable_dependencies,
            "unknown_dynamic_count": self.unknown_dynamic_count,
            "coverage_pct": self.coverage_pct,
            "is_fully_assessed": self.is_fully_assessed,
            "blockers": list(self.blockers),
        }


KNOWN_EXTENSION_PATTERNS = {
    "postgis": [
        re.compile(r"\bST_[A-Za-z0-9_]+\b", re.IGNORECASE),
        re.compile(r"\b(geography|geometry|raster)\b", re.IGNORECASE),
    ],
    "timescaledb": [
        re.compile(r"\b(create_hypertable|time_bucket|add_retention_policy)\b", re.IGNORECASE),
    ],
    "pg_partman": [
        re.compile(r"\b(create_parent|run_maintenance|undo_partition)\b", re.IGNORECASE),
    ],
    "orafce": [
        re.compile(r"\bdbms_output\b", re.IGNORECASE),
        re.compile(r"\b(add_months|months_between|next_day|last_day)\b", re.IGNORECASE),
    ],
    "dblink": [
        re.compile(r"\bdblink(_connect|_exec|_disconnect)?\b", re.IGNORECASE),
    ],
    "pg_cron": [
        re.compile(r"\bcron\.(schedule|unschedule)\b", re.IGNORECASE),
    ],
    "pgcrypto": [
        re.compile(r"\b(gen_salt|crypt|digest|hmac)\b", re.IGNORECASE),
    ],
    "uuid-ossp": [
        re.compile(r"\buuid_generate_v\d\b", re.IGNORECASE),
    ],
}


class DynamicLineageAuditor:
    _RE_PLPGSQL_FORMAT = re.compile(
        r"EXECUTE\s+format\s*\(\s*(['\"].*?['\"])\s*,\s*([^;]+)\)",
        re.IGNORECASE | re.DOTALL,
    )
    _RE_EXECUTE_IMMEDIATE = re.compile(
        r"EXECUTE\s+IMMEDIATE\s+([^;]+)",
        re.IGNORECASE,
    )
    _RE_EXEC_SP_EXECUTESQL = re.compile(
        r"EXEC(?:UTE)?\s+sp_executesql\s+([^;]+)",
        re.IGNORECASE,
    )
    _RE_MYSQL_PREPARE = re.compile(
        r"PREPARE\s+([A-Za-z0-9_]+)\s+FROM\s+([^;]+);",
        re.IGNORECASE,
    )
    _RE_MYSQL_CONCAT_DYN = re.compile(
        r"CONCAT\s*\(\s*['\"].*?['\"]\s*,\s*([^)]+)\)",
        re.IGNORECASE,
    )
    _RE_EXECUTE_CONCAT = re.compile(
        r"EXECUTE\s+(?:'[^']*'|\"[^\"]*\")?\s*\|\|\s*([^;]+)",
        re.IGNORECASE,
    )
    _RE_EXECUTE_PLAIN_VAR = re.compile(
        r"EXECUTE\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+(?:USING|INTO)\b|\s*;)",
        re.IGNORECASE,
    )
    _RE_OPEN_CURSOR_FOR = re.compile(
        r"OPEN\s+([A-Za-z0-9_]+)\s+FOR\s+([^;]+);",
        re.IGNORECASE,
    )
    _RE_DBMS_SQL_PARSE = re.compile(
        r"dbms_sql\.parse\s*\(\s*[^,]+,\s*([^,]+),",
        re.IGNORECASE,
    )
    _RE_EVAL_SQL = re.compile(
        r"\beval\s*\(\s*([^)]+)\)",
        re.IGNORECASE,
    )
    _RE_APP_FSTRING_SQL = re.compile(
        r'f["\'](?:SELECT|INSERT|UPDATE|DELETE|FROM)\b.*?\{([A-Za-z0-9_]+)\}.*?["\']',
        re.IGNORECASE,
    )
    _RE_APP_CONCAT_SQL = re.compile(
        r'["\']\s*(?:SELECT|INSERT|UPDATE|DELETE|FROM)\b.*?["\']\s*\+\s*([A-Za-z0-9_]+)',
        re.IGNORECASE,
    )

    def audit_code(
        self,
        routine_name: str,
        code: str,
        language: str = "sql",
        file_path: Optional[str] = None,
        target_catalogue: Optional[VersionedCatalogue] = None,
        required_extensions: Optional[Dict[str, str]] = None,
    ) -> List[UnknownDynamicLineage]:
        unknowns: List[UnknownDynamicLineage] = []
        lines = code.splitlines()

        for idx, line in enumerate(lines, 1):
            for m in self._RE_PLPGSQL_FORMAT.finditer(line):
                fmt_str, raw_args = m.group(1), m.group(2)
                idents = [arg.strip() for arg in raw_args.split(",") if arg.strip()]
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=idents,
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace capturing concrete table/column bindings for format arguments: {', '.join(idents)}"
                        ],
                        construct_kind="dynamic_identifier" if "%I" in fmt_str else "dynamic_sql",
                        reason="Dynamic identifier construction via format() in PL/pgSQL",
                    )
                )

            for m in self._RE_EXECUTE_IMMEDIATE.finditer(line):
                expr = m.group(1).strip()
                idents = [tok for tok in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expr) if tok.upper() not in {"USING", "INTO", "BULK", "COLLECT"}]
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                kind = "dynamic_identifier" if "||" in expr else "dynamic_sql"
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=idents if idents else [expr],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace capturing executed SQL statement for: {expr}"
                        ],
                        construct_kind=kind,
                        reason="Unresolved dynamic SQL execution via EXECUTE IMMEDIATE",
                    )
                )

            for m in self._RE_EXEC_SP_EXECUTESQL.finditer(line):
                param = m.group(1).strip()
                first_arg = param.split(",")[0].strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[first_arg],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace of sp_executesql execution for parameter: {first_arg}"
                        ],
                        construct_kind="dynamic_sql",
                        reason="Dynamic SQL execution via sp_executesql",
                    )
                )

            for m in self._RE_MYSQL_PREPARE.finditer(line):
                stmt_name, src_var = m.group(1).strip(), m.group(2).strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[src_var],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace of PREPARE statement using variable: {src_var}"
                        ],
                        construct_kind="dynamic_sql",
                        reason="Dynamic statement preparation via MySQL PREPARE",
                    )
                )

            for m in self._RE_MYSQL_CONCAT_DYN.finditer(line):
                args = [a.strip() for a in m.group(1).split(",") if a.strip()]
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=args,
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace resolving concatenated table/query: {', '.join(args)}"
                        ],
                        construct_kind="dynamic_identifier",
                        reason="Dynamic identifier construction via CONCAT",
                    )
                )

            for m in self._RE_EXECUTE_CONCAT.finditer(line):
                concat_expr = m.group(1).strip()
                idents = re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", concat_expr)
                idents = [i for i in idents if i.lower() not in {"quote_ident", "quote_literal", "into", "using"}]
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=idents if idents else [concat_expr],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace resolving concatenated identifier in EXECUTE: {concat_expr}"
                        ],
                        construct_kind="dynamic_identifier",
                        reason="Dynamic identifier concatenation in EXECUTE statement",
                    )
                )

            for m in self._RE_EXECUTE_PLAIN_VAR.finditer(line):
                var_name = m.group(1).strip()
                if var_name.lower() not in {"format", "immediate", "sp_executesql"}:
                    cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                    unknowns.append(
                        UnknownDynamicLineage(
                            lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                            routine_name=routine_name,
                            blocking_identifiers=[var_name],
                            call_sites=[cs],
                            required_runtime_evidence=[
                                f"Approved runtime trace of variable SQL execution for: {var_name}"
                            ],
                            construct_kind="dynamic_sql",
                            reason="Dynamic SQL statement execution via variable in EXECUTE",
                        )
                    )

            for m in self._RE_OPEN_CURSOR_FOR.finditer(line):
                cur_name, cur_expr = m.group(1).strip(), m.group(2).strip()
                if "SELECT" not in cur_expr.upper() or "'" in cur_expr:
                    idents = [tok for tok in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", cur_expr) if tok.upper() not in {"SELECT", "FROM", "WHERE", "USING"}]
                    cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                    unknowns.append(
                        UnknownDynamicLineage(
                            lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                            routine_name=routine_name,
                            blocking_identifiers=idents if idents else [cur_expr],
                            call_sites=[cs],
                            required_runtime_evidence=[
                                f"Approved runtime trace of dynamic cursor query for {cur_name}"
                            ],
                            construct_kind="dynamic_sql",
                            reason="Dynamic cursor query opening",
                        )
                    )

            for m in self._RE_DBMS_SQL_PARSE.finditer(line):
                stmt_var = m.group(1).strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[stmt_var],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace for DBMS_SQL.PARSE statement: {stmt_var}"
                        ],
                        construct_kind="dynamic_sql",
                        reason="Dynamic SQL parsing via DBMS_SQL.PARSE",
                    )
                )

            for m in self._RE_EVAL_SQL.finditer(line):
                eval_arg = m.group(1).strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[eval_arg],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace for eval() execution: {eval_arg}"
                        ],
                        construct_kind="dynamic_sql",
                        reason="Dynamic statement evaluation via eval()",
                    )
                )

            for m in self._RE_APP_FSTRING_SQL.finditer(line):
                ident = m.group(1).strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[ident],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace resolving dynamic application SQL variable: {ident}"
                        ],
                        construct_kind="dynamic_identifier",
                        reason="Interpolated SQL identifier in application code",
                    )
                )

            for m in self._RE_APP_CONCAT_SQL.finditer(line):
                ident = m.group(1).strip()
                cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                unknowns.append(
                    UnknownDynamicLineage(
                        lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                        routine_name=routine_name,
                        blocking_identifiers=[ident],
                        call_sites=[cs],
                        required_runtime_evidence=[
                            f"Approved runtime trace resolving concatenated application SQL identifier: {ident}"
                        ],
                        construct_kind="dynamic_identifier",
                        reason="Concatenated SQL identifier in application code",
                    )
                )

        detected_extensions = self.detect_extensions(code)
        req_exts = dict(required_extensions or {})
        for ext in detected_extensions:
            if ext not in req_exts:
                req_exts[ext] = None

        if target_catalogue is not None:
            for ext, min_ver in req_exts.items():
                if not target_catalogue.has_extension(ext, min_ver):
                    reason_msg = (
                        f"Target catalogue does not support required extension {ext} version >= {min_ver}"
                        if min_ver
                        else f"Target catalogue does not support extension {ext}"
                    )
                    cs = CallSite(file_path=file_path, line_number=1, routine_name=routine_name, statement=f"EXTENSION {ext}")
                    unknowns.append(
                        UnknownDynamicLineage(
                            lineage_id=f"dyn-{uuid.uuid4().hex[:8]}",
                            routine_name=routine_name,
                            blocking_identifiers=[ext],
                            call_sites=[cs],
                            required_runtime_evidence=[
                                f"Target catalogue version specification supporting {ext}"
                                + (f" >= {min_ver}" if min_ver else "")
                            ],
                            construct_kind="unsupported_extension",
                            reason=reason_msg,
                        )
                    )

        return unknowns

    def detect_extensions(self, code: str) -> Set[str]:
        found: Set[str] = set()
        for ext_name, patterns in KNOWN_EXTENSION_PATTERNS.items():
            for pat in patterns:
                if pat.search(code):
                    found.add(ext_name)
                    break
        return found

    def extract_syntax_references(
        self,
        routine_name: str,
        code: str,
        language: str = "sql",
        file_path: Optional[str] = None,
    ) -> List[SyntaxReference]:
        refs: List[SyntaxReference] = []
        lines = code.splitlines()

        re_from = re.compile(r"\bFROM\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)", re.IGNORECASE)
        re_join = re.compile(r"\bJOIN\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)", re.IGNORECASE)
        re_into = re.compile(r"\bINTO\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)", re.IGNORECASE)
        re_update = re.compile(r"\bUPDATE\s+([A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)?)", re.IGNORECASE)

        for idx, line in enumerate(lines, 1):
            if "EXECUTE" in line.upper() and ("FORMAT" in line.upper() or "||" in line):
                continue
            for pat, target_type in [
                (re_from, "table"),
                (re_join, "table"),
                (re_into, "table"),
                (re_update, "table"),
            ]:
                for m in pat.finditer(line):
                    name = m.group(1)
                    if name.upper() in {"FORMAT", "SELECT", "WHERE", "ORDER", "GROUP", "SETOF"}:
                        continue
                    cs = CallSite(file_path=file_path, line_number=idx, routine_name=routine_name, statement=line.strip())
                    refs.append(SyntaxReference(caller=routine_name, target_type=target_type, target_name=name, call_site=cs))
        return refs


class LineageEvidenceLedger:
    def __init__(
        self,
        source_catalogue: Optional[VersionedCatalogue] = None,
        target_catalogue: Optional[VersionedCatalogue] = None,
    ):
        self.source_catalogue = source_catalogue
        self.target_catalogue = target_catalogue
        self.auditor = DynamicLineageAuditor()
        self._syntax_references: Dict[str, List[SyntaxReference]] = {}
        self._runtime_traces: Dict[str, List[RuntimeTrace]] = {}
        self._unknown_lineages: Dict[str, List[UnknownDynamicLineage]] = {}
        self._unavailable_dependencies: Dict[str, Set[str]] = {}

    def register_source_catalogue(self, catalogue: VersionedCatalogue) -> None:
        self.source_catalogue = catalogue

    def register_target_catalogue(self, catalogue: VersionedCatalogue) -> None:
        self.target_catalogue = catalogue

    def record_syntax_reference(self, ref: SyntaxReference) -> None:
        self._syntax_references.setdefault(ref.caller, []).append(ref)

    def record_runtime_trace(self, trace: RuntimeTrace) -> None:
        self._runtime_traces.setdefault(trace.caller, []).append(trace)

    def audit_and_record(
        self,
        routine_name: str,
        code: str,
        language: str = "sql",
        file_path: Optional[str] = None,
        required_extensions: Optional[Dict[str, str]] = None,
    ) -> None:
        unknowns = self.auditor.audit_code(
            routine_name=routine_name,
            code=code,
            language=language,
            file_path=file_path,
            target_catalogue=self.target_catalogue,
            required_extensions=required_extensions,
        )
        self._unknown_lineages[routine_name] = unknowns

        refs = self.auditor.extract_syntax_references(
            routine_name=routine_name,
            code=code,
            language=language,
            file_path=file_path,
        )
        self._syntax_references[routine_name] = refs

        unavail: Set[str] = set()
        active_cat = self.target_catalogue or self.source_catalogue
        if active_cat is not None:
            for ref in refs:
                if ref.target_name in active_cat.unavailable_dependencies:
                    unavail.add(ref.target_name)
        self._unavailable_dependencies[routine_name] = unavail

    def attempt_resolution(self, routine_name: Optional[str] = None) -> None:
        routines = [routine_name] if routine_name else list(self._unknown_lineages.keys())
        for rname in routines:
            traces = self._runtime_traces.get(rname, [])
            approved_traces = [t for t in traces if t.approved]
            unknowns = self._unknown_lineages.get(rname, [])

            for unk in unknowns:
                if unk.resolved:
                    continue
                if unk.construct_kind in {"dynamic_sql", "dynamic_identifier"}:
                    for t in approved_traces:
                        if t.accessed_entities:
                            unk.resolved = True
                            unk.resolved_by = t.trace_id
                            break
                elif unk.construct_kind == "unsupported_extension":
                    if self.target_catalogue:
                        all_resolved = True
                        for ext in unk.blocking_identifiers:
                            if not self.target_catalogue.has_extension(ext):
                                all_resolved = False
                                break
                        if all_resolved:
                            unk.resolved = True
                            unk.resolved_by = "catalogue_update"

    def get_unknown_dynamic_lineages(
        self,
        routine_name: Optional[str] = None,
        unresolved_only: bool = True,
    ) -> List[UnknownDynamicLineage]:
        results: List[UnknownDynamicLineage] = []
        if routine_name:
            items = self._unknown_lineages.get(routine_name, [])
            for item in items:
                if not unresolved_only or not item.resolved:
                    results.append(item)
        else:
            for items in self._unknown_lineages.values():
                for item in items:
                    if not unresolved_only or not item.resolved:
                        results.append(item)
        return results

    def compute_coverage(self, routine_name: str) -> AssessmentCoverage:
        refs = self._syntax_references.get(routine_name, [])
        unknowns = self._unknown_lineages.get(routine_name, [])
        unavail = self._unavailable_dependencies.get(routine_name, set())

        static_resolved = len(refs) - len(unavail)
        if static_resolved < 0:
            static_resolved = 0

        resolved_dynamic = sum(1 for u in unknowns if u.resolved)
        unresolved_dynamic = sum(1 for u in unknowns if not u.resolved)
        unavail_count = len(unavail)

        total = static_resolved + resolved_dynamic + unresolved_dynamic + unavail_count
        resolved = static_resolved + resolved_dynamic

        blockers: List[str] = []
        for u in unknowns:
            if not u.resolved:
                blockers.append(f"{u.construct_kind}: {u.reason} (blocking: {', '.join(u.blocking_identifiers)})")
        for ua in unavail:
            blockers.append(f"unavailable_dependency: {ua}")

        if total == 0:
            if blockers:
                cov_pct = 0.0
                is_full = False
            else:
                cov_pct = 100.0
                is_full = True
        else:
            cov_pct = round((resolved / total) * 100.0, 2)
            if (unresolved_dynamic > 0 or unavail_count > 0) and cov_pct >= 100.0:
                cov_pct = 99.0
            is_full = (unresolved_dynamic == 0 and unavail_count == 0)

        return AssessmentCoverage(
            routine_name=routine_name,
            total_dependencies=total,
            resolved_dependencies=resolved,
            unavailable_dependencies=unavail_count,
            unknown_dynamic_count=unresolved_dynamic,
            coverage_pct=cov_pct,
            is_fully_assessed=is_full,
            blockers=blockers,
        )

    def compute_all_coverage(self) -> Dict[str, AssessmentCoverage]:
        all_routines = set(self._syntax_references.keys()) | set(self._unknown_lineages.keys())
        return {r: self.compute_coverage(r) for r in all_routines}
