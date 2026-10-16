import dataclasses
import hashlib
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from . import config, durable


@dataclasses.dataclass
class QueryPattern:
    query_id: str
    raw_query: str
    normalized_query: str
    dialect: str = "postgres"
    call_count: int = 1
    total_time_ms: float = 0.0
    mean_time_ms: float = 0.0
    max_time_ms: float = 0.0
    min_time_ms: float = 0.0
    rows_examined: int = 0
    rows_sent: int = 0
    tables: List[str] = dataclasses.field(default_factory=list)
    filter_columns: List[str] = dataclasses.field(default_factory=list)
    source_plan: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ExecutionPlan:
    query_id: str
    dialect: str = "postgres"
    execution_time_ms: float = 0.0
    planning_time_ms: float = 0.0
    plan_cost: float = 0.0
    scans: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    joins: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    node_types: List[str] = dataclasses.field(default_factory=list)
    raw_plan: Any = None
    has_seq_scan: bool = False
    has_nested_loop: bool = False
    indexes_used: List[str] = dataclasses.field(default_factory=list)
    tables_accessed: List[str] = dataclasses.field(default_factory=list)
    warnings: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ParityIssue:
    issue_id: str
    query_id: str
    issue_type: str
    severity: str
    table: str
    description: str
    source_metric: Any = None
    target_metric: Any = None
    latency_ratio: float = 1.0
    cost_ratio: float = 1.0
    recommendation: str = ""


@dataclasses.dataclass
class IndexRecommendation:
    table: str
    columns: List[str]
    index_name: str
    sql_ddl: str
    reason: str
    severity: str = "HIGH"
    estimated_impact: str = ""


@dataclasses.dataclass
class QueryParityReport:
    report_id: str
    hop_name: str
    timestamp: float
    verdict: str
    can_cutover: bool
    threshold_latency_ratio: float
    total_queries_analyzed: int
    parity_issues: List[Dict[str, Any]]
    missing_indexes: List[Dict[str, Any]]
    index_recommendations: List[str]
    plan_degradations: List[Dict[str, Any]]
    latency_anomalies: List[Dict[str, Any]]
    summary_stats: Dict[str, Any]
    sla_checks: Dict[str, bool]

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def render(self) -> str:
        lines = [
            "=" * 78,
            " CONTINUOUS QUERY OPTIMIZER & LATENCY PARITY GUARD (SUPER.query-guard)",
            "=" * 78,
            f"Hop: {self.hop_name or 'unnamed'} | Status: {'READY' if self.can_cutover else 'BLOCKED'} | Verdict: {self.verdict}",
            f"Cutover Approved: {self.can_cutover} (Latency regression threshold: {self.threshold_latency_ratio:.1f}x)",
            f"Queries Analyzed: {self.total_queries_analyzed} | Issues Found: {len(self.parity_issues)}",
            "-" * 78,
            "Latency Distribution:",
            f"  Source P50: {self.summary_stats.get('source_p50', 0.0):.2f}ms | Target P50: {self.summary_stats.get('target_p50', 0.0):.2f}ms",
            f"  Source P95: {self.summary_stats.get('source_p95', 0.0):.2f}ms | Target P95: {self.summary_stats.get('target_p95', 0.0):.2f}ms",
            f"  Max Latency Ratio: {self.summary_stats.get('max_latency_ratio', 1.0):.2f}x",
            "-" * 78,
        ]
        if self.parity_issues:
            lines.append("Parity Degradation & Regression Issues:")
            for issue in self.parity_issues[:10]:
                sev = issue.get("severity", "WARN")
                itype = issue.get("issue_type", "ISSUE")
                tbl = issue.get("table", "")
                desc = issue.get("description", "")
                lines.append(f"  [{sev:<8}] {itype:<20} {tbl:<15} {desc}")
            if len(self.parity_issues) > 10:
                lines.append(f"  ... and {len(self.parity_issues) - 10} more issues")
            lines.append("-" * 78)

        if self.index_recommendations:
            lines.append("Actionable Index Recommendations (Execute Before Cutover):")
            for idx, ddl in enumerate(self.index_recommendations[:10], 1):
                lines.append(f"  {idx}. {ddl}")
            if len(self.index_recommendations) > 10:
                lines.append(f"  ... and {len(self.index_recommendations) - 10} more recommendations")
            lines.append("-" * 78)

        lines.append(f"SLA Verdict: {'PASS' if self.can_cutover else 'FAIL'}")
        lines.append("=" * 78)
        return "\n".join(lines)

    def save(self, path: Optional[Union[str, Path]] = None) -> Path:
        if path is None:
            path = Path(config.state_home()) / "query_guard" / f"{self.report_id}.json"
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        durable.write_json(p, self.to_dict(), indent=2)
        return p


def normalize_sql(sql: str) -> Tuple[str, str]:
    cleaned = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    cleaned = re.sub(r"--[^\n]*", "", cleaned)
    cleaned = re.sub(r"'([^'\\]|\\.)*'", "'?'", cleaned)
    cleaned = re.sub(r"\b\d+(\.\d+)?\b", "?", cleaned)
    cleaned = re.sub(r"\bIN\s*\(\s*\?(?:\s*,\s*\?)*\s*\)", "IN (?)", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    norm = cleaned.rstrip(";")
    qid = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]
    return qid, norm


def extract_tables(sql: str) -> List[str]:
    pattern = r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([`\"']?([a-zA-Z0-9_]+)[`\"']?(?:\.[`\"']?([a-zA-Z0-9_]+)[`\"']?)?)"
    matches = re.findall(pattern, sql, flags=re.IGNORECASE)
    tables: List[str] = []
    for full, t1, t2 in matches:
        candidate = t2 if t2 else t1
        candidate = candidate.strip("`\"' ")
        if candidate and candidate.upper() not in {"SELECT", "SET", "WHERE", "VALUES", "JOIN"} and candidate not in tables:
            tables.append(candidate)
    return tables


def extract_filter_columns(sql: str) -> List[str]:
    where_match = re.search(r"\bWHERE\b(.*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|$)", sql, flags=re.IGNORECASE | re.DOTALL)
    if not where_match:
        return []
    clause = where_match.group(1)
    col_pattern = r"([a-zA-Z0-9_]+)\s*(?:=|!=|<>|>=|<=|>|<|\bIN\b|\bLIKE\b|\bBETWEEN\b)"
    found = re.findall(col_pattern, clause, flags=re.IGNORECASE)
    cols: List[str] = []
    for c in found:
        col = c.strip("`\"' ")
        if col.upper() not in {"AND", "OR", "NOT", "NULL", "TRUE", "FALSE"} and col not in cols:
            cols.append(col)
    return cols


class QueryOptimizerParityGuard:
    def __init__(
        self,
        hop=None,
        target_eng=None,
        source_eng=None,
        db: str = "",
        latency_threshold: float = 2.0,
        options: Optional[Dict[str, Any]] = None,
        say: Optional[Callable] = None,
    ):
        self.hop = hop
        self.target_eng = target_eng
        self.source_eng = source_eng
        self.db = db
        self.latency_threshold = float(latency_threshold)
        self.options = options or {}
        self.say = say or (lambda msg: None)

    def extract_from_slow_log(self, log_content: Union[str, Iterable[str]], dialect: str = "auto") -> List[QueryPattern]:
        if isinstance(log_content, str):
            lines = log_content.splitlines()
        else:
            lines = list(log_content)

        patterns_map: Dict[str, Dict[str, Any]] = {}
        curr_time = 0.0
        curr_rows_sent = 0
        curr_rows_examined = 0
        curr_sql_lines: List[str] = []
        is_mysql = False
        is_pg = False

        def commit_entry(sql_text: str, duration_ms: float, rows_sent: int, rows_examined: int, active_dialect: str):
            sql = sql_text.strip()
            if not sql or sql.startswith("#") or sql.upper().startswith("SET TIMESTAMP") or sql.upper().startswith("USE "):
                return
            qid, norm = normalize_sql(sql)
            tbls = extract_tables(sql)
            cols = extract_filter_columns(sql)

            if qid not in patterns_map:
                patterns_map[qid] = {
                    "query_id": qid,
                    "raw_query": sql,
                    "normalized_query": norm,
                    "dialect": active_dialect,
                    "call_count": 0,
                    "total_time_ms": 0.0,
                    "times": [],
                    "rows_examined": 0,
                    "rows_sent": 0,
                    "tables": tbls,
                    "filter_columns": cols,
                }
            rec = patterns_map[qid]
            rec["call_count"] += 1
            rec["total_time_ms"] += duration_ms
            rec["times"].append(duration_ms)
            rec["rows_examined"] += rows_examined
            rec["rows_sent"] += rows_sent

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("# Time:") or stripped.startswith("# User@Host:"):
                is_mysql = True
                if curr_sql_lines:
                    commit_entry(" ".join(curr_sql_lines), curr_time, curr_rows_sent, curr_rows_examined, "mysql")
                    curr_sql_lines = []
                    curr_time = 0.0
                    curr_rows_sent = 0
                    curr_rows_examined = 0
                continue

            if is_mysql and stripped.startswith("# Query_time:"):
                m = re.search(r"Query_time:\s*([\d\.]+)\s+Lock_time:\s*[\d\.]+\s+Rows_sent:\s*(\d+)\s+Rows_examined:\s*(\d+)", stripped)
                if m:
                    curr_time = float(m.group(1)) * 1000.0
                    curr_rows_sent = int(m.group(2))
                    curr_rows_examined = int(m.group(3))
                continue

            pg_match = re.search(r"duration:\s*([\d\.]+)\s*ms\s+(?:statement|execute [^:]+):\s*(.*)", stripped, re.IGNORECASE)
            if pg_match:
                is_pg = True
                if curr_sql_lines:
                    commit_entry(" ".join(curr_sql_lines), curr_time, curr_rows_sent, curr_rows_examined, "postgres")
                    curr_sql_lines = []
                dur = float(pg_match.group(1))
                sql_part = pg_match.group(2)
                curr_sql_lines = [sql_part]
                curr_time = dur
                continue

            if stripped and not stripped.startswith("#"):
                if stripped.upper().startswith("SET ") or stripped.upper().startswith("USE "):
                    continue
                curr_sql_lines.append(stripped)

        if curr_sql_lines:
            d = "mysql" if is_mysql else ("postgres" if is_pg else dialect)
            commit_entry(" ".join(curr_sql_lines), curr_time, curr_rows_sent, curr_rows_examined, d)

        result: List[QueryPattern] = []
        for qid, rec in patterns_map.items():
            cnt = rec["call_count"]
            tot = rec["total_time_ms"]
            times = rec["times"]
            result.append(
                QueryPattern(
                    query_id=qid,
                    raw_query=rec["raw_query"],
                    normalized_query=rec["normalized_query"],
                    dialect=rec["dialect"],
                    call_count=cnt,
                    total_time_ms=tot,
                    mean_time_ms=tot / cnt if cnt > 0 else 0.0,
                    max_time_ms=max(times) if times else 0.0,
                    min_time_ms=min(times) if times else 0.0,
                    rows_examined=rec["rows_examined"],
                    rows_sent=rec["rows_sent"],
                    tables=rec["tables"],
                    filter_columns=rec["filter_columns"],
                )
            )
        result.sort(key=lambda p: p.total_time_ms, reverse=True)
        return result

    def extract_from_pg_stat_statements(self, records: List[Dict[str, Any]]) -> List[QueryPattern]:
        result: List[QueryPattern] = []
        for rec in records:
            sql = rec.get("query", "")
            if not sql or sql.upper().startswith("BEGIN") or sql.upper().startswith("COMMIT"):
                continue
            qid, norm = normalize_sql(sql)
            calls = int(rec.get("calls", 1))
            total_time = float(rec.get("total_exec_time") or rec.get("total_time", 0.0))
            mean_time = float(rec.get("mean_exec_time") or rec.get("mean_time", total_time / max(calls, 1)))
            max_time = float(rec.get("max_exec_time") or rec.get("max_time", mean_time))
            min_time = float(rec.get("min_exec_time") or rec.get("min_time", mean_time))
            rows = int(rec.get("rows", 0))
            tbls = extract_tables(sql)
            cols = extract_filter_columns(sql)

            result.append(
                QueryPattern(
                    query_id=qid,
                    raw_query=sql,
                    normalized_query=norm,
                    dialect="postgres",
                    call_count=calls,
                    total_time_ms=total_time,
                    mean_time_ms=mean_time,
                    max_time_ms=max_time,
                    min_time_ms=min_time,
                    rows_examined=rows,
                    rows_sent=rows,
                    tables=tbls,
                    filter_columns=cols,
                    source_plan=rec.get("plan"),
                    metadata=dict(rec),
                )
            )
        result.sort(key=lambda p: (p.total_time_ms, p.call_count), reverse=True)
        return result

    def extract_from_performance_schema(self, records: List[Dict[str, Any]]) -> List[QueryPattern]:
        result: List[QueryPattern] = []
        for rec in records:
            sql = rec.get("DIGEST_TEXT") or rec.get("digest_text") or rec.get("QUERY_SAMPLE_TEXT") or rec.get("query", "")
            if not sql:
                continue
            qid, norm = normalize_sql(sql)
            calls = int(rec.get("COUNT_STAR") or rec.get("count_star", 1))

            def _to_ms(val: Any) -> float:
                if val is None:
                    return 0.0
                v = float(val)
                if v > 1_000_000:
                    return v / 1_000_000_000.0
                return v

            tot_time = _to_ms(rec.get("SUM_TIMER_WAIT") or rec.get("sum_timer_wait", 0.0))
            avg_time = _to_ms(rec.get("AVG_TIMER_WAIT") or rec.get("avg_timer_wait", tot_time / max(calls, 1)))
            max_time = _to_ms(rec.get("MAX_TIMER_WAIT") or rec.get("max_timer_wait", avg_time))
            min_time = _to_ms(rec.get("MIN_TIMER_WAIT") or rec.get("min_timer_wait", avg_time))
            rows_exam = int(rec.get("SUM_ROWS_EXAMINED") or rec.get("sum_rows_examined", 0))
            rows_sent = int(rec.get("SUM_ROWS_SENT") or rec.get("sum_rows_sent", 0))
            tbls = extract_tables(sql)
            cols = extract_filter_columns(sql)

            result.append(
                QueryPattern(
                    query_id=qid,
                    raw_query=sql,
                    normalized_query=norm,
                    dialect="mysql",
                    call_count=calls,
                    total_time_ms=tot_time,
                    mean_time_ms=avg_time,
                    max_time_ms=max_time,
                    min_time_ms=min_time,
                    rows_examined=rows_exam,
                    rows_sent=rows_sent,
                    tables=tbls,
                    filter_columns=cols,
                    source_plan=rec.get("plan"),
                    metadata=dict(rec),
                )
            )
        result.sort(key=lambda p: (p.total_time_ms, p.call_count), reverse=True)
        return result

    def sample_workload(
        self, patterns: List[QueryPattern], sample_size: int = 50, strategy: str = "hybrid"
    ) -> List[QueryPattern]:
        if len(patterns) <= sample_size:
            return list(patterns)
        if strategy == "frequency":
            return sorted(patterns, key=lambda p: p.call_count, reverse=True)[:sample_size]
        if strategy == "latency":
            return sorted(patterns, key=lambda p: (p.mean_time_ms, p.max_time_ms), reverse=True)[:sample_size]

        half = math.ceil(sample_size / 2.0)
        top_freq = sorted(patterns, key=lambda p: p.call_count, reverse=True)[:half]
        chosen_ids = {p.query_id for p in top_freq}
        rem = [p for p in patterns if p.query_id not in chosen_ids]
        top_lat = sorted(rem, key=lambda p: (p.mean_time_ms, p.max_time_ms), reverse=True)[: (sample_size - len(top_freq))]
        return top_freq + top_lat

    def replay_on_target(
        self,
        pattern: QueryPattern,
        target_eng=None,
        db: str = "",
        analyze: bool = False,
        read_only: bool = True,
        executor: Optional[Callable[[str, bool, bool], Any]] = None,
    ) -> ExecutionPlan:
        eng = target_eng or self.target_eng
        db_name = db or self.db
        sql = pattern.raw_query.strip()
        dialect = getattr(eng, "dialect", pattern.dialect or "postgres")
        is_dml = bool(re.search(r"\b(INSERT|UPDATE|DELETE|REPLACE|MERGE|TRUNCATE|DROP|ALTER)\b", sql, re.IGNORECASE))

        if is_dml and read_only:
            explain_cmd = f"EXPLAIN {sql}"
            needs_rollback = False
        elif is_dml and analyze:
            explain_cmd = f"EXPLAIN (ANALYZE, COSTS, VERBOSE, FORMAT JSON) {sql}" if dialect == "postgres" else f"EXPLAIN ANALYZE {sql}"
            needs_rollback = True
        else:
            if dialect == "postgres":
                explain_cmd = (
                    f"EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) {sql}"
                    if analyze
                    else f"EXPLAIN (COSTS, VERBOSE, FORMAT JSON) {sql}"
                )
            else:
                explain_cmd = f"EXPLAIN FORMAT=JSON {sql}" if not analyze else f"EXPLAIN ANALYZE {sql}"
            needs_rollback = False

        raw_result = None
        if executor is not None:
            raw_result = executor(explain_cmd, analyze, needs_rollback)
        elif eng is not None:
            if hasattr(eng, "explain"):
                raw_result = eng.explain(db_name, explain_cmd, rollback=needs_rollback)
            elif hasattr(eng, "query"):
                if needs_rollback:
                    eng.query(db_name, "BEGIN;")
                    try:
                        raw_result = eng.query(db_name, explain_cmd)
                    finally:
                        eng.query(db_name, "ROLLBACK;")
                else:
                    raw_result = eng.query(db_name, explain_cmd)

        if raw_result is None:
            if pattern.metadata.get("target_plan"):
                raw_result = pattern.metadata["target_plan"]
            elif pattern.source_plan:
                raw_result = pattern.source_plan

        if dialect == "postgres":
            return self.parse_postgres_plan(raw_result, query_id=pattern.query_id)
        return self.parse_mysql_plan(raw_result, query_id=pattern.query_id)

    def parse_postgres_plan(self, plan_data: Any, query_id: str = "") -> ExecutionPlan:
        exec_plan = ExecutionPlan(query_id=query_id, dialect="postgres", raw_plan=plan_data)
        if not plan_data:
            return exec_plan

        root_node = None
        if isinstance(plan_data, list) and plan_data and isinstance(plan_data[0], dict):
            entry = plan_data[0]
            root_node = entry.get("Plan")
            exec_plan.execution_time_ms = float(entry.get("Execution Time", 0.0))
            exec_plan.planning_time_ms = float(entry.get("Planning Time", 0.0))
        elif isinstance(plan_data, dict):
            root_node = plan_data.get("Plan", plan_data)
            exec_plan.execution_time_ms = float(plan_data.get("Execution Time", 0.0))
            exec_plan.planning_time_ms = float(plan_data.get("Planning Time", 0.0))
        elif isinstance(plan_data, str):
            return self._parse_postgres_text_plan(plan_data, query_id)

        if not root_node:
            return exec_plan

        exec_plan.plan_cost = float(root_node.get("Total Cost", 0.0))
        if exec_plan.execution_time_ms == 0.0:
            exec_plan.execution_time_ms = float(root_node.get("Actual Total Time", 0.0))

        def _walk(node: Dict[str, Any]):
            nt = node.get("Node Type", "")
            exec_plan.node_types.append(nt)
            if "Scan" in nt:
                rel = node.get("Relation Name", "")
                idx = node.get("Index Name")
                filt = node.get("Filter")
                cost = float(node.get("Total Cost", 0.0))
                rows = int(node.get("Plan Rows", node.get("Actual Rows", 0)))
                act_time = float(node.get("Actual Total Time", 0.0))
                loops = int(node.get("Actual Loops", 1))
                scan_dict = {
                    "table": rel,
                    "scan_type": nt,
                    "index_name": idx,
                    "filter": filt,
                    "cost": cost,
                    "rows": rows,
                    "actual_time_ms": act_time,
                    "loops": loops,
                }
                exec_plan.scans.append(scan_dict)
                if rel and rel not in exec_plan.tables_accessed:
                    exec_plan.tables_accessed.append(rel)
                if idx and idx not in exec_plan.indexes_used:
                    exec_plan.indexes_used.append(idx)
                if nt == "Seq Scan":
                    exec_plan.has_seq_scan = True

            if "Join" in nt or nt == "Nested Loop":
                jtype = "Nested Loop" if nt == "Nested Loop" else nt
                if nt == "Nested Loop":
                    exec_plan.has_nested_loop = True
                join_dict = {
                    "join_type": jtype,
                    "loops": int(node.get("Actual Loops", 1)),
                    "rows": int(node.get("Plan Rows", node.get("Actual Rows", 0))),
                    "cost": float(node.get("Total Cost", 0.0)),
                    "actual_time_ms": float(node.get("Actual Total Time", 0.0)),
                }
                exec_plan.joins.append(join_dict)

            for child in node.get("Plans", []):
                _walk(child)

        _walk(root_node)
        return exec_plan

    def _parse_postgres_text_plan(self, text: str, query_id: str = "") -> ExecutionPlan:
        plan = ExecutionPlan(query_id=query_id, dialect="postgres", raw_plan=text)
        for line in text.splitlines():
            line_str = line.strip()
            if "Seq Scan on" in line_str:
                m = re.search(r"Seq Scan on\s+([a-zA-Z0-9_]+)", line_str)
                tbl = m.group(1) if m else "unknown"
                plan.has_seq_scan = True
                plan.node_types.append("Seq Scan")
                plan.scans.append({"table": tbl, "scan_type": "Seq Scan", "index_name": None})
            elif "Index Scan" in line_str or "Bitmap Index Scan" in line_str:
                m = re.search(r"(?:Index Scan using|Bitmap Index Scan on)\s+([a-zA-Z0-9_]+)\s+on\s+([a-zA-Z0-9_]+)", line_str)
                idx = m.group(1) if m else "idx"
                tbl = m.group(2) if m else "unknown"
                plan.node_types.append("Index Scan")
                plan.scans.append({"table": tbl, "scan_type": "Index Scan", "index_name": idx})
                plan.indexes_used.append(idx)
            elif "Nested Loop" in line_str:
                plan.has_nested_loop = True
                plan.node_types.append("Nested Loop")
                plan.joins.append({"join_type": "Nested Loop", "loops": 1})
            elif "Hash Join" in line_str:
                plan.node_types.append("Hash Join")
                plan.joins.append({"join_type": "Hash Join", "loops": 1})

            if "Execution Time:" in line_str:
                tm = re.search(r"Execution Time:\s*([\d\.]+)\s*ms", line_str)
                if tm:
                    plan.execution_time_ms = float(tm.group(1))
            if "cost=" in line_str and plan.plan_cost == 0.0:
                cm = re.search(r"cost=[\d\.]+\.\.([\d\.]+)", line_str)
                if cm:
                    plan.plan_cost = float(cm.group(1))

        return plan

    def parse_mysql_plan(self, plan_data: Any, query_id: str = "") -> ExecutionPlan:
        plan = ExecutionPlan(query_id=query_id, dialect="mysql", raw_plan=plan_data)
        if not plan_data:
            return plan

        if isinstance(plan_data, str):
            try:
                plan_data = json.loads(plan_data)
            except Exception:
                return self._parse_mysql_text_plan(plan_data, query_id)

        qb = plan_data.get("query_block", plan_data) if isinstance(plan_data, dict) else {}
        cost_info = qb.get("cost_info", {})
        if "query_cost" in cost_info:
            plan.plan_cost = float(cost_info["query_cost"])

        def _traverse(node: Any):
            if isinstance(node, dict):
                if "table" in node:
                    t = node["table"]
                    tbl = t.get("table_name", "")
                    atype = t.get("access_type", "ALL")
                    key = t.get("key")
                    rows = int(t.get("rows_examined_per_scan", 0))
                    scan_type = "Seq Scan" if atype == "ALL" else "Index Scan"
                    if scan_type == "Seq Scan":
                        plan.has_seq_scan = True
                    plan.scans.append({
                        "table": tbl,
                        "scan_type": scan_type,
                        "index_name": key,
                        "rows": rows,
                        "access_type": atype,
                    })
                    if tbl and tbl not in plan.tables_accessed:
                        plan.tables_accessed.append(tbl)
                    if key and key not in plan.indexes_used:
                        plan.indexes_used.append(key)
                if "nested_loop" in node:
                    plan.has_nested_loop = True
                    plan.joins.append({"join_type": "Nested Loop", "loops": 1})
                    for sub in node["nested_loop"]:
                        _traverse(sub)
                for k, v in node.items():
                    if k not in ("table", "nested_loop"):
                        _traverse(v)
            elif isinstance(node, list):
                for item in node:
                    _traverse(item)

        _traverse(qb)
        return plan

    def _parse_mysql_text_plan(self, text: str, query_id: str = "") -> ExecutionPlan:
        plan = ExecutionPlan(query_id=query_id, dialect="mysql", raw_plan=text)
        for line in text.splitlines():
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 8:
                tbl = parts[2]
                atype = parts[3]
                key = parts[4] if parts[4] != "NULL" else None
                rows = int(parts[7]) if parts[7].isdigit() else 0
                scan_type = "Seq Scan" if atype == "ALL" else "Index Scan"
                if scan_type == "Seq Scan":
                    plan.has_seq_scan = True
                plan.scans.append({"table": tbl, "scan_type": scan_type, "index_name": key, "rows": rows})
                if tbl not in plan.tables_accessed:
                    plan.tables_accessed.append(tbl)
                if key:
                    plan.indexes_used.append(key)
        return plan

    def compare_plans(
        self,
        source_plan: ExecutionPlan,
        target_plan: ExecutionPlan,
        pattern: Optional[QueryPattern] = None,
        threshold_latency_ratio: Optional[float] = None,
    ) -> List[ParityIssue]:
        thresh = threshold_latency_ratio or self.latency_threshold
        issues: List[ParityIssue] = []
        qid = target_plan.query_id or (source_plan.query_id if source_plan else "")

        src_scan_by_tbl: Dict[str, Dict[str, Any]] = {}
        for s in (source_plan.scans if source_plan else []):
            tbl = s.get("table", "")
            if tbl:
                src_scan_by_tbl[tbl] = s

        for tgt_scan in target_plan.scans:
            tbl = tgt_scan.get("table", "")
            if not tbl:
                continue
            tgt_type = tgt_scan.get("scan_type", "")
            tgt_idx = tgt_scan.get("index_name")
            src_scan = src_scan_by_tbl.get(tbl)
            src_type = src_scan.get("scan_type", "") if src_scan else ""
            src_idx = src_scan.get("index_name") if src_scan else None

            if "Seq Scan" in tgt_type or tgt_scan.get("access_type") == "ALL":
                if src_scan and ("Index" in src_type or src_idx):
                    col_hint = pattern.filter_columns[0] if (pattern and pattern.filter_columns) else "id"
                    ddl = self.build_index_ddl(tbl, [col_hint], dialect=target_plan.dialect)
                    issues.append(
                        ParityIssue(
                            issue_id=f"seq_scan_{tbl}_{qid}",
                            query_id=qid,
                            issue_type="SEQ_SCAN_REGRESSION",
                            severity="CRITICAL",
                            table=tbl,
                            description=f"Full Seq Scan on target where source used {src_type} ({src_idx or 'index'})",
                            source_metric={"scan_type": src_type, "index": src_idx},
                            target_metric={"scan_type": tgt_type, "index": tgt_idx},
                            recommendation=ddl,
                        )
                    )
                else:
                    if pattern and pattern.filter_columns:
                        cols = pattern.filter_columns[:2]
                        ddl = self.build_index_ddl(tbl, cols, dialect=target_plan.dialect)
                        issues.append(
                            ParityIssue(
                                issue_id=f"missing_idx_{tbl}_{qid}",
                                query_id=qid,
                                issue_type="MISSING_INDEX",
                                severity="HIGH",
                                table=tbl,
                                description=f"Missing secondary index on filtered columns: {', '.join(cols)}",
                                source_metric=None,
                                target_metric={"scan_type": tgt_type},
                                recommendation=ddl,
                            )
                        )

        src_joins = [j.get("join_type") for j in (source_plan.joins if source_plan else [])]
        tgt_joins = [j.get("join_type") for j in target_plan.joins]

        for tj in target_plan.joins:
            if tj.get("join_type") == "Nested Loop":
                loops = tj.get("loops", 1)
                cost = tj.get("cost", 0.0)
                if ("Hash Join" in src_joins or "Merge Join" in src_joins) or loops >= 50 or cost >= 5000:
                    sev = "CRITICAL" if (loops >= 50 or "Hash Join" in src_joins) else "HIGH"
                    src_j_str = ", ".join(src_joins) if src_joins else "set join"
                    issues.append(
                        ParityIssue(
                            issue_id=f"join_degrad_{qid}",
                            query_id=qid,
                            issue_type="JOIN_DEGRADATION",
                            severity=sev,
                            table=target_plan.tables_accessed[0] if target_plan.tables_accessed else "join",
                            description=f"Nested Loop degradation (loops={loops}, cost={cost}) vs source {src_j_str}",
                            source_metric={"joins": src_joins},
                            target_metric=tj,
                            recommendation="Analyze table statistics (ANALYZE / VACUUM ANALYZE) or adjust join work_mem",
                        )
                    )

        src_time = source_plan.execution_time_ms if source_plan else 0.0
        if src_time == 0.0 and pattern:
            src_time = pattern.mean_time_ms or pattern.total_time_ms / max(pattern.call_count, 1)

        tgt_time = target_plan.execution_time_ms
        if src_time > 0.05 and tgt_time > 0.05:
            ratio = tgt_time / src_time
            if ratio > thresh:
                sev = "CRITICAL" if ratio >= 5.0 else "HIGH"
                issues.append(
                    ParityIssue(
                        issue_id=f"latency_anom_{qid}",
                        query_id=qid,
                        issue_type="LATENCY_ANOMALY",
                        severity=sev,
                        table=target_plan.tables_accessed[0] if target_plan.tables_accessed else "query",
                        description=f"Latency regression {ratio:.2f}x (src: {src_time:.2f}ms, tgt: {tgt_time:.2f}ms, threshold: {thresh:.1f}x)",
                        source_metric=src_time,
                        target_metric=tgt_time,
                        latency_ratio=round(ratio, 3),
                        recommendation="Optimize query execution path or provision missing index",
                    )
                )

        return issues

    def build_index_ddl(self, table: str, columns: List[str], dialect: str = "postgres") -> str:
        col_list = ", ".join(columns) if columns else "id"
        idx_suffix = "_".join(re.sub(r"[^a-zA-Z0-9_]", "", c) for c in columns) if columns else "idx"
        idx_name = f"idx_{table}_{idx_suffix}"
        if dialect == "postgres":
            return f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table} ({col_list});"
        return f"CREATE INDEX {idx_name} ON {table} ({col_list});"

    def audit(
        self,
        workload: List[QueryPattern],
        target_plans: Optional[Dict[str, Union[ExecutionPlan, Dict[str, Any], str]]] = None,
        source_plans: Optional[Dict[str, Union[ExecutionPlan, Dict[str, Any], str]]] = None,
        analyze: bool = False,
        executor: Optional[Callable[[str, bool, bool], Any]] = None,
    ) -> QueryParityReport:
        t_start = time.monotonic()
        rep_id = f"qparity_{int(t_start)}_{hashlib.sha256(str(len(workload)).encode()).hexdigest()[:8]}"
        hop_name = getattr(self.hop, "name", "") if self.hop else ""

        all_issues: List[ParityIssue] = []
        index_recs_map: Dict[str, IndexRecommendation] = {}
        src_latencies: List[float] = []
        tgt_latencies: List[float] = []

        target_plans = target_plans or {}
        source_plans = source_plans or {}

        for pat in workload:
            qid = pat.query_id

            src_plan: Optional[ExecutionPlan] = None
            if qid in source_plans:
                val = source_plans[qid]
                src_plan = val if isinstance(val, ExecutionPlan) else self.parse_postgres_plan(val, query_id=qid)
            elif pat.source_plan:
                src_plan = self.parse_postgres_plan(pat.source_plan, query_id=qid)
            else:
                src_plan = ExecutionPlan(
                    query_id=qid,
                    dialect=pat.dialect,
                    execution_time_ms=pat.mean_time_ms,
                    scans=[{"table": t, "scan_type": "Index Scan", "index_name": f"idx_{t}_pk"} for t in pat.tables],
                )

            tgt_plan: Optional[ExecutionPlan] = None
            if qid in target_plans:
                tval = target_plans[qid]
                tgt_plan = tval if isinstance(tval, ExecutionPlan) else self.parse_postgres_plan(tval, query_id=qid)
            else:
                tgt_plan = self.replay_on_target(pat, analyze=analyze, executor=executor)

            if src_plan and src_plan.execution_time_ms > 0:
                src_latencies.append(src_plan.execution_time_ms)
            elif pat.mean_time_ms > 0:
                src_latencies.append(pat.mean_time_ms)

            if tgt_plan and tgt_plan.execution_time_ms > 0:
                tgt_latencies.append(tgt_plan.execution_time_ms)

            issues = self.compare_plans(src_plan, tgt_plan, pattern=pat)
            all_issues.extend(issues)

            for iss in issues:
                if iss.recommendation.startswith("CREATE INDEX"):
                    tbl = iss.table
                    key = f"{tbl}_{iss.recommendation}"
                    if key not in index_recs_map:
                        cols = pat.filter_columns if (pat and pat.filter_columns) else ["id"]
                        idx_name = f"idx_{tbl}_{'_'.join(cols)}"
                        index_recs_map[key] = IndexRecommendation(
                            table=tbl,
                            columns=cols,
                            index_name=idx_name,
                            sql_ddl=iss.recommendation,
                            reason=iss.description,
                            severity=iss.severity,
                        )

        def _percentile(data: List[float], pct: float) -> float:
            if not data:
                return 0.0
            data_sorted = sorted(data)
            idx = int(math.ceil((pct / 100.0) * len(data_sorted))) - 1
            return round(data_sorted[max(0, min(idx, len(data_sorted) - 1))], 3)

        src_p50 = _percentile(src_latencies, 50)
        src_p95 = _percentile(src_latencies, 95)
        tgt_p50 = _percentile(tgt_latencies, 50)
        tgt_p95 = _percentile(tgt_latencies, 95)
        max_ratio = max([i.latency_ratio for i in all_issues] + [1.0])

        critical_count = sum(1 for i in all_issues if i.severity == "CRITICAL")
        high_count = sum(1 for i in all_issues if i.severity == "HIGH")

        can_cutover = critical_count == 0
        if not can_cutover:
            verdict = "FAIL"
        elif high_count > 0:
            verdict = "WARN"
        else:
            verdict = "PASS"

        sla_checks = {
            "no_critical_plan_regressions": critical_count == 0,
            "no_missing_secondary_indexes": len(index_recs_map) == 0,
            "latency_p95_under_regression_threshold": (tgt_p95 <= (src_p95 * self.latency_threshold)) if src_p95 > 0 else True,
            "overall_parity_passed": can_cutover,
        }

        issues_dict = [dataclasses.asdict(i) for i in all_issues]
        missing_idxs_dict = [dataclasses.asdict(rec) for rec in index_recs_map.values()]
        index_ddls = [rec.sql_ddl for rec in index_recs_map.values()]
        plan_degrads = [dataclasses.asdict(i) for i in all_issues if i.issue_type in ("SEQ_SCAN_REGRESSION", "JOIN_DEGRADATION")]
        latency_anoms = [dataclasses.asdict(i) for i in all_issues if i.issue_type == "LATENCY_ANOMALY"]

        summary_stats = {
            "source_p50": src_p50,
            "source_p95": src_p95,
            "target_p50": tgt_p50,
            "target_p95": tgt_p95,
            "max_latency_ratio": round(max_ratio, 3),
            "critical_issues_count": critical_count,
            "high_issues_count": high_count,
        }

        report = QueryParityReport(
            report_id=rep_id,
            hop_name=hop_name,
            timestamp=t_start,
            verdict=verdict,
            can_cutover=can_cutover,
            threshold_latency_ratio=self.latency_threshold,
            total_queries_analyzed=len(workload),
            parity_issues=issues_dict,
            missing_indexes=missing_idxs_dict,
            index_recommendations=index_ddls,
            plan_degradations=plan_degrads,
            latency_anomalies=latency_anoms,
            summary_stats=summary_stats,
            sla_checks=sla_checks,
        )

        if self.hop and hasattr(self.hop, "report_dir"):
            try:
                rep_dir = Path(self.hop.report_dir(self.db))
                durable.write_json(rep_dir / "query-parity-report.json", report.to_dict(), indent=2)
            except Exception:
                pass

        return report
