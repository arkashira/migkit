import json
from pathlib import Path
import pytest

from migkit.query_guard import (
    ExecutionPlan,
    IndexRecommendation,
    ParityIssue,
    QueryOptimizerParityGuard,
    QueryParityReport,
    QueryPattern,
    extract_filter_columns,
    extract_tables,
    normalize_sql,
)


class MockHop:
    name = "hop_query_guard_test"
    engine = "postgres"

    def __init__(self, root: Path):
        self.root = root

    def report_dir(self, db: str = "") -> Path:
        p = self.root / (db or "")
        p.mkdir(parents=True, exist_ok=True)
        return p


class MockEngine:
    dialect = "postgres"

    def __init__(self):
        self.queries_executed = []

    def explain(self, db, sql, rollback=False):
        self.queries_executed.append((db, sql, rollback))
        return [
            {
                "Plan": {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders",
                    "Total Cost": 1500.0,
                    "Actual Total Time": 12.5,
                    "Filter": "(customer_id = 42)",
                },
                "Execution Time": 12.5,
            }
        ]


def test_normalize_sql_and_extraction():
    sql = """
    /* fetch recent orders */
    SELECT o.id, o.total 
    FROM orders o 
    JOIN customers c ON o.customer_id = c.id
    WHERE o.customer_id = 420 AND o.status IN ('paid', 'shipped') AND o.amount > 99.50;
    """
    qid, norm = normalize_sql(sql)
    assert len(qid) == 16
    assert "/* fetch recent orders */" not in norm
    assert "?" in norm
    assert "420" not in norm
    assert "'paid'" not in norm

    tables = extract_tables(sql)
    assert "orders" in tables
    assert "customers" in tables

    cols = extract_filter_columns(sql)
    assert "customer_id" in cols or "o" in cols or "status" in cols


def test_extract_from_mysql_slow_log():
    sample_log = """
# Time: 2026-10-17T01:00:00.000000Z
# User@Host: app[app] @ [10.0.0.1]
# Query_time: 1.250000  Lock_time: 0.000010 Rows_sent: 50  Rows_examined: 100000
SET timestamp=1760662800;
SELECT * FROM orders WHERE customer_id = 42 AND status = 'pending';

# User@Host: app[app] @ [10.0.0.1]
# Query_time: 0.750000  Lock_time: 0.000005 Rows_sent: 10  Rows_examined: 50000
SELECT * FROM orders WHERE customer_id = 99 AND status = 'shipped';
"""
    guard = QueryOptimizerParityGuard()
    patterns = guard.extract_from_slow_log(sample_log, dialect="mysql")

    assert len(patterns) == 1
    p = patterns[0]
    assert p.call_count == 2
    assert p.dialect == "mysql"
    assert p.total_time_ms == 2000.0
    assert p.mean_time_ms == 1000.0
    assert p.max_time_ms == 1250.0
    assert p.min_time_ms == 750.0
    assert p.rows_examined == 150000
    assert "orders" in p.tables


def test_extract_from_postgres_slow_log():
    sample_log = """
2026-10-17 01:00:00 UTC [12345]: [1-1] user=app,db=appdb LOG:  duration: 450.500 ms  statement: SELECT o.id FROM orders o WHERE o.customer_id = 1001;
2026-10-17 01:00:05 UTC [12345]: [2-1] user=app,db=appdb LOG:  duration: 550.500 ms  statement: SELECT o.id FROM orders o WHERE o.customer_id = 2002;
2026-10-17 01:00:10 UTC [12346]: [1-1] user=app,db=appdb LOG:  duration: 12.000 ms  statement: SELECT id FROM users WHERE email = 'test@example.com';
"""
    guard = QueryOptimizerParityGuard()
    patterns = guard.extract_from_slow_log(sample_log, dialect="postgres")

    assert len(patterns) == 2
    order_pat = next(p for p in patterns if "orders" in p.tables)
    assert order_pat.call_count == 2
    assert order_pat.total_time_ms == 1001.0
    assert order_pat.mean_time_ms == 500.5


def test_extract_from_pg_stat_statements():
    records = [
        {
            "query": "SELECT * FROM orders WHERE customer_id = $1",
            "calls": 1000,
            "total_exec_time": 50000.0,
            "mean_exec_time": 50.0,
            "max_exec_time": 250.0,
            "min_exec_time": 5.0,
            "rows": 1000,
        },
        {
            "query": "SELECT * FROM users WHERE email = $1",
            "calls": 5000,
            "total_exec_time": 10000.0,
            "mean_exec_time": 2.0,
            "max_exec_time": 15.0,
            "min_exec_time": 0.5,
            "rows": 5000,
        },
    ]
    guard = QueryOptimizerParityGuard()
    patterns = guard.extract_from_pg_stat_statements(records)

    assert len(patterns) == 2
    assert patterns[0].total_time_ms == 50000.0
    assert patterns[0].call_count == 1000
    assert "orders" in patterns[0].tables


def test_extract_from_performance_schema():
    records = [
        {
            "DIGEST_TEXT": "SELECT * FROM `orders` WHERE `customer_id` = ?",
            "COUNT_STAR": 500,
            "SUM_TIMER_WAIT": 50000 * 1000000000,
            "AVG_TIMER_WAIT": 100 * 1000000000,
            "MAX_TIMER_WAIT": 250 * 1000000000,
            "MIN_TIMER_WAIT": 10 * 1000000000,
            "SUM_ROWS_EXAMINED": 500000,
            "SUM_ROWS_SENT": 500,
        }
    ]
    guard = QueryOptimizerParityGuard()
    patterns = guard.extract_from_performance_schema(records)

    assert len(patterns) == 1
    p = patterns[0]
    assert p.dialect == "mysql"
    assert p.call_count == 500
    assert p.total_time_ms == 50000.0
    assert p.mean_time_ms == 100.0
    assert p.rows_examined == 500000


def test_sample_workload_strategies():
    guard = QueryOptimizerParityGuard()
    patterns = [
        QueryPattern("q1", "s1", "s1", call_count=10000, mean_time_ms=1.0, total_time_ms=10000.0),
        QueryPattern("q2", "s2", "s2", call_count=8000, mean_time_ms=2.0, total_time_ms=16000.0),
        QueryPattern("q3", "s3", "s3", call_count=50, mean_time_ms=500.0, total_time_ms=25000.0),
        QueryPattern("q4", "s4", "s4", call_count=10, mean_time_ms=2000.0, total_time_ms=20000.0),
    ]

    by_freq = guard.sample_workload(patterns, sample_size=2, strategy="frequency")
    assert [p.query_id for p in by_freq] == ["q1", "q2"]

    by_lat = guard.sample_workload(patterns, sample_size=2, strategy="latency")
    assert [p.query_id for p in by_lat] == ["q4", "q3"]

    hybrid = guard.sample_workload(patterns, sample_size=2, strategy="hybrid")
    assert len(hybrid) == 2
    assert "q1" in [p.query_id for p in hybrid]
    assert "q4" in [p.query_id for p in hybrid]


def test_replay_on_target_mutation_safety():
    eng = MockEngine()
    guard = QueryOptimizerParityGuard(target_eng=eng, db="appdb")

    pat_select = QueryPattern("q_sel", "SELECT * FROM orders WHERE id = 1", "SELECT * FROM orders WHERE id = ?")
    guard.replay_on_target(pat_select, analyze=False, read_only=True)
    assert len(eng.queries_executed) == 1
    assert eng.queries_executed[0][2] is False

    pat_dml = QueryPattern("q_dml", "DELETE FROM orders WHERE id = 1", "DELETE FROM orders WHERE id = ?")
    guard.replay_on_target(pat_dml, analyze=True, read_only=False)
    assert len(eng.queries_executed) == 2
    assert eng.queries_executed[1][2] is False
    assert "ANALYZE" not in eng.queries_executed[1][1]


def test_parse_postgres_plan_json():
    guard = QueryOptimizerParityGuard()
    raw_json = [
        {
            "Plan": {
                "Node Type": "Hash Join",
                "Total Cost": 850.5,
                "Plan Rows": 500,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Relation Name": "orders",
                        "Total Cost": 450.0,
                        "Plan Rows": 10000,
                        "Filter": "(status = 'pending')",
                    },
                    {
                        "Node Type": "Index Scan",
                        "Relation Name": "customers",
                        "Index Name": "idx_customers_id",
                        "Total Cost": 25.0,
                        "Plan Rows": 1,
                    },
                ],
            },
            "Execution Time": 42.15,
            "Planning Time": 0.85,
        }
    ]
    plan = guard.parse_postgres_plan(raw_json, query_id="q_test")

    assert plan.execution_time_ms == 42.15
    assert plan.plan_cost == 850.5
    assert plan.has_seq_scan is True
    assert "orders" in plan.tables_accessed
    assert "customers" in plan.tables_accessed
    assert "idx_customers_id" in plan.indexes_used


def test_detect_seq_scan_regression_and_actionable_ddl():
    guard = QueryOptimizerParityGuard(latency_threshold=2.0)

    src_plan = ExecutionPlan(
        query_id="q1",
        dialect="postgres",
        execution_time_ms=5.0,
        scans=[{"table": "orders", "scan_type": "Index Scan", "index_name": "idx_orders_customer"}],
    )
    tgt_plan = ExecutionPlan(
        query_id="q1",
        dialect="postgres",
        execution_time_ms=25.0,
        scans=[{"table": "orders", "scan_type": "Seq Scan", "index_name": None}],
        tables_accessed=["orders"],
    )
    pattern = QueryPattern("q1", "SELECT * FROM orders WHERE customer_id = 42", "", filter_columns=["customer_id"])

    issues = guard.compare_plans(src_plan, tgt_plan, pattern=pattern)
    assert len(issues) >= 1

    seq_issue = next(i for i in issues if i.issue_type == "SEQ_SCAN_REGRESSION")
    assert seq_issue.severity == "CRITICAL"
    assert "orders" == seq_issue.table
    assert "CREATE INDEX CONCURRENTLY" in seq_issue.recommendation
    assert "idx_orders_customer_id" in seq_issue.recommendation

    lat_issue = next(i for i in issues if i.issue_type == "LATENCY_ANOMALY")
    assert lat_issue.latency_ratio == 5.0


def test_detect_join_strategy_degradation():
    guard = QueryOptimizerParityGuard()

    src_plan = ExecutionPlan(
        query_id="q_join",
        dialect="postgres",
        execution_time_ms=10.0,
        joins=[{"join_type": "Hash Join", "loops": 1, "rows": 1000}],
    )
    tgt_plan = ExecutionPlan(
        query_id="q_join",
        dialect="postgres",
        execution_time_ms=45.0,
        joins=[{"join_type": "Nested Loop", "loops": 500, "rows": 50000}],
        tables_accessed=["order_items"],
    )

    issues = guard.compare_plans(src_plan, tgt_plan)
    join_issue = next(i for i in issues if i.issue_type == "JOIN_DEGRADATION")
    assert join_issue.severity == "CRITICAL"
    assert "Nested Loop degradation" in join_issue.description


def test_audit_workload_generates_parity_report(tmp_path):
    hop = MockHop(tmp_path)
    guard = QueryOptimizerParityGuard(hop=hop, db="appdb", latency_threshold=2.0)

    workload = [
        QueryPattern(
            query_id="q_pass",
            raw_query="SELECT * FROM users WHERE id = 1",
            normalized_query="SELECT * FROM users WHERE id = ?",
            call_count=1000,
            mean_time_ms=2.0,
            total_time_ms=2000.0,
            tables=["users"],
        ),
        QueryPattern(
            query_id="q_regress",
            raw_query="SELECT * FROM orders WHERE customer_id = 42",
            normalized_query="SELECT * FROM orders WHERE customer_id = ?",
            call_count=500,
            mean_time_ms=4.0,
            total_time_ms=2000.0,
            tables=["orders"],
            filter_columns=["customer_id"],
        ),
    ]

    target_plans = {
        "q_pass": ExecutionPlan(
            query_id="q_pass",
            execution_time_ms=2.1,
            scans=[{"table": "users", "scan_type": "Index Scan", "index_name": "users_pkey"}],
        ),
        "q_regress": ExecutionPlan(
            query_id="q_regress",
            execution_time_ms=28.0,
            scans=[{"table": "orders", "scan_type": "Seq Scan", "index_name": None}],
            tables_accessed=["orders"],
        ),
    }

    report = guard.audit(workload, target_plans=target_plans)

    assert isinstance(report, QueryParityReport)
    assert report.total_queries_analyzed == 2
    assert report.verdict == "FAIL"
    assert report.can_cutover is False
    assert len(report.missing_indexes) == 1
    assert "idx_orders_customer_id" in report.index_recommendations[0]
    assert len(report.plan_degradations) >= 1
    assert len(report.latency_anomalies) >= 1

    rendered = report.render()
    assert "CONTINUOUS QUERY OPTIMIZER & LATENCY PARITY GUARD" in rendered
    assert "Verdict: FAIL" in rendered
    assert "CREATE INDEX CONCURRENTLY" in rendered

    rep_file = hop.report_dir("appdb") / "query-parity-report.json"
    assert rep_file.exists()
    data = json.loads(rep_file.read_text())
    assert data["can_cutover"] is False
    assert data["verdict"] == "FAIL"


def test_audit_workload_all_pass_allows_cutover(tmp_path):
    hop = MockHop(tmp_path)
    guard = QueryOptimizerParityGuard(hop=hop, db="appdb", latency_threshold=2.0)

    workload = [
        QueryPattern(
            query_id="q1",
            raw_query="SELECT * FROM users WHERE id = 1",
            normalized_query="SELECT * FROM users WHERE id = ?",
            call_count=100,
            mean_time_ms=1.5,
            total_time_ms=150.0,
            tables=["users"],
        )
    ]

    target_plans = {
        "q1": ExecutionPlan(
            query_id="q1",
            execution_time_ms=1.6,
            scans=[{"table": "users", "scan_type": "Index Scan", "index_name": "users_pkey"}],
        )
    }

    report = guard.audit(workload, target_plans=target_plans)
    assert report.verdict == "PASS"
    assert report.can_cutover is True
    assert len(report.parity_issues) == 0
    assert report.sla_checks["overall_parity_passed"] is True
