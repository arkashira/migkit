import html
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

CHECK_ORDER = ["schema", "counts", "autoinc", "data", "deep", "delta"]

COMMERCIAL_FEATURE_INVENTORY = {
    "oracle_veridata": {
        "vendor": "Oracle",
        "product": "GoldenGate Veridata",
        "version": "12.2.1",
        "official_sources": [
            "https://docs.oracle.com/goldengate/v1221/gg-veridata/GVDUG/working_with_jobs.htm",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Job and group comparison pairs"},
            "mappings_filters": {"status": "documented_present", "notes": "Partition and column-level filters"},
            "database_grouping": {"status": "documented_present", "notes": "Hierarchy by compare pairs and groups"},
            "progress_vs_correctness": {"status": "documented_absent", "notes": "Job status often mixed with sync status"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Out-of-sync and unconfirmed states reported"},
            "diff_categories": {"status": "documented_present", "notes": "Out-of-sync row records with source and target values"},
            "consistency_guarantees": {"status": "documented_present", "notes": "In-flight confirm step via GoldenGate trail checks"},
            "next_actions": {"status": "documented_absent", "notes": "Manual out-of-sync report review without automated fix hints"},
            "structured_export": {"status": "documented_present", "notes": "XML and CLI report generation"},
            "detail_navigation": {"status": "documented_present", "notes": "Drill-down to out-of-sync column values"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No bidirectional drift proofs or schema invariant barriers"},
        },
    },
    "informatica_cdv": {
        "vendor": "Informatica",
        "product": "Cloud Data Validation",
        "version": "July 2024",
        "official_sources": [
            "https://docs.informatica.com/content/dam/source/GUID-B/GUID-BE90AF74-E490-4BA6-9EC6-2D9B079C5D7B/7/en/CDV_July2024_Monitor_en.pdf",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Suite and test case scope configuration"},
            "mappings_filters": {"status": "documented_present", "notes": "Table mapping and SQL expression filters"},
            "database_grouping": {"status": "documented_absent", "notes": "Grouped by test suite, not natural database boundaries"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Execution monitor separated from assertion results"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Unchecked, aborted and skipped tests distinguished"},
            "diff_categories": {"status": "documented_present", "notes": "Matched, unmatched, missing, extra records"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Point-in-time snapshot query only; no live replication proof"},
            "next_actions": {"status": "documented_absent", "notes": "Requires user investigation in source tools"},
            "structured_export": {"status": "documented_present", "notes": "REST API and PDF/CSV execution summaries"},
            "detail_navigation": {"status": "documented_present", "notes": "Drill-down from test case to row differences"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No autoinc headroom, artifact separation, or drift proofs"},
        },
    },
    "qlik_replicate": {
        "vendor": "Qlik",
        "product": "Qlik Replicate",
        "version": "May 2025",
        "official_sources": [
            "https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/Tutorial/RunMonitorTask_sample.htm",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Table selection list and pattern include/exclude"},
            "mappings_filters": {"status": "documented_present", "notes": "Column transformation rules and record filtering"},
            "database_grouping": {"status": "documented_absent", "notes": "Task-centric table list rather than database-grouped summary"},
            "progress_vs_correctness": {"status": "documented_absent", "notes": "Primarily replication progress; validation verification is separate add-on"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Error, queued, and loading states distinct"},
            "diff_categories": {"status": "evidence_unavailable", "notes": "Verification details require Qlik Enterprise Manager"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Transactional CDC ordering supported"},
            "next_actions": {"status": "documented_absent", "notes": "Error flags without direct repair command generators"},
            "structured_export": {"status": "documented_present", "notes": "Enterprise Manager REST API"},
            "detail_navigation": {"status": "documented_present", "notes": "Table progress screen drilldown"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No schema invariant barriers or sequence headroom checks"},
        },
    },
    "redgate_sql_data_compare": {
        "vendor": "Redgate",
        "product": "SQL Data Compare",
        "version": "15.x",
        "official_sources": [
            "https://documentation.red-gate.com/sdc15/reviewing-the-comparison-results/viewing-the-row-differences",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Object mapping list and WHERE clause filters"},
            "mappings_filters": {"status": "documented_present", "notes": "Custom comparison keys and mapped tables"},
            "database_grouping": {"status": "documented_present", "notes": "Database pair root context"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Comparison phase progress separated from grid diff"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Identical, different, and unable to compare groups"},
            "diff_categories": {"status": "documented_present", "notes": "Source only, target only, and different row records"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Static table comparison; no CDC or live drift guarantees"},
            "next_actions": {"status": "documented_present", "notes": "Generates deployment synchronization scripts"},
            "structured_export": {"status": "documented_present", "notes": "CLI XML and HTML export"},
            "detail_navigation": {"status": "documented_present", "notes": "Side-by-side row viewer with highlighted column diffs"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No live drift proofs or replication artifact partitioning"},
        },
    },
    "devart_dbforge": {
        "vendor": "Devart",
        "product": "dbForge Data Compare",
        "version": "5.x",
        "official_sources": [
            "https://docs.devart.com/data-compare-for-mysql/view-comparison-results/view-comparison-results.html",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Object mapping view with schema filter"},
            "mappings_filters": {"status": "documented_present", "notes": "Include/exclude checkboxes and custom filters"},
            "database_grouping": {"status": "documented_present", "notes": "Target/source schema pair root"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Comparison wizard completion separate from diff grid"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Objects without keys marked incomparable"},
            "diff_categories": {"status": "documented_present", "notes": "Different, only in source, only in target, identical"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Point-in-time comparison without replication lock proof"},
            "next_actions": {"status": "documented_present", "notes": "Data synchronization script generation wizard"},
            "structured_export": {"status": "documented_present", "notes": "HTML, Excel, XML, and CSV reports"},
            "detail_navigation": {"status": "documented_present", "notes": "Object grid to bottom row data pane"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No live replication barriers or artifact separation"},
        },
    },
    "datafold": {
        "vendor": "Datafold",
        "product": "Datafold Cloud",
        "version": "2024",
        "official_sources": [
            "https://www.datafold.com/blog/what-the-heck-is-data-diffing/",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Model and table diff overview in web UI and CI PR comments"},
            "mappings_filters": {"status": "documented_present", "notes": "Primary key configuration and column selection"},
            "database_grouping": {"status": "documented_absent", "notes": "dbt model/table flat list rather than database boundaries"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "CI check execution state distinct from diff findings"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Unsupported column types and timed-out diffs stated"},
            "diff_categories": {"status": "documented_present", "notes": "Exclusive rows, shared rows with value differences"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Checksum hashing against warehouse tables; no transactional CDC proof"},
            "next_actions": {"status": "documented_absent", "notes": "Highlights differences without remediation scripts"},
            "structured_export": {"status": "documented_present", "notes": "JSON API, CLI, and GitHub PR comments"},
            "detail_navigation": {"status": "documented_present", "notes": "Interactive web diff with column breakdown"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No engine-level sequence headroom or runtime config invariants"},
        },
    },
    "querysurge": {
        "vendor": "QuerySurge",
        "product": "QuerySurge",
        "version": "2024",
        "official_sources": [
            "https://www.querysurge.com/product-tour/features",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "QueryPairs organized in test suites"},
            "mappings_filters": {"status": "documented_present", "notes": "Source and target custom SQL queries"},
            "database_grouping": {"status": "documented_absent", "notes": "Project/Suite hierarchy rather than database-first grouping"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Execution status (pass/fail/killed) distinct from diff counts"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Killed, warning, and syntax error states explicit"},
            "diff_categories": {"status": "documented_present", "notes": "Row count difference, column failure, data mismatch"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Independent JDBC queries without multi-table lock or proof"},
            "next_actions": {"status": "documented_absent", "notes": "Pure QA testing platform; no automated migration repairs"},
            "structured_export": {"status": "documented_present", "notes": "REST API, CLI, and automated email/HTML reports"},
            "detail_navigation": {"status": "documented_present", "notes": "Drilldown to mismatched cells in tabular grid"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No schema invariant DAGs, artifact checks, or drift proofs"},
        },
    },
    "ibm_asntdiff": {
        "vendor": "IBM",
        "product": "Data Replication asntdiff",
        "version": "10.2.1",
        "official_sources": [
            "https://www.ibm.com/docs/en/idr/10.2.1?topic=tables-table-compare-utility-asntdiff",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_absent", "notes": "Runs per subscription or table pair; no global scope preview"},
            "mappings_filters": {"status": "documented_present", "notes": "WHERE clause predicate support"},
            "database_grouping": {"status": "documented_absent", "notes": "Single table comparison execution per command"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Return codes distinguish execution failure from data mismatch"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Return code 1 indicates differences, 2 indicates error"},
            "diff_categories": {"status": "documented_present", "notes": "Writes differences to a dedicated diff table"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Coordinates with Q Capture commit sequence numbers"},
            "next_actions": {"status": "documented_present", "notes": "Pairs with asntrep for SQL repair generation"},
            "structured_export": {"status": "documented_present", "notes": "Diff stored in SQL table for query extraction"},
            "detail_navigation": {"status": "documented_absent", "notes": "Requires SQL queries against diff table; no unified UI"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No automatic cross-engine translation or invariant barriers"},
        },
    },
    "aws_dms": {
        "vendor": "Amazon Web Services",
        "product": "AWS Database Migration Service",
        "version": "3.5+",
        "official_sources": [
            "https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Validating.html",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Table mapping JSON preview in console"},
            "mappings_filters": {"status": "documented_present", "notes": "Transformation rules, schemas, and table filters"},
            "database_grouping": {"status": "documented_absent", "notes": "Flat table list under task; no multi-database grouping"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Validation pending, validating, and completed states separate"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Validation suspended, no primary key, error states"},
            "diff_categories": {"status": "documented_present", "notes": "Mismatched records, records with errors, deleted records"},
            "consistency_guarantees": {"status": "documented_absent", "notes": "Eventual validation during CDC; prone to false positives under write load"},
            "next_actions": {"status": "documented_absent", "notes": "Leaves `awsdms_apply_exceptions` table without auto-repair scripts"},
            "structured_export": {"status": "documented_present", "notes": "AWS CloudWatch metrics and DescribeTableStatistics API"},
            "detail_navigation": {"status": "documented_absent", "notes": "AWS console only shows error counts; requires manual query on target"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "Does not clean up or isolate its own bookkeeping artifacts"},
        },
    },
    "azure_dms": {
        "vendor": "Microsoft Azure",
        "product": "Azure Database Migration Service",
        "version": "2024",
        "official_sources": [
            "https://learn.microsoft.com/en-us/azure/dms/tutorial-sql-server-to-azure-sql",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Database and table selection screen with preflight validation"},
            "mappings_filters": {"status": "documented_present", "notes": "Database name remapping and table selection"},
            "database_grouping": {"status": "documented_present", "notes": "Database level overview under project"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Migration step state distinct from data validation outcome"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Schema mismatch and unsupported data types flagged in preflight"},
            "diff_categories": {"status": "documented_present", "notes": "Row count mismatches and schema discrepancy reports"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Transaction log restore provides high consistency for SQL Server"},
            "next_actions": {"status": "documented_absent", "notes": "Points to remediation documentation rather than direct SQL fixes"},
            "structured_export": {"status": "documented_present", "notes": "Azure Monitor and Resource Manager REST APIs"},
            "detail_navigation": {"status": "documented_present", "notes": "Activity blade displays database and table summary"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No bidirectional drift proofs or auto-increment headroom proofs"},
        },
    },
    "gcp_dms": {
        "vendor": "Google Cloud",
        "product": "Database Migration Service",
        "version": "2024",
        "official_sources": [
            "https://cloud.google.com/database-migration-service/docs",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Connection profile and migration job configuration summary"},
            "mappings_filters": {"status": "documented_present", "notes": "Schema conversion rules and database mapping"},
            "database_grouping": {"status": "documented_present", "notes": "Database level overview for heterogeneous migrations"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Migration phases (Full Dump, CDC) distinct from verification"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Unsupported flags in preflight assessment"},
            "diff_categories": {"status": "documented_absent", "notes": "Relies on external verification or basic row count comparisons"},
            "consistency_guarantees": {"status": "documented_present", "notes": "AlloyDB/Cloud SQL replication consistency"},
            "next_actions": {"status": "documented_present", "notes": "Schema conversion suggestions provided in workspace"},
            "structured_export": {"status": "documented_present", "notes": "Cloud Logging, Cloud Monitoring, and REST API"},
            "detail_navigation": {"status": "documented_present", "notes": "Migration job details page"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No continuous mathematical hash drift proofs"},
        },
    },
    "tencent_dts": {
        "vendor": "Tencent Cloud",
        "product": "Data Transmission Service (DTS)",
        "version": "2024",
        "official_sources": [
            "https://intl.cloud.tencent.com/document/product/571/18765",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Migration object selection with regex and wildcard filtering"},
            "mappings_filters": {"status": "documented_present", "notes": "DB/table renaming and column selection"},
            "database_grouping": {"status": "documented_present", "notes": "Database level tree view with object counts"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Independent Data Verification task separate from migration task"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Non-comparable tables (no PK) explicitly listed"},
            "diff_categories": {"status": "documented_present", "notes": "Inconsistent row count, content diff, and unverified tables"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Static verification or incremental verification with compensation"},
            "next_actions": {"status": "documented_present", "notes": "Generates data repair SQL scripts for inconsistent rows"},
            "structured_export": {"status": "documented_present", "notes": "Tencent Cloud API and downloadable diff CSV files"},
            "detail_navigation": {"status": "documented_present", "notes": "Object drilldown with primary key values of differing rows"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No replication artifact isolation (creates `dts_*` tables)"},
        },
    },
    "alibaba_dts": {
        "vendor": "Alibaba Cloud",
        "product": "Data Transmission Service (DTS)",
        "version": "2024",
        "official_sources": [
            "https://www.alibabacloud.com/help/en/dts/user-guide/data-verification",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Object mapping and verification scope configuration"},
            "mappings_filters": {"status": "documented_present", "notes": "Table renaming and conditional WHERE clauses"},
            "database_grouping": {"status": "documented_present", "notes": "Database and schema grouped overview"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Data verification progress distinct from migration pipeline"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Inconsistent, identical, and failed verification states"},
            "diff_categories": {"status": "documented_present", "notes": "Count inconsistency and content discrepancy"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Dual-sampling verification during incremental CDC"},
            "next_actions": {"status": "documented_present", "notes": "Automatic repair SQL statements generated for download"},
            "structured_export": {"status": "documented_present", "notes": "OpenAPI and console report download"},
            "detail_navigation": {"status": "documented_present", "notes": "Detailed record diff view in DTS console"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "Leaves tracking tables on target; no schema barrier proofs"},
        },
    },
    "huawei_drs": {
        "vendor": "Huawei Cloud",
        "product": "Data Replication Service (DRS)",
        "version": "2024",
        "official_sources": [
            "https://support.huaweicloud.com/intl/en-us/drs/index.html",
        ],
        "ratings": {
            "scope_preview": {"status": "documented_present", "notes": "Migration flow wizard with object tree selector"},
            "mappings_filters": {"status": "documented_present", "notes": "Database, table and column level mapping rules"},
            "database_grouping": {"status": "documented_present", "notes": "Database-grouped object verification hierarchy"},
            "progress_vs_correctness": {"status": "documented_present", "notes": "Data comparison jobs run alongside or post-migration"},
            "incomplete_unsupported_states": {"status": "documented_present", "notes": "Tables without primary keys or unsupported data types flagged"},
            "diff_categories": {"status": "documented_present", "notes": "Row count difference and row detail mismatch"},
            "consistency_guarantees": {"status": "documented_present", "notes": "Dynamic comparison with CDC log compensation"},
            "next_actions": {"status": "documented_present", "notes": "Exportable repair scripts for mismatched data"},
            "structured_export": {"status": "documented_present", "notes": "REST API and downloadable comparison logs"},
            "detail_navigation": {"status": "documented_present", "notes": "Table and row-level diff drilldown in management console"},
            "unmatched_dimensions": {"status": "documented_absent", "notes": "No bidirectional multiset hash proofs or DDL invalidation barriers"},
        },
    },
}

UNMATCHED_DIMENSIONS = {
    "bidirectional_drift_proof": {
        "name": "Bidirectional Drift Proof",
        "description": "Continuous mathematical multiset hash proof proving zero drift in live bidirectional replication without stopping writes.",
        "barrier_type": "mathematical_proof",
        "commercial_status": "absent_in_commercial_tools",
    },
    "schema_invariant_barriers": {
        "name": "Schema Invariant Barriers",
        "description": "Topological dependency DAG and cycle barrier verification preventing FK violations and unsafe cutover ordering.",
        "barrier_type": "verified_barrier",
        "commercial_status": "absent_in_commercial_tools",
    },
    "replication_artifact_partitioning": {
        "name": "Replication Artifact Partitioning",
        "description": "Strict isolation of mover bookkeeping artifacts (e.g. awsdms_*, debezium_*, dts_*) from application defects with enforced repair guards.",
        "barrier_type": "verified_barrier",
        "commercial_status": "absent_in_commercial_tools",
    },
    "sequence_autoinc_headroom": {
        "name": "Sequence & Auto-increment Headroom Proof",
        "description": "Verification that target sequence next values exceed source high-water marks to guarantee zero duplicate key collisions upon write cutover.",
        "barrier_type": "mathematical_proof",
        "commercial_status": "absent_in_commercial_tools",
    },
    "routine_event_semantic_parity": {
        "name": "Routine & Event Semantic Parity",
        "description": "Dialect-aware semantic and execution compatibility checks for procedures, functions, triggers, and scheduled events.",
        "barrier_type": "verified_barrier",
        "commercial_status": "absent_in_commercial_tools",
    },
    "runtime_parameter_invariants": {
        "name": "Runtime Parameter & Collation Invariants",
        "description": "Cross-engine verification of timezones, character encodings, collations, and transaction isolation levels.",
        "barrier_type": "heuristic_warning",
        "commercial_status": "absent_in_commercial_tools",
    },
    "proof_invalidation_ddl_boundary": {
        "name": "Proof Invalidation on DDL Boundary",
        "description": "Detection of schema alterations during verification runs, invalidating prior checksums and preventing false-positive pass verdicts.",
        "barrier_type": "verified_barrier",
        "commercial_status": "absent_in_commercial_tools",
    },
}


@dataclass
class PracticalProblem:
    object_name: str
    problem: str
    source_state: str = ""
    target_state: str = ""
    impact: str = ""
    defect_class: str = "application_defect"
    is_replication_artifact: bool = False
    artifact_origin: str = ""
    category: str = "data"
    next_action: str = ""
    check: str = ""
    status: str = "diff"
    severity: str = "error"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DatabaseSummary:
    database: str
    status: str
    checks_completed: int
    checks_planned: int
    issues_count: int
    practical_problems: List[PracticalProblem] = field(default_factory=list)
    remaining_work: List[str] = field(default_factory=list)
    categories: Dict[str, int] = field(default_factory=dict)
    unmatched_findings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "status": self.status,
            "checks_completed": self.checks_completed,
            "checks_planned": self.checks_planned,
            "issues_count": self.issues_count,
            "practical_problems": [p.to_dict() for p in self.practical_problems],
            "remaining_work": self.remaining_work,
            "categories": self.categories,
            "unmatched_findings": self.unmatched_findings,
        }


@dataclass
class GroupedResultEnvelope:
    status: str
    has_differences: bool
    databases: Dict[str, DatabaseSummary]
    databases_with_issues: List[str]
    databases_clean: List[str]
    databases_incomplete: List[str]
    total_checks: int
    total_passed: int
    total_issues: int
    scope: Dict[str, Any] = field(default_factory=dict)
    coverage: Dict[str, Any] = field(default_factory=dict)
    observation_window: Dict[str, Any] = field(default_factory=dict)
    unmatched_dimensions: List[Dict[str, Any]] = field(default_factory=list)
    format_version: str = "2.0-result-scope"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format_version": self.format_version,
            "status": self.status,
            "has_differences": self.has_differences,
            "databases": {k: v.to_dict() for k, v in self.databases.items()},
            "databases_with_issues": self.databases_with_issues,
            "databases_clean": self.databases_clean,
            "databases_incomplete": self.databases_incomplete,
            "total_checks": self.total_checks,
            "total_passed": self.total_passed,
            "total_issues": self.total_issues,
            "scope": self.scope,
            "coverage": self.coverage,
            "observation_window": self.observation_window,
            "unmatched_dimensions": self.unmatched_dimensions,
        }


def _db_name_of(scope_str: str) -> str:
    s = str(scope_str or "").strip()
    if not s:
        return "global"
    parts = s.split()
    return parts[0].split(".")[0] if parts else "global"


def _assess_impact(category: str, check: str, detail: str, is_artifact: bool) -> str:
    if is_artifact:
        return "Replication engine leftover; benign for application queries but blocks clean schema validation."
    cat = (category or "").lower()
    chk = (check or "").lower()
    det = (detail or "").lower()
    if "barrier" in det or "barrier" in chk or "foreign key" in det:
        return "Foreign key dependency ordering violated; concurrent writes may fail constraint checks."
    if "drift" in det or "drift" in chk or "bidirectional" in det or "bidirectional" in chk:
        return "Live writes diverted or lost between active database nodes; data convergence violated."
    if "autoinc" in chk or "autoinc" in cat or "sequence" in det:
        return "Target sequence/autoinc is behind source; cutover will produce duplicate primary key errors."
    if "schema" in chk or "schema" in cat:
        return "Schema or object mismatch; queries targeting missing columns, constraints, or tables will fail."
    if "data" in chk or "counts" in chk:
        return "Row data differs; business operations will observe missing or corrupt records."
    return "Verification finding requires operator attention before cutover."


def _suggest_next_action(check: str, status: str, fix_hint: str, is_artifact: bool) -> str:
    if is_artifact:
        return "Isolate or ignore replication table from application repair commands."
    if fix_hint:
        return fix_hint
    chk = (check or "").lower()
    if "autoinc" in chk:
        return "Advance target sequence/auto-increment value beyond source maximum."
    if "counts" in chk or "data" in chk:
        return "Execute repair sync on affected tables (`migkit repair`)."
    if "schema" in chk:
        return "Apply missing DDL constraints or indexes on target."
    return "Inspect detailed evidence and rerun check."


def format_unmatched_dimension(finding: Dict[str, Any]) -> Dict[str, Any]:
    dim_key = finding.get("dimension") or "bidirectional_drift_proof"
    spec = UNMATCHED_DIMENSIONS.get(dim_key, {
        "name": dim_key.replace("_", " ").title(),
        "barrier_type": "verified_barrier",
        "description": "Domain dimension without commercial tool equivalent.",
    })
    return {
        "dimension": dim_key,
        "name": spec.get("name", dim_key),
        "barrier_type": spec.get("barrier_type", "verified_barrier"),
        "database": finding.get("database", "global"),
        "affected_object": finding.get("affected_object") or finding.get("scope", "global"),
        "short_problem": finding.get("short_problem") or finding.get("detail", "Unmatched invariant check"),
        "source_state": str(finding.get("source_state", "")),
        "target_state": str(finding.get("target_state", "")),
        "impact": finding.get("impact") or _assess_impact("unmatched", dim_key, finding.get("detail", ""), False),
        "verdict_certainty": finding.get("verdict_certainty", spec.get("barrier_type", "verified_barrier")),
        "next_action": finding.get("next_action") or "Review dimension barrier requirements before cutover.",
        "barrier_status": finding.get("barrier_status", "passed" if finding.get("status") in ("ok", "same") else "violated"),
    }


def build_grouped_summary(
    records: List[Dict[str, Any]],
    hop: Any = None,
    coverage: Optional[Dict[str, Any]] = None,
    observation_window: Optional[Dict[str, Any]] = None,
) -> GroupedResultEnvelope:
    from . import leftovers, verdict

    db_records: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        db = _db_name_of(r.get("scope", ""))
        db_records.setdefault(db, []).append(r)

    db_summaries: Dict[str, DatabaseSummary] = {}
    databases_with_issues: List[str] = []
    databases_clean: List[str] = []
    databases_incomplete: List[str] = []
    all_unmatched_findings: List[Dict[str, Any]] = []

    total_checks = 0
    total_passed = 0
    total_issues = 0

    all_checks_observed = sorted({r.get("check") for r in records if r.get("check")})
    if coverage and coverage.get("checks"):
        planned_checks = coverage["checks"]
    elif coverage and coverage.get("only"):
        planned_checks = coverage["only"]
    else:
        planned_checks = all_checks_observed or CHECK_ORDER

    for db, recs in db_records.items():
        checks_completed_set = {r.get("check") for r in recs if r.get("check")}
        checks_completed = len(checks_completed_set)
        checks_planned = len(set(planned_checks))

        problems: List[PracticalProblem] = []
        remaining_checks = sorted(set(planned_checks) - checks_completed_set)
        cat_counts: Dict[str, int] = {}
        unmatched_in_db: List[Dict[str, Any]] = []

        db_has_diff = False
        db_has_error = False
        db_incomplete = bool(remaining_checks)

        for r in recs:
            st = r.get("status", "unknown")
            cat = r.get("category", "data")
            chk = r.get("check", "unknown")
            sc = str(r.get("scope", ""))
            det = str(r.get("detail", ""))
            total_checks += 1

            dim_key = r.get("dimension")
            if not dim_key:
                for k in UNMATCHED_DIMENSIONS:
                    if k in chk.lower() or k in cat.lower() or k in det.lower():
                        dim_key = k
                        break

            if dim_key:
                norm_unmatched = format_unmatched_dimension({
                    "dimension": dim_key,
                    "database": db,
                    "affected_object": sc,
                    "short_problem": det,
                    "status": st,
                    "source_state": r.get("source_state", ""),
                    "target_state": r.get("target_state", ""),
                    "detail": det,
                })
                unmatched_in_db.append(norm_unmatched)
                all_unmatched_findings.append(norm_unmatched)

            if st in ("ok", "same"):
                total_passed += 1
                continue

            if st == "diff":
                db_has_diff = True
            elif st == "error":
                db_has_error = True
            elif st in ("skip", "warn", "incomplete"):
                db_incomplete = True

            total_issues += 1
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

            target_ident = sc.replace("/", ".").split()[-1] if sc else ""
            tbl = target_ident.split(".")[-1] if target_ident else ""
            sch = target_ident.split(".")[-2] if len(target_ident.split(".")) > 1 else None
            origin = leftovers.artifact_origin(tbl, sch)
            is_artifact = bool(origin)

            prob = PracticalProblem(
                object_name=sc or db,
                problem=det or f"{chk} {st}",
                source_state=str(r.get("source_state", "")),
                target_state=str(r.get("target_state", "")),
                impact=_assess_impact(cat, chk, det, is_artifact),
                defect_class="replication_artifact" if is_artifact else "application_defect",
                is_replication_artifact=is_artifact,
                artifact_origin=origin or "",
                category=cat,
                next_action=_suggest_next_action(chk, st, r.get("fix_hint", ""), is_artifact),
                check=chk,
                status=st,
                severity="error" if st not in ("diff", "warn", "skip") else st,
            )
            problems.append(prob)

        if db_has_error:
            db_status = "error"
            databases_with_issues.append(db)
        elif db_has_diff:
            db_status = "different"
            databases_with_issues.append(db)
        elif db_incomplete:
            db_status = "incomplete"
            databases_incomplete.append(db)
        else:
            db_status = "same"
            databases_clean.append(db)

        db_summaries[db] = DatabaseSummary(
            database=db,
            status=db_status,
            checks_completed=checks_completed,
            checks_planned=checks_planned,
            issues_count=len(problems),
            practical_problems=problems,
            remaining_work=remaining_checks,
            categories=cat_counts,
            unmatched_findings=unmatched_in_db,
        )

    if any(s.status == "error" for s in db_summaries.values()):
        overall_status = "error"
    elif any(s.status == "different" for s in db_summaries.values()):
        overall_status = "different"
    elif any(s.status == "incomplete" for s in db_summaries.values()):
        overall_status = "incomplete"
    else:
        overall_status = "same"

    hop_scope_data = {}
    if hop is not None:
        try:
            hop_scope_data = verdict.hop_scope(hop)
        except Exception:
            hop_scope_data = {}

    return GroupedResultEnvelope(
        status=overall_status,
        has_differences=bool(databases_with_issues),
        databases=db_summaries,
        databases_with_issues=databases_with_issues,
        databases_clean=databases_clean,
        databases_incomplete=databases_incomplete,
        total_checks=total_checks,
        total_passed=total_passed,
        total_issues=total_issues,
        scope=hop_scope_data,
        coverage=coverage or {},
        observation_window=observation_window or {},
        unmatched_dimensions=all_unmatched_findings,
    )


def format_console(envelope: GroupedResultEnvelope) -> str:
    lines = []
    lines.append("=" * 80)
    lines.append(f"MIGRATION RESULT SUMMARY: {envelope.status.upper()} "
                 f"({envelope.total_passed}/{envelope.total_checks} checks passed, {envelope.total_issues} issues)")
    lines.append("=" * 80)

    lines.append(f"{'DATABASE':<20} {'CHECKS':<12} {'STATUS':<14} {'ISSUES':<8} {'PRACTICAL SUMMARY'}")
    lines.append("-" * 80)

    for db_name, summary in envelope.databases.items():
        checks_str = f"{summary.checks_completed}/{summary.checks_planned}"
        if summary.practical_problems:
            first_prob = summary.practical_problems[0]
            desc = first_prob.problem
            if first_prob.is_replication_artifact:
                desc = f"[{first_prob.artifact_origin} artifact] {desc}"
        elif summary.remaining_work:
            desc = f"Remaining checks: {', '.join(summary.remaining_work)}"
        else:
            desc = "All checks verified clean"
        if len(desc) > 35:
            desc = desc[:32] + "..."
        lines.append(f"{db_name:<20} {checks_str:<12} {summary.status.upper():<14} {summary.issues_count:<8} {desc}")

    if envelope.databases_clean:
        lines.append("")
        lines.append(f"Clean databases ({len(envelope.databases_clean)}): {', '.join(envelope.databases_clean)}")

    if envelope.databases_with_issues:
        lines.append("")
        lines.append("-" * 80)
        lines.append("ACTIONABLE ISSUES & IMPACT:")
        for db_name in envelope.databases_with_issues:
            summary = envelope.databases[db_name]
            lines.append(f"Database: {db_name} [{summary.status.upper()}]")
            for p in summary.practical_problems:
                badge = f" ({p.artifact_origin} artifact)" if p.is_replication_artifact else ""
                lines.append(f"  * [{p.severity.upper()}] {p.object_name}{badge}: {p.problem}")
                if p.impact:
                    lines.append(f"    Impact: {p.impact}")
                if p.next_action:
                    lines.append(f"    Action: {p.next_action}")

    if envelope.unmatched_dimensions:
        lines.append("")
        lines.append("-" * 80)
        lines.append("DIMENSIONS WITHOUT COMMERCIAL EQUIVALENT:")
        for u in envelope.unmatched_dimensions:
            lines.append(f"  * {u['name']} [{u['barrier_status'].upper()}]: {u['database']} - {u['affected_object']}")
            lines.append(f"    Barrier Type: {u['barrier_type']} | Certainty: {u['verdict_certainty']}")
            lines.append(f"    Impact: {u['impact']}")
            lines.append(f"    Action: {u['next_action']}")

    lines.append("=" * 80)
    return "\n".join(lines)


def format_html(envelope: GroupedResultEnvelope) -> str:
    db_rows = []
    for db_name, summary in envelope.databases.items():
        status_class = "ok" if summary.status == "same" else ("diff" if summary.status == "different" else "error")
        checks_str = f"{summary.checks_completed}/{summary.checks_planned}"
        if summary.practical_problems:
            first_p = summary.practical_problems[0]
            summary_desc = html.escape(first_p.problem)
            if first_p.is_replication_artifact:
                summary_desc = f'<span class="fix" style="font-size:11px;margin:0 4px 0 0;">{html.escape(first_p.artifact_origin)} artifact</span>' + summary_desc
        elif summary.remaining_work:
            summary_desc = f"Remaining: {html.escape(', '.join(summary.remaining_work))}"
        else:
            summary_desc = '<span style="color:#0ca30c;">All checks verified clean</span>'

        db_rows.append(
            f'<tr><td class="db"><strong>{html.escape(db_name)}</strong></td>'
            f'<td>{checks_str}</td>'
            f'<td><span class="chip {status_class}"><span class="dot {status_class}"></span>{summary.status.upper()}</span></td>'
            f'<td>{summary.issues_count}</td>'
            f'<td>{summary_desc}</td></tr>'
        )

    unmatched_cards = []
    for u in envelope.unmatched_dimensions:
        status_color = "#0ca30c" if u["barrier_status"] == "passed" else "#d03b3b"
        unmatched_cards.append(
            f'<div class="finding" style="border-left-color:{status_color};">'
            f'<div class="head"><strong>{html.escape(u["name"])}</strong> &middot; '
            f'<span style="font-family:ui-monospace,monospace;font-size:12px;">{html.escape(u["database"])} / {html.escape(u["affected_object"])}</span>'
            f'<span class="fix" style="margin:0 0 0 8px;padding:2px 6px;font-size:11px;">{html.escape(u["barrier_type"])}</span></div>'
            f'<div class="what">{html.escape(u["short_problem"])}</div>'
            f'<div style="font-size:12.5px;color:#52514e;margin-top:6px;"><strong>Impact:</strong> {html.escape(u["impact"])}</div>'
            f'<div class="fix" style="margin-top:6px;"><strong>Next action:</strong> {html.escape(u["next_action"])}</div>'
            f'</div>'
        )

    unmatched_section = ""
    if unmatched_cards:
        unmatched_section = (
            f'<div class="card"><h2>dimensions without commercial equivalent (unmatched)</h2>'
            f'<div class="sub" style="margin-bottom:12px;">Invariants and mathematical proofs unique to migkit architecture.</div>'
            f'{"".join(unmatched_cards)}</div>'
        )

    return f"""<div class="card"><h2>database summary (database-first grouping)</h2>
<table><thead><tr><th>database</th><th>checks (comp/plan)</th><th>status</th><th>issues</th><th>practical summary</th></tr></thead>
<tbody>{"".join(db_rows)}</tbody></table></div>
{unmatched_section}"""


def get_commercial_inventory() -> Dict[str, Any]:
    return COMMERCIAL_FEATURE_INVENTORY


def format_commercial_comparison_console(selected_products: Optional[List[str]] = None) -> str:
    inventory = COMMERCIAL_FEATURE_INVENTORY
    keys = selected_products or list(inventory.keys())

    dimensions = [
        ("scope_preview", "Scope Preview"),
        ("database_grouping", "Database Grouping"),
        ("progress_vs_correctness", "Progress vs Correctness"),
        ("incomplete_unsupported_states", "Incomplete States"),
        ("diff_categories", "Diff Categorization"),
        ("consistency_guarantees", "Consistency Proofs"),
        ("next_actions", "Actionable Fix Hints"),
        ("structured_export", "Structured Export"),
        ("unmatched_dimensions", "Unmatched Invariants"),
    ]

    lines = []
    lines.append("=" * 100)
    lines.append("COMMERCIAL COMPARISON INVENTORY: migkit vs Commercial DMS / DTS Products")
    lines.append("=" * 100)
    header = f"{'DIMENSION':<26} {'migkit':<14}" + "".join(f"{inventory[k]['product'][:14]:<16}" for k in keys[:4])
    lines.append(header)
    lines.append("-" * 100)

    for dim_key, dim_name in dimensions:
        row = f"{dim_name:<26} {'YES (proven)':<14}"
        for k in keys[:4]:
            rating = inventory[k]["ratings"].get(dim_key, {})
            st = rating.get("status", "unknown")
            disp = "YES" if st == "documented_present" else ("NO" if st == "documented_absent" else "N/A")
            row += f"{disp:<16}"
        lines.append(row)

    lines.append("=" * 100)
    return "\n".join(lines)


def format_commercial_comparison_html(selected_products: Optional[List[str]] = None) -> str:
    inventory = COMMERCIAL_FEATURE_INVENTORY
    keys = selected_products or list(inventory.keys())

    dimensions = [
        ("scope_preview", "Scope Preview & Mapping"),
        ("database_grouping", "Database-Grouped Summary"),
        ("progress_vs_correctness", "Progress vs Correctness"),
        ("incomplete_unsupported_states", "Explicit Incomplete States"),
        ("diff_categories", "Diff & Defect Categorization"),
        ("consistency_guarantees", "Consistency Guarantees / Proofs"),
        ("next_actions", "Actionable Next Actions / Fix Hints"),
        ("structured_export", "Versioned Structured JSON API"),
        ("unmatched_dimensions", "Unmatched Dimensions (Drift/Barriers)"),
    ]

    head_cols = "".join(f"<th>{html.escape(inventory[k]['product'])}</th>" for k in keys)
    rows = []
    for dim_key, dim_name in dimensions:
        cells = []
        for k in keys:
            rating = inventory[k]["ratings"].get(dim_key, {})
            st = rating.get("status", "unknown")
            notes = rating.get("notes", "")
            if st == "documented_present":
                badge = f'<span class="chip ok" title="{html.escape(notes)}"><span class="dot ok"></span>YES</span>'
            elif st == "documented_absent":
                badge = f'<span class="chip error" title="{html.escape(notes)}"><span class="dot error"></span>NO</span>'
            else:
                badge = f'<span class="chip skip" title="{html.escape(notes)}"><span class="dot skip"></span>UNAVAIL</span>'
            cells.append(f"<td>{badge}</td>")
        rows.append(f'<tr><td><strong>{html.escape(dim_name)}</strong></td>'
                    f'<td><span class="chip ok"><span class="dot ok"></span>YES (native)</span></td>'
                    f'{"".join(cells)}</tr>')

    return f"""<div class="card"><details><summary><strong>Commercial feature comparison (migkit vs commercial DTS/DMS)</strong></summary>
<div style="margin-top:12px;overflow-x:auto;">
<table><thead><tr><th>Capability Dimension</th><th>migkit</th>{head_cols}</tr></thead>
<tbody>{"".join(rows)}</tbody></table></div></details></div>"""
