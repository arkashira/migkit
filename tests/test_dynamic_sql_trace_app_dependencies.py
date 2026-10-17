"""Tests for dynamic SQL tracer and external application dependencies (backlog RCH21, row 629).

Extends AST scanning with bounded configuration-aware constant propagation
and sandbox/runtime traces where approved.
Tracks ORM mappings, prepared statements, generated SQL, report definitions,
and external jobs as unresolved or discovered dependencies.
Never treats heuristic absence as proof of no use.
Ensures parameterized SQL, generated identifiers, and encrypted/unavailable code
are reflected in readiness coverage; migration waves cannot cut over a dependency
whose consumer still points at the old shape.
"""

import ast
import json
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest

from migkit.dynamic_sql_tracer import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    AppDependency,
    BoundedConstantPropagator,
    CallSite,
    CareBadge,
    CutoverGateVerdict,
    DependencySourceKind,
    DynamicSqlASTScanner,
    DynamicSqlTracerRegistry,
    MigrationWave,
    ORMMappingParser,
    ReadinessCoverage,
    ReportDefinitionParser,
    ResolutionState,
    RuntimeTrace,
    SchemaContract,
    TableShape,
    extract_parameter_markers,
    extract_sql_columns,
    extract_sql_tables,
    CorruptCheckpointError,
    CorruptLockError,
    DynamicSQLParseError,
    decide_dynamic_sql_dependency,
    dynamic_sql_dependency_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "verified", "holds"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "verified", "refuses"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "fragment_parameterization_review", "holds"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "fragment_parameterization_review", "refuses"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "dynamic_target_review", "holds"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "dynamic_target_review", "refuses"),
    ("dynamic_sql_tracer", "dynamic_sql_dependency_rungs", "unresolved_dependency_action", "holds"),
]



@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test_dyn_sql_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_bounded_constant_propagation_strings_and_depth():
    config = {
        "TABLE_PREFIX": "app_",
        "SCHEMA": "finance",
        "nested": {"table": "ledger"},
    }
    prop = BoundedConstantPropagator(config=config, max_depth=5, max_steps=50)

    # Simple variable assignment and concatenation
    prop.set_variable("base_table", "invoices")
    code = "TABLE_PREFIX + base_table"
    node = compile(code, "<string>", "eval", ast.PyCF_ONLY_AST).body
    val, is_dyn, blockers = prop.evaluate_node(node)
    assert is_dyn is False
    assert val == "app_invoices"
    assert len(blockers) == 0

    # Subscript config lookup
    code2 = "nested['table']"
    node2 = compile(code2, "<string>", "eval", ast.PyCF_ONLY_AST).body
    val2, is_dyn2, blockers2 = prop.evaluate_node(node2)
    assert is_dyn2 is False
    assert val2 == "ledger"

    # Dynamic identifier not in scope
    code3 = "TABLE_PREFIX + unknown_var"
    node3 = compile(code3, "<string>", "eval", ast.PyCF_ONLY_AST).body
    val3, is_dyn3, blockers3 = prop.evaluate_node(node3)
    assert is_dyn3 is True
    assert "unknown_var" in blockers3
    assert val3 == "app_"

    # Depth bound check
    prop_shallow = BoundedConstantPropagator(config={}, max_depth=1, max_steps=10)
    code_deep = "('a' + 'b') + ('c' + 'd')"
    node_deep = compile(code_deep, "<string>", "eval", ast.PyCF_ONLY_AST).body
    val_deep, is_dyn_deep, blockers_deep = prop_shallow.evaluate_node(node_deep)
    assert is_dyn_deep is True
    assert "DEPTH_OR_STEP_LIMIT_EXCEEDED" in blockers_deep


def test_ast_scanning_fstrings_and_format_calls():
    config = {"ENV_PREFIX": "prod_"}
    scanner = DynamicSqlASTScanner(config=config)

    code = """
def load_orders(cursor, tenant_id):
    tbl_name = "orders"
    # Resolved f-string with constant
    cursor.execute(f"SELECT id, amount FROM {tbl_name} WHERE tenant_id = %s", (tenant_id,))

    # Format call with config
    cursor.execute("SELECT * FROM {}{}".format(ENV_PREFIX, "customers"))

    # Dynamic query with runtime parameter
    dyn_table = get_dynamic_table(tenant_id)
    cursor.execute(f"SELECT * FROM {dyn_table}")
"""
    deps = scanner.scan_code(code, file_path="orders_service.py", consumer_name="order_service")
    assert len(deps) == 3

    # First query: resolved f-string, parameterized
    d1 = deps[0]
    assert d1.resolution_state == ResolutionState.RESOLVED_CONSTANT_PROPAGATION
    assert "orders" in d1.accessed_tables
    assert d1.is_parameterized is True
    assert "%s" in d1.parameter_markers
    assert d1.badge == CareBadge.PASS

    # Second query: format call with config
    d2 = deps[1]
    assert d2.resolution_state == ResolutionState.RESOLVED_CONSTANT_PROPAGATION
    assert "prod_customers" in d2.accessed_tables
    assert d2.badge == CareBadge.PASS

    # Third query: dynamic table
    d3 = deps[2]
    assert d3.resolution_state == ResolutionState.UNRESOLVED_DYNAMIC
    assert len(d3.blocking_identifiers) > 0
    assert d3.badge == CareBadge.ACTION


def test_orm_mapping_parser_sqlalchemy_and_django():
    models_code = """
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Order(Base):
    __tablename__ = 'public.orders'
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer)
    status = Column(String)

class LegacyAccount:
    class Meta:
        db_table = 'legacy_accounts'
    account_number = Column(String)
"""
    deps = ORMMappingParser.parse_python_models(models_code, file_path="models.py", consumer_name="billing_orm")
    assert len(deps) == 2

    # SQLAlchemy
    d_order = [d for d in deps if "orders" in d.raw_query_or_expression][0]
    assert d_order.source_kind == DependencySourceKind.ORM_MAPPING
    assert "public.orders" in d_order.accessed_tables
    assert "id" in d_order.accessed_columns
    assert "customer_id" in d_order.accessed_columns
    assert d_order.badge == CareBadge.PASS

    # Django
    d_acc = [d for d in deps if "legacy_accounts" in d.raw_query_or_expression][0]
    assert "legacy_accounts" in d_acc.accessed_tables
    assert d_acc.badge == CareBadge.PASS


def test_orm_mapping_parser_jpa_annotations():
    java_code = """
package com.example.model;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Column;

@Entity
@Table(name = "customers_v2")
public class Customer {
    @Column(name = "customer_id")
    private Long id;

    @Column(name = "email_address")
    private String email;
}
"""
    deps = ORMMappingParser.parse_jpa_annotations(java_code, file_path="Customer.java", consumer_name="java_service")
    assert len(deps) == 1
    dep = deps[0]
    assert dep.source_kind == DependencySourceKind.ORM_MAPPING
    assert "customers_v2" in dep.accessed_tables
    assert "customer_id" in dep.accessed_columns
    assert "email_address" in dep.accessed_columns
    assert dep.resolution_state == ResolutionState.RESOLVED_STATIC


def test_prepared_statements_tracking():
    registry = DynamicSqlTracerRegistry()
    dep = registry.register_prepared_statement(
        stmt_name="find_active_subscriptions",
        sql_template="SELECT sub_id, plan_code FROM subscriptions WHERE user_id = $1 AND status = $2",
        consumer_name="subscription_worker",
        file_path="worker.go",
        line_number=42,
    )
    assert dep.is_parameterized is True
    assert dep.parameter_markers == ["$1", "$2"]
    assert "subscriptions" in dep.accessed_tables
    assert "sub_id" in dep.accessed_columns
    assert dep.badge == CareBadge.PASS


def test_generated_sql_builder_tracking():
    registry = DynamicSqlTracerRegistry()
    dep = registry.register_generated_sql(
        generator_name="dynamic_filter_builder",
        builder_expression="QueryBuilder('audit_logs').where_in('action', actions).filter_date(range)",
        target_tables=["audit_logs"],
        target_columns=["action", "created_at"],
        is_dynamic=True,
        consumer_name="audit_reporter",
    )
    assert dep.resolution_state == ResolutionState.PARTIALLY_RESOLVED
    assert dep.badge == CareBadge.REVIEW
    assert "audit_logs" in dep.accessed_tables


def test_report_definitions_bi_parsing():
    json_config = """
    [
        {
            "report_id": "monthly_revenue",
            "sql": "SELECT SUM(amount) AS total, currency FROM payments WHERE payment_date >= :start_date GROUP BY currency",
            "dataset": "payments"
        },
        {
            "report_id": "user_signups",
            "sql": "SELECT user_id, signup_time FROM users WHERE signup_time > '2026-01-01'",
            "dataset": "users"
        }
    ]
    """
    deps = ReportDefinitionParser.parse_report_config(json_config, consumer_name="metabase")
    assert len(deps) == 2
    d1 = deps[0]
    assert "payments" in d1.accessed_tables
    assert d1.is_parameterized is True
    assert ":start_date" in d1.parameter_markers
    assert d1.badge == CareBadge.PASS

    d2 = deps[1]
    assert "users" in d2.accessed_tables
    assert d2.badge == CareBadge.PASS


def test_external_jobs_and_airflow_queries():
    registry = DynamicSqlTracerRegistry()
    dep = registry.register_external_job(
        job_name="daily_settlement_dag",
        job_schedule="0 2 * * *",
        sql_or_commands="INSERT INTO settlements SELECT * FROM pending_transfers WHERE processed = 0",
        target_tables=["settlements", "pending_transfers"],
        consumer_name="airflow_scheduler",
    )
    assert dep.source_kind == DependencySourceKind.EXTERNAL_JOB
    assert "settlements" in dep.accessed_tables
    assert "pending_transfers" in dep.accessed_tables
    assert dep.badge == CareBadge.PASS


def test_opaque_and_encrypted_code_tracking():
    registry = DynamicSqlTracerRegistry()
    dep = registry.register_opaque_or_encrypted_code(
        module_identifier="liblegacy_payroll.so",
        reason="proprietary C binary without source code; access logs indicate database calls",
        suspected_tables=["payroll_ledger"],
        consumer_name="legacy_payroll",
    )
    assert dep.resolution_state == ResolutionState.UNAVAILABLE_SOURCE
    assert dep.badge == CareBadge.ACTION
    assert "SOURCE_CODE_UNAVAILABLE" in dep.blocking_identifiers

    # Verify coverage flags this as opaque code blocker
    cov = registry.compute_readiness_coverage()
    assert cov.opaque_code_count == 1
    assert cov.badge == CareBadge.ACTION
    assert any("Opaque/encrypted" in b for b in cov.blockers)


def test_runtime_trace_sandbox_approved_vs_unapproved():
    registry = DynamicSqlTracerRegistry()

    # Register an unresolved dynamic query
    app_code = """
def dynamic_worker(conn, target_name):
    # Dynamic table name
    conn.execute("SELECT * FROM " + target_name)
"""
    deps = registry.register_python_code(app_code, file_path="dynamic_worker.py", consumer_name="worker")
    dep = deps[0]
    assert dep.resolution_state == ResolutionState.UNRESOLVED_DYNAMIC
    assert dep.badge == CareBadge.ACTION

    # Ingest unapproved trace
    trace_unapproved = RuntimeTrace(
        trace_id="trace_001",
        call_site=CallSite(file_path="dynamic_worker.py", line_number=4),
        concrete_sql="SELECT * FROM tenant_partition_2026",
        accessed_tables=["tenant_partition_2026"],
        accessed_columns=["id", "val"],
        approved=False,
        environment="sandbox",
    )
    registry.ingest_runtime_trace(trace_unapproved)

    # With unapproved trace, badge is REVIEW, not PASS, and state is PARTIALLY_RESOLVED
    assert dep.associated_trace_id == "trace_001"
    assert dep.resolution_state == ResolutionState.PARTIALLY_RESOLVED
    assert dep.badge == CareBadge.REVIEW

    cov = registry.compute_readiness_coverage()
    assert cov.unapproved_trace_count == 1
    assert cov.badge == CareBadge.REVIEW

    # Approve the trace
    ok = registry.approve_runtime_trace("trace_001", approved_by="sre_team")
    assert ok is True
    assert dep.resolution_state == ResolutionState.RESOLVED_APPROVED_TRACE
    assert dep.resolved_sql == "SELECT * FROM tenant_partition_2026"
    assert "tenant_partition_2026" in dep.accessed_tables
    assert dep.badge == CareBadge.PASS

    cov_after = registry.compute_readiness_coverage()
    assert cov_after.approved_trace_count == 1
    assert cov_after.badge == CareBadge.PASS
    assert cov_after.coverage_pct == 100.0


def test_schema_contract_old_shape_detection():
    contract = SchemaContract()
    # Current shape has table 'orders' with column 'status_code'
    contract.add_current_shape(TableShape(
        table_name="orders",
        schema_name="public",
        columns={"id": "bigint", "status_code": "varchar", "created_at": "timestamp"},
    ))
    # Target schema dropped 'status_code' and renamed 'orders' to 'orders_v2'
    contract.record_table_rename("orders", "orders_v2")
    contract.record_dropped_column("orders", "status_code")

    is_old, reasons = contract.evaluate_consumer_compatibility(
        accessed_tables=["orders"],
        accessed_columns=["status_code"],
        raw_query="SELECT status_code FROM orders WHERE id = 1",
    )
    assert is_old is True
    assert any("renamed to 'orders_v2'" in r for r in reasons)
    assert any("dropped column 'status_code'" in r for r in reasons)

    # Compatible consumer pointing at target shape
    is_old_clean, reasons_clean = contract.evaluate_consumer_compatibility(
        accessed_tables=["orders_v2"],
        accessed_columns=["id", "created_at"],
        raw_query="SELECT id, created_at FROM orders_v2",
    )
    assert is_old_clean is False
    assert len(reasons_clean) == 0


def test_migration_wave_cutover_gate_blocks_on_old_shape():
    contract = SchemaContract()
    contract.record_table_rename("legacy_billing", "billing_v2")
    contract.record_dropped_column("billing_v2", "tax_ssn")

    registry = DynamicSqlTracerRegistry(schema_contract=contract)

    # Consumer 1: updated to target shape
    registry.add_dependency(AppDependency(
        dependency_id="dep_clean_service",
        source_kind=DependencySourceKind.AST_PYTHON,
        consumer_name="clean_service",
        raw_query_or_expression="SELECT id FROM billing_v2",
        accessed_tables=["billing_v2"],
        accessed_columns=["id"],
        resolution_state=ResolutionState.RESOLVED_STATIC,
        badge=CareBadge.PASS,
    ))

    # Consumer 2: still pointing at old table name
    registry.add_dependency(AppDependency(
        dependency_id="dep_stale_service",
        source_kind=DependencySourceKind.AST_PYTHON,
        consumer_name="stale_service",
        raw_query_or_expression="SELECT id FROM legacy_billing",
        accessed_tables=["legacy_billing"],
        accessed_columns=["id"],
        resolution_state=ResolutionState.RESOLVED_STATIC,
    ))

    # Migration wave for billing
    wave = MigrationWave(
        wave_id="wave_billing",
        wave_name="Billing Migration Wave",
        target_tables={"legacy_billing", "billing_v2"},
    )
    registry.register_migration_wave(wave)

    verdict = registry.validate_cutover_gate(wave)
    assert verdict.can_cutover is False
    assert verdict.badge == CareBadge.ACTION
    assert verdict.old_shape_consumer_count == 1
    assert "dep_stale_service" in verdict.blocking_dependencies
    assert any("still points at old schema shape" in r for r in verdict.reasons)

    # Check unix report
    report = verdict.format_unix_report()
    assert "[wave_billing]" in report
    assert "STATUS       : ACTION" in report
    assert "CAN CUTOVER  : NO" in report


def test_migration_wave_cutover_gate_blocks_on_unresolved_dynamic_sql():
    registry = DynamicSqlTracerRegistry()

    # Unresolved dynamic SQL touching target table
    registry.add_dependency(AppDependency(
        dependency_id="dep_dyn_sql",
        source_kind=DependencySourceKind.AST_PYTHON,
        consumer_name="reporting_service",
        raw_query_or_expression="cursor.execute('SELECT * FROM ' + dynamic_name)",
        accessed_tables=["public.payments"],
        resolution_state=ResolutionState.UNRESOLVED_DYNAMIC,
        blocking_identifiers=["dynamic_name"],
        badge=CareBadge.ACTION,
    ))

    wave = MigrationWave(
        wave_id="wave_payments",
        wave_name="Payments Migration",
        target_tables={"public.payments"},
    )
    verdict = registry.validate_cutover_gate(wave)
    assert verdict.can_cutover is False
    assert verdict.badge == CareBadge.ACTION
    assert verdict.unresolved_dynamic_count == 1
    assert "dep_dyn_sql" in verdict.blocking_dependencies


def test_migration_wave_cutover_gate_passes_when_all_consumers_clean():
    contract = SchemaContract()
    contract.add_target_shape(TableShape(
        table_name="inventory",
        schema_name="public",
        columns={"item_id": "uuid", "quantity": "integer"},
    ))
    registry = DynamicSqlTracerRegistry(schema_contract=contract)

    # Clean static dependency
    registry.add_dependency(AppDependency(
        dependency_id="dep_inv_read",
        source_kind=DependencySourceKind.AST_PYTHON,
        consumer_name="inventory_api",
        raw_query_or_expression="SELECT item_id, quantity FROM inventory",
        accessed_tables=["inventory"],
        accessed_columns=["item_id", "quantity"],
        resolution_state=ResolutionState.RESOLVED_STATIC,
        badge=CareBadge.PASS,
    ))

    # Clean prepared statement
    registry.register_prepared_statement(
        stmt_name="update_inv",
        sql_template="UPDATE inventory SET quantity = quantity - $1 WHERE item_id = $2",
        consumer_name="checkout_service",
    )

    wave = MigrationWave(
        wave_id="wave_inventory",
        wave_name="Inventory Table Wave",
        target_tables={"inventory"},
    )

    verdict = registry.validate_cutover_gate(wave)
    assert verdict.can_cutover is True
    assert verdict.badge == CareBadge.PASS
    assert len(verdict.blocking_dependencies) == 0
    assert verdict.total_consumers_checked == 2


def test_durable_checkpoint_atomic_save_and_restore(tmp_dir):
    registry = DynamicSqlTracerRegistry()
    registry.register_config({"ENV": "prod", "REGION": "us-east-1"})

    dep = AppDependency(
        dependency_id="dep_chk_1",
        source_kind=DependencySourceKind.PREPARED_STATEMENT,
        consumer_name="auth_service",
        raw_query_or_expression="SELECT * FROM auth_tokens WHERE token = $1",
        accessed_tables=["auth_tokens"],
        resolution_state=ResolutionState.RESOLVED_STATIC,
        is_parameterized=True,
        parameter_markers=["$1"],
        badge=CareBadge.PASS,
    )
    registry.add_dependency(dep)

    wave = MigrationWave(
        wave_id="wave_auth",
        wave_name="Auth Wave",
        target_tables={"auth_tokens"},
    )
    registry.register_migration_wave(wave)

    chk_file = tmp_dir / "tracer_checkpoint.json"
    checksum = registry.save_checkpoint(chk_file)
    assert chk_file.exists()
    assert len(checksum) == 64

    # Restore in O(1)
    restored = DynamicSqlTracerRegistry.load_checkpoint(chk_file)
    assert restored.config.get("ENV") == "prod"
    assert "dep_chk_1" in restored.dependencies
    r_dep = restored.dependencies["dep_chk_1"]
    assert r_dep.consumer_name == "auth_service"
    assert r_dep.is_parameterized is True
    assert "auth_tokens" in r_dep.accessed_tables
    assert "wave_auth" in restored.waves

    # Check corrupted checkpoint detection
    with open(chk_file, "r", encoding="ascii") as f:
        data = json.load(f)
    data["payload"]["config"]["ENV"] = "tampered"
    with open(chk_file, "w", encoding="ascii") as f:
        json.dump(data, f)

    with pytest.raises(ValueError, match="checksum mismatch"):
        DynamicSqlTracerRegistry.load_checkpoint(chk_file)


def test_active_lease_mutual_exclusion(tmp_dir):
    reg1 = DynamicSqlTracerRegistry()
    reg2 = DynamicSqlTracerRegistry()

    # reg1 acquires lease
    acquired = reg1.acquire_lease(tmp_dir, timeout_sec=10.0)
    assert acquired is True
    assert (tmp_dir / ".run.lock").exists()

    # reg2 attempts to acquire lease while active -> fails
    acquired2 = reg2.acquire_lease(tmp_dir, timeout_sec=10.0)
    assert acquired2 is False

    # reg1 releases lease
    reg1.release_lease()
    assert not (tmp_dir / ".run.lock").exists()

    # reg2 can now acquire
    acquired3 = reg2.acquire_lease(tmp_dir, timeout_sec=10.0)
    assert acquired3 is True
    reg2.release_lease()


def test_sql_extract_helpers():
    sql = """
    -- sample comment
    /* block comment */
    SELECT u.id, u.email, o.total_amount
    FROM public.users u
    JOIN public.orders o ON u.id = o.user_id
    WHERE u.active = 1 AND o.created_at >= '2026-01-01'
    """
    tables = extract_sql_tables(sql)
    assert "public.users" in tables
    assert "public.orders" in tables

    cols = extract_sql_columns(sql)
    assert any("id" in c for c in cols)
    assert any("email" in c for c in cols)

    markers = extract_parameter_markers("SELECT * FROM tbl WHERE id = ? AND name = :name AND x = $1 AND y = %(param)s")
    assert "?" in markers
    assert ":name" in markers
    assert "$1" in markers
    assert "%(param)s" in markers


def test_readiness_coverage_unix_report():
    cov = ReadinessCoverage(
        total_dependencies=10,
        resolved_count=8,
        partially_resolved_count=1,
        unresolved_count=1,
        opaque_code_count=0,
        approved_trace_count=3,
        unapproved_trace_count=1,
        old_shape_consumer_count=1,
        coverage_pct=80.0,
        badge=CareBadge.ACTION,
        blockers=["Consumer 'order_api' points at old shape", "Unresolved query in 'job_batch'"],
    )
    report = cov.format_unix_report()
    assert "=== APPLICATION DEPENDENCY READINESS COVERAGE ===" in report
    assert "STATUS BADGE : ACTION" in report
    assert "COVERAGE PCT : 80.00%" in report
    assert "TOTAL DEPENDENCIES DISCOVERED: 10" in report
    assert "[ACTION] Consumer 'order_api' points at old shape" in report

    # Verify 100% pure ASCII, zero emojis
    assert report.isascii() is True
    assert all(ord(c) < 128 for c in report)


def test_dynamic_sql_dependency_rungs_coverage():
    with watched() as seen:
        # 1. verified holds
        f1 = {
            "is_parsed": True,
            "unbound_table_count": 0,
            "has_unparameterized_fragments": False,
            "all_objects_in_current_wave": True,
            "contract_verified": True,
            "target_resolvable": True,
        }
        c1 = decide_dynamic_sql_dependency(f1)
        assert c1.rung.name == "verified"

        # 2. verified refuses, fragment_parameterization_review refuses, dynamic_target_review refuses -> unresolved_dependency_action holds
        f2 = {
            "is_parsed": True,
            "unbound_table_count": 1,
            "has_unparameterized_fragments": False,
            "all_objects_in_current_wave": True,
            "contract_verified": True,
            "target_resolvable": True,
        }
        c2 = decide_dynamic_sql_dependency(f2)
        assert c2.rung.name == "unresolved_dependency_action"

        # 3. verified refuses, fragment_parameterization_review holds
        f3 = {
            "is_parsed": True,
            "unbound_table_count": 0,
            "has_unparameterized_fragments": True,
            "all_objects_in_current_wave": True,
            "contract_verified": True,
            "target_resolvable": True,
        }
        c3 = decide_dynamic_sql_dependency(f3)
        assert c3.rung.name == "fragment_parameterization_review"

        # 4. verified refuses, fragment_parameterization_review refuses, dynamic_target_review holds
        f4 = {
            "is_parsed": True,
            "unbound_table_count": 0,
            "has_unparameterized_fragments": False,
            "all_objects_in_current_wave": True,
            "contract_verified": True,
            "target_resolvable": False,
        }
        c4 = decide_dynamic_sql_dependency(f4)
        assert c4.rung.name == "dynamic_target_review"


def test_corrupt_lock_and_checkpoint_errors(tmp_dir):
    # Corrupt lock file
    lock_file = tmp_dir / ".run.lock"
    lock_file.write_text("NOT_VALID_JSON{{{")
    reg = DynamicSqlTracerRegistry()
    with pytest.raises(CorruptLockError):
        reg.acquire_lease(tmp_dir)

    # Corrupt checkpoint
    chk_file = tmp_dir / "chk.json"
    chk_file.write_text(json.dumps({"checksum": "bad_hash", "payload": {"foo": "bar"}}))
    with pytest.raises(CorruptCheckpointError):
        DynamicSqlTracerRegistry.load_checkpoint(chk_file)

