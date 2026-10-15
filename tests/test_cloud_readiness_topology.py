import json
import pytest

from migkit.change_sheet import (
    ChangeSheet,
    ChangeTask,
    create_from_plan,
)
from migkit.upgrade import (
    _mongo,
    _mssql,
    run_upgrade,
    upgrade_goal_runner,
)
from migkit.workflow_exporter import (
    export_airflow_dag,
    export_argo_workflow,
    export_dagster_job,
    export_workflow,
)
from migkit.engines.mysql import managed_retention


# --- Item 433 (R25.2): Cloud Readiness Auditor ---

def test_change_sheet_creation_and_cutover_gating():
    sheet = create_from_plan("orders-hop", ticket_prefix="TICK")
    assert sheet.service == "orders-hop"
    assert len(sheet.tasks) > 0
    assert sheet.tasks[0].ticket_id == "TICK-01"

    # Initially not ready for cutover because tasks are pending
    ready, reasons = sheet.is_ready_for_cutover()
    assert not ready
    assert any("expected completed" in r for r in reasons)

    # Mark pre-cutover tasks completed but without evidence
    sheet.update_progress("TICK-01", "completed")
    ready, reasons = sheet.is_ready_for_cutover()
    assert not ready
    assert any("lacks evidence ID" in r for r in reasons)

    # Complete all pre-cutover tasks with evidence IDs
    for t in sheet.tasks:
        if t.phase in ("assess", "plan", "copy", "verify"):
            sheet.update_progress(t.ticket_id, "completed", evidence_id=f"EV-{t.ticket_id}-RUN01",
                                  summary="passed 100% verification")

    ready, reasons = sheet.is_ready_for_cutover()
    assert ready
    assert len(reasons) == 0

    # Exports
    md = sheet.export_markdown()
    assert "# Enterprise Change Implementation Sheet" in md
    assert "| **TICK-01** |" in md
    assert "`EV-TICK-01-RUN01`" in md

    json_text = sheet.export_json()
    parsed = json.loads(json_text)
    assert parsed["service"] == "orders-hop"
    assert len(parsed["tasks"]) == len(sheet.tasks)

    csv_text = sheet.export_csv()
    assert "change_id,ticket_id,step_id" in csv_text
    assert "TICK-01" in csv_text


# --- Item 317 (R3.topo): Multi-Node Topologies ---

def test_upgrade_mongo_deep_checks():
    class DummyClient:
        class Admin:
            def command(self, cmd):
                if "featureCompatibilityVersion" in cmd:
                    return {"featureCompatibilityVersion": {"version": "4.4"}}
                if "serverStatus" in cmd:
                    return {"storageEngine": {"name": "wiredTiger"}}
                return {}
        admin = Admin()

        def __getitem__(self, dbname):
            class Coll:
                def list_indexes(self):
                    return [{"name": "geo_idx", "key": {"loc": "geoHaystack"}}]
                def count_documents(self, q):
                    return 2
            class DB:
                def list_collection_names(self):
                    return ["places", "system.js"]
                def __getitem__(self, name):
                    return Coll()
            return DB()

    class DummyEngine:
        def _client(self, side):
            return DummyClient()
        def _d(self, side, db):
            return "testdb"

    class DummySide:
        def __init__(self, eng):
            self.eng = eng

    # Call _mongo directly with mocked engine
    import migkit.upgrade
    orig_side = migkit.upgrade._side
    migkit.upgrade._side = lambda eng, side: eng

    try:
        rows = _mongo(None, DummyEngine(), "testdb", "6.0")
        assert any(r["item"] == "compatibility" and r["level"] == "fail" for r in rows)
        assert any(r["item"] == "indexes" and r["level"] == "fail" for r in rows)
        assert any(r["item"] == "stored javascript" and r["level"] == "warn" for r in rows)
    finally:
        migkit.upgrade._side = orig_side


def test_upgrade_mssql_deep_checks():
    class DummyMssqlEngine:
        def _rows(self, side, db, query):
            q = query.lower()
            if "compatibility_level" in q:
                return [(130,)]  # SQL Server 2016
            if "sys.assemblies" in q:
                return [("CustomClrHelper", "UNSAFE")]
            if "extended_procedures" in q:
                return [("xp_custom_calc",)]
            if "sys.fulltext_catalogs" in q:
                return [("MainCatalog",)]
            if "sys.servers" in q:
                return [("REMOTE_ERP",)]
            if "sys.dm_os_performance_counters" in q:
                return []
            return []

    import migkit.upgrade
    orig_side = migkit.upgrade._side
    migkit.upgrade._side = lambda eng, side: eng

    try:
        rows = _mssql(None, DummyMssqlEngine(), "testdb", "15.0")  # SQL Server 2019
        assert any(r["item"] == "compatibility level" and r["level"] == "warn" for r in rows)
        assert any(r["item"] == "clr assemblies" and r["level"] == "warn" for r in rows)
        assert any(r["item"] == "extended stored procedures" and r["level"] == "warn" for r in rows)
        assert any(r["item"] == "full-text catalogs" and r["level"] == "pass" for r in rows)
        assert any(r["item"] == "linked servers" and r["level"] == "warn" for r in rows)
    finally:
        migkit.upgrade._side = orig_side


def test_upgrade_goal_runner():
    class DummyHop:
        engine = "postgres"
    class DummyEngine:
        databases = lambda self: ["shop"]

    import migkit.upgrade
    orig_checks = migkit.upgrade.checks
    orig_choose = migkit.upgrade.choose
    orig_plan_lines = migkit.upgrade.plan_lines

    migkit.upgrade.checks = lambda hop, eng, db, quick=False: [
        {"level": "pass", "item": "versions", "detail": "14 to 16"},
        {"level": "pass", "item": "settings", "detail": "compatible"}
    ]
    migkit.upgrade.choose = lambda hop, eng, db: "logical move"
    migkit.upgrade.plan_lines = lambda hop, eng, db: ["upgrade: logical move into version 16"]

    try:
        # Test plan_only
        res = run_upgrade(DummyHop(), DummyEngine(), "shop", plan_only=True)
        assert res["status"] == "planned"
        assert res["goal"] == "upgrade"
        assert len(res["plan"]) > 0

        # Test execution
        res_exec = upgrade_goal_runner(DummyHop(), DummyEngine(), "shop")
        assert res_exec["status"] == "completed"
        assert res_exec["verdict"] == "same"

        # Test fail_fast on check failures
        migkit.upgrade.checks = lambda hop, eng, db, quick=False: [
            {"level": "fail", "item": "versions", "detail": "unsupported major downgrade"}
        ]
        res_fail = run_upgrade(DummyHop(), DummyEngine(), "shop", fail_fast=True)
        assert res_fail["status"] == "failed"
        assert "unsupported" in res_fail["error"]
    finally:
        migkit.upgrade.checks = orig_checks
        migkit.upgrade.choose = orig_choose
        migkit.upgrade.plan_lines = orig_plan_lines


# --- Item 424 (R23.8): Workflow Exporter for Airflow, Dagster, Argo ---

def test_workflow_exporter_airflow_dag():
    dag_code = export_airflow_dag("customer-hop", schedule="@daily")
    assert "from airflow import DAG" in dag_code
    assert "from migkit.integrations import MigkitOperator" in dag_code
    assert "dag_id='migkit_migration_customer_hop'" in dag_code
    assert "task_assess >> task_plan >> task_copy >> task_verify >> task_cutover" in dag_code


def test_workflow_exporter_dagster_job():
    job_code = export_dagster_job("inventory-hop")
    assert "from dagster import job" in job_code
    assert "from migkit.integrations import migkit_op" in job_code
    assert "@job(name='migkit_inventory_hop_job')" in job_code
    assert "op_assess" in job_code
    assert "op_cutover" in job_code


def test_workflow_exporter_argo_yaml():
    argo_yaml = export_argo_workflow("billing-hop", image="migkit:v2.5")
    assert "apiVersion: argoproj.io/v1alpha1" in argo_yaml
    assert "kind: Workflow" in argo_yaml
    assert "generateName: migkit-billing-hop-" in argo_yaml
    assert "template: run-migkit-cmd" in argo_yaml
    assert "image: migkit:v2.5" in argo_yaml
    assert "dependencies: [step-copy]" in argo_yaml


def test_unified_workflow_export_routing():
    res_af = export_workflow("airflow", "my-hop")
    assert "from airflow import DAG" in res_af

    res_dag = export_workflow("dagster", "my-hop")
    assert "@job" in res_dag

    res_argo = export_workflow("argo", "my-hop")
    assert "kind: Workflow" in res_argo

    with pytest.raises(ValueError, match="unsupported workflow target"):
        export_workflow("unknown_engine", "my-hop")

    # Test through integrations and workflow re-exports
    import migkit.integrations as mi
    assert hasattr(mi, "export_workflow")
    assert "DAG" in mi.export_workflow("airflow", "my-hop")

    import migkit.workflow as mw
    assert hasattr(mw, "export_workflow")
    assert "apiVersion" in mw.export_workflow("argo", "my-hop")


# --- Item 304 (32.rds): Managed MySQL RDS Binlog Retention Reader ---

def test_managed_retention_rds_hours_set():
    def mock_ask(sql):
        q = sql.lower()
        if "information_schema.routines" in q:
            return [(1,)]
        if "aurora_version" in q:
            return []
        if "call mysql.rds_show_configuration" in q:
            return [("binlog retention hours", "168")]
        return []

    res = managed_retention(mock_ask)
    assert res is not None
    assert res["service"] == "RDS for MySQL"
    assert res["state"] == "hours"
    assert res["hours"] == 168
    assert res["seconds"] == 168 * 3600


def test_managed_retention_rds_null_and_zero():
    # NULL retention
    def mock_null(sql):
        if "routines" in sql:
            return [(1,)]
        if "aurora_version" in sql:
            return []
        if "rds_show_configuration" in sql:
            return [("binlog retention hours", "NULL")]
        return []

    res_null = managed_retention(mock_null)
    assert res_null is not None
    assert res_null["state"] == "unset"
    assert res_null["seconds"] == 300  # OWN_PURGE

    # 0 hours retention
    def mock_zero(sql):
        if "routines" in sql:
            return [(1,)]
        if "aurora_version" in sql:
            return []
        if "rds_show_configuration" in sql:
            return [("binlog retention hours", 0)]
        return []

    res_zero = managed_retention(mock_zero)
    assert res_zero is not None
    assert res_zero["state"] == "zero"
    assert res_zero["seconds"] == 300


def test_managed_retention_aurora_and_permission_denied():
    # Aurora MySQL with caveat
    def mock_aurora(sql):
        if "routines" in sql:
            return [(1,)]
        if "aurora_version" in sql:
            return [("aurora_version", "3.04.0")]
        if "rds_show_configuration" in sql:
            return [("binlog retention hours", "72")]
        return []

    res_aurora = managed_retention(mock_aurora)
    assert res_aurora["service"] == "Aurora MySQL"
    assert "Aurora keeps its binary log" in res_aurora["caveat"]
    assert res_aurora["hours"] == 72

    # Permission denied on rds_show_configuration
    def mock_denied(sql):
        if "routines" in sql:
            return [(1,)]
        if "aurora_version" in sql:
            return []
        if "rds_show_configuration" in sql:
            raise PermissionError("Access denied for user 'migkit'@'%' to procedure 'rds_show_configuration'")
        return []

    res_denied = managed_retention(mock_denied)
    assert res_denied["state"] == "unknown"
    assert "could not read" in res_denied["why"]

    # Non-managed server returns None
    def mock_plain(sql):
        return []
    assert managed_retention(mock_plain, hinted=False) is None
