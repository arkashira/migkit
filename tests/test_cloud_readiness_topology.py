import json
import pytest

from migkit.cloud_readiness import (
    CloudReadinessAuditor,
    MaintenanceAuditor,
    NetworkAuditor,
    SizingAuditor,
    SnapshotAuditor,
    audit,
    items as cloud_items,
)
from migkit.change_sheet import (
    ChangeSheet,
    ChangeTask,
    create_from_plan,
)
from migkit.topology import (
    ChangeEvent,
    TopologyType,
    full_mesh,
    many_to_one,
    one_to_many,
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

def test_cloud_readiness_network_auditor():
    net = NetworkAuditor()

    # Same VPC -> pass
    findings = net.audit_vpc({"id": "vpc-1", "cidr": "10.0.0.0/16"}, {"id": "vpc-1", "cidr": "10.0.0.0/16"})
    assert any(f.level == "pass" and "same VPC" in f.detail for f in findings)

    # Different VPC with collision without peering -> fail
    findings = net.audit_vpc({"id": "vpc-1", "cidr": "10.0.0.0/16", "peered": False},
                             {"id": "vpc-2", "cidr": "10.0.0.0/16", "peered": False})
    assert any(f.level == "fail" and f.gates for f in findings)

    # Peered VPC -> pass
    findings = net.audit_vpc({"id": "vpc-1", "cidr": "10.0.0.0/16", "peered": True},
                             {"id": "vpc-2", "cidr": "10.0.0.0/16", "peered": True})
    assert any(f.level == "pass" and "peering" in f.detail for f in findings)

    # Subnets available IPs check
    subnets = [
        {"id": "subnet-a", "az": "us-east-1a", "available_ips": 12},
        {"id": "subnet-b", "az": "us-east-1b", "available_ips": 2},
    ]
    findings = net.audit_subnets(subnets, min_available_ips=5)
    assert any(f.level == "fail" and "subnet-b" in f.resource for f in findings)

    # Security groups port check
    sg_rules = [{"direction": "ingress", "from_port": 5432, "to_port": 5432}]
    assert any(f.level == "pass" for f in net.audit_security_groups(sg_rules, 5432))
    assert any(f.level == "fail" for f in net.audit_security_groups(sg_rules, 3306))


def test_cloud_readiness_maintenance_and_sizing():
    maint = MaintenanceAuditor()
    # Overlapping maintenance window
    findings = maint.audit_window("Sun:03:00-Sun:04:00", {"day": "Sun", "hour": 3})
    assert any(f.level == "warn" and "overlap" in f.resource for f in findings)

    # Non-overlapping
    findings = maint.audit_window("Sun:03:00-Sun:04:00", {"day": "Wed", "hour": 10})
    assert any(f.level == "pass" for f in findings)

    # Forced pending maintenance -> fail gate
    pending = [{"name": "OS_PATCH", "forced": True}]
    findings = maint.audit_pending(pending)
    assert any(f.level == "fail" and f.gates for f in findings)

    sizing = SizingAuditor()
    src = {"cpu": 16, "memory_gb": 64, "storage_gb": 500}
    dst = {"cpu": 8, "memory_gb": 32, "storage_gb": 400}
    findings = sizing.audit_specs(src, dst)
    assert any(f.resource == "sizing:cpu" and f.level == "warn" for f in findings)
    assert any(f.resource == "sizing:storage" and f.level == "fail" and f.gates for f in findings)


def test_cloud_readiness_snapshot_and_full_audit():
    snap = SnapshotAuditor()
    findings = snap.audit_readiness({"can_snapshot": False, "pitr_enabled": False, "backup_retention_days": 0})
    assert any(f.resource == "snapshot:create_api" and f.level == "fail" for f in findings)
    assert any(f.resource == "snapshot:pitr" and f.level == "warn" for f in findings)

    src_info = {
        "vpc": {"id": "vpc-1", "cidr": "10.0.0.0/16"},
        "cpu": 8, "memory_gb": 32, "storage_gb": 200,
    }
    dst_info = {
        "vpc": {"id": "vpc-1", "cidr": "10.0.0.0/16"},
        "subnets": [
            {"id": "sub-1", "az": "us-east-1a", "available_ips": 50},
            {"id": "sub-2", "az": "us-east-1b", "available_ips": 50},
        ],
        "port": 5432,
        "security_groups": [{"direction": "ingress", "from_port": 5432, "to_port": 5432}],
        "maintenance_window": "Sun:02:00-Sun:03:00",
        "pending_maintenance": [],
        "cpu": 8, "memory_gb": 32, "storage_gb": 300,
        "can_snapshot": True, "pitr_enabled": True, "backup_retention_days": 7,
    }
    all_findings = audit(src_info, dst_info, cutover_window={"day": "Sat", "hour": 22})
    assert all(f.level == "pass" for f in all_findings)

    # Test compatibility items adapter
    class DummyHop:
        cloud_src = src_info
        cloud_dst = dst_info
    items = cloud_items(DummyHop())
    assert len(items) > 0
    assert all("level" in it and "scope" in it for it in items)


# --- Item 434 (R25.3): Org Change Sheet Exporter ---

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

def test_topology_many_to_one():
    sources = [
        {"id": "shard_1", "endpoint": "pg://s1/db", "shard_prefix": "s1_"},
        {"id": "shard_2", "endpoint": "pg://s2/db", "shard_prefix": "s2_"},
    ]
    target = {"id": "central", "endpoint": "pg://central/db"}
    topo = many_to_one(sources, target)

    assert topo.topo_type == TopologyType.MANY_TO_ONE
    valid, errors = topo.validate()
    assert valid
    assert len(topo.legs) == 2

    # Collision test: missing or duplicate shard prefix
    sources_dup = [
        {"id": "shard_1", "endpoint": "pg://s1/db", "shard_prefix": "dup_"},
        {"id": "shard_2", "endpoint": "pg://s2/db", "shard_prefix": "dup_"},
    ]
    topo_dup = many_to_one(sources_dup, target)
    valid, errors = topo_dup.validate()
    assert not valid
    assert any("duplicate shard_prefix" in e for e in errors)


def test_topology_one_to_many():
    source = {"id": "primary", "endpoint": "pg://primary/db"}
    targets = [
        {"id": "read_replica_1", "endpoint": "pg://rr1/db"},
        {"id": "analytics_node", "endpoint": "pg://an/db"},
    ]
    topo = one_to_many(source, targets)
    assert topo.topo_type == TopologyType.ONE_TO_MANY
    valid, errors = topo.validate()
    assert valid
    assert len(topo.legs) == 2


def test_topology_full_mesh_and_loop_suppression():
    nodes = [
        {"id": "node_a", "endpoint": "pg://a/db"},
        {"id": "node_b", "endpoint": "pg://b/db"},
        {"id": "node_c", "endpoint": "pg://c/db"},
    ]
    topo = full_mesh(nodes)
    assert topo.topo_type == TopologyType.FULL_MESH
    valid, errors = topo.validate()
    assert valid
    # 3 nodes * 2 = 6 directional legs
    assert len(topo.legs) == 6

    # Test loop suppression routing
    event = ChangeEvent(event_id="evt_01", origin_id="node_a", table="users", key="123")
    # When node_a sends to node_b:
    next_hops = topo.route_change(event, "node_a")
    assert "node_b" in next_hops
    assert "node_c" in next_hops
    assert "node_a" not in next_hops

    # Now event at node_b: it should route to node_c, but NOT back to origin node_a or visited node_b
    next_hops_b = topo.route_change(event, "node_b")
    assert "node_c" in next_hops_b
    assert "node_a" not in next_hops_b

    # Now at node_c: all peers visited or origin, so no forward hops
    next_hops_c = topo.route_change(event, "node_c")
    assert len(next_hops_c) == 0

    # Convergence
    converged, max_lag = topo.is_converged(max_allowed_lag_sec=1.0)
    assert converged
    assert max_lag == 0.0


# --- Item 423 (R23.7): Major Version Upgrade Goal Runner & Extension Checks ---

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
