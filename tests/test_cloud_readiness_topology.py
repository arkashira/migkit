import pytest

from migkit.upgrade import (
    _mongo,
    _mssql,
    run_upgrade,
    upgrade_goal_runner,
)
from migkit.engines.mysql import managed_retention


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
