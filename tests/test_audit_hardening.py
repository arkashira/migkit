"""Unit tests for audit hardening across tail state, startup checks, schema drift,
MySQL unified connections, neutral shape comparison, and tool use (Items 120, 122,
125, 127, 129, 11, 17).
"""
import json
import os
from pathlib import Path
import pytest
from unittest.mock import MagicMock

from migkit import audit_guard


# ---- Item 120 (AUD-B): Durable tail state & follow loop guard ----


def test_position_envelope_serialization_and_durable_file(tmp_path):
    pos = audit_guard.Position(
        v=1,
        engine="mysql",
        source="db_orders",
        position={"binlog": "mysql-bin.000042", "pos": 1048576},
        batch=42,
        rung="streaming",
        next="mysql-bin.000043:120",
    )
    p_path = audit_guard.position_path(str(tmp_path), "orders_hop", "orders_tail")
    audit_guard.write_durable_position(p_path, pos)

    loaded = audit_guard.read_durable_position(p_path)
    assert loaded is not None
    assert loaded.v == 1
    assert loaded.engine == "mysql"
    assert loaded.batch == 42
    assert loaded.position["binlog"] == "mysql-bin.000042"
    assert loaded.rung == "streaming"


def test_durable_tail_state_sqlite_transactions(tmp_path):
    db_file = tmp_path / "tail_state.sqlite"
    store = audit_guard.DurableTailState(str(db_file))

    store.commit_batch(
        batch_id=1,
        position="pos_100",
        count=500,
        table_states={"orders": ("pos_100", 500, "active")},
        accounts={"acc_main": "bal_100"},
    )
    f_id = store.add_spool_frame("range_orders_1", b"binary_data_frame")
    assert f_id > 0

    batch = store.get_last_batch()
    assert batch["batch_id"] == 1
    assert batch["count"] == 500

    tbl_state = store.get_table_state("orders")
    assert tbl_state["row_count"] == 500
    assert tbl_state["status"] == "active"

    released = store.release_spool_frames("range_orders_1")
    assert released == 1
    store.close()


def test_single_follow_loop_guard_mutual_exclusion(tmp_path):
    guard = audit_guard.SingleFollowLoopGuard(str(tmp_path), ttl_seconds=10.0)
    rec1 = guard.acquire("tail_scope_a")
    assert rec1["scope"] == "tail_scope_a"
    assert rec1["pid"] == os.getpid()

    # Second acquisition by active process on same scope must raise ConcurrentTailError
    with pytest.raises(audit_guard.ConcurrentTailError):
        guard.acquire("tail_scope_a")

    guard.renew("tail_scope_a")
    guard.release("tail_scope_a")

    # After release, re-acquisition succeeds
    rec2 = guard.acquire("tail_scope_a")
    assert rec2["scope"] == "tail_scope_a"
    guard.release("tail_scope_a")


# ---- Item 122 (AUD-D): Strict options, environment and addresses validation ----


def test_strict_options_validation_with_suggestions():
    allowed = {"host", "port", "user", "password", "database", "timeout"}
    options = {
        "host": "127.0.0.1",
        "port": 3306,
        "passwrd": "secret",  # typo
        "unkwn_setting": True,
    }
    rep = audit_guard.validate_options(options, allowed, prefix="hops.main.source")
    assert not rep.ok
    assert any("hops.main.source.passwrd" in err for err in rep.errors)
    assert rep.suggestions.get("hops.main.source.passwrd") == "hops.main.source.password"


def test_validate_environment_identifies_undeclared_keys():
    env = {
        "MIGKIT_READ_TIMEOUT": "3600",
        "MIGKIT_RED_TIMEOUT": "1800",  # typo
        "PATH": "/usr/bin",
    }
    rep = audit_guard.validate_environment(env)
    assert any("MIGKIT_RED_TIMEOUT" in w for w in rep.warnings)
    assert rep.suggestions.get("MIGKIT_RED_TIMEOUT") == "MIGKIT_READ_TIMEOUT"


def test_validate_address_syntax_and_secrets():
    # Valid address
    rep1 = audit_guard.validate_address("mysql://app_user@db.example.internal:3306/sales", side="src", hop="hop1")
    assert rep1.ok

    # Invalid missing scheme and host
    rep2 = audit_guard.validate_address("invalid_address", side="dst", hop="hop1")
    assert not rep2.ok

    # Secret reference naming conventions
    rep3 = audit_guard.validate_address("secret:hops.hop1.src.password", side="src", hop="hop1")
    assert rep3.ok

    rep4 = audit_guard.validate_address("secret:unscoped_secret", side="src", hop="hop1")
    assert not rep4.ok
    assert any("hops.hop1.src" in err for err in rep4.errors)


def test_resolve_state_root_and_secure_permissions(tmp_path, monkeypatch):
    custom_state = tmp_path / "custom_state"
    monkeypatch.setenv("XDG_STATE_HOME", str(custom_state))
    resolved = audit_guard.resolve_state_root()
    assert str(custom_state / "migkit") == str(resolved)

    audit_guard.secure_permissions(resolved, is_dir=True)
    assert resolved.exists()
    assert oct(resolved.stat().st_mode & 0o777) == "0o700"

    test_file = resolved / "test.json"
    test_file.write_text("{}")
    audit_guard.secure_permissions(test_file, is_dir=False)
    assert oct(test_file.stat().st_mode & 0o777) == "0o600"


# ---- Item 125 (AUD-F): Single catalogue read schema drift detector ----


def test_single_catalogue_drift_detector_classifies_changes():
    detector = audit_guard.SingleCatalogueDriftDetector()

    before = {
        "users": {
            "columns": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "email", "type": "varchar(255)", "nullable": False},
            ],
            "partitioning": None,
            "indexes": ["PRIMARY"],
        },
        "orders": {
            "columns": [{"name": "id", "type": "int", "nullable": False}],
            "partitioning": "RANGE (created_at)",
            "indexes": ["PRIMARY"],
        },
        "old_archive": {
            "columns": [{"name": "id", "type": "int"}],
        },
    }

    after = {
        "users": {
            "columns": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "email", "type": "varchar(255)", "nullable": False},
                {"name": "is_active", "type": "boolean", "nullable": False},  # Added NOT NULL col without default
            ],
            "partitioning": None,
            "indexes": ["PRIMARY", "idx_users_email"],
        },
        "orders": {
            "columns": [{"name": "id", "type": "int", "nullable": False}],
            "partitioning": "RANGE (updated_at)",  # Partitioning changed -> needs reload
            "indexes": ["PRIMARY"],
        },
        "new_audit": {
            "columns": [{"name": "id", "type": "int"}],
        },
    }

    rep = detector.detect_drift(before, after)
    assert rep.has_drift
    assert rep.created == ["new_audit"]
    assert rep.dropped == ["old_archive"]
    assert "users" in rep.altered
    assert "orders" in rep.altered
    assert "orders" in rep.needs_reload
    assert "users" in rep.needs_backfill


# ---- Item 127 (AUD-G2): MySQL unified single _connect with TLS enforcing timeout ----


def test_mysql_unified_connect_kwargs_and_tls():
    class DummyEndpoint:
        host = "db.mysql.internal"
        port = 3307
        user = "migrator"
        password = "pwd"

        def mysql_tls(self):
            return {"ssl_ca": "/etc/ssl/ca.pem", "check_hostname": True}

    ep = DummyEndpoint()
    kwargs = audit_guard.build_mysql_connect_kwargs(
        ep,
        side="dst",
        read_timeout=15,
        write_mode=True,
    )
    assert kwargs["host"] == "db.mysql.internal"
    assert kwargs["port"] == 3307
    assert kwargs["connect_timeout"] == 15
    assert kwargs["read_timeout"] == 15
    assert kwargs["ssl_ca"] == "/etc/ssl/ca.pem"
    assert "time_zone = '+00:00'" in kwargs["init_command"]
    assert "STRICT_ALL_TABLES" in kwargs["init_command"]


def test_mysql_unified_connect_invokes_connect_with_keepalive():
    mock_conn = MagicMock()
    mock_sock = MagicMock()
    mock_conn._sock = mock_sock
    mock_connect_fn = MagicMock(return_value=mock_conn)

    class SimpleEndpoint:
        host = "127.0.0.1"
        port = 3306
        user = "root"
        password = ""

    conn = audit_guard.unified_mysql_connect(
        SimpleEndpoint(),
        side="src",
        connect_fn=mock_connect_fn,
    )
    assert conn == mock_conn
    mock_connect_fn.assert_called_once()
    assert mock_sock.setsockopt.called


def test_mysql_engine_has_connect_method():
    from migkit.engines.mysql import MySQLEngine
    assert hasattr(MySQLEngine, "_connect")
    assert callable(getattr(MySQLEngine, "_connect"))


# ---- Item 129 (AUD-H): Neutral shape schema comparer across engine pairs ----


def test_neutral_shape_schema_comparer_hetero():
    comparer = audit_guard.NeutralShapeSchemaComparer()

    src = {
        "accounts": audit_guard.NeutralTable(
            name="accounts",
            columns={
                "id": audit_guard.NeutralColumn("id", "BIGINT", nullable=False),
                "name": audit_guard.NeutralColumn("name", "VARCHAR", capacity=100, nullable=False),
                "description": audit_guard.NeutralColumn("description", "TEXT", nullable=True),
            },
            primary_key=("id",),
        )
    }

    # Compatible target
    dst_ok = {
        "accounts": audit_guard.NeutralTable(
            name="accounts",
            columns={
                "id": audit_guard.NeutralColumn("id", "BIGINT", nullable=False),
                "name": audit_guard.NeutralColumn("name", "VARCHAR", capacity=100, nullable=False),
                "description": audit_guard.NeutralColumn("description", "TEXT", nullable=True),
            },
            primary_key=("id",),
        )
    }
    res_ok = comparer.compare(src, dst_ok)
    assert res_ok.compatible
    assert len(res_ok.differences) == 0

    # Incompatible target (capacity narrowing and primary key mismatch)
    dst_bad = {
        "accounts": audit_guard.NeutralTable(
            name="accounts",
            columns={
                "id": audit_guard.NeutralColumn("id", "INT", nullable=False),  # BIGINT -> INT narrowing
                "name": audit_guard.NeutralColumn("name", "VARCHAR", capacity=50, nullable=False),  # 100 -> 50
                "description": audit_guard.NeutralColumn("description", "TEXT", nullable=False),  # nullable -> not null
            },
            primary_key=("name",),
        )
    }
    res_bad = comparer.compare(src, dst_bad)
    assert not res_bad.compatible
    assert any("capacity narrowing" in d for d in res_bad.differences)
    assert any("primary key mismatch" in d for d in res_bad.differences)


def test_probe_accumulator_prevents_false_ok():
    comparer = audit_guard.NeutralShapeSchemaComparer()
    probes = audit_guard.ProbeAccumulator()
    probes.record_probed("secret_orders")
    probes.record_unreadable("secret_orders", "partitions", "Access denied for information_schema.partitions")

    tables = {
        "secret_orders": audit_guard.NeutralTable(
            name="secret_orders",
            columns={"id": audit_guard.NeutralColumn("id", "INT")},
        )
    }
    res = comparer.compare(tables, tables, probes=probes)
    assert not res.compatible
    assert len(res.unreadable) == 1
    assert any("Cannot verify" in d for d in res.differences)


# ---- Items 11 (0f-DVT) & 17 (0f-REST): Audit helpers with fallback ----


def test_dvt_audit_helper_feature_matrix_and_bisection():
    configured = {"row_hash", "custom_queries", "threshold", "labels"}
    audit = audit_guard.DVTAuditHelper.audit_dvt(configured)

    assert audit["row_hash"][0] == "used"
    assert audit["grouped_columns"][0] == "topped up"
    assert audit["random_row_sampling"][0] == "unused"

    # Bisect and row compare top-up
    def mock_dvt_grouped(table, group_col):
        return {"US": 0, "EU": 14, "APAC": 0}

    checked_groups = []

    def mock_migkit_row(table, grp):
        checked_groups.append(grp)
        return True

    differing = audit_guard.DVTAuditHelper.bisect_and_row_compare(
        "sales", "region", mock_dvt_grouped, mock_migkit_row
    )
    assert differing == ["EU"]
    assert checked_groups == ["EU"]


def test_rest_tool_audit_helper_and_safe_fallback():
    audit_info = audit_guard.RestToolAuditHelper.audit_tool("pg_dump")
    assert "jobs" in audit_info
    assert audit_info["jobs"][0] == "used"
    assert audit_info["blobs"][0] == "unused"

    # Test run_with_audit_fallback when primary tool fails
    log = []

    def fail_primary():
        raise RuntimeError("mongosync /start connection timed out")

    def ok_fallback():
        return "migkit native change follow engaged"

    result = audit_guard.RestToolAuditHelper.run_with_audit_fallback(
        "mongosync", fail_primary, ok_fallback, audit_log=log
    )
    assert result == "migkit native change follow engaged"
    assert len(log) == 1
    assert log[0]["tool"] == "mongosync"
    assert log[0]["status"] == "fallback_engaged"
