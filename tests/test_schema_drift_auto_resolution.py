import time
import pytest

from migkit.ddl_shield import (
    CDCReplicationApplier,
    ColumnSchema,
    SchemaCache,
    TransactionalFence,
    VerificationSliceManager,
)
from migkit.schema_drift import (
    BackfillOrchestrator,
    BackfillTask,
    DriftAction,
    DriftResolutionResult,
    DriftType,
    SchemaDriftError,
    SchemaDriftResolver,
    check_type_widening,
    generate_alter_statements,
    map_type_to_dialect,
)


def test_type_widening_rules():
    assert check_type_widening("INT", "BIGINT")[0] is True
    assert check_type_widening("INTEGER", "BIGINT")[0] is True
    assert check_type_widening("SMALLINT", "INT")[0] is True
    assert check_type_widening("TINYINT", "BIGINT")[0] is True
    assert check_type_widening("INT", "INT UNSIGNED")[0] is True
    assert check_type_widening("BIGINT", "INT")[0] is False
    assert check_type_widening("INT", "INT")[0] is False

    assert check_type_widening("VARCHAR(50)", "VARCHAR(255)")[0] is True
    assert check_type_widening("CHAR(10)", "VARCHAR(50)")[0] is True
    assert check_type_widening("VARCHAR(100)", "TEXT")[0] is True
    assert check_type_widening("TEXT", "LONGTEXT")[0] is True
    assert check_type_widening("VARCHAR(255)", "VARCHAR(50)")[0] is False
    assert check_type_widening("VARCHAR(100)", "VARCHAR(100)")[0] is False

    assert check_type_widening("DECIMAL(10, 2)", "DECIMAL(18, 4)")[0] is True
    assert check_type_widening("NUMERIC(8, 2)", "NUMERIC(14, 2)")[0] is True
    assert check_type_widening("INT", "DECIMAL(18, 4)")[0] is True
    assert check_type_widening("DECIMAL(10, 2)", "DECIMAL")[0] is True
    assert check_type_widening("DECIMAL(18, 4)", "DECIMAL(10, 2)")[0] is False
    assert check_type_widening("DECIMAL(10, 2)", "DECIMAL(10, 2)")[0] is False

    assert check_type_widening("FLOAT", "DOUBLE PRECISION")[0] is True
    assert check_type_widening("REAL", "DOUBLE")[0] is True
    assert check_type_widening("DOUBLE", "FLOAT")[0] is False

    assert check_type_widening("VARBINARY(100)", "VARBINARY(500)")[0] is True
    assert check_type_widening("VARBINARY(100)", "BLOB")[0] is True
    assert check_type_widening("BLOB", "LONGBLOB")[0] is True
    assert check_type_widening("LONGBLOB", "BLOB")[0] is False


def test_map_type_to_target_dialects():
    assert map_type_to_dialect("BIGINT", "postgres") == "BIGINT"
    assert map_type_to_dialect("INT UNSIGNED", "postgres") == "BIGINT"
    assert map_type_to_dialect("VARCHAR(255)", "postgres") == "VARCHAR(255)"
    assert map_type_to_dialect("DECIMAL(18, 4)", "postgres") == "NUMERIC(18, 4)"
    assert map_type_to_dialect("DOUBLE", "postgres") == "DOUBLE PRECISION"

    assert map_type_to_dialect("INTEGER", "mysql") == "INT"
    assert map_type_to_dialect("BIGINT", "mysql") == "BIGINT"
    assert map_type_to_dialect("VARCHAR(255)", "mysql") == "VARCHAR(255)"
    assert map_type_to_dialect("DECIMAL(18, 4)", "mysql") == "DECIMAL(18, 4)"

    assert map_type_to_dialect("BIGINT", "sqlite") == "INTEGER"
    assert map_type_to_dialect("VARCHAR(255)", "sqlite") == "TEXT"
    assert map_type_to_dialect("DECIMAL(18, 4)", "sqlite") == "NUMERIC"
    assert map_type_to_dialect("DOUBLE PRECISION", "sqlite") == "REAL"


def test_generate_alter_statements_postgres():
    drifts = [
        DriftAction(
            drift_type=DriftType.NEW_COLUMN,
            table="orders",
            column_name="loyalty_tier",
            new_type="VARCHAR(50)",
            new_nullable=False,
            new_default="standard",
            requires_backfill=True,
        ),
        DriftAction(
            drift_type=DriftType.TYPE_WIDENING,
            table="orders",
            column_name="amount",
            target_type="DECIMAL(10, 2)",
            new_type="DECIMAL(18, 4)",
        ),
        DriftAction(
            drift_type=DriftType.RELAXED_NULLABILITY,
            table="orders",
            column_name="user_ref",
            old_nullable=False,
            new_nullable=True,
        ),
        DriftAction(
            drift_type=DriftType.ADDED_DEFAULT,
            table="orders",
            column_name="status",
            new_default="PENDING",
            requires_backfill=True,
        ),
    ]

    stmts = generate_alter_statements("orders", drifts, target_dialect="postgres")
    assert len(stmts) == 4
    assert 'ALTER TABLE "orders" ADD COLUMN "loyalty_tier" VARCHAR(50) DEFAULT \'standard\' NOT NULL' in stmts[0]
    assert 'ALTER TABLE "orders" ALTER COLUMN "amount" TYPE NUMERIC(18, 4)' in stmts[1]
    assert 'ALTER TABLE "orders" ALTER COLUMN "user_ref" DROP NOT NULL' in stmts[2]
    assert 'ALTER TABLE "orders" ALTER COLUMN "status" SET DEFAULT \'PENDING\'' in stmts[3]


def test_generate_alter_statements_mysql():
    drifts = [
        DriftAction(
            drift_type=DriftType.NEW_COLUMN,
            table="accounts",
            column_name="country_code",
            new_type="CHAR(3)",
            new_nullable=True,
            new_default="USA",
        ),
        DriftAction(
            drift_type=DriftType.TYPE_WIDENING,
            table="accounts",
            column_name="txn_count",
            target_type="INT",
            new_type="BIGINT",
            new_nullable=False,
        ),
        DriftAction(
            drift_type=DriftType.RELAXED_NULLABILITY,
            table="accounts",
            column_name="email",
            target_type="VARCHAR(100)",
            new_type="VARCHAR(100)",
        ),
        DriftAction(
            drift_type=DriftType.ADDED_DEFAULT,
            table="accounts",
            column_name="tier",
            new_default="BRONZE",
        ),
    ]

    stmts = generate_alter_statements("accounts", drifts, target_dialect="mysql")
    assert len(stmts) == 4
    assert "ALTER TABLE `accounts` ADD COLUMN `country_code` CHAR(3) DEFAULT 'USA' NULL" in stmts[0]
    assert "ALTER TABLE `accounts` MODIFY COLUMN `txn_count` BIGINT NOT NULL" in stmts[1]
    assert "ALTER TABLE `accounts` MODIFY COLUMN `email` VARCHAR(100) NULL" in stmts[2]
    assert "ALTER TABLE `accounts` ALTER COLUMN `tier` SET DEFAULT 'BRONZE'" in stmts[3]


def test_generate_alter_statements_sqlite_table_rewrite():
    cache = SchemaCache()
    cache.add_table(
        "products",
        columns=[
            ColumnSchema(name="id", data_type="INTEGER", is_primary_key=True),
            ColumnSchema(name="sku", data_type="TEXT", nullable=False),
            ColumnSchema(name="price", data_type="NUMERIC", nullable=False),
        ],
    )

    drifts = [
        DriftAction(
            drift_type=DriftType.NEW_COLUMN,
            table="products",
            column_name="description",
            new_type="VARCHAR(500)",
            new_nullable=True,
        ),
        DriftAction(
            drift_type=DriftType.RELAXED_NULLABILITY,
            table="products",
            column_name="price",
            old_nullable=False,
            new_nullable=True,
        ),
    ]

    stmts = generate_alter_statements("products", drifts, target_dialect="sqlite", table_schema=cache.get_table("products"))
    assert len(stmts) == 4
    assert 'CREATE TABLE "products_drift_new"' in stmts[0]
    assert 'INSERT INTO "products_drift_new"' in stmts[1]
    assert 'DROP TABLE "products"' in stmts[2]
    assert 'ALTER TABLE "products_drift_new" RENAME TO "products"' in stmts[3]


def test_detect_drift_from_column_metadata():
    cache = SchemaCache()
    cache.add_table(
        "users",
        columns=[
            ColumnSchema(name="id", data_type="BIGINT", is_primary_key=True),
            ColumnSchema(name="age", data_type="INT", nullable=False),
            ColumnSchema(name="nickname", data_type="VARCHAR(20)", nullable=False),
            ColumnSchema(name="status", data_type="VARCHAR(20)", default=None),
        ],
    )

    resolver = SchemaDriftResolver(
        source_dialect="mysql",
        target_dialect="postgres",
        schema_cache=cache,
    )

    source_cols = {
        "id": {"data_type": "BIGINT", "nullable": False},
        "age": {"data_type": "BIGINT", "nullable": False},
        "nickname": {"data_type": "VARCHAR(100)", "nullable": True},
        "status": {"data_type": "VARCHAR(20)", "nullable": True, "default": "active"},
        "signup_ip": {"data_type": "VARCHAR(45)", "nullable": True, "default": "10.0.0.1"},
    }

    drifts = resolver.detect_drift_from_columns("users", source_cols)
    assert len(drifts) == 5

    drift_types = {d.column_name: d.drift_type for d in drifts}
    assert drift_types["age"] == DriftType.TYPE_WIDENING
    assert drift_types["nickname"] == DriftType.TYPE_WIDENING
    assert drift_types["status"] == DriftType.ADDED_DEFAULT
    assert drift_types["signup_ip"] == DriftType.NEW_COLUMN

    relaxed = [d for d in drifts if d.column_name == "nickname" and d.drift_type == DriftType.RELAXED_NULLABILITY]
    assert len(relaxed) == 1
    assert relaxed[0].old_nullable is False
    assert relaxed[0].new_nullable is True


def test_detect_drift_from_cdc_payload_inference():
    cache = SchemaCache()
    cache.add_table(
        "payments",
        columns=[
            ColumnSchema(name="id", data_type="INTEGER", is_primary_key=True),
            ColumnSchema(name="amount", data_type="INT", nullable=False),
            ColumnSchema(name="memo", data_type="TEXT", nullable=False),
        ],
    )

    resolver = SchemaDriftResolver(schema_cache=cache)

    event = {
        "type": "INSERT",
        "table": "payments",
        "data": {
            "id": 101,
            "amount": 9000000000,
            "memo": None,
            "currency": "USD",
        },
    }

    tbl, drifts = resolver.detect_drift_from_event(event)
    assert tbl == "payments"
    assert len(drifts) == 3

    col_drifts = {d.column_name: d.drift_type for d in drifts}
    assert col_drifts["amount"] == DriftType.TYPE_WIDENING
    assert col_drifts["memo"] == DriftType.RELAXED_NULLABILITY
    assert col_drifts["currency"] == DriftType.NEW_COLUMN


def test_detect_drift_from_ddl_events():
    cache = SchemaCache()
    cache.add_table(
        "customers",
        columns=[
            ColumnSchema(name="id", data_type="BIGINT", is_primary_key=True),
            ColumnSchema(name="level", data_type="INT", nullable=False),
        ],
    )

    resolver = SchemaDriftResolver(schema_cache=cache)

    ddl_event_1 = {
        "type": "DDL",
        "sql": "ALTER TABLE customers ADD COLUMN email VARCHAR(120) DEFAULT 'unverified' NOT NULL",
    }
    tbl1, drifts1 = resolver.detect_drift_from_event(ddl_event_1)
    assert tbl1 == "customers"
    assert len(drifts1) == 1
    assert drifts1[0].drift_type == DriftType.NEW_COLUMN
    assert drifts1[0].column_name == "email"
    assert drifts1[0].new_default == "'unverified'"
    assert drifts1[0].new_nullable is False

    ddl_event_2 = {
        "type": "DDL",
        "sql": "ALTER TABLE customers MODIFY COLUMN level BIGINT NULL",
    }
    tbl2, drifts2 = resolver.detect_drift_from_event(ddl_event_2)
    assert tbl2 == "customers"
    assert len(drifts2) == 2
    types2 = {d.drift_type for d in drifts2}
    assert DriftType.TYPE_WIDENING in types2
    assert DriftType.RELAXED_NULLABILITY in types2


def test_atomic_cdc_barrier_fence_execution_and_selective_merkle_invalidation():
    cache = SchemaCache()
    cache.add_table(
        "users",
        columns=[
            ColumnSchema(name="id", data_type="INT", is_primary_key=True),
            ColumnSchema(name="name", data_type="VARCHAR(50)"),
        ],
    )
    cache.add_table(
        "orders",
        columns=[
            ColumnSchema(name="id", data_type="INT", is_primary_key=True),
            ColumnSchema(name="total", data_type="NUMERIC(10, 2)"),
        ],
    )

    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("slice_u1", "users", key_range=(0, 1000), merkle_root="root_u1")
    slice_mgr.register_slice("slice_u2", "users", key_range=(1000, 2000), merkle_root="root_u2")
    slice_mgr.register_slice("slice_o1", "orders", key_range=(0, 1000), merkle_root="root_o1")

    executed_sqls = []

    def mock_executor(sql: str):
        executed_sqls.append(sql)
        return 0

    applier = CDCReplicationApplier()
    resolver = SchemaDriftResolver(
        source_dialect="mysql",
        target_dialect="postgres",
        schema_cache=cache,
        slice_manager=slice_mgr,
        applier=applier,
        target_executor=mock_executor,
    )

    drifts = [
        DriftAction(
            drift_type=DriftType.NEW_COLUMN,
            table="users",
            column_name="activated",
            new_type="BOOLEAN",
            new_nullable=True,
            new_default="TRUE",
            requires_backfill=True,
        ),
        DriftAction(
            drift_type=DriftType.TYPE_WIDENING,
            table="users",
            column_name="name",
            target_type="VARCHAR(50)",
            new_type="VARCHAR(200)",
        ),
    ]

    res = resolver.execute_barrier("users", drifts)

    assert res.status == "RESOLVED"
    assert res.table == "users"
    assert len(res.generated_statements) == 2
    assert "BEGIN" in executed_sqls
    assert "COMMIT" in executed_sqls
    assert any('ALTER TABLE "users" ADD COLUMN "activated"' in s for s in executed_sqls)
    assert any('ALTER TABLE "users" ALTER COLUMN "name" TYPE VARCHAR(200)' in s for s in executed_sqls)

    assert applier.pause_count == 1
    assert applier.resume_count == 1
    assert applier.is_paused is False

    updated_users = cache.get_table("users")
    assert "activated" in updated_users.columns
    assert updated_users.columns["activated"].data_type == "BOOLEAN"
    assert updated_users.columns["name"].data_type == "VARCHAR(200)"

    assert slice_mgr.get_slice("slice_u1").status == "INVALID"
    assert slice_mgr.get_slice("slice_u2").status == "INVALID"
    assert slice_mgr.get_slice("slice_o1").status == "VALID"


def test_asynchronous_default_backfill_orchestration():
    executed_updates = []

    def mock_executor(sql: str):
        executed_updates.append(sql)
        return 42

    orchestrator = BackfillOrchestrator(target_executor=mock_executor, async_mode=False)

    task = orchestrator.schedule_backfill(
        table="members",
        column_name="is_active",
        default_value=True,
        batch_size=500,
    )

    assert task.status == "PENDING"
    completed = orchestrator.run_all_pending()
    assert len(completed) == 1
    assert task.status == "COMPLETED"
    assert task.rows_updated == 42
    assert any('UPDATE "members" SET "is_active" = TRUE WHERE "is_active" IS NULL' in s for s in executed_updates)


def test_async_backfill_background_thread_worker():
    executed_updates = []

    def mock_executor(sql: str):
        time.sleep(0.05)
        executed_updates.append(sql)
        return 15

    orchestrator = BackfillOrchestrator(target_executor=mock_executor, async_mode=True)
    try:
        task = orchestrator.schedule_backfill(
            table="devices",
            column_name="firmware_ver",
            default_value="1.0.0",
        )
        assert task.status in ("PENDING", "RUNNING")
        ok = orchestrator.wait_all(timeout=3.0)
        assert ok is True
        assert task.status == "COMPLETED"
        assert task.rows_updated == 15
        assert len(executed_updates) == 1
    finally:
        orchestrator.stop()


def test_process_cdc_stream_without_stalling_active_replication():
    cache = SchemaCache()
    cache.add_table(
        "inventory",
        columns=[
            ColumnSchema(name="item_id", data_type="INTEGER", is_primary_key=True),
            ColumnSchema(name="qty", data_type="INT", nullable=False),
        ],
    )

    executed_ddl = []
    applier = CDCReplicationApplier()

    resolver = SchemaDriftResolver(
        source_dialect="mysql",
        target_dialect="postgres",
        schema_cache=cache,
        applier=applier,
        target_executor=lambda sql: executed_ddl.append(sql),
    )

    stream = [
        {"type": "INSERT", "table": "inventory", "data": {"item_id": 1, "qty": 10}},
        {"type": "INSERT", "table": "inventory", "data": {"item_id": 2, "qty": 20}},
        {
            "type": "SCHEMA_CHANGE",
            "table": "inventory",
            "schema": {
                "columns": {
                    "item_id": {"data_type": "INTEGER", "nullable": False},
                    "qty": {"data_type": "BIGINT", "nullable": True},
                    "location": {"data_type": "VARCHAR(50)", "nullable": True, "default": "WH-1"},
                }
            },
        },
        {"type": "INSERT", "table": "inventory", "data": {"item_id": 3, "qty": 9999999999, "location": "WH-2"}},
    ]

    out_stream = list(resolver.process_cdc_stream(stream))

    assert len(out_stream) == 4
    assert out_stream[0]["type"] == "INSERT"
    assert out_stream[1]["type"] == "INSERT"
    assert out_stream[2]["type"] == "SCHEMA_DRIFT_BARRIER"
    assert out_stream[2]["status"] == "RESOLVED"
    assert "NEW_COLUMN" in out_stream[2]["drifts"]
    assert "TYPE_WIDENING" in out_stream[2]["drifts"]
    assert "RELAXED_NULLABILITY" in out_stream[2]["drifts"]
    assert out_stream[3]["type"] == "INSERT"

    assert len(applier.applied_events) == 3
    assert applier.applied_events[2]["data"]["item_id"] == 3
    assert any('ALTER TABLE "inventory" ADD COLUMN "location"' in s for s in executed_ddl)
    assert any('ALTER TABLE "inventory" ALTER COLUMN "qty" TYPE BIGINT' in s for s in executed_ddl)
    assert any('ALTER TABLE "inventory" ALTER COLUMN "qty" DROP NOT NULL' in s for s in executed_ddl)


def test_barrier_rollback_on_failure_resumes_applier():
    cache = SchemaCache()
    cache.add_table("users", columns=[ColumnSchema(name="id", data_type="INT")])
    applier = CDCReplicationApplier()

    def failing_executor(sql: str):
        if "FAIL" in sql or "ADD COLUMN" in sql:
            raise RuntimeError("Database connection severed")
        return 0

    resolver = SchemaDriftResolver(
        schema_cache=cache,
        applier=applier,
        target_executor=failing_executor,
    )

    drifts = [
        DriftAction(
            drift_type=DriftType.NEW_COLUMN,
            table="users",
            column_name="metadata",
            new_type="TEXT",
        )
    ]

    with pytest.raises(SchemaDriftError) as exc_info:
        resolver.execute_barrier("users", drifts)

    assert "Database connection severed" in str(exc_info.value)
    assert applier.is_paused is False
    assert applier.pause_count == 1
    assert applier.resume_count == 1
    assert "metadata" not in cache.get_table("users").columns


def test_schema_evolution_rungs_ladder_climb():
    from migkit import decide
    from migkit.schema_drift import schema_evolution_rungs

    rungs = schema_evolution_rungs()
    assert len(rungs) == 3

    f_safe = decide.Facts({
        "has_drift": True,
        "all_inline_safe": True,
        "requires_backfill": False,
        "has_key": True,
        "incompatible": False,
    })
    c_safe = decide.climb("schema_evolution", rungs, facts=f_safe)
    assert c_safe.rung.name == "admit_inline_safe_evolution"

    f_backfill = decide.Facts({
        "has_drift": True,
        "all_inline_safe": False,
        "requires_backfill": True,
        "has_key": True,
        "incompatible": False,
    })
    c_backfill = decide.climb("schema_evolution", rungs, facts=f_backfill)
    assert c_backfill.rung.name == "admit_evolution_with_batched_backfill"

    f_incompat = decide.Facts({
        "has_drift": True,
        "all_inline_safe": False,
        "requires_backfill": False,
        "has_key": True,
        "incompatible": True,
    })
    c_incompat = decide.climb("schema_evolution", rungs, facts=f_incompat)
    assert c_incompat.rung.name == "refuse_incompatible_schema_drift"


def test_wire_drift_resolver_into_tailctl_apply_unit():
    from migkit import tailctl
    from migkit.schema_drift import SchemaDriftController

    executed_ddl = []
    cache = SchemaCache()
    cache.add_table("orders", columns=[
        ColumnSchema(name="id", data_type="BIGINT", is_primary_key=True),
        ColumnSchema(name="amount", data_type="INT"),
    ])

    resolver = SchemaDriftController(
        target_dialect="postgres",
        schema_cache=cache,
        target_executor=lambda sql: executed_ddl.append(sql),
    )

    applied_changes = []

    class MockEngine:
        def __init__(self):
            self.drift_resolver = resolver

        def neutral_key(self, side, db, table):
            return ["id"]

        def apply_columns(self, side, db, cols):
            raise tailctl.NotColumns("fallback to records")

    eng = MockEngine()
    changes = [
        {"op": "insert", "table": "orders", "key": {"id": 1}, "values": {"amount": 100}},
        {
            "type": "SCHEMA_CHANGE",
            "table": "orders",
            "schema": {
                "columns": {
                    "id": {"data_type": "BIGINT", "nullable": False},
                    "amount": {"data_type": "BIGINT", "nullable": True},
                    "channel": {"data_type": "VARCHAR(32)", "nullable": True},
                }
            },
        },
        {"op": "insert", "table": "orders", "key": {"id": 2}, "values": {"amount": 9999999999, "channel": "web"}},
    ]

    def mock_apply(records):
        applied_changes.extend(records)
        return len(records)

    tailctl.apply_unit(eng, "orders_db", {"pos": 100}, changes, mock_apply)

    assert len(applied_changes) == 3
    assert any('ALTER TABLE "orders" ADD COLUMN "channel"' in s for s in executed_ddl)
    assert any('ALTER TABLE "orders" ALTER COLUMN "amount" TYPE BIGINT' in s for s in executed_ddl)
    assert "channel" in cache.get_table("orders").columns


def test_batched_keyset_chunked_backfill_with_aimd_and_durable_resume(tmp_path):
    executed_sql = []

    def mock_executor(sql: str):
        executed_sql.append(sql)
        return 50

    chk_file = tmp_path / "backfill.checkpoint.json"
    orchestrator = BackfillOrchestrator(target_executor=mock_executor, async_mode=False)

    task = orchestrator.schedule_backfill(
        table="users",
        column_name="verified",
        default_value=True,
        key_column="id",
        batch_size=100,
        min_key=1,
        max_key=250,
        checkpoint_path=str(chk_file),
    )

    orchestrator.execute_task(task)

    assert task.status == "COMPLETED"
    assert task.rows_updated > 0
    assert len(executed_sql) >= 2
    assert any('WHERE "verified" IS NULL AND "id" >= 1 AND "id" < ' in s for s in executed_sql)
    assert chk_file.exists()

    task2 = orchestrator.schedule_backfill(
        table="users",
        column_name="verified",
        default_value=True,
        key_column="id",
        batch_size=100,
        min_key=1,
        max_key=250,
        checkpoint_path=str(chk_file),
    )
    executed_count_before = len(executed_sql)
    orchestrator.execute_task(task2)
    assert len(executed_sql) == executed_count_before
    assert task2.status == "COMPLETED"


def test_alter_inference_inspects_source_catalog_metadata():
    cache = SchemaCache()
    cache.add_table("accounts", columns=[
        ColumnSchema(name="id", data_type="BIGINT", is_primary_key=True),
        ColumnSchema(name="balance", data_type="INT", nullable=False),
    ])

    catalog_queries = []

    def mock_catalog_inspector(tbl: str):
        catalog_queries.append(tbl)
        return {
            "id": {"data_type": "BIGINT", "nullable": False, "default": None},
            "balance": {"data_type": "NUMERIC(18, 4)", "nullable": True, "default": None},
            "risk_score": {"data_type": "DECIMAL(5, 2)", "nullable": True, "default": "0.00"},
        }

    resolver = SchemaDriftResolver(
        schema_cache=cache,
        source_catalog_inspector=mock_catalog_inspector,
    )

    event = {
        "type": "INSERT",
        "table": "accounts",
        "data": {
            "id": 10,
            "balance": 500,
            "risk_score": 12.5,
        },
    }

    tbl, drifts = resolver.detect_drift_from_event(event)

    assert tbl == "accounts"
    assert len(catalog_queries) == 1
    assert catalog_queries[0] == "accounts"

    col_drifts = {d.column_name: d for d in drifts}
    assert "risk_score" in col_drifts
    assert col_drifts["risk_score"].drift_type == DriftType.NEW_COLUMN
    assert col_drifts["risk_score"].new_type == "DECIMAL(5, 2)"
    assert col_drifts["risk_score"].new_default == "0.00"
    assert col_drifts["balance"].drift_type == DriftType.TYPE_WIDENING
    assert col_drifts["balance"].new_type == "NUMERIC(18, 4)"


def test_replay_idempotency_strict_key_requirement():
    from migkit import canon

    with pytest.raises(ValueError) as exc:
        canon.change("insert", "tbl", key={}, values={"val": 10})
    assert "with no key cannot be applied" in str(exc.value)

    rec = canon.change("insert", "tbl", key={"id": 1}, values={"val": 10})
    assert rec["key"] == {"id": 1}
    assert rec["values"] == {"val": 10}

