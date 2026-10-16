import pytest

from migkit.ddl_shield import (
    CDCReplicationApplier,
    ColumnSchema,
    DDLBarrierError,
    DDLEvent,
    DDLTranslator,
    OnlineDDLShield,
    SchemaCache,
    ShadowTableInfo,
    TransactionalFence,
    VerificationSliceManager,
    parse_shadow_table,
)


def test_parse_shadow_table_ghost_and_ptosc():
    # gh-ost tables
    gho = parse_shadow_table("_users_gho")
    assert gho is not None
    assert gho.tool == "gh-ost"
    assert gho.role == "ghost"
    assert gho.base_table == "users"

    ghc = parse_shadow_table("_users_ghc")
    assert ghc is not None
    assert ghc.tool == "gh-ost"
    assert ghc.role == "changelog"
    assert ghc.base_table == "users"

    del_tbl = parse_shadow_table("_users_del")
    assert del_tbl is not None
    assert del_tbl.tool == "gh-ost"
    assert del_tbl.role == "tombstone"
    assert del_tbl.base_table == "users"

    dated_del = parse_shadow_table("_users_20261017_del")
    assert dated_del is not None
    assert dated_del.base_table == "users"

    # Standalone names
    assert parse_shadow_table("_gho").role == "ghost"
    assert parse_shadow_table("_ghc").role == "changelog"
    assert parse_shadow_table("_del").role == "tombstone"

    # pt-online-schema-change tables
    pt_new1 = parse_shadow_table("_users_new")
    assert pt_new1 is not None
    assert pt_new1.tool == "pt-osc"
    assert pt_new1.role == "new"
    assert pt_new1.base_table == "users"

    pt_new2 = parse_shadow_table("__users_new")
    assert pt_new2 is not None
    assert pt_new2.tool == "pt-osc"
    assert pt_new2.role == "new"
    assert pt_new2.base_table == "users"

    pt_old = parse_shadow_table("__users_old")
    assert pt_old is not None
    assert pt_old.role == "old"
    assert pt_old.base_table == "users"

    assert parse_shadow_table("__new").role == "new"
    assert parse_shadow_table("_new").role == "new"

    # Regular table
    assert parse_shadow_table("users") is None
    assert parse_shadow_table("orders_history") is None


def test_intercept_ddl_events():
    shield = OnlineDDLShield(source_dialect="mysql", target_dialect="postgres")

    # Regular DDL
    ev1 = shield.intercept_ddl("ALTER TABLE users MODIFY COLUMN age BIGINT NOT NULL")
    assert ev1.is_shadow is False
    assert ev1.table == "users"
    assert ev1.event_type == "ALTER"

    # gh-ost shadow DDL
    ev2 = shield.intercept_ddl("ALTER TABLE _users_gho ADD COLUMN email_verified BOOLEAN DEFAULT FALSE")
    assert ev2.is_shadow is True
    assert ev2.shadow_info is not None
    assert ev2.shadow_info.tool == "gh-ost"
    assert ev2.shadow_info.base_table == "users"

    # pt-osc shadow DDL
    ev3 = shield.intercept_ddl("ALTER TABLE __accounts_new MODIFY COLUMN balance DECIMAL(18, 4) NOT NULL")
    assert ev3.is_shadow is True
    assert ev3.shadow_info is not None
    assert ev3.shadow_info.tool == "pt-osc"
    assert ev3.shadow_info.base_table == "accounts"

    # gh-ost cutover rename
    cutover_sql = "RENAME TABLE `db`.`users` TO `db`.`_users_del`, `db`.`_users_gho` TO `db`.`users`;"
    ev4 = shield.intercept_ddl(cutover_sql)
    assert ev4.is_shadow is True
    assert ev4.event_type == "SHADOW_CUTOVER"
    assert ev4.shadow_info is not None
    assert ev4.shadow_info.is_cutover is True
    assert ev4.shadow_info.base_table == "users"
    assert ev4.shadow_info.shadow_table == "_users_gho"
    assert ev4.shadow_info.old_table == "_users_del"


def test_translate_mysql_modify_column_to_postgres():
    translator = DDLTranslator(source_dialect="mysql", target_dialect="postgres")

    # MODIFY COLUMN with type, not null, default
    sql = "ALTER TABLE users MODIFY COLUMN age BIGINT NOT NULL DEFAULT 18"
    tbl, stmts, actions = translator.translate(sql)
    assert tbl == "users"
    assert any('ALTER COLUMN "age" TYPE BIGINT USING "age"::BIGINT' in s for s in stmts)
    assert any('ALTER COLUMN "age" SET NOT NULL' in s for s in stmts)
    assert any('ALTER COLUMN "age" SET DEFAULT 18' in s for s in stmts)

    # MODIFY COLUMN tinyint(1) to boolean
    sql2 = "ALTER TABLE users MODIFY COLUMN is_active TINYINT(1) DEFAULT 1 NOT NULL"
    tbl2, stmts2, _ = translator.translate(sql2)
    assert any('ALTER COLUMN "is_active" TYPE BOOLEAN USING "is_active"::BOOLEAN' in s for s in stmts2)
    assert any('ALTER COLUMN "is_active" SET DEFAULT TRUE' in s for s in stmts2)
    assert any('ALTER COLUMN "is_active" SET NOT NULL' in s for s in stmts2)

    # MODIFY COLUMN drop not null
    sql3 = "ALTER TABLE users MODIFY COLUMN bio TEXT NULL"
    tbl3, stmts3, _ = translator.translate(sql3)
    assert any('ALTER COLUMN "bio" TYPE TEXT USING "bio"::TEXT' in s for s in stmts3)
    assert any('ALTER COLUMN "bio" DROP NOT NULL' in s for s in stmts3)


def test_translate_mysql_change_column_to_postgres():
    translator = DDLTranslator(source_dialect="mysql", target_dialect="postgres")

    sql = "ALTER TABLE users CHANGE COLUMN old_email email VARCHAR(255) NOT NULL"
    tbl, stmts, actions = translator.translate(sql)
    assert tbl == "users"
    assert any('RENAME COLUMN "old_email" TO "email"' in s for s in stmts)
    assert any('ALTER COLUMN "email" TYPE VARCHAR(255) USING "email"::VARCHAR(255)' in s for s in stmts)
    assert any('ALTER COLUMN "email" SET NOT NULL' in s for s in stmts)


def test_translate_mysql_add_column_strips_position_and_maps_auto_increment():
    translator = DDLTranslator(source_dialect="mysql", target_dialect="postgres")

    # AFTER clause stripped
    sql = "ALTER TABLE users ADD COLUMN phone VARCHAR(20) NOT NULL AFTER email"
    tbl, stmts, actions = translator.translate(sql)
    assert tbl == "users"
    assert len(stmts) == 1
    assert 'ALTER TABLE "users" ADD COLUMN "phone" VARCHAR(20) NOT NULL' in stmts[0]
    assert "AFTER" not in stmts[0]

    # AUTO_INCREMENT converted to IDENTITY
    sql2 = "ALTER TABLE orders ADD COLUMN order_id BIGINT AUTO_INCREMENT PRIMARY KEY"
    tbl2, stmts2, actions2 = translator.translate(sql2)
    assert tbl2 == "orders"
    assert "GENERATED BY DEFAULT AS IDENTITY" in stmts2[0]
    assert "PRIMARY KEY" in stmts2[0]


def test_translate_postgres_to_mysql():
    translator = DDLTranslator(source_dialect="postgres", target_dialect="mysql")

    sql = 'ALTER TABLE "users" ALTER COLUMN "age" TYPE BIGINT'
    tbl, stmts, _ = translator.translate(sql)
    assert tbl == "users"
    assert any("ALTER TABLE `users` MODIFY COLUMN `age` BIGINT" in s for s in stmts)


def test_ddl_barrier_pauses_applier_executes_fence_and_resumes():
    executed_sql = []

    def mock_executor(sql):
        executed_sql.append(sql)

    applier = CDCReplicationApplier()
    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        applier=applier,
        target_executor=mock_executor,
    )

    # Initial state
    assert applier.is_paused is False
    assert applier.pause_count == 0
    assert applier.resume_count == 0

    res = shield.apply_with_barrier("ALTER TABLE users MODIFY COLUMN age BIGINT NOT NULL")

    assert res.status == "COMMITTED"
    assert res.affected_table == "users"
    assert applier.is_paused is False
    assert applier.pause_count == 1
    assert applier.resume_count == 1

    # Verify transactional fence statements
    assert executed_sql[0] == "BEGIN"
    assert any('ALTER COLUMN "age" TYPE BIGINT' in s for s in executed_sql)
    assert executed_sql[-1] == "COMMIT"


def test_ddl_barrier_rollback_on_failure():
    executed_sql = []

    def failing_executor(sql):
        executed_sql.append(sql)
        if "ERROR_TRIGGER" in sql:
            raise RuntimeError("Database error during DDL execution")

    applier = CDCReplicationApplier()
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("s1", "users", (0, 100))

    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        applier=applier,
        target_executor=failing_executor,
        slice_manager=slice_mgr,
    )

    # Trigger failure
    failing_sql = "ALTER TABLE users ADD COLUMN ERROR_TRIGGER INT"
    with pytest.raises(DDLBarrierError) as exc_info:
        shield.apply_with_barrier(failing_sql)

    assert "Database error during DDL execution" in str(exc_info.value)
    # Applier must be resumed safely
    assert applier.is_paused is False
    assert applier.pause_count == 1
    assert applier.resume_count == 1

    # Fence must have called ROLLBACK
    assert "ROLLBACK" in executed_sql
    assert "COMMIT" not in executed_sql

    # Verification slices should NOT be invalidated on failure
    assert slice_mgr.get_slice("s1").status == "VALID"


def test_schema_cache_updates_accurately():
    cache = SchemaCache()
    cache.add_table(
        "users",
        columns=[
            ColumnSchema(name="id", data_type="INTEGER", is_primary_key=True),
            ColumnSchema(name="age", data_type="INTEGER", nullable=True),
        ],
        primary_key=["id"],
    )

    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        schema_cache=cache,
        target_executor=lambda sql: None,
    )

    # 1. Modify column
    shield.apply_with_barrier("ALTER TABLE users MODIFY COLUMN age BIGINT NOT NULL")
    tbl = cache.get_table("users")
    assert tbl.columns["age"].data_type == "BIGINT"
    assert tbl.columns["age"].nullable is False

    # 2. Add column
    shield.apply_with_barrier("ALTER TABLE users ADD COLUMN email VARCHAR(255) DEFAULT 'none'")
    assert "email" in tbl.columns
    assert tbl.columns["email"].data_type == "VARCHAR(255)"

    # 3. Rename column
    shield.apply_with_barrier("ALTER TABLE users CHANGE COLUMN email user_email VARCHAR(255)")
    assert "email" not in tbl.columns
    assert "user_email" in tbl.columns

    # 4. Drop column
    shield.apply_with_barrier("ALTER TABLE users DROP COLUMN user_email")
    assert "user_email" not in tbl.columns

    # 5. Drop table
    shield.apply_with_barrier("DROP TABLE users")
    assert cache.has_table("users") is False


def test_selective_invalidation_of_verification_slices():
    slice_mgr = VerificationSliceManager()

    # Register slices across different tables
    for i in range(5):
        slice_mgr.register_slice(f"users_{i}", "users", (i * 1000, (i + 1) * 1000), merkle_root=f"root_u_{i}")

    for i in range(10):
        slice_mgr.register_slice(f"orders_{i}", "orders", (i * 1000, (i + 1) * 1000), merkle_root=f"root_o_{i}")

    for i in range(4):
        slice_mgr.register_slice(f"items_{i}", "items", (i * 1000, (i + 1) * 1000), merkle_root=f"root_i_{i}")

    assert slice_mgr.total_slices() == 19
    assert slice_mgr.invalid_count() == 0

    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        slice_manager=slice_mgr,
        target_executor=lambda sql: None,
    )

    # Mutate table 'users'
    res = shield.apply_with_barrier("ALTER TABLE users ADD COLUMN nickname VARCHAR(50)")

    # Exactly the 5 slices of 'users' must be invalidated
    assert len(res.invalidated_slices) == 5
    assert set(res.invalidated_slices) == {f"users_{i}" for i in range(5)}

    invalid_users = slice_mgr.get_invalid_slices("users")
    assert len(invalid_users) == 5
    for s in invalid_users:
        assert s.status == "INVALID"
        assert s.invalidated_at is not None
        assert "DDL_MUTATION" in s.invalidation_reason

    # ALL slices of 'orders' and 'items' must remain VALID
    assert len(slice_mgr.get_invalid_slices("orders")) == 0
    assert len(slice_mgr.get_valid_slices("orders")) == 10

    assert len(slice_mgr.get_invalid_slices("items")) == 0
    assert len(slice_mgr.get_valid_slices("items")) == 4

    # Revalidate one slice
    slice_mgr.revalidate_slice("users_0", new_merkle_root="new_root_0")
    assert slice_mgr.get_slice("users_0").status == "VALID"
    assert slice_mgr.get_slice("users_0").merkle_root == "new_root_0"
    assert len(slice_mgr.get_invalid_slices("users")) == 4


def test_ghost_shadow_table_intercept_map_and_cutover():
    executed_statements = []

    def executor(sql):
        executed_statements.append(sql)

    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("u1", "users", (0, 500))
    slice_mgr.register_slice("o1", "orders", (0, 500))

    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        slice_manager=slice_mgr,
        target_executor=executor,
        shadow_mode="intercept_and_map",
        suppress_auxiliary_shadow_tables=True,
    )

    # Step 1: DDL on gh-ost shadow table `_users_gho`
    # In 'intercept_and_map' mode, it targets `users` on target
    res1 = shield.apply_with_barrier("ALTER TABLE _users_gho ADD COLUMN status VARCHAR(20) DEFAULT 'active'")
    assert res1.affected_table == "users"
    assert any('ALTER TABLE "users" ADD COLUMN "status"' in s for s in executed_statements)
    assert slice_mgr.get_slice("u1").status == "INVALID"
    assert slice_mgr.get_slice("o1").status == "VALID"

    # Step 2: Auxiliary shadow table DDL (e.g. drop changelog `_users_ghc`)
    # Should be SKIPPED on target
    executed_statements.clear()
    res2 = shield.apply_with_barrier("DROP TABLE _users_ghc")
    assert res2.status == "SKIPPED"
    assert len(executed_statements) == 0

    # Step 3: gh-ost cutover rename
    slice_mgr.revalidate_slice("u1")
    assert slice_mgr.get_slice("u1").status == "VALID"

    cutover_sql = "RENAME TABLE `app`.`users` TO `app`.`_users_del`, `app`.`_users_gho` TO `app`.`users`;"
    res3 = shield.apply_with_barrier(cutover_sql)
    assert res3.affected_table == "users"
    assert slice_mgr.get_slice("u1").status == "INVALID"


def test_cdc_stream_pipeline_processing():
    applied_dml = []
    executed_ddl = []

    class MockApplier(CDCReplicationApplier):
        def apply_dml(self, event):
            super().apply_dml(event)
            applied_dml.append(event)

    applier = MockApplier()
    shield = OnlineDDLShield(
        source_dialect="mysql",
        target_dialect="postgres",
        applier=applier,
        target_executor=lambda sql: executed_ddl.append(sql),
    )

    stream = [
        {"type": "INSERT", "table": "users", "data": {"id": 1, "name": "Alice"}},
        {"type": "INSERT", "table": "users", "data": {"id": 2, "name": "Bob"}},
        {"type": "DDL", "sql": "ALTER TABLE users ADD COLUMN phone VARCHAR(20) NOT NULL AFTER name"},
        {"type": "INSERT", "table": "users", "data": {"id": 3, "name": "Charlie", "phone": "12345"}},
    ]

    processed = list(shield.process_cdc_stream(stream))

    assert len(processed) == 4
    assert processed[0]["type"] == "INSERT"
    assert processed[1]["type"] == "INSERT"
    assert processed[2]["type"] == "DDL_BARRIER"
    assert processed[2]["status"] == "COMMITTED"
    assert processed[3]["type"] == "INSERT"

    assert len(applied_dml) == 3
    assert any('ALTER TABLE "users" ADD COLUMN "phone"' in s for s in executed_ddl)


def test_selective_invalidation_by_key_range():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("slice_low", "users", key_range=(0, 1000))
    slice_mgr.register_slice("slice_mid", "users", key_range=(1000, 2000))
    slice_mgr.register_slice("slice_high", "users", key_range=(2000, 3000))
    slice_mgr.register_slice("slice_orders", "orders", key_range=(0, 1000))

    # Invalidate range (500, 800) for users: should only invalidate slice_low
    invalidated = slice_mgr.invalidate_table("users", key_range=(500, 800))
    assert invalidated == ["slice_low"]

    assert slice_mgr.get_slice("slice_low").status == "INVALID"
    assert slice_mgr.get_slice("slice_mid").status == "VALID"
    assert slice_mgr.get_slice("slice_high").status == "VALID"
    assert slice_mgr.get_slice("slice_orders").status == "VALID"


def test_create_table_and_index_translation():
    translator = DDLTranslator(source_dialect="mysql", target_dialect="postgres")

    create_sql = "CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, title VARCHAR(100) NOT NULL, price DECIMAL(10, 2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    tbl, stmts, _ = translator.translate(create_sql)
    assert tbl == "products"
    assert len(stmts) == 1
    assert "ENGINE=InnoDB" not in stmts[0]
    assert "CHARSET" not in stmts[0]


def test_transactional_fence_with_mock_conn():
    class MockCursor:
        def __init__(self, executed):
            self.executed = executed

        def execute(self, sql):
            self.executed.append(sql)

    class MockConn:
        def __init__(self):
            self.autocommit = True
            self.commits = 0
            self.rollbacks = 0
            self.executed = []

        def cursor(self):
            return MockCursor(self.executed)

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    conn = MockConn()
    fence = TransactionalFence(conn=conn)

    with fence:
        fence.execute('ALTER TABLE "users" ADD COLUMN "note" TEXT')

    assert conn.autocommit is False
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert 'ALTER TABLE "users" ADD COLUMN "note" TEXT' in conn.executed

    # Test rollback
    conn2 = MockConn()
    fence2 = TransactionalFence(conn=conn2)
    try:
        with fence2:
            fence2.execute("STMT1")
            raise ValueError("boom")
    except ValueError:
        pass

    assert conn2.commits == 0
    assert conn2.rollbacks == 1


def test_schema_cache_clone_and_isolation():
    cache = SchemaCache()
    cache.add_table(
        "users",
        columns=[ColumnSchema(name="id", data_type="INTEGER", is_primary_key=True)],
        primary_key=["id"],
    )

    clone = cache.clone()
    clone.add_table("orders", columns=[ColumnSchema(name="id", data_type="BIGINT")])

    assert clone.has_table("orders") is True
    assert cache.has_table("orders") is False

