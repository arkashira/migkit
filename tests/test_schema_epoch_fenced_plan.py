import pytest
from migkit.ddl_shield import (
    CDCReplicationApplier,
    ColumnSchema,
    TransactionalFence,
    VerificationSliceManager,
)
from migkit.schema_epoch_plan import (
    ActiveSnapshotContext,
    ColumnIdentity,
    CompatibilityType,
    CompatibilityVerdict,
    DDLCompatibilityMatrix,
    DDLOperation,
    FencedEpochPlan,
    SchemaEpochPlanner,
    SchemaEpochState,
    UnrepresentableDDLError,
    dialect_syntax_rungs,
    quote_ident_dialect,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ('schema_epoch_plan', 'dialect_syntax_rungs', 'ansi_quotes_dialect_syntax', 'holds'),
    ('schema_epoch_plan', 'dialect_syntax_rungs', 'ansi_quotes_dialect_syntax', 'refuses'),
    ('schema_epoch_plan', 'dialect_syntax_rungs', 'backtick_quotes_dialect_syntax', 'holds'),
    ('schema_epoch_plan', 'dialect_syntax_rungs', 'backtick_quotes_dialect_syntax', 'refuses'),
    ('schema_epoch_plan', 'dialect_syntax_rungs', 'unquoted_dialect_syntax', 'holds'),
]


def test_unrepresentable_ddl_error_raised_for_lossy_narrowing():
    matrix = DDLCompatibilityMatrix()

    with pytest.raises(UnrepresentableDDLError) as excinfo:
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="orders",
            column="amount",
            data_type="INT",
            old_data_type="BIGINT",
        )
        matrix.classify(op, current_columns={"amount": ColumnIdentity("col-1", "amount", 1, "BIGINT")}, strict=True)
    assert excinfo.value.table == "orders"
    assert excinfo.value.column == "amount"
    assert excinfo.value.source_type == "BIGINT"
    assert excinfo.value.target_type == "INT"

    with pytest.raises(UnrepresentableDDLError):
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="users",
            column="name",
            data_type="VARCHAR(20)",
            old_data_type="VARCHAR(100)",
        )
        matrix.classify(op, current_columns={"name": ColumnIdentity("col-2", "name", 1, "VARCHAR(100)")}, strict=True)

    with pytest.raises(UnrepresentableDDLError):
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="prices",
            column="rate",
            data_type="DECIMAL(10, 2)",
            old_data_type="DECIMAL(18, 4)",
        )
        matrix.classify(op, current_columns={"rate": ColumnIdentity("col-3", "rate", 1, "DECIMAL(18, 4)")}, strict=True)

    with pytest.raises(UnrepresentableDDLError):
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="metrics",
            column="val",
            data_type="FLOAT",
            old_data_type="DOUBLE PRECISION",
        )
        matrix.classify(op, current_columns={"val": ColumnIdentity("col-4", "val", 1, "DOUBLE PRECISION")}, strict=True)

    with pytest.raises(UnrepresentableDDLError):
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="docs",
            column="body",
            data_type="VARCHAR(255)",
            old_data_type="TEXT",
        )
        matrix.classify(op, current_columns={"body": ColumnIdentity("col-5", "body", 1, "TEXT")}, strict=True)

    with pytest.raises(UnrepresentableDDLError):
        op = DDLOperation(
            op_type="MODIFY_COLUMN",
            table="logs",
            column="payload",
            data_type="INT",
            old_data_type="VARCHAR(100)",
        )
        matrix.classify(op, current_columns={"payload": ColumnIdentity("col-6", "payload", 1, "VARCHAR(100)")}, strict=True)


def test_unrepresentable_ddl_on_not_null_without_default():
    matrix = DDLCompatibilityMatrix()
    op = DDLOperation(
        op_type="ADD_COLUMN",
        table="orders",
        column="status",
        data_type="VARCHAR(20)",
        nullable=False,
        default=None,
    )
    with pytest.raises(UnrepresentableDDLError) as excinfo:
        matrix.classify(op, current_columns={}, strict=True)
    assert "NOT NULL without default" in str(excinfo.value)


def test_unrepresentable_ddl_on_dropping_primary_key_without_replacement():
    matrix = DDLCompatibilityMatrix()
    op = DDLOperation(
        op_type="CHANGE_PK",
        table="orders",
        primary_key_columns=[],
        old_primary_key_columns=["id"],
    )
    with pytest.raises(UnrepresentableDDLError) as excinfo:
        matrix.classify(
            op,
            current_columns={"id": ColumnIdentity("col-id", "id", 1, "BIGINT", is_primary_key=True)},
            strict=True,
        )
    assert "primary key removed without replacement" in str(excinfo.value).lower()


def test_compatible_widening_planned():
    planner = SchemaEpochPlanner(target_dialect="postgres")
    planner.register_table(
        "accounts",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("balance", "INT", True),
        ],
        primary_key=["id"],
    )
    op = DDLOperation(
        op_type="MODIFY_COLUMN",
        table="accounts",
        column="balance",
        data_type="BIGINT",
    )
    plan = planner.plan_ddl(op)
    assert plan.verdict.is_compatible is True
    assert plan.verdict.compatibility_type in (CompatibilityType.BACKWARD_COMPATIBLE, CompatibilityType.BREAKING_FENCED)
    assert any("BIGINT" in s for s in plan.target_statements)
    executed = planner.apply_plan(plan)
    assert executed.status == "COMMITTED"
    state = planner.get_schema_at_epoch("accounts", executed.to_epoch)
    assert state.columns["balance"].data_type == "BIGINT"


def test_add_column_with_default_backfill_and_fencing():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("slice_orders_1", "orders", (1, 1000))
    slice_mgr.register_slice("slice_cust_1", "customers", (1, 500))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "orders",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("amount", "DECIMAL(10, 2)", False),
        ],
        primary_key=["id"],
    )

    op = DDLOperation(
        op_type="ADD_COLUMN",
        table="orders",
        column="priority",
        data_type="VARCHAR(20)",
        nullable=False,
        default="'NORMAL'",
    )
    plan = planner.plan_ddl(op)
    assert plan.verdict.requires_backfill is True
    assert plan.verdict.requires_fence is True
    assert len(plan.backfill_tasks) == 1
    assert plan.backfill_tasks[0].table == "orders"
    assert plan.backfill_tasks[0].default_value == "'NORMAL'"

    executed = planner.apply_plan(plan)
    assert executed.status == "COMMITTED"

    assert slice_mgr.get_slice("slice_orders_1").status == "INVALID"
    assert slice_mgr.get_slice("slice_cust_1").status == "VALID"
    assert "slice_orders_1" in executed.invalidated_evidence
    assert "slice_cust_1" not in executed.invalidated_evidence

    pre_ddl_row = {
        "table": "orders",
        "op": "INSERT",
        "epoch": 1,
        "data": {"id": 42, "amount": 19.99},
    }
    decoded = planner.decode_change(pre_ddl_row)
    assert decoded["data"]["priority"] == "'NORMAL'"
    assert decoded["data"]["id"] == 42


def test_column_rename_preserves_identity_and_translates_changes():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("slice_users_1", "users", (1, 100))
    slice_mgr.register_slice("slice_items_1", "items", (1, 200))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "users",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("user_email", "VARCHAR(255)", False),
        ],
        primary_key=["id"],
    )

    old_state = planner.get_schema_at_epoch("users", 1)
    original_col_id = old_state.columns["user_email"].column_id

    op = DDLOperation(
        op_type="RENAME_COLUMN",
        table="users",
        column="user_email",
        new_column_name="email",
    )
    plan = planner.plan_ddl(op)
    assert plan.verdict.is_compatible is True
    assert plan.from_epoch == 1
    assert plan.to_epoch == 2
    assert any("RENAME COLUMN" in s for s in plan.target_statements)

    executed = planner.apply_plan(plan)
    assert executed.status == "COMMITTED"

    new_state = planner.get_schema_at_epoch("users", 2)
    assert "email" in new_state.columns
    assert "user_email" not in new_state.columns
    assert new_state.columns["email"].column_id == original_col_id
    assert new_state.columns["email"].generation == 1

    assert slice_mgr.get_slice("slice_users_1").status == "INVALID"
    assert slice_mgr.get_slice("slice_items_1").status == "VALID"

    pre_rename_event = {
        "table": "users",
        "op": "INSERT",
        "epoch": 1,
        "data": {"id": 10, "user_email": "alice@example.com"},
    }
    decoded_pre = planner.decode_change(pre_rename_event)
    assert "email" in decoded_pre["data"]
    assert decoded_pre["data"]["email"] == "alice@example.com"
    assert "user_email" not in decoded_pre["data"]

    post_rename_event = {
        "table": "users",
        "op": "INSERT",
        "epoch": 2,
        "data": {"id": 11, "email": "bob@example.com"},
    }
    decoded_post = planner.decode_change(post_rename_event)
    assert decoded_post["data"]["email"] == "bob@example.com"


def test_drop_and_readd_same_name_increments_generation_and_fences():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("s_prod_1", "products", (1, 100))
    slice_mgr.register_slice("s_other_1", "other", (1, 100))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "products",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("meta", "VARCHAR(100)", True),
        ],
        primary_key=["id"],
    )

    state_e1 = planner.get_schema_at_epoch("products", 1)
    meta_gen1_id = state_e1.columns["meta"].column_id
    assert state_e1.columns["meta"].generation == 1

    drop_op = DDLOperation(
        op_type="DROP_COLUMN",
        table="products",
        column="meta",
    )
    plan_drop = planner.plan_ddl(drop_op)
    planner.apply_plan(plan_drop)
    state_e2 = planner.get_schema_at_epoch("products", 2)
    assert "meta" not in state_e2.columns

    slice_mgr.revalidate_slice("s_prod_1")
    assert slice_mgr.get_slice("s_prod_1").status == "VALID"

    readd_op = DDLOperation(
        op_type="ADD_COLUMN",
        table="products",
        column="meta",
        data_type="JSON",
        nullable=True,
    )
    plan_readd = planner.plan_ddl(readd_op)
    assert plan_readd.verdict.requires_fence is True
    planner.apply_plan(plan_readd)

    state_e3 = planner.get_schema_at_epoch("products", 3)
    meta_gen2 = state_e3.columns["meta"]
    assert meta_gen2.generation == 2
    assert meta_gen2.column_id != meta_gen1_id
    assert meta_gen2.data_type == "JSON"

    assert slice_mgr.get_slice("s_prod_1").status == "INVALID"
    assert slice_mgr.get_slice("s_other_1").status == "VALID"

    e1_change = {
        "table": "products",
        "op": "UPDATE",
        "epoch": 1,
        "data": {"id": 1, "meta": "raw_old_string"},
    }
    decoded_e1 = planner.decode_change(e1_change)
    assert decoded_e1["column_identities"]["meta"].generation == 1
    assert decoded_e1["data"]["meta"] is None or decoded_e1["is_dropped_generation"] is True

    e3_change = {
        "table": "products",
        "op": "UPDATE",
        "epoch": 3,
        "data": {"id": 1, "meta": {"sku": "A123"}},
    }
    decoded_e3 = planner.decode_change(e3_change)
    assert decoded_e3["column_identities"]["meta"].generation == 2
    assert decoded_e3["data"]["meta"] == {"sku": "A123"}


def test_changed_primary_key_requires_resync_and_invalidates_all_slices():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("s_ord_1", "orders", (1, 500))
    slice_mgr.register_slice("s_ord_2", "orders", (501, 1000))
    slice_mgr.register_slice("s_vend_1", "vendors", (1, 100))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "orders",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("tenant_id", "INT", False),
            ColumnSchema("total", "DECIMAL(10, 2)", False),
        ],
        primary_key=["id"],
    )

    pk_op = DDLOperation(
        op_type="CHANGE_PK",
        table="orders",
        primary_key_columns=["id", "tenant_id"],
    )
    plan = planner.plan_ddl(pk_op)
    assert plan.verdict.requires_resync is True
    assert plan.verdict.requires_fence is True
    assert plan.resync_required is True

    executed = planner.apply_plan(plan)
    assert executed.status == "COMMITTED"
    assert "s_ord_1" in executed.invalidated_evidence
    assert "s_ord_2" in executed.invalidated_evidence
    assert "s_vend_1" not in executed.invalidated_evidence
    assert slice_mgr.get_slice("s_ord_1").status == "INVALID"
    assert slice_mgr.get_slice("s_ord_2").status == "INVALID"
    assert slice_mgr.get_slice("s_vend_1").status == "VALID"

    new_state = planner.get_schema_at_epoch("orders", 2)
    assert new_state.primary_key == ["id", "tenant_id"]

    e1_change = {
        "table": "orders",
        "op": "UPDATE",
        "epoch": 1,
        "key": {"id": 100},
        "data": {"id": 100, "tenant_id": 5, "total": 45.00},
    }
    dec_e1 = planner.decode_change(e1_change)
    assert dec_e1["key_columns"] == ["id"]

    e2_change = {
        "table": "orders",
        "op": "UPDATE",
        "epoch": 2,
        "key": {"id": 100, "tenant_id": 5},
        "data": {"id": 100, "tenant_id": 5, "total": 50.00},
    }
    dec_e2 = planner.decode_change(e2_change)
    assert dec_e2["key_columns"] == ["id", "tenant_id"]


def test_concurrent_ddl_during_active_snapshot_maintains_coherence():
    planner = SchemaEpochPlanner(target_dialect="postgres")
    planner.register_table(
        "inventory",
        columns=[
            ColumnSchema("sku", "VARCHAR(64)", False, is_primary_key=True),
            ColumnSchema("stock", "INT", False),
        ],
        primary_key=["sku"],
    )

    snap = planner.start_snapshot("snap_inv_01", "inventory")
    assert snap.start_epoch == 1
    assert snap.status == "ACTIVE"

    concurrent_op = DDLOperation(
        op_type="ADD_COLUMN",
        table="inventory",
        column="warehouse_id",
        data_type="INT",
        nullable=False,
        default="1",
    )
    plan = planner.plan_ddl(concurrent_op)
    planner.apply_plan(plan)

    curr_state = planner.get_current_schema("inventory")
    assert curr_state.epoch == 2
    assert "warehouse_id" in curr_state.columns

    planner.record_concurrent_ddl("snap_inv_01", concurrent_op)
    assert len(snap.concurrent_ddls) == 1

    snapshot_row = {
        "table": "inventory",
        "op": "SNAPSHOT_ROW",
        "snapshot_id": "snap_inv_01",
        "data": {"sku": "SKU-999", "stock": 50},
    }
    decoded = planner.decode_snapshot_row(snapshot_row)
    assert decoded["data"]["sku"] == "SKU-999"
    assert decoded["data"]["warehouse_id"] == 1
    assert decoded["target_coherent"] is True

    planner.complete_snapshot("snap_inv_01")
    assert snap.status == "COMPLETED"


def test_restart_and_checkpoint_recovery_is_idempotent():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("sl_rec_1", "payments", (1, 100))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "payments",
        columns=[
            ColumnSchema("tx_id", "VARCHAR(64)", False, is_primary_key=True),
            ColumnSchema("amount", "DECIMAL(10, 2)", False),
        ],
        primary_key=["tx_id"],
    )

    op = DDLOperation(
        op_type="ADD_COLUMN",
        table="payments",
        column="currency",
        data_type="VARCHAR(3)",
        nullable=True,
    )
    plan = planner.plan_ddl(op)
    planner.apply_plan(plan)

    checkpoint = planner.checkpoint_state()
    assert checkpoint["tables"]["payments"]["current_epoch"] == 2

    restored_slice_mgr = VerificationSliceManager()
    restored_slice_mgr.register_slice("sl_rec_1", "payments", (1, 100))
    restored_planner = SchemaEpochPlanner.restore_checkpoint(checkpoint, slice_manager=restored_slice_mgr)

    assert restored_planner.get_current_schema("payments").epoch == 2
    assert "currency" in restored_planner.get_current_schema("payments").columns

    replayed = restored_planner.apply_plan(plan)
    assert replayed.status == "SKIPPED_ALREADY_APPLIED"
    assert restored_planner.get_current_schema("payments").epoch == 2


def test_fenced_execution_with_applier_pausing():
    applier = CDCReplicationApplier()
    executed_statements = []

    def mock_executor(sql: str):
        executed_statements.append(sql)

    planner = SchemaEpochPlanner(
        target_dialect="postgres",
        applier=applier,
        target_executor=mock_executor,
    )
    planner.register_table(
        "ledger",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("val", "INT", False),
        ],
        primary_key=["id"],
    )

    op = DDLOperation(
        op_type="MODIFY_COLUMN",
        table="ledger",
        column="val",
        data_type="BIGINT",
    )
    plan = planner.plan_ddl(op)
    fence = TransactionalFence(target_executor=mock_executor)

    res = planner.apply_plan(plan, fence=fence)
    assert res.status == "COMMITTED"
    assert applier.is_paused is False
    assert len(executed_statements) > 0
    assert any("BIGINT" in stmt for stmt in executed_statements)


def test_mysql_dialect_plan_and_execution():
    executed_statements = []

    def mock_executor(sql: str):
        executed_statements.append(sql)

    planner = SchemaEpochPlanner(target_dialect="mysql", target_executor=mock_executor)
    planner.register_table(
        "items",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("name", "VARCHAR(100)", False),
        ],
        primary_key=["id"],
    )

    op_add = DDLOperation(
        op_type="ADD_COLUMN",
        table="items",
        column="cost",
        data_type="DECIMAL(10, 2)",
        nullable=True,
    )
    plan_add = planner.plan_ddl(op_add)
    assert any("`items` ADD COLUMN `cost`" in s for s in plan_add.target_statements)
    planner.apply_plan(plan_add)

    op_rename = DDLOperation(
        op_type="RENAME_COLUMN",
        table="items",
        column="name",
        new_column_name="title",
    )
    plan_rename = planner.plan_ddl(op_rename)
    assert any("`items` RENAME COLUMN `name` TO `title`" in s for s in plan_rename.target_statements)
    planner.apply_plan(plan_rename)

    op_modify = DDLOperation(
        op_type="MODIFY_COLUMN",
        table="items",
        column="title",
        data_type="VARCHAR(255)",
    )
    plan_modify = planner.plan_ddl(op_modify)
    assert any("`items` MODIFY COLUMN `title`" in s for s in plan_modify.target_statements)
    planner.apply_plan(plan_modify)

    op_pk = DDLOperation(
        op_type="CHANGE_PK",
        table="items",
        primary_key_columns=["id", "title"],
    )
    plan_pk = planner.plan_ddl(op_pk)
    assert any("`items` DROP PRIMARY KEY" in s for s in plan_pk.target_statements)
    assert any("`items` ADD PRIMARY KEY (`id`, `title`)" in s for s in plan_pk.target_statements)
    planner.apply_plan(plan_pk)

    assert planner.get_current_schema("items").epoch == 5
    assert planner.get_current_schema("items").primary_key == ["id", "title"]


def test_sqlite_dialect_plan_and_execution():
    executed_statements = []

    def mock_executor(sql: str):
        executed_statements.append(sql)

    planner = SchemaEpochPlanner(target_dialect="sqlite", target_executor=mock_executor)
    planner.register_table(
        "notes",
        columns=[
            ColumnSchema("id", "INTEGER", False, is_primary_key=True),
            ColumnSchema("content", "TEXT", True),
        ],
        primary_key=["id"],
    )

    op = DDLOperation(
        op_type="ADD_COLUMN",
        table="notes",
        column="created_at",
        data_type="TEXT",
        nullable=True,
    )
    plan = planner.plan_ddl(op)
    assert any('"notes" ADD COLUMN "created_at"' in s for s in plan.target_statements)
    res = planner.apply_plan(plan)
    assert res.status == "COMMITTED"
    assert planner.get_current_schema("notes").epoch == 2


def test_multi_table_evidence_isolation():
    slice_mgr = VerificationSliceManager()
    slice_mgr.register_slice("s_orders_1", "orders", (1, 100))
    slice_mgr.register_slice("s_orders_2", "orders", (101, 200))
    slice_mgr.register_slice("s_cust_1", "customers", (1, 50))
    slice_mgr.register_slice("s_prod_1", "products", (1, 50))

    planner = SchemaEpochPlanner(target_dialect="postgres", slice_manager=slice_mgr)
    planner.register_table(
        "orders",
        columns=[ColumnSchema("id", "BIGINT", False, is_primary_key=True)],
        primary_key=["id"],
    )
    planner.register_table(
        "customers",
        columns=[ColumnSchema("id", "BIGINT", False, is_primary_key=True)],
        primary_key=["id"],
    )
    planner.register_table(
        "products",
        columns=[ColumnSchema("id", "BIGINT", False, is_primary_key=True)],
        primary_key=["id"],
    )

    op = DDLOperation(
        op_type="ADD_COLUMN",
        table="orders",
        column="note",
        data_type="TEXT",
        nullable=True,
    )
    plan = planner.plan_ddl(op)
    planner.apply_plan(plan)

    assert slice_mgr.get_slice("s_orders_1").status == "INVALID"
    assert slice_mgr.get_slice("s_orders_2").status == "INVALID"
    assert slice_mgr.get_slice("s_cust_1").status == "VALID"
    assert slice_mgr.get_slice("s_prod_1").status == "VALID"


def test_chained_multi_epoch_evolution_and_stream_decoding():
    planner = SchemaEpochPlanner(target_dialect="postgres")
    planner.register_table(
        "records",
        columns=[
            ColumnSchema("id", "BIGINT", False, is_primary_key=True),
            ColumnSchema("data_val", "VARCHAR(50)", False),
        ],
        primary_key=["id"],
    )

    op_e2 = DDLOperation(
        op_type="ADD_COLUMN",
        table="records",
        column="status",
        data_type="VARCHAR(20)",
        default="'DRAFT'",
        nullable=False,
    )
    planner.apply_plan(planner.plan_ddl(op_e2))

    op_e3 = DDLOperation(
        op_type="RENAME_COLUMN",
        table="records",
        column="data_val",
        new_column_name="payload",
    )
    planner.apply_plan(planner.plan_ddl(op_e3))

    op_e4 = DDLOperation(
        op_type="MODIFY_COLUMN",
        table="records",
        column="payload",
        data_type="VARCHAR(255)",
    )
    planner.apply_plan(planner.plan_ddl(op_e4))

    assert planner.get_current_schema("records").epoch == 4

    e1_change = {
        "table": "records",
        "op": "INSERT",
        "epoch": 1,
        "data": {"id": 1, "data_val": "initial"},
    }
    dec_e1 = planner.decode_change(e1_change)
    assert dec_e1["data"]["id"] == 1
    assert dec_e1["data"]["payload"] == "initial"
    assert dec_e1["data"]["status"] == "'DRAFT'"

    e3_change = {
        "table": "records",
        "op": "UPDATE",
        "epoch": 3,
        "data": {"id": 1, "payload": "renamed_data", "status": "'ACTIVE'"},
    }
    dec_e3 = planner.decode_change(e3_change)
    assert dec_e3["data"]["payload"] == "renamed_data"
    assert dec_e3["data"]["status"] == "'ACTIVE'"


def test_serialization_roundtrip_all_dataclasses():
    col_ident = ColumnIdentity(
        column_id="col_123",
        name="email",
        generation=2,
        data_type="VARCHAR(255)",
        nullable=False,
        default="'test'",
        is_primary_key=False,
        created_epoch=3,
        dropped_epoch=None,
    )
    col_ident_restored = ColumnIdentity.from_dict(col_ident.to_dict())
    assert col_ident_restored == col_ident

    ddl_op = DDLOperation(
        op_type="ADD_COLUMN",
        table="users",
        column="email",
        data_type="VARCHAR(255)",
        nullable=False,
        default="'test'",
        primary_key_columns=["id"],
    )
    ddl_op_restored = DDLOperation.from_dict(ddl_op.to_dict())
    assert ddl_op_restored.op_type == ddl_op.op_type
    assert ddl_op_restored.column == ddl_op.column

    verdict = CompatibilityVerdict(
        compatibility_type=CompatibilityType.REQUIRES_BACKFILL,
        is_compatible=True,
        requires_fence=True,
        requires_backfill=True,
        requires_slice_invalidation=True,
        requires_resync=False,
        affected_columns=["email"],
        reason="Needs backfill",
    )
    verdict_restored = CompatibilityVerdict.from_dict(verdict.to_dict())
    assert verdict_restored.compatibility_type == CompatibilityType.REQUIRES_BACKFILL
    assert verdict_restored.requires_backfill is True

    snap = ActiveSnapshotContext(
        snapshot_id="snap_1",
        table="users",
        start_epoch=2,
        concurrent_ddls=[ddl_op],
    )
    snap_restored = ActiveSnapshotContext.from_dict(snap.to_dict())
    assert snap_restored.snapshot_id == "snap_1"
    assert len(snap_restored.concurrent_ddls) == 1


def test_dialect_syntax_decide_rungs():
    with watched() as seen:
        q1 = quote_ident_dialect("users", "postgres")
        assert q1 == '"users"'
        q2 = quote_ident_dialect("users", "mysql")
        assert q2 == "`users`"
        q3 = quote_ident_dialect("users", "generic_plain")
        assert q3 == "users"

    assert set(branch(seen, "ansi_quotes_dialect_syntax")) == {HOLDS, REFUSES}
    assert set(branch(seen, "backtick_quotes_dialect_syntax")) == {HOLDS, REFUSES}
    assert branch(seen, "unquoted_dialect_syntax") == [HOLDS]


def test_tailctl_apply_unit_with_epoch_planner():
    from migkit import tailctl
    planner = SchemaEpochPlanner(target_dialect="sqlite")
    col = ColumnSchema("status", "INT", nullable=True)
    planner.register_table("orders", [col], ["id"])

    applied_stmts = []

    class FakeEngine:
        def __init__(self, p):
            self.epoch_planner = p
            self.name = "sqlite"

        def apply_columns(self, side, db, cols):
            return 1

    eng = FakeEngine(planner)
    op = DDLOperation(op_type="ADD_COLUMN", table="orders", column="note", data_type="TEXT")
    changes = [
        {"type": "DDL", "ddl_operation": op, "executor": lambda sql: applied_stmts.append(sql)},
    ]
    tailctl.apply_unit(eng, "main", 100, changes, lambda c: len(c))
    assert len(applied_stmts) == 1
    assert "ALTER TABLE orders ADD COLUMN note TEXT" in applied_stmts[0] or 'ALTER TABLE "orders" ADD COLUMN "note" TEXT' in applied_stmts[0]
    assert planner.get_current_epoch("orders") == 2

