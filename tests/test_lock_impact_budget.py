import time
import pytest

from migkit.locks import (
    LockBehavior,
    LockBudget,
    LockBudgetExceeded,
    LockContentionError,
    LockWaitPrediction,
    ScopeViolationError,
    check_scope,
    execute_plan_with_budget,
    execute_with_lock_budget,
    extract_table,
    live_workload_probe_sql,
    lock_timeout_sql,
    parse_version,
    plan_expand_backfill_contract,
    predict_lock_wait,
    probe_live_locks,
    probe_lock_behavior,
    lock_impact_rungs,
    decide_lock_impact,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("locks", "lock_impact_rungs", "immediate_execution_within_budget", "holds"),
    ("locks", "lock_impact_rungs", "immediate_execution_within_budget", "refuses"),
    ("locks", "lock_impact_rungs", "bounded_backoff_and_retry", "holds"),
    ("locks", "lock_impact_rungs", "bounded_backoff_and_retry", "refuses"),
    ("locks", "lock_impact_rungs", "expand_contract_phased_execution", "holds"),
    ("locks", "lock_impact_rungs", "expand_contract_phased_execution", "refuses"),
    ("locks", "lock_impact_rungs", "quarantine_and_defer_to_maintenance_window", "holds"),
    ("locks", "lock_impact_rungs", "quarantine_and_defer_to_maintenance_window", "refuses"),
    ("locks", "lock_impact_rungs", "refuse_unsafe_lock_contention", "holds"),
]


def test_parse_version():
    assert parse_version("14.5") == (14, 5)
    assert parse_version("8.0.32-commercial") == (8, 0, 32)
    assert parse_version((16, 1)) == (16, 1)
    assert parse_version(15) == (15,)
    assert parse_version(None) is None


def test_extract_table():
    assert extract_table("ALTER TABLE orders ADD COLUMN x int;") == "orders"
    assert extract_table('ALTER TABLE ONLY "public"."orders" DROP COLUMN x;') == "public.orders"
    assert extract_table("CREATE INDEX idx ON orders (x);") == "orders"
    assert extract_table("DROP TABLE if exists archived_orders;") == "archived_orders"
    assert extract_table("TRUNCATE TABLE staging;") == "staging"


def test_scope_authorization():
    check_scope("orders", authorized_scope=["orders", "items"])
    check_scope("public.orders", authorized_scope=["orders"])
    check_scope("orders", authorized_scope=None)

    with pytest.raises(ScopeViolationError):
        check_scope("users", authorized_scope=["orders", "items"])


def test_lock_timeout_sql():
    assert lock_timeout_sql(2.0, "postgres") == "SET LOCAL lock_timeout = '2000ms';"
    assert lock_timeout_sql(2.0, "mysql") == "SET SESSION lock_wait_timeout = 2;"
    assert lock_timeout_sql(0.5, "postgres") == "SET LOCAL lock_timeout = '500ms';"


def test_pg_add_column_volatile_default_rewrites():
    b1 = probe_lock_behavior("ALTER TABLE t ADD COLUMN c timestamp DEFAULT now();", "postgres", version=(14, 0))
    assert b1.table_rewrite is True
    assert b1.lock_mode == "AccessExclusiveLock"
    assert b1.blocks_reads is True
    assert b1.blocks_writes is True

    b2 = probe_lock_behavior("ALTER TABLE t ADD COLUMN c uuid DEFAULT gen_random_uuid();", "postgres", version=(16, 0))
    assert b2.table_rewrite is True


def test_pg_add_column_constant_default_version_behavior():
    b_old = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int DEFAULT 0;", "postgres", version=(10, 0))
    assert b_old.table_rewrite is True

    b_new = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int DEFAULT 0;", "postgres", version=(11, 0))
    assert b_new.table_rewrite is False
    assert b_new.metadata_only is True

    b_none = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int;", "postgres", version=(10, 0))
    assert b_none.table_rewrite is False


def test_pg_alter_column_type_behavior():
    b_type = probe_lock_behavior("ALTER TABLE t ALTER COLUMN c TYPE bigint;", "postgres", version=(14, 0))
    assert b_type.table_rewrite is True
    assert b_type.blocks_writes is True
    assert "expand/backfill/contract" in b_type.safe_alternative

    b_varchar = probe_lock_behavior("ALTER TABLE t ALTER COLUMN c TYPE varchar(100);", "postgres", version=(14, 0))
    assert b_varchar.table_rewrite is False
    assert b_varchar.metadata_only is True


def test_pg_set_not_null_version_behavior():
    b11 = probe_lock_behavior("ALTER TABLE t ALTER COLUMN c SET NOT NULL;", "postgres", version=(11, 0))
    assert b11.requires_validation is True

    b12 = probe_lock_behavior("ALTER TABLE t ALTER COLUMN c SET NOT NULL;", "postgres", version=(12, 0))
    assert b12.metadata_only is True
    assert b12.requires_validation is False


def test_pg_index_and_constraint_probes():
    b_idx_c = probe_lock_behavior("CREATE INDEX CONCURRENTLY idx ON t (a);", "postgres")
    assert b_idx_c.lock_mode == "ShareUpdateExclusiveLock"
    assert b_idx_c.blocks_reads is False
    assert b_idx_c.blocks_writes is False

    b_idx = probe_lock_behavior("CREATE INDEX idx ON t (a);", "postgres")
    assert b_idx.lock_mode == "ShareLock"
    assert b_idx.blocks_writes is True
    assert "CONCURRENTLY" in b_idx.safe_alternative

    b_fk_nv = probe_lock_behavior("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (b_id) REFERENCES b (id) NOT VALID;", "postgres")
    assert b_fk_nv.metadata_only is True
    assert b_fk_nv.requires_validation is True

    b_fk = probe_lock_behavior("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (b_id) REFERENCES b (id);", "postgres")
    assert b_fk.requires_validation is True
    assert "NOT VALID" in b_fk.safe_alternative

    b_val = probe_lock_behavior("ALTER TABLE t VALIDATE CONSTRAINT fk;", "postgres")
    assert b_val.lock_mode == "ShareUpdateExclusiveLock"
    assert b_val.blocks_writes is False


def test_mysql_operation_and_version_probes():
    b_add_57 = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int;", "mysql", version=(5, 7, 30))
    assert b_add_57.algorithm == "INPLACE"
    assert b_add_57.table_rewrite is True

    b_add_80 = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int;", "mysql", version=(8, 0, 28))
    assert b_add_80.algorithm == "INSTANT"
    assert b_add_80.table_rewrite is False
    assert b_add_80.metadata_only is True

    b_drop_57 = probe_lock_behavior("ALTER TABLE t DROP COLUMN c;", "mysql", version=(5, 7, 30))
    assert b_drop_57.table_rewrite is True

    b_drop_8029 = probe_lock_behavior("ALTER TABLE t DROP COLUMN c;", "mysql", version=(8, 0, 29))
    assert b_drop_8029.algorithm == "INSTANT"
    assert b_drop_8029.table_rewrite is False

    b_mod = probe_lock_behavior("ALTER TABLE t MODIFY COLUMN c bigint;", "mysql", version=(8, 0, 28))
    assert b_mod.algorithm == "COPY"
    assert b_mod.lock_mode == "SHARED"
    assert b_mod.blocks_writes is True
    assert "expand/backfill/contract" in b_mod.safe_alternative

    b_idx = probe_lock_behavior("CREATE INDEX idx ON t (c);", "mysql")
    assert b_idx.algorithm == "INPLACE"
    assert b_idx.lock_mode == "NONE"
    assert b_idx.blocks_writes is False


def test_dynamic_probe_with_runner():
    def pg_runner(sql):
        if "SHOW server_version_num" in sql:
            return [("150002",)]
        return []

    b_pg = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int DEFAULT 10;", "postgres", runner=pg_runner)
    assert b_pg.metadata_only is True
    assert b_pg.table_rewrite is False

    def mysql_runner(sql):
        if "SELECT VERSION()" in sql:
            return [("8.0.31",)]
        return []

    b_my = probe_lock_behavior("ALTER TABLE t ADD COLUMN c int;", "mysql", runner=mysql_runner)
    assert b_my.algorithm == "INSTANT"
    assert b_my.table_rewrite is False


def test_expand_backfill_contract_plan_type_change():
    stmt = "ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12, 2);"
    plan = plan_expand_backfill_contract(stmt, engine="postgres", authorized_scope=["orders"])
    assert plan.table == "orders"
    assert len(plan.steps) == 4

    expand = plan.steps[0]
    assert expand.phase == "expand"
    assert "ADD COLUMN amount_migkit_new numeric(12, 2)" in expand.sql
    assert "DROP COLUMN IF EXISTS amount_migkit_new" in expand.rollback_sql

    backfill = plan.steps[1]
    assert backfill.phase == "backfill"
    assert backfill.is_batched is True

    contract = plan.steps[2]
    assert contract.phase == "contract"
    assert "RENAME COLUMN amount TO amount_migkit_old" in contract.sql

    cleanup = plan.steps[3]
    assert cleanup.phase == "cleanup"
    assert "DROP COLUMN amount_migkit_old" in cleanup.sql


def test_expand_backfill_contract_plan_foreign_key():
    stmt = "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id);"
    plan = plan_expand_backfill_contract(stmt, engine="postgres")
    assert len(plan.steps) == 2
    assert "NOT VALID" in plan.steps[0].sql
    assert plan.steps[0].phase == "expand"
    assert plan.steps[1].phase == "backfill"
    assert "VALIDATE CONSTRAINT fk_user" in plan.steps[1].sql


def test_expand_backfill_contract_plan_check_constraint():
    stmt = "ALTER TABLE accounts ADD CONSTRAINT chk_bal CHECK (balance >= 0);"
    plan = plan_expand_backfill_contract(stmt, engine="postgres")
    assert len(plan.steps) == 2
    assert "NOT VALID" in plan.steps[0].sql
    assert "VALIDATE CONSTRAINT chk_bal" in plan.steps[1].sql


def test_expand_backfill_contract_plan_index():
    stmt = "CREATE INDEX idx_orders_cust ON orders (customer_id);"
    plan_pg = plan_expand_backfill_contract(stmt, engine="postgres")
    assert "CONCURRENTLY" in plan_pg.steps[0].sql

    plan_my = plan_expand_backfill_contract(stmt, engine="mysql")
    assert "ALGORITHM=INPLACE LOCK=NONE" in plan_my.steps[0].sql


def test_plan_rejects_unauthorized_scope():
    stmt = "ALTER TABLE secret_keys ALTER COLUMN k TYPE text;"
    with pytest.raises(ScopeViolationError):
        plan_expand_backfill_contract(stmt, engine="postgres", authorized_scope=["orders", "items"])


def test_predict_lock_wait_zero_when_idle():
    workload = []
    pred = predict_lock_wait(workload, target_table="orders", operation="ALTER TABLE orders ADD COLUMN x int;")
    assert pred.predicted_wait_seconds == 0.0
    assert pred.queue_depth == 0
    assert pred.exceeds_budget is False
    assert pred.recommendation == "proceed"


def test_predict_lock_wait_ignores_unrelated_tables():
    workload = [
        {"table": "audit_log", "query": "INSERT INTO audit_log VALUES (1);", "duration": 4.5, "waiting": False}
    ]
    pred = predict_lock_wait(workload, target_table="orders", operation="ALTER TABLE orders ADD COLUMN x int;")
    assert pred.predicted_wait_seconds == 0.0
    assert pred.conflicting_transactions == []


def test_predict_lock_wait_detects_conflicts_and_budget_exceeded():
    workload = [
        {"table": "orders", "query": "UPDATE orders SET status = 1;", "duration": 3.8, "waiting": False, "lock_mode": "RowExclusiveLock"},
        {"table": "orders", "query": "SELECT * FROM orders FOR UPDATE;", "duration": 1.2, "waiting": True, "lock_mode": "RowShareLock"},
    ]
    budget = LockBudget(max_lock_timeout=2.0)
    pred = predict_lock_wait(workload, target_table="orders", operation="ALTER TABLE orders ADD COLUMN x int;", budget=budget)
    assert pred.predicted_wait_seconds >= 3.8
    assert pred.queue_depth == 1
    assert pred.exceeds_budget is True
    assert pred.recommendation in ("defer", "abort")


def test_execute_with_lock_budget_success():
    sent = []

    def mock_runner(sql):
        sent.append(sql)
        return "OK"

    b = LockBudget(max_lock_timeout=2.0, max_retries=3)
    res = execute_with_lock_budget(mock_runner, "ALTER TABLE orders ADD COLUMN c int;", budget=b, engine="postgres")
    assert res["status"] == "applied"
    assert res["attempts"] == 1
    assert any("lock_timeout = '2000ms'" in s for s in sent)
    assert any("ADD COLUMN c int" in s for s in sent)


def test_execute_with_lock_budget_retries_and_recovers_on_contention():
    calls = []
    sleeps = []

    def mock_runner(sql):
        calls.append(sql)
        if "ADD COLUMN" in sql:
            if len([c for c in calls if "ADD COLUMN" in c]) < 3:
                raise LockContentionError("canceling statement due to lock timeout: 55P03")
        return "OK"

    b = LockBudget(max_lock_timeout=2.0, max_retries=5, initial_backoff=0.01, backoff_factor=2.0)
    res = execute_with_lock_budget(
        mock_runner,
        "ALTER TABLE orders ADD COLUMN c int;",
        budget=b,
        engine="postgres",
        sleep_fn=lambda s: sleeps.append(s),
    )
    assert res["status"] == "applied"
    assert res["attempts"] == 3
    assert len(sleeps) == 2
    assert any("ROLLBACK" in s for s in calls)


def test_execute_with_lock_budget_aborts_when_retries_exhausted():
    calls = []

    def failing_runner(sql):
        calls.append(sql)
        if "ADD COLUMN" in sql:
            raise Exception("ERROR: 55P03 lock_not_available")
        return "OK"

    b = LockBudget(max_lock_timeout=2.0, max_retries=2, initial_backoff=0.001)
    with pytest.raises(LockBudgetExceeded) as exc_info:
        execute_with_lock_budget(
            failing_runner,
            "ALTER TABLE orders ADD COLUMN c int;",
            budget=b,
            engine="postgres",
            sleep_fn=lambda _: None,
        )
    assert "max retries (2) exhausted" in str(exc_info.value)
    assert any("ROLLBACK" in s for s in calls)


def test_execute_with_lock_budget_non_lock_error_raises_immediately():
    calls = []

    def syntax_error_runner(sql):
        calls.append(sql)
        if "BAD DDL" in sql:
            raise ValueError("syntax error at or near BAD")
        return "OK"

    b = LockBudget(max_retries=3)
    with pytest.raises(ValueError):
        execute_with_lock_budget(
            syntax_error_runner,
            "BAD DDL;",
            budget=b,
            engine="postgres",
            sleep_fn=lambda _: None,
        )
    assert len([c for c in calls if "BAD DDL" in c]) == 1


def test_execute_plan_with_budget_rollback_on_partial_failure():
    history = []

    def mock_runner(sql):
        history.append(sql)
        if "UPDATE orders SET" in sql:
            raise LockContentionError("canceling statement due to lock timeout")
        return "OK"

    stmt = "ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12, 2);"
    plan = plan_expand_backfill_contract(stmt, engine="postgres")

    b = LockBudget(max_retries=1, initial_backoff=0.001)
    with pytest.raises(LockBudgetExceeded):
        execute_plan_with_budget(
            mock_runner,
            plan,
            budget=b,
            engine="postgres",
            sleep_fn=lambda _: None,
        )

    assert any("amount_migkit_new numeric(12, 2)" in h for h in history)
    assert any("DROP COLUMN IF EXISTS amount_migkit_new" in h for h in history)


def test_live_probe_sql_generation():
    pg_sql = live_workload_probe_sql("postgres", table="orders")
    assert "pg_stat_activity" in pg_sql
    assert "orders" in pg_sql

    my_sql = live_workload_probe_sql("mysql")
    assert "innodb_trx" in my_sql


def test_probe_live_locks_parsing():
    def mock_pg_live(sql):
        return [(1234, "UPDATE orders SET x = 1", 3.2, "lock", "relation", "RowExclusiveLock", "orders", True)]

    locks = probe_live_locks(mock_pg_live, engine="postgres", table="orders")
    assert len(locks) == 1
    assert locks[0]["pid"] == 1234
    assert locks[0]["lock_mode"] == "RowExclusiveLock"
    assert locks[0]["duration"] == 3.2
    assert locks[0]["waiting"] is False


def test_lock_budget_from_facts():
    facts = {
        "max_lock_timeout": 1.5,
        "max_retries": 4,
        "initial_backoff": 0.1,
        "backoff_factor": 1.8,
        "max_backoff": 4.0,
        "total_timeout_budget": 25.0,
    }
    b = LockBudget.from_facts(facts)
    assert b.max_lock_timeout == 1.5
    assert b.max_retries == 4
    assert b.initial_backoff == 0.1
    assert b.backoff_factor == 1.8
    assert b.max_backoff == 4.0
    assert b.total_timeout_budget == 25.0


def test_lock_impact_rungs_under_watched():
    with watched():
        # 1. immediate_execution_within_budget
        c1 = decide_lock_impact({
            "predicted_wait_seconds": 0.5,
            "max_lock_timeout": 2.0,
            "conflicting_transactions": [],
            "queue_depth": 0,
        })
        assert c1.rung.name == "immediate_execution_within_budget"

        # 2. bounded_backoff_and_retry
        c2 = decide_lock_impact({
            "predicted_wait_seconds": 5.0,
            "max_lock_timeout": 2.0,
            "conflicting_transactions": [{"pid": 123}],
            "queue_depth": 1,
            "total_timeout_budget": 30.0,
            "attempt": 1,
            "max_retries": 5,
        })
        assert c2.rung.name == "bounded_backoff_and_retry"

        # 3. expand_contract_phased_execution
        c3 = decide_lock_impact({
            "predicted_wait_seconds": 35.0,
            "max_lock_timeout": 2.0,
            "total_timeout_budget": 30.0,
            "phased_plan_available": True,
            "table_rewrite": True,
        })
        assert c3.rung.name == "expand_contract_phased_execution"

        # 4. quarantine_and_defer_to_maintenance_window
        c4 = decide_lock_impact({
            "predicted_wait_seconds": 35.0,
            "max_lock_timeout": 2.0,
            "total_timeout_budget": 30.0,
            "phased_plan_available": False,
            "deferral_permitted": True,
            "sustained_contention": True,
        })
        assert c4.rung.name == "quarantine_and_defer_to_maintenance_window"

        # 5. refuse_unsafe_lock_contention
        c5 = decide_lock_impact({
            "predicted_wait_seconds": 35.0,
            "max_lock_timeout": 2.0,
            "total_timeout_budget": 30.0,
            "phased_plan_available": False,
            "deferral_permitted": False,
        })
        assert c5.rung.name == "refuse_unsafe_lock_contention"
