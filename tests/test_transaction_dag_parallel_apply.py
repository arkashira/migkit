"""Tests for parallel transaction DAG apply with explicit dependency guarantees (RCH03.transaction-dag)."""
from __future__ import annotations

import datetime
import decimal
from pathlib import Path
import uuid

import pytest

from migkit import decide, transaction_dag
from migkit.dag_scheduler import BarrierFence, BarrierType, CDCMutation, Transaction
from migkit.transaction_dag import (
    ActiveVerificationLeaseLock,
    CONTENTION_PACE_LADDER,
    CUTOVER_PREFIX_LADDER,
    CareBadge,
    ContentionTelemetry,
    DependencyKind,
    ForeignKeyConstraint,
    HUGE_TXN_LADDER,
    LeaseConflictError,
    ParallelTransactionDAGExecutor,
    REORDER_CONTRACT_LADDER,
    SchemaConstraintCatalog,
    TransactionAtomicityError,
    TransactionDependencyDAG,
    TransactionNode,
    TransactionalTargetStore,
    TypedDurableDAGCheckpoint,
    choose_cutover_prefix_action,
    choose_huge_transaction_strategy,
    choose_reorder_mode,
    choose_worker_concurrency,
)
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "dag_topological_parallel", "holds"),
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "dag_topological_parallel", "refuses"),
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "wave_barrier_commit", "holds"),
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "wave_barrier_commit", "refuses"),
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "serial_source_commit_order", "holds"),
    ("transaction_dag", "REORDER_CONTRACT_LADDER", "serial_source_commit_order", "refuses"),
    ("transaction_dag", "HUGE_TXN_LADDER", "atomic_single_worker", "holds"),
    ("transaction_dag", "HUGE_TXN_LADDER", "atomic_single_worker", "refuses"),
    ("transaction_dag", "HUGE_TXN_LADDER", "savepoint_windowed_atomic", "holds"),
    ("transaction_dag", "HUGE_TXN_LADDER", "savepoint_windowed_atomic", "refuses"),
    ("transaction_dag", "HUGE_TXN_LADDER", "staged_journal_barrier", "holds"),
    ("transaction_dag", "HUGE_TXN_LADDER", "staged_journal_barrier", "refuses"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "backoff_on_contention", "holds"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "backoff_on_contention", "refuses"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "scale_to_dag_width", "holds"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "scale_to_dag_width", "refuses"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "hold_steady_pace", "holds"),
    ("transaction_dag", "CONTENTION_PACE_LADDER", "hold_steady_pace", "refuses"),
    ("transaction_dag", "CUTOVER_PREFIX_LADDER", "contiguous_prefix_certified", "holds"),
    ("transaction_dag", "CUTOVER_PREFIX_LADDER", "contiguous_prefix_certified", "refuses"),
    ("transaction_dag", "CUTOVER_PREFIX_LADDER", "drain_and_reconcile_holes", "holds"),
    ("transaction_dag", "CUTOVER_PREFIX_LADDER", "drain_and_reconcile_holes", "refuses"),
]


def test_every_transaction_dag_rung_holds_and_refuses_under_watched():
    with watched() as seen:
        c1 = choose_reorder_mode({
            "consistency_contract": "causal_dag",
            "strict_commit_order": False,
            "schema_keys_complete": True,
            "has_untracked_triggers": False,
            "lock_wait_ratio": 0.05,
            "max_lock_wait_ratio": 0.30,
            "supports_commit_barrier": True,
            "has_monotonic_lsn": True,
        })
        assert c1.path == "dag_topological_parallel"

        c2 = choose_reorder_mode({
            "consistency_contract": "strict_commit_order",
            "strict_commit_order": True,
            "schema_keys_complete": True,
            "has_untracked_triggers": False,
            "lock_wait_ratio": 0.05,
            "max_lock_wait_ratio": 0.30,
            "supports_commit_barrier": True,
            "has_monotonic_lsn": True,
        })
        assert c2.path == "wave_barrier_commit"

        c3 = choose_reorder_mode({
            "consistency_contract": "strict_serial",
            "strict_commit_order": True,
            "schema_keys_complete": False,
            "supports_commit_barrier": False,
            "has_monotonic_lsn": True,
        })
        assert c3.path == "serial_source_commit_order"

        c4 = choose_reorder_mode({
            "consistency_contract": "strict_serial",
            "strict_commit_order": True,
            "schema_keys_complete": False,
            "supports_commit_barrier": False,
            "has_monotonic_lsn": False,
        })
        assert c4.rung is None

        h1 = choose_huge_transaction_strategy({
            "target_supports_transactions": True,
            "target_supports_savepoints": True,
            "can_create_staging": True,
            "tx_mutations": 50,
            "tx_bytes": 4096,
            "max_atomic_mutations": 500,
            "max_atomic_bytes": 65536,
            "max_connection_hold_bytes": 524288,
            "estimated_tx_seconds": 0.2,
            "max_tx_hold_seconds": 10.0,
            "free_staging_bytes": 2097152,
        })
        assert h1.path == "atomic_single_worker"

        h2 = choose_huge_transaction_strategy({
            "target_supports_transactions": True,
            "target_supports_savepoints": True,
            "can_create_staging": True,
            "tx_mutations": 1200,
            "tx_bytes": 131072,
            "max_atomic_mutations": 500,
            "max_atomic_bytes": 65536,
            "max_connection_hold_bytes": 524288,
            "estimated_tx_seconds": 1.5,
            "max_tx_hold_seconds": 10.0,
            "free_staging_bytes": 2097152,
        })
        assert h2.path == "savepoint_windowed_atomic"

        h3 = choose_huge_transaction_strategy({
            "target_supports_transactions": True,
            "target_supports_savepoints": False,
            "can_create_staging": True,
            "tx_mutations": 5000,
            "tx_bytes": 1048576,
            "max_atomic_mutations": 500,
            "max_atomic_bytes": 65536,
            "max_connection_hold_bytes": 524288,
            "estimated_tx_seconds": 25.0,
            "max_tx_hold_seconds": 10.0,
            "free_staging_bytes": 4194304,
        })
        assert h3.path == "staged_journal_barrier"

        h4 = choose_huge_transaction_strategy({
            "target_supports_transactions": False,
            "target_supports_savepoints": False,
            "can_create_staging": False,
            "tx_mutations": 5000,
            "tx_bytes": 1048576,
            "max_atomic_mutations": 100,
            "max_atomic_bytes": 1024,
            "max_connection_hold_bytes": 2048,
            "estimated_tx_seconds": 50.0,
            "max_tx_hold_seconds": 5.0,
            "free_staging_bytes": 512,
        })
        assert h4.rung is None

        p1 = choose_worker_concurrency({
            "current_workers": 4,
            "available_connections": 8,
            "dag_ready_width": 6,
            "deadlock_count": 1,
            "lock_wait_ratio": 0.45,
            "latency_inflation": 2.5,
            "low_contention_bound": 0.10,
            "contention_ceiling": 0.30,
            "latency_ceiling": 2.0,
        })
        assert p1.path == "backoff_on_contention"

        p2 = choose_worker_concurrency({
            "current_workers": 2,
            "available_connections": 8,
            "dag_ready_width": 6,
            "deadlock_count": 0,
            "lock_wait_ratio": 0.02,
            "latency_inflation": 1.05,
            "low_contention_bound": 0.10,
            "contention_ceiling": 0.30,
            "latency_ceiling": 2.0,
        })
        assert p2.path == "scale_to_dag_width"

        p3 = choose_worker_concurrency({
            "current_workers": 4,
            "available_connections": 4,
            "dag_ready_width": 4,
            "deadlock_count": 0,
            "lock_wait_ratio": 0.18,
            "latency_inflation": 1.3,
            "low_contention_bound": 0.10,
            "contention_ceiling": 0.30,
            "latency_ceiling": 2.0,
        })
        assert p3.path == "hold_steady_pace"

        p4 = choose_worker_concurrency({
            "current_workers": 0,
            "available_connections": 0,
            "dag_ready_width": 0,
            "deadlock_count": 0,
            "lock_wait_ratio": 0.0,
            "latency_inflation": 1.0,
            "low_contention_bound": 0.10,
            "contention_ceiling": 0.30,
            "latency_ceiling": 2.0,
        })
        assert p4.rung is None

        k1 = choose_cutover_prefix_action({
            "hole_lsns": (),
            "contiguous_lsn": 500,
            "target_cutover_lsn": 500,
            "in_flight_count": 0,
            "aborted_count": 0,
            "pending_barriers": 0,
            "estimated_drain_seconds": 0.0,
            "max_drain_window_seconds": 5.0,
        })
        assert k1.path == "contiguous_prefix_certified"

        k2 = choose_cutover_prefix_action({
            "hole_lsns": (300,),
            "contiguous_lsn": 200,
            "target_cutover_lsn": 500,
            "in_flight_count": 1,
            "aborted_count": 0,
            "pending_barriers": 0,
            "estimated_drain_seconds": 1.2,
            "max_drain_window_seconds": 5.0,
        })
        assert k2.path == "drain_and_reconcile_holes"

        k3 = choose_cutover_prefix_action({
            "hole_lsns": (300,),
            "contiguous_lsn": 200,
            "target_cutover_lsn": 500,
            "in_flight_count": 0,
            "aborted_count": 1,
            "pending_barriers": 1,
            "estimated_drain_seconds": 12.0,
            "max_drain_window_seconds": 5.0,
        })
        assert k3.rung is None

    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        assert expected_branch in branch(seen, rung_name), (rung_name, expected_branch)
        if expected_branch == REFUSES:
            assert why(seen, rung_name)


def test_unique_key_swap_and_row_pk_dependency_edges():
    catalog = SchemaConstraintCatalog(
        primary_keys={"users": ("id",)},
        unique_keys={"users": {"uk_users_email": ("email",)}},
    )
    store = TransactionalTargetStore(catalog)

    # Seed initial users u1 (email=a@example.com) and u2 (email=b@example.com)
    seed_tx = Transaction(
        tx_id="tx_seed",
        commit_lsn=10,
        mutations=[
            CDCMutation(
                table="users",
                op="insert",
                lsn=10,
                key={"id": 1},
                values={"id": 1, "email": "a@example.com"},
            ),
            CDCMutation(
                table="users",
                op="insert",
                lsn=10,
                key={"id": 2},
                values={"id": 2, "email": "b@example.com"},
            ),
        ],
    )

    # Tx 1 frees a@example.com from id=1 by moving id=1 to temp@example.com
    tx_free_a = Transaction(
        tx_id="tx_free_a",
        commit_lsn=20,
        mutations=[
            CDCMutation(
                table="users",
                op="update",
                lsn=20,
                key={"id": 1},
                old_values={"id": 1, "email": "a@example.com"},
                values={"id": 1, "email": "temp@example.com"},
            ),
        ],
    )

    # Tx 2 assigns a@example.com to id=2 (freeing b@example.com)
    tx_claim_a = Transaction(
        tx_id="tx_claim_a",
        commit_lsn=30,
        mutations=[
            CDCMutation(
                table="users",
                op="update",
                lsn=30,
                key={"id": 2},
                old_values={"id": 2, "email": "b@example.com"},
                values={"id": 2, "email": "a@example.com"},
            ),
        ],
    )

    # Tx 3 assigns b@example.com to id=1
    tx_claim_b = Transaction(
        tx_id="tx_claim_b",
        commit_lsn=40,
        mutations=[
            CDCMutation(
                table="users",
                op="update",
                lsn=40,
                key={"id": 1},
                old_values={"id": 1, "email": "temp@example.com"},
                values={"id": 1, "email": "b@example.com"},
            ),
        ],
    )

    dag = TransactionDependencyDAG(catalog)
    for t in (seed_tx, tx_free_a, tx_claim_a, tx_claim_b):
        dag.add_transaction(t)

    uk_edges = [e for e in dag.edges if e.kind == DependencyKind.UNIQUE_KEY]
    assert any(e.from_tx_id == "tx_free_a" and e.to_tx_id == "tx_claim_a" for e in uk_edges)
    assert any(e.from_tx_id == "tx_claim_a" and e.to_tx_id == "tx_claim_b" for e in uk_edges)

    executor = ParallelTransactionDAGExecutor(catalog=catalog, store=store)
    report = executor.execute([seed_tx, tx_free_a, tx_claim_a, tx_claim_b])

    assert report.care_badge == CareBadge.PASS
    assert store.get_row("users", 1)["email"] == "b@example.com"
    assert store.get_row("users", 2)["email"] == "a@example.com"


def test_cross_table_fk_cascades_and_intra_tx_mutation_ordering():
    catalog = SchemaConstraintCatalog(
        primary_keys={
            "accounts": ("id",),
            "orders": ("id",),
            "order_items": ("id",),
        },
        foreign_keys=[
            ForeignKeyConstraint(
                child_table="orders",
                child_columns=("account_id",),
                parent_table="accounts",
                parent_columns=("id",),
                on_delete="CASCADE",
            ),
            ForeignKeyConstraint(
                child_table="order_items",
                child_columns=("order_id",),
                parent_table="orders",
                parent_columns=("id",),
                on_delete="CASCADE",
            ),
        ],
    )
    store = TransactionalTargetStore(catalog)

    # Out-of-order mutations inside one source transaction: child before parent
    tx_insert_tree = Transaction(
        tx_id="tx_insert_tree",
        commit_lsn=100,
        mutations=[
            CDCMutation(
                table="order_items",
                op="insert",
                lsn=100,
                key={"id": 1000},
                values={"id": 1000, "order_id": 100, "sku": "SKU-1"},
            ),
            CDCMutation(
                table="orders",
                op="insert",
                lsn=100,
                key={"id": 100},
                values={"id": 100, "account_id": 10},
            ),
            CDCMutation(
                table="accounts",
                op="insert",
                lsn=100,
                key={"id": 10},
                values={"id": 10, "name": "acme"},
            ),
        ],
    )

    # Independent account 20 insert can run in parallel with account 10 child updates
    tx_independent_acct = Transaction(
        tx_id="tx_independent_acct",
        commit_lsn=110,
        mutations=[
            CDCMutation(
                table="accounts",
                op="insert",
                lsn=110,
                key={"id": 20},
                values={"id": 20, "name": "globex"},
            ),
        ],
    )

    # Cross-table FK cascade delete on parent account 10
    tx_cascade_del = Transaction(
        tx_id="tx_cascade_del",
        commit_lsn=120,
        mutations=[
            CDCMutation(
                table="accounts",
                op="delete",
                lsn=120,
                key={"id": 10},
                old_values={"id": 10, "name": "acme"},
                is_cascade=True,
            ),
        ],
    )

    # Subsequent insert into orders for account 20 ordered after cascade fence
    tx_after_cascade = Transaction(
        tx_id="tx_after_cascade",
        commit_lsn=130,
        mutations=[
            CDCMutation(
                table="orders",
                op="insert",
                lsn=130,
                key={"id": 200},
                values={"id": 200, "account_id": 20},
            ),
        ],
    )

    executor = ParallelTransactionDAGExecutor(catalog=catalog, store=store)
    report = executor.execute([
        tx_insert_tree,
        tx_independent_acct,
        tx_cascade_del,
        tx_after_cascade,
    ])

    assert report.care_badge == CareBadge.PASS
    assert store.get_row("accounts", 10) is None
    assert store.get_row("orders", 100) is None
    assert store.get_row("order_items", 1000) is None
    assert store.get_row("accounts", 20) == {"id": 20, "name": "globex"}
    assert store.get_row("orders", 200) == {"id": 200, "account_id": 20}


def test_barriers_for_ddl_sequence_and_cutover_preserve_strict_fence():
    catalog = SchemaConstraintCatalog(primary_keys={"ledger": ("id",)})
    store = TransactionalTargetStore(catalog)
    timeline = []

    tx_before = Transaction(
        tx_id="tx_before",
        commit_lsn=10,
        mutations=[
            CDCMutation(
                table="ledger",
                op="insert",
                lsn=10,
                key={"id": 1},
                values={"id": 1, "amount": 50},
            ),
        ],
    )
    ddl_barrier = BarrierFence(
        lsn=20,
        barrier_type=BarrierType.DDL,
        table="ledger",
        action=lambda: timeline.append("ddl"),
        metadata={"sql": "ALTER TABLE ledger ADD COLUMN currency TEXT", "add_column": "currency"},
    )
    seq_barrier = BarrierFence(
        lsn=30,
        barrier_type=BarrierType.SEQUENCE,
        table="ledger",
        action=lambda: timeline.append("seq"),
        metadata={"sequence_name": "ledger_id_seq", "value": 5000},
    )
    tx_after = Transaction(
        tx_id="tx_after",
        commit_lsn=40,
        mutations=[
            CDCMutation(
                table="ledger",
                op="insert",
                lsn=40,
                key={"id": 5000},
                values={"id": 5000, "amount": 75, "currency": "USD"},
            ),
        ],
    )
    cutover_barrier = BarrierFence(
        lsn=50,
        barrier_type=BarrierType.CUTOVER,
        action=lambda: timeline.append("cutover"),
        metadata={"mark": "cutover-fence-50"},
    )

    def hook(tx_id, idx, mut):
        timeline.append(f"{tx_id}:{mut.lsn}")

    executor = ParallelTransactionDAGExecutor(
        catalog=catalog,
        store=store,
        mutation_hook=hook,
    )
    report = executor.execute([tx_before, ddl_barrier, seq_barrier, tx_after, cutover_barrier])

    assert report.care_badge == CareBadge.PASS
    assert report.barriers_executed == 3
    assert timeline == ["tx_before:10", "ddl", "seq", "tx_after:40", "cutover"]
    assert store.ddl_history == ["ALTER TABLE ledger ADD COLUMN currency TEXT"]
    assert store._sequences["ledger_id_seq"] == 5000
    assert report.contiguous_committed_lsn == 50


def test_huge_transaction_strategies_savepoint_and_staged_and_refusal():
    catalog = SchemaConstraintCatalog(primary_keys={"events": ("id",)})
    store = TransactionalTargetStore(catalog)

    # Savepoint-windowed huge transaction (exceeds max_atomic_mutations=2, fits connection hold)
    muts_sp = [
        CDCMutation(table="events", op="insert", lsn=100, key={"id": i}, values={"id": i, "v": f"row-{i}"})
        for i in range(1, 7)
    ]
    tx_savepoint = TransactionNode(
        tx_id="tx_huge_sp",
        commit_lsn=100,
        mutations=muts_sp,
        metadata={
            "max_atomic_mutations": 2,
            "max_atomic_bytes": 128,
            "max_connection_hold_bytes": 65536,
            "target_supports_savepoints": True,
            "estimated_tx_seconds": 0.5,
            "max_tx_hold_seconds": 10.0,
        },
    )

    # Staged-journal huge transaction (savepoints disabled, staging enabled)
    muts_staged = [
        CDCMutation(table="events", op="insert", lsn=200, key={"id": i}, values={"id": i, "v": f"st-{i}"})
        for i in range(10, 15)
    ]
    tx_staged = TransactionNode(
        tx_id="tx_huge_staged",
        commit_lsn=200,
        mutations=muts_staged,
        metadata={
            "max_atomic_mutations": 2,
            "max_atomic_bytes": 128,
            "target_supports_savepoints": False,
            "can_create_staging": True,
            "free_staging_bytes": 131072,
        },
    )

    executor = ParallelTransactionDAGExecutor(catalog=catalog, store=store)
    report = executor.execute([tx_savepoint, tx_staged])

    assert report.huge_tx_choices["tx_huge_sp"].path == "savepoint_windowed_atomic"
    assert report.huge_tx_choices["tx_huge_staged"].path == "staged_journal_barrier"
    assert len(store.table_rows("events")) == 11

    # Refusal when huge transaction exceeds all safe atomic budgets
    tx_impossible = TransactionNode(
        tx_id="tx_impossible",
        commit_lsn=300,
        mutations=[
            CDCMutation(table="events", op="insert", lsn=300, key={"id": 999}, values={"id": 999})
        ],
        metadata={
            "max_atomic_mutations": 0,
            "target_supports_savepoints": False,
            "can_create_staging": False,
        },
    )
    with pytest.raises(TransactionAtomicityError) as exc_info:
        executor.execute([tx_impossible])
    assert exc_info.value.care_badge == CareBadge.ACTION
    assert store.get_row("events", 999) is None


def test_kill_mid_transaction_rolls_back_atomically_blocks_cutover_on_hole_and_resumes_o1(tmp_path: Path):
    ckpt_path = tmp_path / "dag_checkpoint.json"
    lock_path = tmp_path / ".run.lock"

    catalog = SchemaConstraintCatalog(
        primary_keys={"orders": ("id",), "payments": ("id",), "audit": ("id",)},
        foreign_keys=[("payments", "order_id", "orders", "id")],
    )
    store = TransactionalTargetStore(catalog)

    composite_cursor = (
        datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        decimal.Decimal("999.95"),
    )

    # Tx 1 (lsn=100): commits cleanly with native typed cursor key
    tx1 = TransactionNode(
        tx_id="tx_100",
        commit_lsn=100,
        mutations=[
            CDCMutation(table="orders", op="insert", lsn=100, key={"id": 1}, values={"id": 1, "total": 50}),
            CDCMutation(table="payments", op="insert", lsn=100, key={"id": 10}, values={"id": 10, "order_id": 1}),
        ],
        metadata={"cursor_key": composite_cursor},
    )

    # Tx 2 (lsn=200): multi-mutation transaction killed after first mutation!
    tx2 = TransactionNode(
        tx_id="tx_200",
        commit_lsn=200,
        mutations=[
            CDCMutation(table="orders", op="insert", lsn=200, key={"id": 2}, values={"id": 2, "total": 80}),
            CDCMutation(table="payments", op="insert", lsn=200, key={"id": 20}, values={"id": 20, "order_id": 2}),
        ],
    )

    # Tx 3 (lsn=300): independent table `audit` in same wave as Tx 2; succeeds while Tx 2 fails!
    tx3 = TransactionNode(
        tx_id="tx_300",
        commit_lsn=300,
        mutations=[
            CDCMutation(table="audit", op="insert", lsn=300, key={"id": 300}, values={"id": 300, "msg": "ok"}),
        ],
    )

    should_crash = {"tx_200": True}

    def crash_on_tx2_second_mutation(tx_id, idx, mut):
        if should_crash.get(tx_id) and idx == 1:
            raise RuntimeError("simulated worker kill mid-transaction")

    telemetry = ContentionTelemetry(initial_workers=4, available_connections=8)
    ckpt1 = TypedDurableDAGCheckpoint(ckpt_path)
    exec1 = ParallelTransactionDAGExecutor(
        catalog=catalog,
        store=store,
        checkpoint=ckpt1,
        lock_path=lock_path,
        telemetry=telemetry,
        mutation_hook=crash_on_tx2_second_mutation,
    )

    with pytest.raises(RuntimeError, match="simulated worker kill mid-transaction"):
        exec1.execute([tx1, tx2, tx3])

    # Source transaction atomicity preserved: orders id=2 from tx_200 was rolled back!
    assert store.get_row("orders", 1) is not None
    assert store.get_row("orders", 2) is None
    assert store.get_row("payments", 20) is None
    # Independent tx_300 committed out-of-order, creating a hole at LSN 200
    assert store.get_row("audit", 300) is not None
    assert ckpt1.contiguous_committed_lsn == 100
    assert ckpt1.hole_lsns(up_to_lsn=300) == [200]

    # Cutover prefix proof MUST block cutover when hole LSN 200 is uncommitted/aborted
    proof_blocked = ckpt1.verify_cutover_prefix(target_cutover_lsn=300)
    assert not proof_blocked.allowed
    assert proof_blocked.care_badge == CareBadge.ACTION
    assert proof_blocked.hole_lsns == (200,)
    assert proof_blocked.aborted_lsns == (200,)

    # Restart with a fresh checkpoint loaded from disk; verify native types round-tripped
    should_crash["tx_200"] = False
    ckpt2 = TypedDurableDAGCheckpoint(ckpt_path)
    assert ckpt2.last_cursor_key == composite_cursor
    assert isinstance(ckpt2.last_cursor_key[0], datetime.datetime)
    assert isinstance(ckpt2.last_cursor_key[1], uuid.UUID)
    assert isinstance(ckpt2.last_cursor_key[2], decimal.Decimal)

    reexecuted = []
    exec2 = ParallelTransactionDAGExecutor(
        catalog=catalog,
        store=store,
        checkpoint=ckpt2,
        lock_path=lock_path,
        mutation_hook=lambda tx_id, idx, mut: reexecuted.append((tx_id, idx)),
    )
    report2 = exec2.execute([tx1, tx2, tx3])

    assert report2.care_badge == CareBadge.PASS
    # O(1) skip: tx_100 and tx_300 were skipped without re-execution; only tx_200 ran!
    assert set(report2.skipped_tx_ids) == {"tx_100", "tx_300"}
    assert report2.committed_tx_ids == ["tx_200"]
    assert reexecuted == [("tx_200", 0), ("tx_200", 1)]
    assert store.get_row("orders", 2) == {"id": 2, "total": 80}
    assert store.get_row("payments", 20) == {"id": 20, "order_id": 2}
    assert ckpt2.contiguous_committed_lsn == 300
    assert ckpt2.hole_lsns(up_to_lsn=300) == []

    # Cutover prefix proof now certifies cutover as PASS
    proof_ready = ckpt2.verify_cutover_prefix(target_cutover_lsn=300)
    assert proof_ready.allowed
    assert proof_ready.care_badge == CareBadge.PASS
    assert proof_ready.chosen_way == "contiguous_prefix_certified"


def test_active_lease_lock_mutual_exclusion(tmp_path: Path):
    lock_file = tmp_path / ".run.lock"
    with ActiveVerificationLeaseLock(lock_file) as primary_lease:
        primary_lease.heartbeat()
        competing_lease = ActiveVerificationLeaseLock(lock_file)
        with pytest.raises(LeaseConflictError, match="Active DAG lease lock already held"):
            competing_lease.acquire()

    # Once released, a new runner can acquire the lock
    with ActiveVerificationLeaseLock(lock_file) as second_lease:
        assert second_lease.heartbeat_at >= second_lease.acquired_at


def test_contention_telemetry_scales_and_backs_off_by_measured_metrics():
    telemetry = ContentionTelemetry(
        initial_workers=2,
        available_connections=8,
        low_contention_bound=0.10,
        contention_ceiling=0.25,
        latency_ceiling=2.0,
    )
    # Fast, low-contention samples -> scale up toward DAG ready width=6
    telemetry.record_execution(mutations_count=10, apply_seconds=0.01, lock_wait_seconds=0.0002)
    telemetry.record_execution(mutations_count=10, apply_seconds=0.01, lock_wait_seconds=0.0002)
    workers_up, choice_up = telemetry.adjust_workers(dag_ready_width=6)
    assert choice_up.path == "scale_to_dag_width"
    assert workers_up > 2

    # Inject deadlock / high contention -> multiplicative backoff
    telemetry.record_execution(
        mutations_count=10,
        apply_seconds=0.05,
        lock_wait_seconds=0.04,
        deadlock=True,
    )
    workers_down, choice_down = telemetry.adjust_workers(dag_ready_width=6)
    assert choice_down.path == "backoff_on_contention"
    assert workers_down < workers_up


def test_wave_barrier_and_serial_commit_order_contracts():
    catalog = SchemaConstraintCatalog(primary_keys={"t_a": ("id",), "t_b": ("id",)})
    events = [
        Transaction(
            tx_id="tx_1",
            commit_lsn=10,
            mutations=[CDCMutation(table="t_a", op="insert", lsn=10, key={"id": 1}, values={"id": 1})],
        ),
        Transaction(
            tx_id="tx_2",
            commit_lsn=20,
            mutations=[CDCMutation(table="t_b", op="insert", lsn=20, key={"id": 2}, values={"id": 2})],
        ),
    ]

    exec_wave = ParallelTransactionDAGExecutor(
        catalog=catalog,
        facts={
            "consistency_contract": "strict_commit_order",
            "strict_commit_order": True,
            "schema_keys_complete": True,
            "supports_commit_barrier": True,
        },
    )
    rep_wave = exec_wave.execute(events)
    assert rep_wave.reorder_choice.path == "wave_barrier_commit"
    assert rep_wave.care_badge == CareBadge.PASS

    exec_serial = ParallelTransactionDAGExecutor(
        catalog=catalog,
        facts={
            "consistency_contract": "strict_serial",
            "strict_commit_order": True,
            "schema_keys_complete": False,
            "supports_commit_barrier": False,
            "has_monotonic_lsn": True,
        },
    )
    rep_serial = exec_serial.execute(events)
    assert rep_serial.reorder_choice.path == "serial_source_commit_order"
    assert rep_serial.committed_tx_ids == ["tx_1", "tx_2"]


def test_fk_restrict_violation_and_savepoint_rollback_preserve_atomicity():
    catalog = SchemaConstraintCatalog(
        primary_keys={"depts": ("id",), "emps": ("id",)},
        foreign_keys=[
            ForeignKeyConstraint(
                child_table="emps",
                child_columns=("dept_id",),
                parent_table="depts",
                parent_columns=("id",),
                on_delete="RESTRICT",
            )
        ],
    )
    store = TransactionalTargetStore(catalog)

    store.begin_tx("tx_init")
    store.apply_mutation(
        "tx_init",
        CDCMutation(table="depts", op="insert", lsn=1, key={"id": 10}, values={"id": 10, "name": "eng"}),
    )
    store.create_savepoint("tx_init", "sp_after_dept")
    store.apply_mutation(
        "tx_init",
        CDCMutation(table="emps", op="insert", lsn=1, key={"id": 100}, values={"id": 100, "dept_id": 10}),
    )
    # Roll back to savepoint and verify employee is undone while dept remains in tx snapshot
    store.rollback_to_savepoint("tx_init", "sp_after_dept")
    store.commit_tx("tx_init")
    assert store.get_row("depts", 10) == {"id": 10, "name": "eng"}
    assert store.get_row("emps", 100) is None

    # Insert employee and then attempt restricted parent delete inside a multi-mutation transaction
    tx_emp = Transaction(
        tx_id="tx_emp",
        commit_lsn=2,
        mutations=[
            CDCMutation(table="emps", op="insert", lsn=2, key={"id": 101}, values={"id": 101, "dept_id": 10}),
        ],
    )
    tx_bad_del = Transaction(
        tx_id="tx_bad_del",
        commit_lsn=3,
        mutations=[
            CDCMutation(table="depts", op="insert", lsn=3, key={"id": 20}, values={"id": 20, "name": "sales"}),
            CDCMutation(table="depts", op="delete", lsn=3, key={"id": 10}, old_values={"id": 10, "name": "eng"}),
        ],
    )
    executor = ParallelTransactionDAGExecutor(catalog=catalog, store=store)
    executor.execute([tx_emp])
    with pytest.raises(TransactionAtomicityError, match="Foreign key restrict violation"):
        executor.execute([tx_bad_del])
    # Neither dept 20 insert nor dept 10 delete persisted
    assert store.get_row("depts", 20) is None
    assert store.get_row("depts", 10) == {"id": 10, "name": "eng"}

