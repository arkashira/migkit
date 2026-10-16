import threading
import time
import pytest

from migkit.dag_scheduler import (
    BarrierFence,
    BarrierType,
    CDCMutation,
    CommitWatermarkTracker,
    ReplayStats,
    TableDependencyGraph,
    Transaction,
    TransactionDAGScheduler,
    WorkStealingQueue,
)


def test_table_dependency_graph_foreign_keys_and_components():
    fks = [
        ("orders", "users"),
        ("order_items", "orders"),
        ("inventory", "warehouse_id", "warehouses", "id"),
    ]
    graph = TableDependencyGraph(fks)

    assert "users" in graph.get_parents("orders")
    assert "orders" in graph.get_children("users")
    assert "orders" in graph.get_parents("order_items")
    assert "order_items" in graph.get_children("orders")
    assert "warehouses" in graph.get_parents("inventory")

    assert graph.has_dependency("order_items", "users")
    assert not graph.has_dependency("users", "order_items")
    assert not graph.has_dependency("inventory", "users")

    comps = graph.connected_components()
    comp_sets = [set(c) for c in comps]
    assert {"users", "orders", "order_items"} in comp_sets
    assert {"inventory", "warehouses"} in comp_sets


def test_topological_sort_insert_and_delete():
    fks = [
        ("orders", "users"),
        ("order_items", "orders"),
    ]
    graph = TableDependencyGraph(fks)

    insert_order = graph.topological_order(["order_items", "users", "orders"], op="insert")
    assert insert_order.index("users") < insert_order.index("orders")
    assert insert_order.index("orders") < insert_order.index("order_items")

    delete_order = graph.topological_order(["order_items", "users", "orders"], op="delete")
    assert delete_order.index("order_items") < delete_order.index("orders")
    assert delete_order.index("orders") < delete_order.index("users")


def test_sort_mutations_within_transaction():
    fks = [("orders", "users"), ("order_items", "orders")]
    graph = TableDependencyGraph(fks)

    mut_order_items = CDCMutation(table="order_items", op="insert", lsn=10, key={"id": 1})
    mut_users = CDCMutation(table="users", op="insert", lsn=10, key={"id": 1})
    mut_orders = CDCMutation(table="orders", op="insert", lsn=10, key={"id": 1})

    sorted_ins = graph.sort_mutations([mut_order_items, mut_users, mut_orders])
    assert [m.table for m in sorted_ins] == ["users", "orders", "order_items"]

    del_users = CDCMutation(table="users", op="delete", lsn=20, key={"id": 1})
    del_orders = CDCMutation(table="orders", op="delete", lsn=20, key={"id": 1})
    del_items = CDCMutation(table="order_items", op="delete", lsn=20, key={"id": 1})

    sorted_del = graph.sort_mutations([del_users, del_items, del_orders])
    assert [m.table for m in sorted_del] == ["order_items", "orders", "users"]


def test_commit_watermark_monotonic_ordering():
    commits = []
    tracker = CommitWatermarkTracker(on_commit=lambda lsn: commits.append(lsn))

    for lsn in (10, 20, 30, 40):
        tracker.register_lsn(lsn)

    tracker.mark_completed(20)
    assert commits == []
    assert tracker.committed_lsn == -1

    tracker.mark_completed(10)
    assert commits == [10, 20]
    assert tracker.committed_lsn == 20

    tracker.mark_completed(40)
    assert commits == [10, 20]
    assert tracker.committed_lsn == 20

    tracker.mark_completed(30)
    assert commits == [10, 20, 30, 40]
    assert tracker.committed_lsn == 40
    assert tracker.is_drained()


def test_work_stealing_queue():
    q = WorkStealingQueue(queue_id=0)
    assert q.is_empty()

    item1 = CDCMutation(table="t1", op="insert", lsn=1)
    item2 = CDCMutation(table="t2", op="insert", lsn=2)
    q.push(item1)
    q.push(item2)

    assert len(q) == 2
    popped = q.pop()
    assert popped == item1

    stolen = q.steal_safe(can_execute_table_fn=lambda t: t == "t2")
    assert stolen == item2
    assert q.is_empty()


def test_parallel_replay_independent_tables():
    executed = []
    exec_lock = threading.Lock()

    def record_apply(mut: CDCMutation):
        time.sleep(0.005)
        with exec_lock:
            executed.append((mut.lsn, mut.table, mut.op))

    fks = [("orders", "users")]
    scheduler = TransactionDAGScheduler(
        foreign_keys=fks,
        num_workers=4,
        apply_fn=record_apply,
    )

    events = [
        {"table": "users", "op": "insert", "lsn": 10, "key": {"id": 1}},
        {"table": "products", "op": "insert", "lsn": 11, "key": {"id": 100}},
        {"table": "warehouses", "op": "insert", "lsn": 12, "key": {"id": 500}},
        {"table": "audit_log", "op": "insert", "lsn": 13, "key": {"id": 900}},
    ]

    stats = scheduler.replay(events, timeout=10.0)

    assert stats.total_mutations == 4
    assert stats.committed_lsn == 13
    assert len(stats.worker_counts) >= 2
    assert stats.commit_order == [10, 11, 12, 13]


def test_dynamic_barrier_schema_ddl():
    log = []
    log_lock = threading.Lock()

    def apply_fn(mut: CDCMutation):
        time.sleep(0.01)
        with log_lock:
            log.append(f"mut:{mut.lsn}:{mut.table}")

    def ddl_action():
        with log_lock:
            log.append("barrier:ddl:executed")

    scheduler = TransactionDAGScheduler(
        num_workers=3,
        apply_fn=apply_fn,
    )

    events = [
        {"table": "t1", "op": "insert", "lsn": 10},
        {"table": "t2", "op": "insert", "lsn": 20},
        {
            "op": "ddl",
            "lsn": 30,
            "table": "t1",
            "action": ddl_action,
            "metadata": {"sql": "ALTER TABLE t1 ADD COLUMN x INT"},
        },
        {"table": "t1", "op": "insert", "lsn": 40},
        {"table": "t2", "op": "insert", "lsn": 50},
    ]

    stats = scheduler.replay(events, timeout=10.0)

    assert stats.barriers_encountered == 1
    assert stats.committed_lsn == 50
    assert stats.commit_order == [10, 20, 30, 40, 50]

    with log_lock:
        ddl_idx = log.index("barrier:ddl:executed")
        prior_muts = [x for x in log[:ddl_idx] if x.startswith("mut:")]
        after_muts = [x for x in log[ddl_idx + 1:] if x.startswith("mut:")]

        assert "mut:10:t1" in prior_muts
        assert "mut:20:t2" in prior_muts
        assert "mut:40:t1" in after_muts
        assert "mut:50:t2" in after_muts


def test_dynamic_barrier_fk_cascade_and_sequence_reset():
    log = []
    log_lock = threading.Lock()

    def apply_fn(mut: CDCMutation):
        time.sleep(0.005)
        with log_lock:
            log.append(f"mut:{mut.lsn}:{mut.table}")

    def cascade_action():
        with log_lock:
            log.append("barrier:cascade")

    def seq_action():
        with log_lock:
            log.append("barrier:seq_reset")

    scheduler = TransactionDAGScheduler(
        foreign_keys=[("orders", "users")],
        num_workers=4,
        apply_fn=apply_fn,
    )

    events = [
        {"table": "users", "op": "insert", "lsn": 10},
        {"table": "orders", "op": "insert", "lsn": 20},
        {
            "table": "users",
            "op": "delete",
            "lsn": 30,
            "is_cascade": True,
            "action": cascade_action,
        },
        {"table": "users", "op": "insert", "lsn": 40},
        {
            "table": "users",
            "op": "sequence_reset",
            "lsn": 50,
            "is_sequence_reset": True,
            "action": seq_action,
        },
        {"table": "users", "op": "insert", "lsn": 60},
    ]

    stats = scheduler.replay(events, timeout=10.0)

    assert stats.barriers_encountered == 2
    assert stats.committed_lsn == 60
    assert stats.commit_order == [10, 20, 30, 40, 50, 60]

    with log_lock:
        c_idx = log.index("barrier:cascade")
        s_idx = log.index("barrier:seq_reset")
        assert c_idx < s_idx
        assert "mut:10:users" in log[:c_idx]
        assert "mut:20:orders" in log[:c_idx]
        assert "mut:40:users" in log[c_idx:s_idx]
        assert "mut:60:users" in log[s_idx:]


def test_dynamic_barrier_cutover_mark():
    cutover_executed = []

    def cutover_action():
        cutover_executed.append(True)

    scheduler = TransactionDAGScheduler(num_workers=2)

    events = [
        {"table": "accounts", "op": "insert", "lsn": 100},
        {"table": "ledger", "op": "insert", "lsn": 101},
        BarrierFence(
            lsn=102,
            barrier_type=BarrierType.CUTOVER,
            action=cutover_action,
            metadata={"mark": "drain-cutover-999"},
        ),
        {"table": "accounts", "op": "insert", "lsn": 103},
    ]

    stats = scheduler.replay(events, timeout=10.0)

    assert stats.barriers_encountered == 1
    assert cutover_executed == [True]
    assert stats.committed_lsn == 103
    assert stats.commit_order == [100, 101, 102, 103]


def test_transaction_batching_with_topological_guarantees():
    exec_history = []
    lock = threading.Lock()

    def record(mut: CDCMutation):
        time.sleep(0.002)
        with lock:
            exec_history.append((mut.table, mut.op))

    fks = [("orders", "users"), ("order_items", "orders")]
    scheduler = TransactionDAGScheduler(foreign_keys=fks, num_workers=4, apply_fn=record)

    txn1 = Transaction(
        tx_id=1,
        commit_lsn=10,
        mutations=[
            CDCMutation(table="order_items", op="insert", lsn=10),
            CDCMutation(table="users", op="insert", lsn=10),
            CDCMutation(table="orders", op="insert", lsn=10),
        ],
    )

    txn2 = Transaction(
        tx_id=2,
        commit_lsn=20,
        mutations=[
            CDCMutation(table="users", op="delete", lsn=20),
            CDCMutation(table="order_items", op="delete", lsn=20),
            CDCMutation(table="orders", op="delete", lsn=20),
        ],
    )

    stats = scheduler.replay([txn1, txn2], timeout=10.0)

    assert stats.total_transactions == 2
    assert stats.committed_lsn == 20

    with lock:
        t1_ops = exec_history[:3]
        t2_ops = exec_history[3:]

        assert t1_ops == [("users", "insert"), ("orders", "insert"), ("order_items", "insert")]
        assert t2_ops == [("order_items", "delete"), ("orders", "delete"), ("users", "delete")]


def test_stress_concurrent_replay_and_work_stealing():
    commits = []
    commit_lock = threading.Lock()

    def on_commit(lsn: int):
        with commit_lock:
            commits.append(lsn)

    scheduler = TransactionDAGScheduler(
        foreign_keys=[("t_child", "t_parent")],
        num_workers=6,
        on_commit=on_commit,
    )

    events = []
    cur_lsn = 1
    for i in range(1, 101):
        if i % 25 == 0:
            events.append(
                BarrierFence(lsn=cur_lsn, barrier_type=BarrierType.CUSTOM, metadata={"step": i})
            )
        else:
            tbl = f"tbl_{i % 8}"
            events.append({"table": tbl, "op": "insert", "lsn": cur_lsn, "key": {"id": i}})
        cur_lsn += 1

    stats = scheduler.replay(events, timeout=15.0)

    assert stats.barriers_encountered == 4
    assert stats.committed_lsn == 100
    with commit_lock:
        assert len(commits) == 100
        assert commits == sorted(commits)
        assert len(set(commits)) == 100


def test_barriers_module_re_exports():
    from migkit import barriers
    assert hasattr(barriers, "BarrierFence")
    assert hasattr(barriers, "BarrierType")
    assert hasattr(barriers, "TransactionDAGScheduler")
    assert barriers.BarrierFence is BarrierFence
    assert barriers.BarrierType is BarrierType
    assert barriers.TransactionDAGScheduler is TransactionDAGScheduler


def test_detect_barrier_variants():
    e1 = {"op": "cascade_update", "lsn": 10, "table": "a"}
    b1 = TransactionDAGScheduler.detect_barrier(e1)
    assert b1 is not None and b1.barrier_type == BarrierType.CASCADE

    e2 = {"op": "sequence_jump", "lsn": 20, "table": "b"}
    b2 = TransactionDAGScheduler.detect_barrier(e2)
    assert b2 is not None and b2.barrier_type == BarrierType.SEQUENCE

    e3 = {"op": "mark", "lsn": 30, "table": "c"}
    b3 = TransactionDAGScheduler.detect_barrier(e3)
    assert b3 is not None and b3.barrier_type == BarrierType.CUTOVER

    e4 = {"op": "schema", "lsn": 40, "table": "d"}
    b4 = TransactionDAGScheduler.detect_barrier(e4)
    assert b4 is not None and b4.barrier_type == BarrierType.DDL

    e5 = {"op": "insert", "lsn": 50, "table": "e"}
    assert TransactionDAGScheduler.detect_barrier(e5) is None


def test_cyclic_and_self_referential_fks():
    graph = TableDependencyGraph([
        ("employees", "manager_id", "employees", "id"),
        ("node_a", "node_b"),
        ("node_b", "node_a"),
    ])
    assert "employees" in graph.all_tables()
    order = graph.topological_order(["node_a", "node_b"], op="insert")
    assert set(order) == {"node_a", "node_b"}

