import os
import tempfile
import time
import pytest
from migkit.cancel_recovery import (
    DiagnosticEvidence,
    SnapshotTx,
    SnapshotTxState,
    SnapshotTxStore,
    TxValidationError,
    TxStateError,
    decide_cancel_recovery,
)
from migkit import lease
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("cancel_recovery", "cancel_recovery_rungs", "commit_active_transaction", "holds"),
    ("cancel_recovery", "cancel_recovery_rungs", "commit_active_transaction", "refuses"),
    ("cancel_recovery", "cancel_recovery_rungs", "rollback_owned_work_and_cancel", "holds"),
    ("cancel_recovery", "cancel_recovery_rungs", "rollback_owned_work_and_cancel", "refuses"),
    ("cancel_recovery", "cancel_recovery_rungs", "quarantine_and_recover_stranded", "holds"),
]


def test_transactional_lifecycle_commit():
    store = SnapshotTxStore()
    store.set_global_offsets({"lsn": "0/16B2000", "timeline": 1})

    tx = store.begin_tx(
        tx_id="tx_01",
        table_name="orders",
        keys=("order_id",),
        filter_expr="status = 'COMPLETED'",
    )
    assert tx.state == SnapshotTxState.INITIALIZING
    assert tx.tx_id == "tx_01"
    assert len(store.list_active()) == 0

    published = store.publish_tx("tx_01")
    assert published.state == SnapshotTxState.ACTIVE
    assert len(store.list_active()) == 1

    store.stage_chunk("tx_01", "chunk_0", [{"order_id": 1}, {"order_id": 2}])
    store.stage_chunk("tx_01", "chunk_1", [{"order_id": 3}])

    committed = store.commit_tx("tx_01")
    assert committed.state == SnapshotTxState.COMMITTED
    assert committed.is_terminal() is True
    assert len(store.list_active()) == 0

    offsets = store.get_global_offsets()
    assert offsets["lsn"] == "0/16B2000"


def test_invalid_state_transition_prevented():
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_invalid_jump", "items", ("id",))
    assert tx.state == SnapshotTxState.INITIALIZING

    with pytest.raises(TxStateError):
        store.commit_tx("tx_invalid_jump")


def test_initialization_validation_invalid_table_not_published():
    catalog = {"customers": {"columns": {"id": "int", "email": "text"}, "pks": ("id",)}}
    store = SnapshotTxStore(schema_catalog=catalog)
    store.set_global_offsets({"binlog_pos": 4200})

    tx = store.begin_tx("tx_bad_table", table_name="non_existent_table", keys=("id",))
    assert tx.state == SnapshotTxState.INITIALIZING

    with pytest.raises(TxValidationError) as exc:
        store.publish_tx("tx_bad_table")
    assert "non_existent_table" in str(exc.value)

    assert tx.state == SnapshotTxState.FAILED
    assert len(store.list_active()) == 0

    diagnostics = store.get_diagnostics("tx_bad_table")
    assert len(diagnostics) == 1
    assert diagnostics[0].error_code == "INVALID_TABLE"
    assert diagnostics[0].phase == "INITIALIZATION"

    assert store.get_global_offsets() == {"binlog_pos": 4200}


def test_initialization_validation_invalid_keys_not_published():
    catalog = {"customers": {"columns": {"id": "int", "name": "text"}, "pks": ("id",)}}
    store = SnapshotTxStore(schema_catalog=catalog)
    store.set_global_offsets({"binlog_pos": 5000})

    with pytest.raises(TxValidationError):
        store.begin_tx("tx_no_keys", "customers", keys=())

    tx = store.begin_tx("tx_wrong_keys", "customers", keys=("invalid_col",))
    with pytest.raises(TxValidationError) as exc:
        store.publish_tx("tx_wrong_keys")
    assert "invalid_col" in str(exc.value)

    assert tx.state == SnapshotTxState.FAILED
    assert len(store.list_active()) == 0

    diag = store.get_diagnostics("tx_wrong_keys")
    assert len(diag) >= 1
    assert diag[0].error_code == "INVALID_KEY"
    assert store.get_global_offsets() == {"binlog_pos": 5000}


def test_initialization_validation_invalid_filter_syntax():
    store = SnapshotTxStore()
    store.set_global_offsets({"offset": 100})

    tx = store.begin_tx(
        "tx_bad_syntax",
        "orders",
        keys=("id",),
        filter_expr="status = 'OPEN' AND (created_at > '2026-01-01'",
    )
    with pytest.raises(TxValidationError) as exc:
        store.publish_tx("tx_bad_syntax")
    assert "parentheses" in str(exc.value).lower()

    assert tx.state == SnapshotTxState.FAILED
    assert len(store.list_active()) == 0

    diag = store.get_diagnostics("tx_bad_syntax")
    assert len(diag) == 1
    assert diag[0].error_code == "FILTER_SYNTAX_ERROR"
    assert store.get_global_offsets() == {"offset": 100}


def test_filter_runtime_failure_rolls_back_only_owned_work():
    store = SnapshotTxStore()
    store.set_global_offsets({"wal_lsn": 9999})

    tx = store.begin_tx("tx_filter_run", "orders", ("id",), filter_expr="amount / divisor > 10")
    store.publish_tx("tx_filter_run")

    store.stage_chunk("tx_filter_run", "chk_1", [{"id": 1, "amount": 100, "divisor": 5}])
    store.stage_chunk("tx_filter_run", "chk_2", [{"id": 2, "amount": 200, "divisor": 0}])

    with pytest.raises(ZeroDivisionError):
        def bad_eval(row):
            return row["amount"] / row["divisor"] > 10
        store.evaluate_filter_on_chunk("tx_filter_run", "chk_2", bad_eval)

    assert tx.state == SnapshotTxState.FAILED
    assert len(tx.staged_records) == 0
    assert len(tx.owned_chunks) == 0
    assert len(store.list_active()) == 0

    diag = store.get_diagnostics("tx_filter_run")
    assert any(d.error_code == "FILTER_EXECUTION_FAILURE" for d in diag)
    assert store.get_global_offsets() == {"wal_lsn": 9999}

    # Workflow is not stranded: subsequent request succeeds cleanly
    tx2 = store.begin_tx("tx_subsequent", "orders", ("id",), filter_expr="amount > 10")
    store.publish_tx("tx_subsequent")
    store.stage_chunk("tx_subsequent", "chk_3", [{"id": 3, "amount": 50}])
    store.commit_tx("tx_subsequent")
    assert tx2.state == SnapshotTxState.COMMITTED


def test_cancellation_during_active_rolls_back_owned_work():
    store = SnapshotTxStore()
    store.set_global_offsets({"source_lsn": 88000})

    tx = store.begin_tx("tx_cancel_me", "large_table", ("id",))
    store.publish_tx("tx_cancel_me")
    store.stage_chunk("tx_cancel_me", "c1", [{"id": 10}])
    store.stage_chunk("tx_cancel_me", "c2", [{"id": 20}])

    assert len(tx.owned_chunks) == 2
    assert len(tx.staged_records) == 2

    cancelled = store.cancel_tx("tx_cancel_me", reason="operator triggered cancel")
    assert cancelled.state == SnapshotTxState.CANCELLED
    assert len(cancelled.owned_chunks) == 0
    assert len(cancelled.staged_records) == 0
    assert len(store.list_active()) == 0

    diag = store.get_diagnostics("tx_cancel_me")
    assert len(diag) == 1
    assert diag[0].error_code == "CLIENT_CANCELLED"
    assert "operator triggered cancel" in diag[0].message

    # Global offsets must never be reset
    assert store.get_global_offsets() == {"source_lsn": 88000}


def test_cancellation_during_initializing_never_publishes():
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_cancel_early", "items", ("id",))
    assert tx.state == SnapshotTxState.INITIALIZING

    cancelled = store.cancel_tx("tx_cancel_early", reason="aborted before publish")
    assert cancelled.state == SnapshotTxState.CANCELLED
    assert len(store.list_active()) == 0

    with pytest.raises(TxStateError):
        store.publish_tx("tx_cancel_early")


def test_concurrent_transactions_and_isolation():
    store = SnapshotTxStore()
    store.set_global_offsets({"lsn": 1000})

    tx1 = store.begin_tx("tx_1", "users", ("id",))
    tx2 = store.begin_tx("tx_2", "orders", ("order_id",))
    store.publish_tx("tx_1")
    store.publish_tx("tx_2")

    store.stage_chunk("tx_1", "u1", [{"id": 1}])
    store.stage_chunk("tx_2", "o1", [{"order_id": 100}])
    store.stage_chunk("tx_2", "o2", [{"order_id": 200}])

    # Stream offset moves forward
    store.set_global_offsets({"lsn": 1500})

    # Cancel tx2
    store.cancel_tx("tx_2", reason="timeout")
    assert tx2.state == SnapshotTxState.CANCELLED
    assert len(tx2.staged_records) == 0

    # tx1 is unaffected and its staged records are preserved
    assert tx1.state == SnapshotTxState.ACTIVE
    assert "u1" in tx1.owned_chunks
    assert len(tx1.staged_records["u1"]) == 1

    # tx1 can commit cleanly
    store.commit_tx("tx_1")
    assert tx1.state == SnapshotTxState.COMMITTED

    # Global stream offset was never rewound or wiped
    assert store.get_global_offsets() == {"lsn": 1500}


def test_process_death_recovery_of_active_transaction(tmp_path):
    store_file = tmp_path / "snapshot_tx_state.json"
    store = SnapshotTxStore(storage_path=store_file)
    store.set_global_offsets({"stream_pos": 77777})

    # Fake dead PID (non-existent process PID 999999)
    tx = store.begin_tx("tx_died", "accounts", ("id",), owner_pid=999999, lease_timeout=1.0)
    store.publish_tx("tx_died")
    store.stage_chunk("tx_died", "chk_acc_1", [{"id": "acc-101"}])
    store.save_to_disk()

    # New process starts up and loads from disk
    recovered_store = SnapshotTxStore(storage_path=store_file)
    recovered_store.load_from_disk()

    # Before recovery, transaction was recorded as ACTIVE by dead process
    assert len(recovered_store.list_active()) == 1

    # Reconcile stranded transactions
    recovered = recovered_store.recover_stranded_transactions(
        pid_alive_checker=lambda pid: False
    )
    assert len(recovered) == 1
    assert recovered[0].tx_id == "tx_died"
    assert recovered[0].state == SnapshotTxState.FAILED
    assert len(recovered[0].staged_records) == 0
    assert len(recovered_store.list_active()) == 0

    # Diagnostics recorded
    diag = recovered_store.get_diagnostics("tx_died")
    assert len(diag) >= 1
    assert diag[0].error_code == "ORPHANED_PROCESS_DEATH"
    assert diag[0].phase == "PROCESS_DEATH"

    # Global offset intact
    assert recovered_store.get_global_offsets() == {"stream_pos": 77777}

    # Workflow is unstranded: next transaction immediately runs and commits
    next_tx = recovered_store.begin_tx("tx_next", "accounts", ("id",))
    recovered_store.publish_tx("tx_next")
    recovered_store.commit_tx("tx_next")
    assert next_tx.state == SnapshotTxState.COMMITTED


def test_process_death_recovery_of_initializing_transaction(tmp_path):
    store_file = tmp_path / "stranded_init.json"
    store = SnapshotTxStore(storage_path=store_file)
    store.set_global_offsets({"seq": 500})

    tx = store.begin_tx("tx_dead_init", "products", ("sku",), owner_pid=999999, lease_timeout=0.1)
    # Never published
    assert tx.state == SnapshotTxState.INITIALIZING
    store.save_to_disk()

    recovered_store = SnapshotTxStore(storage_path=store_file)
    recovered_store.load_from_disk()

    recovered = recovered_store.recover_stranded_transactions(
        pid_alive_checker=lambda pid: False
    )
    assert len(recovered) == 1
    assert recovered[0].tx_id == "tx_dead_init"
    assert recovered[0].state == SnapshotTxState.FAILED
    assert len(recovered_store.list_active()) == 0

    diag = recovered_store.get_diagnostics("tx_dead_init")
    assert any(d.error_code == "ORPHANED_PROCESS_DEATH" for d in diag)
    assert recovered_store.get_global_offsets() == {"seq": 500}


def test_lease_heartbeat_expiry():
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_timeout", "events", ("id",), owner_pid=os.getpid(), lease_timeout=0.2)
    store.publish_tx("tx_timeout")
    assert len(store.list_active()) == 1

    time.sleep(0.3)
    recovered = store.recover_stranded_transactions(
        now=time.time(),
        pid_alive_checker=lambda pid: True
    )
    assert len(recovered) == 1
    assert recovered[0].tx_id == "tx_timeout"
    assert recovered[0].state == SnapshotTxState.FAILED
    assert len(store.list_active()) == 0


def test_cancel_already_committed_raises():
    store = SnapshotTxStore()
    store.begin_tx("tx_done", "records", ("id",))
    store.publish_tx("tx_done")
    store.commit_tx("tx_done")

    with pytest.raises(TxStateError):
        store.cancel_tx("tx_done")


def test_duplicate_active_tx_id_prevented():
    store = SnapshotTxStore()
    store.begin_tx("tx_dup", "records", ("id",))
    with pytest.raises(TxStateError):
        store.begin_tx("tx_dup", "records", ("id",))


def test_heartbeat_renewal_prevents_lease_expiry():
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_renew", "events", ("id",), owner_pid=os.getpid(), lease_timeout=0.2)
    store.publish_tx("tx_renew")

    time.sleep(0.15)
    tx.record_heartbeat()
    time.sleep(0.1)

    recovered = store.recover_stranded_transactions(
        now=time.time(),
        pid_alive_checker=lambda pid: True
    )
    assert len(recovered) == 0
    assert tx.state == SnapshotTxState.ACTIVE


def test_stage_chunk_in_invalid_state_raises():
    store = SnapshotTxStore()
    store.begin_tx("tx_init", "records", ("id",))
    with pytest.raises(TxStateError):
        store.stage_chunk("tx_init", "c1", [{"id": 1}])


def test_subsequent_request_after_failed_validation_on_same_table():
    catalog = {"logs": {"columns": {"id": "int", "level": "text"}, "pks": ("id",)}}
    store = SnapshotTxStore(schema_catalog=catalog)

    tx_bad = store.begin_tx("tx_bad_key", "logs", keys=("bad_col",))
    with pytest.raises(TxValidationError):
        store.publish_tx("tx_bad_key")
    assert tx_bad.state == SnapshotTxState.FAILED

    tx_good = store.begin_tx("tx_good", "logs", keys=("id",))
    store.publish_tx("tx_good")
    store.stage_chunk("tx_good", "c1", [{"id": 1, "level": "INFO"}])
    store.commit_tx("tx_good")
    assert tx_good.state == SnapshotTxState.COMMITTED


def test_disallowed_sql_in_filter_rejected():
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_inject", "users", ("id",), filter_expr="id = 1; DROP TABLE users;")
    with pytest.raises(TxValidationError) as exc:
        store.publish_tx("tx_inject")
    assert "disallowed" in str(exc.value).lower()
    assert tx.state == SnapshotTxState.FAILED


def test_diagnostics_filtering_by_phase():
    store = SnapshotTxStore()
    tx1 = store.begin_tx("tx_f1", "items", ("id",), filter_expr="1 = 1 AND (")
    with pytest.raises(TxValidationError):
        store.publish_tx("tx_f1")

    tx2 = store.begin_tx("tx_c1", "items", ("id",))
    store.publish_tx("tx_c1")
    store.cancel_tx("tx_c1", reason="user cancel")

    init_diags = store.get_diagnostics(phase="INITIALIZATION")
    cancel_diags = store.get_diagnostics(phase="CANCELLATION")

    assert len(init_diags) >= 1
    assert all(d.phase == "INITIALIZATION" for d in init_diags)
    assert len(cancel_diags) >= 1
    assert all(d.phase == "CANCELLATION" for d in cancel_diags)


def test_cancel_recovery_uses_lease_term(monkeypatch):
    monkeypatch.setenv("MIGKIT_LEASE_SECONDS", "45")
    store = SnapshotTxStore()
    tx = store.begin_tx("tx_lease_term", "items", ("id",))
    assert tx.lease_timeout == 45.0
    assert tx.lease_timeout == lease.term()


def test_cancel_recovery_rungs_under_watched():
    with watched() as seen:
        # 1. commit_active_transaction holds
        f1 = {
            "is_active": True,
            "cancel_requested": False,
            "lease_expired": False,
            "can_rollback": True,
        }
        c1 = decide_cancel_recovery(f1)
        assert c1.rung.name == "commit_active_transaction"

        # 2. commit_active_transaction refuses, rollback_owned_work_and_cancel holds
        f2 = {
            "is_active": True,
            "cancel_requested": True,
            "lease_expired": False,
            "can_rollback": True,
        }
        c2 = decide_cancel_recovery(f2)
        assert c2.rung.name == "rollback_owned_work_and_cancel"

        # 3. rollback_owned_work_and_cancel refuses, quarantine_and_recover_stranded holds
        f3 = {
            "is_active": False,
            "cancel_requested": False,
            "lease_expired": True,
            "can_rollback": False,
        }
        c3 = decide_cancel_recovery(f3)
        assert c3.rung.name == "quarantine_and_recover_stranded"

