import time
import pytest

from migkit.progress_cause import (
    CausalPhase,
    Checkpoint,
    ChildProcessDiagnostic,
    EtaStatus,
    ProgressCauseTracker,
    ProgressDiagnosis,
    RequiredUserAction,
    RestoreDiagnostic,
    normalize_phase,
    progress_cause_rungs,
    decide_progress_cause,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("progress_cause", "progress_cause_rungs", "steady_progress_observed", "holds"),
    ("progress_cause", "progress_cause_rungs", "steady_progress_observed", "refuses"),
    ("progress_cause", "progress_cause_rungs", "actionable_user_intervention_required", "holds"),
    ("progress_cause", "progress_cause_rungs", "actionable_user_intervention_required", "refuses"),
    ("progress_cause", "progress_cause_rungs", "automatic_worker_backoff_and_retry", "holds"),
    ("progress_cause", "progress_cause_rungs", "automatic_worker_backoff_and_retry", "refuses"),
    ("progress_cause", "progress_cause_rungs", "degrade_concurrency_under_lock_pressure", "holds"),
    ("progress_cause", "progress_cause_rungs", "degrade_concurrency_under_lock_pressure", "refuses"),
    ("progress_cause", "progress_cause_rungs", "halt_and_report_fatal_blocker", "holds"),
]



def test_causal_phase_constants_and_normalization():
    assert CausalPhase.WAITING_BACKUP == "waiting_backup"
    assert CausalPhase.LOCK_CONTENTION == "lock_contention"
    assert CausalPhase.SNAPSHOT_QUEUE == "snapshot_queue"
    assert CausalPhase.LOG_DRAIN_LAG == "log_drain_lag"
    assert CausalPhase.NETWORK_ROUTE == "network_route"
    assert CausalPhase.MISSING_PERMISSION == "missing_permission"
    assert CausalPhase.CHILD_PROCESS_STALL == "child_process_stall"
    assert CausalPhase.ACTIVE_PROGRESS == "active_progress"

    assert normalize_phase("waiting backup") == CausalPhase.WAITING_BACKUP
    assert normalize_phase("waiting_backup") == CausalPhase.WAITING_BACKUP
    assert normalize_phase("lock") == CausalPhase.LOCK_CONTENTION
    assert normalize_phase("lock contention") == CausalPhase.LOCK_CONTENTION
    assert normalize_phase("snapshot") == CausalPhase.SNAPSHOT_QUEUE
    assert normalize_phase("snapshot queue") == CausalPhase.SNAPSHOT_QUEUE
    assert normalize_phase("log drain") == CausalPhase.LOG_DRAIN_LAG
    assert normalize_phase("log drain lag") == CausalPhase.LOG_DRAIN_LAG
    assert normalize_phase("network") == CausalPhase.NETWORK_ROUTE
    assert normalize_phase("network route") == CausalPhase.NETWORK_ROUTE
    assert normalize_phase("permission") == CausalPhase.MISSING_PERMISSION
    assert normalize_phase("missing permission") == CausalPhase.MISSING_PERMISSION
    assert normalize_phase("child process") == CausalPhase.CHILD_PROCESS_STALL
    assert normalize_phase("child process stall") == CausalPhase.CHILD_PROCESS_STALL
    assert normalize_phase("unknown_phase") == "unknown_phase"


def test_observed_checkpoints_for_liveness():
    tracker = ProgressCauseTracker(stall_threshold_seconds=10.0)
    t0 = 1000.0

    tracker.record_checkpoint(checkpoint_id=1, position=100, units_completed=100, units_total=1000, timestamp=t0)
    assert tracker.is_live(now=t0 + 2.0)
    assert tracker.last_checkpoint_timestamp == t0

    tracker.record_checkpoint(checkpoint_id=2, position=250, units_completed=250, units_total=1000, timestamp=t0 + 5.0)
    assert tracker.is_live(now=t0 + 8.0)

    # Checkpoint advances
    diag = tracker.evaluate_progress(now=t0 + 8.0)
    assert diag.phase == CausalPhase.ACTIVE_PROGRESS
    assert diag.liveness is True
    assert diag.is_stalled is False
    assert diag.progress_rate > 0.0
    assert diag.eta_status == EtaStatus.COMPUTABLE
    assert diag.eta_seconds is not None
    assert diag.fake_percentage_suppressed is False

    # Checkpoint stalled beyond threshold
    assert not tracker.is_live(now=t0 + 20.0)
    diag_stalled = tracker.evaluate_progress(now=t0 + 20.0)
    assert diag_stalled.liveness is False
    assert diag_stalled.is_stalled is True
    assert diag_stalled.progress_rate == 0.0
    assert diag_stalled.fake_percentage_suppressed is True


def test_suppress_fake_percentage_progress_when_stalled():
    tracker = ProgressCauseTracker(stall_threshold_seconds=5.0)
    tracker.record_checkpoint(checkpoint_id="c1", position=500, units_completed=50, units_total=100, timestamp=100.0)

    # At t=101, progressing
    d1 = tracker.evaluate_progress(now=101.0)
    assert d1.fake_percentage_suppressed is False
    assert d1.percentage == 50.0

    # At t=110, stalled (no new checkpoints)
    d2 = tracker.evaluate_progress(now=110.0)
    assert d2.fake_percentage_suppressed is True
    assert d2.is_stalled is True
    # Fake percentage not generated; percentage stays at verified checkpoint level
    assert d2.percentage == 50.0
    assert d2.eta_status in (EtaStatus.UNCERTAIN, EtaStatus.STALLED)
    assert d2.eta_seconds is None


def test_separate_eta_uncertainty_from_hidden_required_user_actions():
    tracker = ProgressCauseTracker(stall_threshold_seconds=10.0)
    tracker.record_checkpoint(checkpoint_id=1, position=100, units_completed=10, units_total=100, timestamp=100.0)

    # Register required user action (missing permission)
    req = RequiredUserAction(
        action_type="grant_permission",
        resource="postgres:public.customer_audit",
        command_recommendation="GRANT SELECT ON TABLE customer_audit TO migkit_worker;",
        blocking_operation="COPY customer_audit TO STDOUT",
        description="missing SELECT privilege on target table",
    )
    tracker.record_required_user_action(req)

    diag = tracker.evaluate_progress(now=105.0)
    assert diag.phase == CausalPhase.MISSING_PERMISSION
    assert diag.eta_status == EtaStatus.BLOCKED_BY_USER_ACTION
    assert diag.eta_seconds is None
    assert diag.is_blocked is True
    assert diag.required_user_action is not None
    assert diag.required_user_action.command_recommendation == "GRANT SELECT ON TABLE customer_audit TO migkit_worker;"
    assert "GRANT SELECT" in diag.next_step_recommendation


def test_lock_contention_diagnosis_and_safe_next_step():
    tracker = ProgressCauseTracker(stall_threshold_seconds=5.0)
    tracker.record_checkpoint(checkpoint_id=1, position=50, units_completed=50, units_total=200, timestamp=10.0)

    tracker.record_lock_contention(
        relation="public.orders",
        blocking_pid=4912,
        lock_type="AccessExclusiveLock",
        query="VACUUM FULL public.orders;",
        duration_seconds=45.0,
        engine="postgres",
    )

    diag = tracker.evaluate_progress(now=20.0)
    assert diag.phase == CausalPhase.LOCK_CONTENTION
    assert diag.is_blocked is True
    assert diag.blocking_operation == "lock contention on public.orders by pid 4912"
    assert "SELECT pg_cancel_backend(4912);" in diag.next_step_recommendation
    assert diag.required_user_action is not None
    assert diag.required_user_action.action_type == "release_lock"
    assert diag.eta_status == EtaStatus.BLOCKED_BY_USER_ACTION


def test_mysql_lock_contention_diagnosis_and_safe_next_step():
    tracker = ProgressCauseTracker()
    tracker.record_lock_contention(
        relation="sales.invoices",
        blocking_pid=182,
        lock_type="MetadataLock",
        query="ALTER TABLE sales.invoices ADD COLUMN x INT",
        duration_seconds=60.0,
        engine="mysql",
    )
    diag = tracker.evaluate_progress(now=10.0)
    assert diag.phase == CausalPhase.LOCK_CONTENTION
    assert "KILL QUERY 182;" in diag.next_step_recommendation
    assert diag.required_user_action.resource == "sales.invoices"


def test_waiting_backup_and_snapshot_queue_causal_states():
    tracker = ProgressCauseTracker()
    tracker.record_backup_wait(
        backup_id="snap-20261017-01",
        status="pending_export",
        expected_available_at=None,
        detail="RDS automated snapshot export to S3 pending",
    )
    diag = tracker.evaluate_progress(now=10.0)
    assert diag.phase == CausalPhase.WAITING_BACKUP
    assert diag.is_blocked is True
    assert diag.eta_status == EtaStatus.UNCERTAIN
    assert "snap-20261017-01" in diag.blocking_operation
    assert "describe-export-tasks" in diag.next_step_recommendation

    tracker.record_snapshot_queue(
        queue_position=4,
        snapshot_id="snap-20261017-02",
        queue_depth=5,
        detail="throttled by snapshot concurrency limit",
    )
    diag2 = tracker.evaluate_progress(now=12.0)
    assert diag2.phase == CausalPhase.SNAPSHOT_QUEUE
    assert "snapshot queue position 4/5" in diag2.blocking_operation


def test_log_drain_lag_causal_state():
    tracker = ProgressCauseTracker()
    tracker.record_log_drain_lag(
        current_lsn="0/16B4000",
        target_lsn="0/2000000",
        lag_bytes=10485760,
        drain_rate=1024,
        detail="CDC receiver drain throughput throttled by disk write IOPS",
    )
    diag = tracker.evaluate_progress(now=10.0)
    assert diag.phase == CausalPhase.LOG_DRAIN_LAG
    assert diag.is_blocked is True
    assert "10485760 bytes lag" in diag.blocking_operation
    assert diag.next_step_recommendation is not None


def test_network_route_causal_state():
    tracker = ProgressCauseTracker()
    tracker.record_network_route(
        target_host="10.0.0.12",
        port=5432,
        error_snippet="no route to host / connection timed out",
    )
    diag = tracker.evaluate_progress(now=10.0)
    assert diag.phase == CausalPhase.NETWORK_ROUTE
    assert diag.is_blocked is True
    assert "10.0.0.12:5432" in diag.blocking_operation
    assert "nc -zv 10.0.0.12 5432" in diag.next_step_recommendation


def test_stalled_restore_diagnostics():
    tracker = ProgressCauseTracker(stall_threshold_seconds=5.0)

    # Simulate stalled restore on pg_restore holding lock
    res = tracker.diagnose_restore_stall(
        restore_info={
            "tool": "pg_restore",
            "pid": 8012,
            "target_table": "public.lineitems",
            "last_read_bytes": 1048576,
            "elapsed_seconds": 120.0,
            "lock_held_by": 9015,
            "lock_type": "AccessExclusiveLock",
            "blocking_query": "SELECT * FROM public.lineitems FOR UPDATE",
            "engine": "postgres",
        },
        now=120.0,
    )
    assert isinstance(res, ProgressDiagnosis)
    assert res.phase == CausalPhase.LOCK_CONTENTION
    assert res.is_stalled is True
    assert "9015" in res.blocking_operation
    assert "SELECT pg_cancel_backend(9015);" in res.next_step_recommendation

    # Simulate stalled restore waiting on child process pipe
    res_pipe = tracker.diagnose_restore_stall(
        restore_info={
            "tool": "myloader",
            "pid": 4321,
            "target_table": "crm.customers",
            "pipe_stall": True,
            "last_active_seconds_ago": 65.0,
        },
        now=150.0,
    )
    assert res_pipe.phase == CausalPhase.CHILD_PROCESS_STALL
    assert res_pipe.is_stalled is True
    assert "myloader" in res_pipe.blocking_operation
    assert "4321" in res_pipe.next_step_recommendation


def test_failed_child_process_diagnostics_permission_denied():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=3012,
        command="pg_dump -U migkit_user -t accounts",
        returncode=1,
        stderr="pg_dump: error: query failed: ERROR: permission denied for table accounts\n42501",
    )
    assert diag.phase == CausalPhase.MISSING_PERMISSION
    assert diag.is_blocked is True
    assert diag.required_user_action is not None
    assert diag.required_user_action.action_type == "grant_permission"
    assert "GRANT SELECT" in diag.required_user_action.command_recommendation
    assert "GRANT SELECT" in diag.next_step_recommendation


def test_failed_child_process_diagnostics_killed_by_signal():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=7819,
        command="dsbulk load -k analytics -t events",
        returncode=-9,
        stderr="",
    )
    assert diag.phase == CausalPhase.CHILD_PROCESS_STALL
    assert "SIGKILL" in diag.blocking_operation or "OOM" in diag.blocking_operation
    assert "memory" in diag.next_step_recommendation.lower()


def test_failed_child_process_diagnostics_network_unreachable():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=9001,
        command="pg_dump -h 10.0.0.5 -p 5432",
        returncode=1,
        stderr="connection to server at 10.0.0.5 port 5432 failed: No route to host",
    )
    assert diag.phase == CausalPhase.NETWORK_ROUTE
    assert "10.0.0.5" in diag.blocking_operation
    assert "nc -zv 10.0.0.5 5432" in diag.next_step_recommendation


def test_terse_unix_formatting():
    diag = ProgressDiagnosis(
        phase=CausalPhase.LOCK_CONTENTION,
        is_stalled=True,
        is_blocked=True,
        liveness=False,
        progress_rate=0.0,
        eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
        eta_seconds=None,
        fake_percentage_suppressed=True,
        blocking_operation="lock on public.payments held by pid 3311",
        next_step_recommendation="SELECT pg_cancel_backend(3311);",
        required_user_action=RequiredUserAction(
            action_type="release_lock",
            resource="public.payments",
            command_recommendation="SELECT pg_cancel_backend(3311);",
            blocking_operation="lock on public.payments",
            description="blocking lock held by concurrent transaction",
        ),
    )
    terse = diag.format_terse()
    assert "phase: lock_contention" in terse
    assert "status: blocked" in terse
    assert "blocking_operation: lock on public.payments held by pid 3311" in terse
    assert "action: SELECT pg_cancel_backend(3311);" in terse
    assert "eta: blocked by user action" in terse
    assert all(ord(c) < 128 for c in terse)


def test_clear_causes_and_state_recovery():
    tracker = ProgressCauseTracker(stall_threshold_seconds=10.0)
    tracker.record_checkpoint(checkpoint_id=1, position=10, units_completed=10, units_total=100, timestamp=10.0)
    tracker.record_lock_contention(
        relation="public.users",
        blocking_pid=1122,
        engine="postgres",
    )
    d1 = tracker.evaluate_progress(now=12.0)
    assert d1.phase == CausalPhase.LOCK_CONTENTION
    assert d1.is_blocked is True

    # Clear lock contention
    tracker.clear_lock_contention()
    d2 = tracker.evaluate_progress(now=14.0)
    assert d2.phase == CausalPhase.ACTIVE_PROGRESS
    assert d2.is_blocked is False


def test_child_process_deadlock_and_lock_timeout_diagnostics():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=5510,
        command="pg_restore -d mydb dump.bin",
        returncode=1,
        stderr="pg_restore: error: could not execute query: ERROR: deadlock detected\n40P01",
    )
    assert diag.phase == CausalPhase.LOCK_CONTENTION
    assert diag.is_blocked is True
    assert "deadlock" in diag.blocking_operation.lower()
    assert "backoff" in diag.next_step_recommendation.lower()


def test_child_process_disk_full_diagnostics():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=6601,
        command="mysqlimport mydb dump.csv",
        returncode=1,
        stderr="ERROR 53100: could not extend file: No space left on device",
    )
    assert diag.phase == CausalPhase.CHILD_PROCESS_STALL
    assert diag.is_blocked is True
    assert diag.required_user_action is not None
    assert diag.required_user_action.action_type == "expand_storage"
    assert "storage" in diag.next_step_recommendation.lower() or "clean" in diag.next_step_recommendation.lower()


def test_child_process_missing_binary_exit_127():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=1234,
        command="myloader --directory /tmp/dump",
        returncode=127,
        stderr="bash: myloader: command not found",
    )
    assert diag.phase == CausalPhase.CHILD_PROCESS_STALL
    assert diag.is_blocked is True
    assert "doctor --install" in diag.next_step_recommendation


def test_child_process_sigsegv_diagnostics():
    tracker = ProgressCauseTracker()
    diag = tracker.diagnose_child_process_failure(
        pid=4002,
        command="worker-binary",
        returncode=-11,
        stderr="",
    )
    assert diag.phase == CausalPhase.CHILD_PROCESS_STALL
    assert "SIGSEGV" in diag.blocking_operation


def test_streaming_checkpoint_without_units_total():
    tracker = ProgressCauseTracker(stall_threshold_seconds=10.0)
    tracker.record_checkpoint(checkpoint_id=1, position="0/1000", units_completed=100, units_total=None, timestamp=10.0)
    tracker.record_checkpoint(checkpoint_id=2, position="0/2000", units_completed=200, units_total=None, timestamp=15.0)

    diag = tracker.evaluate_progress(now=16.0)
    assert diag.phase == CausalPhase.ACTIVE_PROGRESS
    assert diag.liveness is True
    assert diag.percentage is None
    assert diag.eta_status == EtaStatus.UNCERTAIN
    assert diag.eta_seconds is None


def test_no_emojis_in_source_or_tests():
    import migkit.progress_cause as pc
    with open(pc.__file__, "r", encoding="utf-8") as f:
        src = f.read()
    assert all(ord(c) < 128 for c in src)


def test_progress_cause_rungs_under_watched():
    with watched():
        # 1. steady_progress_observed holds
        f1 = {
            "throughput_rate": 500.0,
            "seconds_since_checkpoint": 2.0,
            "stall_threshold_seconds": 30.0,
            "has_blocking_errors": False,
            "actionable_cause_identified": False,
            "safe_remediation_provided": True,
            "transient_error": False,
            "retry_attempts": 0,
            "max_retries": 3,
            "lock_pressure": False,
            "active_workers": 1,
        }
        c1 = decide_progress_cause(f1)
        assert c1.rung.name == "steady_progress_observed"

        # 2. steady_progress_observed refuses, actionable_user_intervention_required holds
        f2 = {
            "throughput_rate": 0.0,
            "seconds_since_checkpoint": 45.0,
            "stall_threshold_seconds": 30.0,
            "has_blocking_errors": True,
            "actionable_cause_identified": True,
            "safe_remediation_provided": True,
            "transient_error": False,
            "retry_attempts": 0,
            "max_retries": 3,
            "lock_pressure": False,
            "active_workers": 1,
        }
        c2 = decide_progress_cause(f2)
        assert c2.rung.name == "actionable_user_intervention_required"

        # 3. actionable_user_intervention_required refuses, automatic_worker_backoff_and_retry holds
        f3 = {
            "throughput_rate": 0.0,
            "seconds_since_checkpoint": 45.0,
            "stall_threshold_seconds": 30.0,
            "has_blocking_errors": True,
            "actionable_cause_identified": False,
            "safe_remediation_provided": False,
            "transient_error": True,
            "retry_attempts": 1,
            "max_retries": 3,
            "lock_pressure": False,
            "active_workers": 1,
        }
        c3 = decide_progress_cause(f3)
        assert c3.rung.name == "automatic_worker_backoff_and_retry"

        # 4. automatic_worker_backoff_and_retry refuses, degrade_concurrency_under_lock_pressure holds
        f4 = {
            "throughput_rate": 0.0,
            "seconds_since_checkpoint": 45.0,
            "stall_threshold_seconds": 30.0,
            "has_blocking_errors": True,
            "actionable_cause_identified": False,
            "safe_remediation_provided": False,
            "transient_error": False,
            "retry_attempts": 5,
            "max_retries": 3,
            "lock_pressure": True,
            "active_workers": 4,
        }
        c4 = decide_progress_cause(f4)
        assert c4.rung.name == "degrade_concurrency_under_lock_pressure"

        # 5. degrade_concurrency_under_lock_pressure refuses, halt_and_report_fatal_blocker holds
        f5 = {
            "throughput_rate": 0.0,
            "seconds_since_checkpoint": 45.0,
            "stall_threshold_seconds": 30.0,
            "has_blocking_errors": True,
            "actionable_cause_identified": False,
            "safe_remediation_provided": False,
            "transient_error": False,
            "retry_attempts": 5,
            "max_retries": 3,
            "lock_pressure": False,
            "active_workers": 1,
        }
        c5 = decide_progress_cause(f5)
        assert c5.rung.name == "halt_and_report_fatal_blocker"


