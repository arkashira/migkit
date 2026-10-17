import signal
import pytest

from migkit.process_memory import (
    FailureClassification,
    MemoryBudgetExceededError,
    MemoryCategory,
    ProcessDiagnostic,
    ProcessFailureInfo,
    ProcessMemoryError,
    ProcessMemoryGovernor,
    ReconciliationRequiredError,
    ResumptionPlan,
    TargetReconciler,
    TargetState,
    UnverifiedResumeRefusedError,
    classify_process_failure,
    estimate_worker_memory,
    reconcile_target_state,
)


def test_governor_budget_allocation_and_release():
    total_budget = 256 * 1024 * 1024
    reserve_bytes = 16 * 1024 * 1024
    gov = ProcessMemoryGovernor(total_budget_bytes=total_budget, reserve_bytes=reserve_bytes)

    assert gov.total_budget_bytes == total_budget
    assert gov.reserve_bytes == reserve_bytes
    assert gov.usable_budget_bytes == total_budget - reserve_bytes
    assert gov.allocated_bytes() == 0
    assert gov.available_bytes() == 240 * 1024 * 1024

    gov.allocate(MemoryCategory.CHILD_HEAP, 40 * 1024 * 1024, owner_id="worker-1")
    gov.allocate(MemoryCategory.DECODER_BUFFER, 20 * 1024 * 1024, owner_id="worker-1")
    gov.allocate(MemoryCategory.DRIVER_PREFETCH, 15 * 1024 * 1024, owner_id="worker-1")
    gov.allocate(MemoryCategory.COMPRESSION, 10 * 1024 * 1024, owner_id="worker-1")
    gov.allocate(MemoryCategory.QUEUE, 25 * 1024 * 1024, owner_id="worker-1")
    gov.allocate(MemoryCategory.CONCURRENT_PROOFS, 30 * 1024 * 1024, owner_id="verifier-1")

    assert gov.allocated_bytes() == 140 * 1024 * 1024
    assert gov.available_bytes() == 100 * 1024 * 1024

    breakdown = gov.breakdown()
    assert breakdown["child_heap"] == 40 * 1024 * 1024
    assert breakdown["decoder_buffer"] == 20 * 1024 * 1024
    assert breakdown["driver_prefetch"] == 15 * 1024 * 1024
    assert breakdown["compression"] == 10 * 1024 * 1024
    assert breakdown["queue"] == 25 * 1024 * 1024
    assert breakdown["concurrent_proofs"] == 30 * 1024 * 1024

    gov.release(MemoryCategory.CONCURRENT_PROOFS, 30 * 1024 * 1024, owner_id="verifier-1")
    assert gov.allocated_bytes(MemoryCategory.CONCURRENT_PROOFS) == 0
    assert gov.available_bytes() == 130 * 1024 * 1024

    gov.release_owner("worker-1")
    assert gov.allocated_bytes() == 0
    assert gov.available_bytes() == 240 * 1024 * 1024


def test_governor_budget_exceeded_raises_error():
    gov = ProcessMemoryGovernor(total_budget_bytes=64 * 1024 * 1024, reserve_bytes=4 * 1024 * 1024)
    gov.allocate(MemoryCategory.CHILD_HEAP, 50 * 1024 * 1024)

    assert not gov.try_allocate(MemoryCategory.QUEUE, 15 * 1024 * 1024)

    with pytest.raises(MemoryBudgetExceededError) as exc_info:
        gov.allocate(MemoryCategory.QUEUE, 15 * 1024 * 1024)
    assert "budget exceeded" in str(exc_info.value).lower()


def test_governor_context_manager_reservation():
    gov = ProcessMemoryGovernor(total_budget_bytes=64 * 1024 * 1024)
    with gov.reserve(MemoryCategory.COMPRESSION, 20 * 1024 * 1024, owner_id="tx-compress"):
        assert gov.allocated_bytes(MemoryCategory.COMPRESSION) == 20 * 1024 * 1024
        assert gov.available_bytes() == 44 * 1024 * 1024
    assert gov.allocated_bytes(MemoryCategory.COMPRESSION) == 0
    assert gov.available_bytes() == 64 * 1024 * 1024


def test_estimate_worker_memory_and_safe_workers_constrained():
    row_size = 2 * 1024 * 1024
    req = estimate_worker_memory(row_size_bytes=row_size, batch_size=50, prefetch_rows=20, queue_capacity=20)

    assert req[MemoryCategory.CHILD_HEAP] >= row_size * 50
    assert req[MemoryCategory.DECODER_BUFFER] >= row_size
    assert req[MemoryCategory.DRIVER_PREFETCH] >= row_size * 20
    assert req[MemoryCategory.COMPRESSION] > 0
    assert req[MemoryCategory.QUEUE] >= row_size * 20

    per_worker = sum(req.values())
    budget = per_worker * 3 + (32 * 1024 * 1024)
    gov = ProcessMemoryGovernor(total_budget_bytes=budget)

    safe = gov.calculate_safe_workers(
        row_size_bytes=row_size,
        batch_size=50,
        prefetch_rows=20,
        queue_capacity=20,
        concurrent_proofs=1,
        proof_memory_bytes=32 * 1024 * 1024,
    )
    assert safe == 3

    strict_gov = ProcessMemoryGovernor(total_budget_bytes=per_worker // 2)
    zero_workers = strict_gov.calculate_safe_workers(
        row_size_bytes=row_size,
        batch_size=50,
        prefetch_rows=20,
        queue_capacity=20,
    )
    assert zero_workers == 0

    adapted_w, adapted_batch = strict_gov.admit_constrained_worker(row_size_bytes=row_size)
    assert adapted_w <= 1
    if adapted_w == 1:
        assert adapted_batch * row_size <= strict_gov.usable_budget_bytes


def test_concurrent_phases_cannot_exceed_global_budget():
    gov = ProcessMemoryGovernor(total_budget_bytes=100 * 1024 * 1024)

    p1 = gov.admit_phase(
        phase_name="bulk_copy",
        requested_workers=2,
        row_size_bytes=64 * 1024,
        batch_size=100,
        proof_workers=0,
    )
    assert p1.admitted
    assert p1.allocated_bytes <= 100 * 1024 * 1024

    p2 = gov.admit_phase(
        phase_name="cdc_replication",
        requested_workers=2,
        row_size_bytes=64 * 1024,
        batch_size=100,
        proof_workers=0,
    )
    assert p2.admitted

    p3 = gov.admit_phase(
        phase_name="concurrent_proof",
        requested_workers=4,
        row_size_bytes=2 * 1024 * 1024,
        batch_size=500,
        proof_workers=2,
    )
    assert not p3.admitted or p3.allocated_bytes <= gov.available_bytes()
    assert gov.allocated_bytes() <= gov.usable_budget_bytes


def test_classify_process_failure_sigkill_oom():
    d1 = classify_process_failure(returncode=-9, stderr="fatal: killed")
    assert d1.classification == FailureClassification.OS_KILL_OOM
    assert d1.action_badge == "ACTION"
    assert "SIGKILL" in d1.detail or "OOM" in d1.detail

    d2 = classify_process_failure(returncode=137, stderr="kernel: Out of memory: Kill process 1234")
    assert d2.classification == FailureClassification.OS_KILL_OOM
    assert d2.action_badge == "ACTION"


def test_classify_process_failure_driver_memory_error():
    d1 = classify_process_failure(returncode=1, stderr="psycopg2.OperationalError: out of memory (allocated 1048576)")
    assert d1.classification == FailureClassification.DRIVER_MEMORY_ERROR
    assert d1.action_badge == "ACTION"
    assert "driver" in d1.detail.lower() or "memory" in d1.detail.lower()

    d2 = classify_process_failure(returncode=1, stderr="java.lang.OutOfMemoryError: Java heap space")
    assert d2.classification == FailureClassification.DRIVER_MEMORY_ERROR
    assert d2.action_badge == "ACTION"


def test_classify_process_failure_throughput_stall():
    d = classify_process_failure(
        returncode=None,
        is_active=True,
        stall_duration_seconds=45.0,
        stall_threshold_seconds=30.0,
        throughput_bytes_per_sec=0.0,
    )
    assert d.classification == FailureClassification.THROUGHPUT_STALL
    assert d.action_badge == "REVIEW"
    assert "stall" in d.detail.lower()


def test_classify_process_failure_other_or_success():
    d_succ = classify_process_failure(returncode=0, throughput_bytes_per_sec=1024.0)
    assert d_succ.classification == FailureClassification.NONE
    assert d_succ.action_badge == "PASS"

    d_other = classify_process_failure(returncode=2, stderr="syntax error at or near 'SELECT'")
    assert d_other.classification == FailureClassification.OTHER_ERROR
    assert d_other.action_badge == "REVIEW"


def test_reconcile_target_state_rolls_back_unverified_rows():
    discarded = []

    def mock_cleaner(from_pos, to_pos):
        discarded.append((from_pos, to_pos))
        return 15

    target_state = TargetState(table="events", reported_watermark=1500)
    reconciled = reconcile_target_state(
        target_state=target_state,
        verified_position=1000,
        unverified_cleanup_fn=mock_cleaner,
    )

    assert reconciled.reconciled
    assert reconciled.safe_position == 1000
    assert reconciled.unverified_rows_discarded == 15
    assert discarded == [(1000, 1500)]


def test_reconcile_target_state_refuses_unverified_restart():
    target_state = TargetState(table="events", reported_watermark=500)

    with pytest.raises(UnverifiedResumeRefusedError):
        reconcile_target_state(
            target_state=target_state,
            verified_position=None,
            unverified_cleanup_fn=None,
        )


def test_reconcile_before_changing_mover_or_workers():
    gov = ProcessMemoryGovernor(total_budget_bytes=64 * 1024 * 1024)
    reconciler = TargetReconciler(governor=gov)

    with pytest.raises(ReconciliationRequiredError):
        reconciler.adapt_execution(
            new_mover="builtin",
            new_workers=2,
            row_size_bytes=1024,
        )

    target_state = TargetState(table="large_docs", reported_watermark=2000)
    reconciler.reconcile(
        target_state=target_state,
        verified_position=1200,
        unverified_cleanup_fn=lambda a, b: 800,
    )

    plan = reconciler.adapt_execution(
        new_mover="builtin",
        new_workers=4,
        row_size_bytes=512 * 1024,
        batch_size=50,
    )

    assert isinstance(plan, ResumptionPlan)
    assert plan.mover == "builtin"
    assert plan.reconciled_position == 1200
    assert plan.workers > 0
    assert plan.unverified_rows_discarded == 800
    assert plan.total_allocated <= gov.usable_budget_bytes


def test_governor_preserves_invariants_under_failure_and_resume():
    gov = ProcessMemoryGovernor(total_budget_bytes=128 * 1024 * 1024)
    reconciler = TargetReconciler(governor=gov)

    gov.allocate(MemoryCategory.CHILD_HEAP, 60 * 1024 * 1024, owner_id="mover-pgloader")
    gov.allocate(MemoryCategory.DRIVER_PREFETCH, 40 * 1024 * 1024, owner_id="mover-pgloader")

    diag = classify_process_failure(returncode=-9, stderr="killed")
    assert diag.classification == FailureClassification.OS_KILL_OOM

    gov.release_owner("mover-pgloader")
    assert gov.allocated_bytes() == 0

    reconciler.reconcile(
        target_state=TargetState(table="orders", reported_watermark=5000),
        verified_position=3000,
        unverified_cleanup_fn=lambda a, b: 2000,
    )

    plan = reconciler.adapt_execution(
        new_mover="builtin",
        new_workers=2,
        row_size_bytes=1024 * 1024,
        batch_size=10,
        proof_workers=1,
    )

    assert plan.mover == "builtin"
    assert plan.reconciled_position == 3000
    assert plan.workers <= 2
    assert plan.total_allocated <= gov.usable_budget_bytes
