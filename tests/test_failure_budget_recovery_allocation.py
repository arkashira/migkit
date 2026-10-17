"""Tests for Row 645: RCH37.failure-budget (Allocate recovery effort by progress and deadline value)."""
from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path
from typing import Any, List, Sequence, Tuple

import pytest

import tests.rung_branches
from migkit import decide, faults
from migkit.failure_budget import (
    CareBadge,
    CostLedger,
    LeaseLockHeldError,
    ProgressSnapshot,
    RecoveryBudgetCheckpoint,
    RecoveryLeaseLock,
    RepairStateTracker,
    ResourceValidityWindow,
    TerminationReason,
    UnsafeHostError,
    allocate_recovery_budget,
    compute_validity_ceiling,
    execute_budgeted_recovery,
    recovery_budget_plan,
    validate_safe_host,
)

PROVES_RUNGS = [
    ("failure_budget", "recovery_budget_plan", "halt_diagnosis_failure", "holds"),
    ("failure_budget", "recovery_budget_plan", "halt_diagnosis_failure", "refuses"),
    ("failure_budget", "recovery_budget_plan", "halt_validity_expired", "holds"),
    ("failure_budget", "recovery_budget_plan", "halt_validity_expired", "refuses"),
    ("failure_budget", "recovery_budget_plan", "reconcile_uncertain", "holds"),
    ("failure_budget", "recovery_budget_plan", "reconcile_uncertain", "refuses"),
    ("failure_budget", "recovery_budget_plan", "compound_replan_and_probe", "holds"),
    ("failure_budget", "recovery_budget_plan", "compound_replan_and_probe", "refuses"),
    ("failure_budget", "recovery_budget_plan", "progress_weighted_recovery", "holds"),
    ("failure_budget", "recovery_budget_plan", "progress_weighted_recovery", "refuses"),
    ("failure_budget", "recovery_budget_plan", "terminate_bounded", "holds"),
]


def test_every_failure_budget_rung_holds_and_refuses_under_watched() -> None:
    """Exercises both holds and refuses for every rung in recovery_budget_plan."""
    with tests.rung_branches.watched() as seen:
        # 1. halt_diagnosis_failure holds
        c1 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 2,
                "effective_window_seconds": 30.0,
                "min_step_seconds": 0.5,
                "uncertain": False,
                "can_check": True,
                "compound_fault": False,
                "admits_recovery": True,
                "progress_viable": True,
            }
        )
        assert c1.path == "halt_diagnosis_failure"

        # 2. halt_validity_expired holds (and halt_diagnosis_failure refuses)
        c2 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 0,
                "effective_window_seconds": 0.2,
                "min_step_seconds": 1.0,
                "uncertain": False,
                "can_check": True,
                "compound_fault": False,
                "admits_recovery": True,
                "progress_viable": True,
            }
        )
        assert c2.path == "halt_validity_expired"

        # 3. reconcile_uncertain holds (and halt_validity_expired refuses)
        c3 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 0,
                "effective_window_seconds": 25.0,
                "min_step_seconds": 0.5,
                "uncertain": True,
                "can_check": True,
                "reconciled_known": False,
                "compound_fault": False,
                "admits_recovery": True,
                "progress_viable": True,
            }
        )
        assert c3.path == "reconcile_uncertain"

        # 4. compound_replan_and_probe holds (and reconcile_uncertain refuses)
        c4 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 0,
                "effective_window_seconds": 40.0,
                "min_step_seconds": 0.5,
                "uncertain": False,
                "can_check": True,
                "reconciled_known": True,
                "compound_fault": True,
                "admits_recovery": True,
                "progress_viable": True,
            }
        )
        assert c4.path == "compound_replan_and_probe"

        # 5. progress_weighted_recovery holds (and compound_replan_and_probe refuses)
        c5 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 0,
                "effective_window_seconds": 40.0,
                "min_step_seconds": 0.5,
                "uncertain": False,
                "can_check": True,
                "reconciled_known": True,
                "compound_fault": False,
                "admits_recovery": True,
                "progress_viable": True,
            }
        )
        assert c5.path == "progress_weighted_recovery"

        # 6. terminate_bounded holds (and progress_weighted_recovery refuses)
        c6 = recovery_budget_plan(
            {
                "same_state_repair_repeats": 0,
                "effective_window_seconds": 40.0,
                "min_step_seconds": 0.5,
                "uncertain": False,
                "can_check": False,
                "reconciled_known": False,
                "compound_fault": False,
                "admits_recovery": False,
                "progress_viable": False,
            }
        )
        assert c6.path == "terminate_bounded"

    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        branches = tests.rung_branches.branch(seen, rung_name)
        assert expected_branch in branches, f"{rung_name} missing branch {expected_branch}: {branches}"


def test_budget_allocation_by_progress_deadline_irreversibility_and_uncertainty() -> None:
    """High progress and ample deadline earn more retry budget; high irreversibility shifts to reconcile/probe."""
    high_progress = ProgressSnapshot(
        unit_id="orders_part_1",
        units_completed=80,
        units_total=100,
        elapsed_seconds=20.0,
        deadline_seconds=120.0,
        attempt_cost_seconds=3.0,
        probe_cost_seconds=0.5,
        reconcile_cost_seconds=1.0,
        repair_cost_seconds=2.0,
        irreversibility=0.0,
        uncertainty=0.0,
    )
    windows = [ResourceValidityWindow(name="lease", ttl_seconds=90.0)]
    b_high, _ = allocate_recovery_budget(["connectivity"], high_progress, windows=windows)
    assert b_high.strategy_path == "progress_weighted_recovery"
    assert b_high.retry_tries >= 3
    assert b_high.reconcile_tries == 0
    assert b_high.care_badge == CareBadge.PASS

    # High irreversibility reduces blind retries and requires reconcile + probe first
    irrev_progress = ProgressSnapshot(
        unit_id="orders_part_2",
        units_completed=80,
        units_total=100,
        elapsed_seconds=20.0,
        deadline_seconds=120.0,
        attempt_cost_seconds=3.0,
        probe_cost_seconds=0.5,
        reconcile_cost_seconds=1.0,
        repair_cost_seconds=2.0,
        irreversibility=0.9,
        uncertainty=0.2,
    )
    b_irrev, _ = allocate_recovery_budget(["connectivity"], irrev_progress, windows=windows)
    assert b_irrev.strategy_path == "progress_weighted_recovery"
    assert b_irrev.reconcile_tries == 1
    assert b_irrev.probe_tries == 1
    assert b_irrev.retry_tries < b_high.retry_tries

    # Uncertain outcome allocates reconcile + probe and zero blind retries
    uncertain_progress = ProgressSnapshot(
        unit_id="orders_part_3",
        units_completed=50,
        units_total=100,
        elapsed_seconds=25.0,
        deadline_seconds=100.0,
        uncertainty=0.8,
    )
    b_uncert, _ = allocate_recovery_budget(
        ["uncertain_commit"], uncertain_progress, windows=windows, can_check=True
    )
    assert b_uncert.strategy_path == "reconcile_uncertain"
    assert b_uncert.reconcile_tries >= 1
    assert b_uncert.retry_tries == 0
    assert b_uncert.repair_tries == 0
    assert b_uncert.care_badge == CareBadge.REVIEW


def test_stalled_progress_near_deadline_terminates_without_waste() -> None:
    """Zero progress past 75% of the deadline refuses recovery to prevent deadline waste."""
    stalled = ProgressSnapshot(
        unit_id="stalled_table",
        units_completed=0,
        units_total=100,
        elapsed_seconds=85.0,
        deadline_seconds=100.0,
        attempt_cost_seconds=5.0,
    )
    budget, choice = allocate_recovery_budget(["connectivity"], stalled)
    assert choice.path == "terminate_bounded"
    assert budget.total_tries() == 0
    assert budget.care_badge == CareBadge.ACTION


def test_repeated_same_state_repairs_treated_as_diagnosis_failure_not_success() -> None:
    """Repeated same-state repairs must fail with DIAGNOSIS_FAILURE and ACTION badge."""
    tracker = RepairStateTracker()
    state_a = {"schema_version": 1, "missing_col": "discount_pct"}
    state_b = {"schema_version": 2, "missing_col": None}

    # First repair changes state A -> state B: valid progress
    failed_1, msg_1 = tracker.record_repair("tbl_invoices", "replan_schema", state_a, state_b, 1.2)
    assert not failed_1
    assert msg_1 == ""

    # Second repair leaves state unchanged (state_b -> state_b): immediate diagnosis failure
    failed_2, msg_2 = tracker.record_repair("tbl_invoices", "replan_schema", state_b, state_b, 1.1)
    assert failed_2
    assert "left state unchanged" in msg_2
    assert tracker.repeat_count("tbl_invoices") >= 1

    # Also test oscillating A -> B -> A repair cycle in execute_budgeted_recovery
    oscillating_tracker = RepairStateTracker()
    progress = ProgressSnapshot(
        unit_id="tbl_oscillate",
        units_completed=40,
        units_total=100,
        elapsed_seconds=10.0,
        deadline_seconds=100.0,
        attempt_cost_seconds=2.0,
        repair_cost_seconds=1.5,
    )
    windows = [ResourceValidityWindow(name="lease", ttl_seconds=60.0)]

    def broken_repair(action: str, cur_state: Any) -> Tuple[Any, float]:
        # Leaves state identical to before repair
        return dict(cur_state), 1.5

    def step_fn(wave: int, wave_faults: Sequence[str]) -> Tuple[bool, Any, float]:
        return False, {"schema_version": 1}, 2.0

    report = execute_budgeted_recovery(
        unit_id="tbl_oscillate",
        progress=progress,
        windows=windows,
        fault_sequence=[["schema_change"]],
        step_fn=step_fn,
        repair_fn=broken_repair,
        initial_state={"schema_version": 1},
        repair_tracker=oscillating_tracker,
        host="127.0.0.1",
    )
    assert not report.recovered
    assert report.termination_reason == TerminationReason.DIAGNOSIS_FAILURE
    assert report.care_badge == CareBadge.ACTION
    assert "diagnosis failure" in report.explanation
    assert "[ACTION]" in report.format_unix_line()


def test_compound_faults_invalidate_prior_cost_data() -> None:
    """Compound faults must evict prior decide.Costs measurements and increment cost generation."""
    ledger = CostLedger()
    ledger.record("parallel_copy", units=1000, seconds=10.0)
    ledger.record("stream_batch", units=500, seconds=7.5)
    assert ledger.get("parallel_copy") == pytest.approx(0.01)
    assert ledger.generation == 0

    progress = ProgressSnapshot(
        unit_id="accounts_shard_1",
        units_completed=60,
        units_total=100,
        elapsed_seconds=15.0,
        deadline_seconds=120.0,
    )
    windows = [
        ResourceValidityWindow(
            name="credential",
            ttl_seconds=10.0,
            renewable=True,
            max_renewals=2,
            renewal_cost_seconds=1.0,
            renewed_ttl_seconds=60.0,
        ),
        ResourceValidityWindow(name="wan_lease", ttl_seconds=80.0),
    ]

    budget, choice = allocate_recovery_budget(
        fault_classes=["expired_credential", "connectivity", "schema_change"],
        progress=progress,
        windows=windows,
        cost_ledger=ledger,
    )
    assert choice.path == "compound_replan_and_probe"
    assert budget.cost_data_invalidated is True
    assert ledger.generation == 1
    assert ledger.get("parallel_copy") is None
    assert ledger.get("stream_batch") is None
    assert ledger.invalidated_epochs[0]["evicted_ways"] == ["parallel_copy", "stream_batch"]
    assert ledger.invalidated_epochs[0]["fault_classes"] == [
        "expired_credential",
        "connectivity",
        "schema_change",
    ]


def test_credential_expiry_plus_wan_partition_plus_schema_drift_recovers_bounded() -> None:
    """Proves credential expiry + WAN partition + schema drift recovers with explainable state and bounded waste."""
    ledger = CostLedger()
    ledger.record("pre_fault_copier", units=500, seconds=5.0)
    tracker = RepairStateTracker()

    progress = ProgressSnapshot(
        unit_id="customer_ledger",
        units_completed=70,
        units_total=100,
        elapsed_seconds=20.0,
        deadline_seconds=100.0,
        attempt_cost_seconds=2.5,
        probe_cost_seconds=0.5,
        reconcile_cost_seconds=1.0,
        repair_cost_seconds=2.0,
        irreversibility=0.4,
        uncertainty=0.2,
    )
    windows = [
        ResourceValidityWindow(
            name="credential",
            ttl_seconds=4.0,
            renewable=True,
            max_renewals=2,
            renewal_cost_seconds=0.8,
            renewed_ttl_seconds=50.0,
        ),
        ResourceValidityWindow(name="wan_session_window", ttl_seconds=45.0, renewable=False),
        ResourceValidityWindow(name="snapshot_retention", ttl_seconds=60.0, renewable=False),
    ]

    fault_waves = [
        ["expired_credential", "connectivity", "schema_change"],
        [],
    ]

    def renew_fn(res_name: str) -> Tuple[bool, float]:
        return True, 0.8

    def reconcile_fn() -> Tuple[str, float]:
        return faults.NOTHING, 1.0

    def probe_fn() -> Tuple[bool, float]:
        return True, 0.5

    def repair_fn(action: str, state: Any) -> Tuple[Any, float]:
        updated = dict(state)
        updated["schema_epoch"] = int(updated.get("schema_epoch", 1)) + 1
        updated["columns_aligned"] = True
        return updated, 2.0

    def step_fn(wave_idx: int, wave_faults: Sequence[str]) -> Tuple[bool, Any, float]:
        if wave_idx == 0:
            # First attempt after initial repair/probe hits transient tail of WAN healing, then succeeds on retry
            return False, {"schema_epoch": 2, "columns_aligned": True, "cursor": 70}, 2.5
        return True, {"schema_epoch": 2, "columns_aligned": True, "cursor": 100}, 2.5

    report = execute_budgeted_recovery(
        unit_id="customer_ledger",
        progress=progress,
        windows=windows,
        fault_sequence=fault_waves,
        step_fn=step_fn,
        reconcile_fn=reconcile_fn,
        probe_fn=probe_fn,
        repair_fn=repair_fn,
        renew_fn=renew_fn,
        initial_state={"schema_epoch": 1, "columns_aligned": False, "cursor": 70},
        cost_ledger=ledger,
        repair_tracker=tracker,
        host="10.0.0.1",
    )

    assert report.recovered is True
    assert report.termination_reason == TerminationReason.RECOVERED
    assert report.compound_fault_detected is True
    assert report.cost_data_invalidated is True
    assert set(report.faults_observed) == {"expired_credential", "connectivity", "schema_change"}
    assert report.waste_seconds <= report.allocated_budget.max_waste_seconds
    assert report.elapsed_seconds <= report.validity_ceiling_seconds
    # New post-recovery cost measurement was recorded after invalidation
    assert ledger.get("post_recovery_way") is not None
    line = report.format_unix_line()
    assert line.startswith("[REVIEW] unit=customer_ledger status=RECOVERED")
    assert all(ord(c) < 128 for c in line)


def test_compound_faults_never_outlive_resource_validity_windows() -> None:
    """Proves no retry loop can outlive resource validity windows under compound faults."""
    progress = ProgressSnapshot(
        unit_id="short_window_unit",
        units_completed=50,
        units_total=100,
        elapsed_seconds=10.0,
        deadline_seconds=120.0,
        attempt_cost_seconds=3.0,
        probe_cost_seconds=0.5,
        reconcile_cost_seconds=0.5,
        repair_cost_seconds=1.5,
    )
    # Non-renewable snapshot window of only 4.0 seconds total
    windows = [
        ResourceValidityWindow(
            name="credential",
            ttl_seconds=2.0,
            renewable=True,
            max_renewals=1,
            renewal_cost_seconds=0.5,
            renewed_ttl_seconds=20.0,
        ),
        ResourceValidityWindow(
            name="non_renewable_snapshot",
            ttl_seconds=4.0,
            renewable=False,
        ),
    ]

    eff_ceiling, imm_ceiling, limiting = compute_validity_ceiling(progress, windows)
    assert eff_ceiling == pytest.approx(4.0)
    assert imm_ceiling == pytest.approx(2.0)
    assert limiting == "non_renewable_snapshot"

    attempts_made: List[int] = []

    def step_fn(wave_idx: int, wave_faults: Sequence[str]) -> Tuple[bool, Any, float]:
        attempts_made.append(wave_idx)
        return False, {"wave": wave_idx}, 3.0

    report = execute_budgeted_recovery(
        unit_id="short_window_unit",
        progress=progress,
        windows=windows,
        fault_sequence=[
            ["expired_credential", "connectivity", "schema_change"],
            ["connectivity"],
            ["connectivity"],
        ],
        step_fn=step_fn,
        probe_fn=lambda: (True, 0.5),
        repair_fn=lambda act, st: ({"repaired": True, "wave": 1}, 1.5),
        renew_fn=lambda name: (True, 0.5),
        initial_state={"repaired": False},
        host="10.0.0.2",
    )

    assert report.recovered is False
    assert report.termination_reason == TerminationReason.VALIDITY_WINDOW_EXPIRED
    assert report.care_badge == CareBadge.ACTION
    # Renewal (0.5s) + probe (0.5s) + repair (1.5s) = 2.5s; remaining snapshot TTL is 1.5s < attempt_cost (3.0s)
    # So the retry loop refuses to start an attempt that would outlive the 4.0s snapshot validity window!
    assert len(attempts_made) == 0
    assert report.elapsed_seconds <= 4.0
    assert report.waste_seconds <= 4.0


def test_o1_typed_durable_checkpoint_and_active_lease_lock(tmp_path: Path) -> None:
    """Verifies O(1) typed checkpoint resume (datetime, UUID, Decimal, composite key) and .run.lock."""
    ckpt_path = tmp_path / "failure_budget_ckpt.json"
    ckpt = RecoveryBudgetCheckpoint(ckpt_path)

    composite_cursor = {
        "pk_tuple": ("tenant_a", 42),
        "watermark": datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc),
        "tx_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "amount": decimal.Decimal("99999.123456"),
    }
    ckpt.mark_completed("unit_alpha", composite_cursor, waste_seconds=1.25)

    reloaded = RecoveryBudgetCheckpoint(ckpt_path)
    assert reloaded.is_completed("unit_alpha") is True
    assert reloaded.is_completed("unit_beta") is False
    restored = reloaded.get_cursor("unit_alpha")
    assert restored["pk_tuple"] == ("tenant_a", 42)
    assert isinstance(restored["pk_tuple"], tuple)
    assert restored["watermark"] == composite_cursor["watermark"]
    assert restored["tx_id"] == composite_cursor["tx_id"]
    assert restored["amount"] == composite_cursor["amount"]

    # Completed unit skips execution in O(1) with 0 calls to step_fn
    called = False

    def fail_if_called(w: int, f: Sequence[str]) -> Tuple[bool, Any, float]:
        nonlocal called
        called = True
        return False, None, 1.0

    progress = ProgressSnapshot("unit_alpha", 100, 100, 10.0, 60.0)
    report = execute_budgeted_recovery(
        unit_id="unit_alpha",
        progress=progress,
        windows=[ResourceValidityWindow("lease", 30.0)],
        fault_sequence=[["connectivity"]],
        step_fn=fail_if_called,
        checkpoint=reloaded,
        host="127.0.0.1",
    )
    assert report.recovered is True
    assert called is False
    assert report.allocated_budget.strategy_path == "cached_checkpoint_skip"

    # Mutual exclusion .run.lock
    clock = [1000.0]
    with RecoveryLeaseLock(tmp_path, owner_id="worker-a", ttl_seconds=20.0, now_fn=lambda: clock[0]) as lock_a:
        lock_a.heartbeat()
        with pytest.raises(LeaseLockHeldError):
            RecoveryLeaseLock(
                tmp_path, owner_id="worker-b", ttl_seconds=20.0, now_fn=lambda: clock[0]
            ).acquire()

    # Unsafe host check
    with pytest.raises(UnsafeHostError):
        validate_safe_host("192.168.99.99")
