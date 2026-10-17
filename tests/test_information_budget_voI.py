"""Tests for Row 587: SMART-N4.information-budget (Choose what to measure by its decision value).

Verifies:
1. Every `decide.Rung` in `migkit/information_budget.py` (`PROBE_ADMISSION_LADDER`,
   `SCANPLAN_FUSION_LADDER`, `STOPPING_RULE_LADDER`, and `CONSERVATIVE_BASELINE_LADDER`)
   reaches both `holds` and `refuses` under `tests.rung_branches.watched()` and
   is declared in `PROVES_RUNGS`.
2. Mathematical Value of Information (VoI):
   - Computes expected reduction of decision loss.
   - Produces VoI = 0.0 when uncertainty cannot change the winning plan or feasibility.
   - Identifies high-value probes when uncertain fact determines plan feasibility.
3. Multi-dimensional probe cost accounting:
   - Compares VoI with probe time, source CPU cores, I/O bytes, lock hold duration,
     money cost, and deadline slack margin.
   - Refuses probes with negative net value, excessive source impact, or deadline risk.
4. Scanplan fusion (`scanplan`):
   - Fuses compatible reads targeting the same table to amortize locks and round trips.
   - Selects partitioned range probing for large splittable datasets.
5. Conservative baseline preservation:
   - Unknown is never assumed support; unproven assumptions enforce conservative baseline.
   - Proved optimistic plans adopted only after measurements verify feasibility.
6. Universal typed durable resume in O(1):
   - Checkpoints with native Python types (`datetime`, `Decimal`, `UUID`, composite tuples).
   - Skips previously probed facts in O(1) without rescan.
7. Active verification lease lock (`.run.lock`) with heartbeat preventing overlapping runs.
8. Adaptive AIMD probe batch sizing and cold-start byte-weighted ETA.
9. Normalized external tool wrapper with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
"""

from __future__ import annotations

import datetime
import decimal
import math
import time
import uuid
from pathlib import Path
from typing import List

import pytest
import tests.rung_branches
from tests.rung_branches import branch

from migkit import information_budget as ib

PROVES_RUNGS = [
    # CONSERVATIVE_BASELINE_LADDER
    ("information_budget", "CONSERVATIVE_BASELINE_LADDER", "adopt_optimistic_plan_with_proof", "holds"),
    ("information_budget", "CONSERVATIVE_BASELINE_LADDER", "adopt_optimistic_plan_with_proof", "refuses"),
    ("information_budget", "CONSERVATIVE_BASELINE_LADDER", "enforce_conservative_baseline", "holds"),
    ("information_budget", "CONSERVATIVE_BASELINE_LADDER", "enforce_conservative_baseline", "refuses"),
    # PROBE_ADMISSION_LADDER
    ("information_budget", "PROBE_ADMISSION_LADDER", "admit_high_net_value_probe", "holds"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "admit_high_net_value_probe", "refuses"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "admit_fused_scanplan_probe", "holds"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "admit_fused_scanplan_probe", "refuses"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_negative_voi", "holds"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_negative_voi", "refuses"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_excessive_source_impact", "holds"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_excessive_source_impact", "refuses"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_deadline_slack_breach", "holds"),
    ("information_budget", "PROBE_ADMISSION_LADDER", "refuse_deadline_slack_breach", "refuses"),
    # SCANPLAN_FUSION_LADDER
    ("information_budget", "SCANPLAN_FUSION_LADDER", "fused_single_scan_batch", "holds"),
    ("information_budget", "SCANPLAN_FUSION_LADDER", "fused_single_scan_batch", "refuses"),
    ("information_budget", "SCANPLAN_FUSION_LADDER", "partitioned_range_probe", "holds"),
    ("information_budget", "SCANPLAN_FUSION_LADDER", "partitioned_range_probe", "refuses"),
    ("information_budget", "SCANPLAN_FUSION_LADDER", "isolated_single_probe", "holds"),
    ("information_budget", "SCANPLAN_FUSION_LADDER", "isolated_single_probe", "refuses"),
    # STOPPING_RULE_LADDER
    ("information_budget", "STOPPING_RULE_LADDER", "stop_uncertainty_immaterial", "holds"),
    ("information_budget", "STOPPING_RULE_LADDER", "stop_uncertainty_immaterial", "refuses"),
    ("information_budget", "STOPPING_RULE_LADDER", "stop_budget_exhausted", "holds"),
    ("information_budget", "STOPPING_RULE_LADDER", "stop_budget_exhausted", "refuses"),
    ("information_budget", "STOPPING_RULE_LADDER", "continue_probing_viable", "holds"),
    ("information_budget", "STOPPING_RULE_LADDER", "continue_probing_viable", "refuses"),
]


def _make_sample_telemetry(
    *,
    available_slack_s: float = 300.0,
    total_deadline_s: float = 1200.0,
    disk_read_bps: float = 100_000_000.0,
) -> ib.TelemetryReadings:
    return ib.TelemetryReadings(
        measured_disk_read_bps=disk_read_bps,
        measured_cpu_seconds_per_core_sec=1.0,
        measured_dollar_per_wall_second=0.01,
        available_deadline_slack_s=available_slack_s,
        total_deadline_budget_s=total_deadline_s,
        source_cold_start_headroom_ratio=0.75,
    )


def _make_sample_limits(
    *,
    max_wall_s: float = 30.0,
    max_cpu_s: float = 60.0,
    max_io_bytes: int = 500_000_000,
    max_lock_s: float = 1.0,
) -> ib.InformationBudgetLimits:
    return ib.InformationBudgetLimits(
        max_probe_wall_s=max_wall_s,
        max_source_cpu_core_seconds=max_cpu_s,
        max_source_io_bytes=max_io_bytes,
        max_source_lock_duration_s=max_lock_s,
        max_money_budget_dollars=10.0,
        min_deadline_slack_margin_ratio=0.10,
        min_material_loss_reduction_s=0.5,
    )


def test_every_decide_rung_is_exercised_on_both_branches():
    """Verify that every decide rung in information_budget reaches holds and refuses under watched()."""
    telemetry = _make_sample_telemetry()
    limits = _make_sample_limits()

    with tests.rung_branches.watched() as seen:
        # 1. PROBE_ADMISSION_LADDER
        # A) admit_high_net_value_probe holds
        p_good = ib.ProbeCostProfile(
            probe_id="p_good",
            fact_key="wal_level",
            target_host="127.0.0.1",
            expected_duration_s=0.2,
            source_cpu_cores=0.5,
            source_io_bytes=1000,
            lock_mode=ib.ProbeLockMode.NONE,
            lock_duration_s=0.0,
            money_cost_dollars=0.0,
        )
        ch1 = ib.decide_probe_admission(
            "test:p_good",
            probe=p_good,
            raw_voi_s=100.0,
            readings=telemetry,
            budget_limits=limits,
            spent_wall_s=0.0,
            spent_cpu_s=0.0,
            spent_io_bytes=0,
            spent_lock_s=0.0,
        )
        assert ch1.path == "admit_high_net_value_probe"

        # B) admit_fused_scanplan_probe holds (when is_scanplan_batch is True and standalone lacks raw VoI margin)
        p_fused = ib.ProbeCostProfile(
            probe_id="p_fused",
            fact_key="col_stats",
            target_host="10.0.0.12",
            expected_duration_s=5.0,
            source_cpu_cores=1.0,
            source_io_bytes=10_000,
            lock_mode=ib.ProbeLockMode.SHARED_READ,
            lock_duration_s=0.1,
            money_cost_dollars=0.0,
            is_scanplan_compatible=True,
            scanplan_table="orders",
        )
        # Raw standalone VoI has net_voi_s <= min_material_loss_reduction, but fused_net_voi_s is high
        ch2 = ib.decide_probe_admission(
            "test:p_fused",
            probe=p_fused,
            raw_voi_s=3.0,
            readings=telemetry,
            budget_limits=limits,
            spent_wall_s=0.0,
            spent_cpu_s=0.0,
            spent_io_bytes=0,
            spent_lock_s=0.0,
            is_scanplan_batch=True,
            fused_cost_s=0.5,
        )
        assert ch2.path == "admit_fused_scanplan_probe"

        # C) refuse_negative_voi holds
        p_useless = ib.ProbeCostProfile(
            probe_id="p_useless",
            fact_key="unimportant_flag",
            target_host="127.0.0.1",
            expected_duration_s=1.0,
            source_cpu_cores=1.0,
            source_io_bytes=5000,
            lock_mode=ib.ProbeLockMode.NONE,
            lock_duration_s=0.0,
            money_cost_dollars=0.0,
        )
        ch3 = ib.decide_probe_admission(
            "test:p_useless",
            probe=p_useless,
            raw_voi_s=0.0,
            readings=telemetry,
            budget_limits=limits,
            spent_wall_s=0.0,
            spent_cpu_s=0.0,
            spent_io_bytes=0,
            spent_lock_s=0.0,
        )
        assert ch3.path == "refuse_negative_voi"

        # D) refuse_excessive_source_impact holds (e.g. lock duration breaches budget)
        p_heavy = ib.ProbeCostProfile(
            probe_id="p_heavy",
            fact_key="heavy_check",
            target_host="127.0.0.1",
            expected_duration_s=2.0,
            source_cpu_cores=4.0,
            source_io_bytes=1000,
            lock_mode=ib.ProbeLockMode.EXCLUSIVE_TABLE_LOCK,
            lock_duration_s=5.0,  # exceeds max_lock_s of 1.0
            money_cost_dollars=0.0,
        )
        ch4 = ib.decide_probe_admission(
            "test:p_heavy",
            probe=p_heavy,
            raw_voi_s=500.0,
            readings=telemetry,
            budget_limits=limits,
            spent_wall_s=0.0,
            spent_cpu_s=0.0,
            spent_io_bytes=0,
            spent_lock_s=0.0,
        )
        assert ch4.path == "refuse_excessive_source_impact"

        # E) refuse_deadline_slack_breach holds (probe would deplete remaining deadline slack)
        p_slow = ib.ProbeCostProfile(
            probe_id="p_slow",
            fact_key="slow_check",
            target_host="127.0.0.1",
            expected_duration_s=250.0,  # leaves less than 10% slack of 1200
            source_cpu_cores=0.1,
            source_io_bytes=1000,
            lock_mode=ib.ProbeLockMode.NONE,
            lock_duration_s=0.0,
            money_cost_dollars=0.0,
        )
        tight_telemetry = _make_sample_telemetry(available_slack_s=260.0, total_deadline_s=1200.0)
        ch5 = ib.decide_probe_admission(
            "test:p_slow",
            probe=p_slow,
            raw_voi_s=500.0,
            readings=tight_telemetry,
            budget_limits=limits,
            spent_wall_s=0.0,
            spent_cpu_s=0.0,
            spent_io_bytes=0,
            spent_lock_s=0.0,
        )
        assert ch5.path == "refuse_deadline_slack_breach"

        # 2. SCANPLAN_FUSION_LADDER
        # A) fused_single_scan_batch holds
        p1 = ib.ProbeCostProfile(
            "p1", "f1", "127.0.0.1", 0.5, 0.5, 1000, ib.ProbeLockMode.SHARED_READ, 0.01, 0.0,
            is_scanplan_compatible=True, scanplan_table="t1"
        )
        p2 = ib.ProbeCostProfile(
            "p2", "f2", "127.0.0.1", 0.6, 0.5, 1000, ib.ProbeLockMode.SHARED_READ, 0.01, 0.0,
            is_scanplan_compatible=True, scanplan_table="t1"
        )
        ch_fused = ib.decide_scanplan_fusion("test:fusion_batch", probes=[p1, p2])
        assert ch_fused.path == "fused_single_scan_batch"

        # B) partitioned_range_probe holds (splittable keys + large dataset)
        p3 = ib.ProbeCostProfile(
            "p3", "f3", "127.0.0.1", 2.0, 1.0, 50000, ib.ProbeLockMode.SHARED_READ, 0.05, 0.0,
            is_scanplan_compatible=False, scanplan_table="t2"
        )
        ch_part = ib.decide_scanplan_fusion(
            "test:partition",
            probes=[p3],
            splittable_key_ranges=True,
            dataset_large=True,
        )
        assert ch_part.path == "partitioned_range_probe"

        # C) isolated_single_probe holds
        ch_iso = ib.decide_scanplan_fusion(
            "test:isolated",
            probes=[p3],
            splittable_key_ranges=False,
            dataset_large=False,
            isolation_required=True,
        )
        assert ch_iso.path == "isolated_single_probe"

        # 3. STOPPING_RULE_LADDER
        # A) stop_uncertainty_immaterial holds
        ch_stop_imm = ib.decide_stopping_rule(
            "test:stop_imm",
            max_remaining_voi_s=0.1,  # < min_material of 0.5
            decision_margin_s=10.0,
            all_critical_facts_known=True,
            budget_limits=limits,
            spent_wall_s=5.0,
            spent_cpu_s=5.0,
            spent_io_bytes=1000,
            spent_lock_s=0.01,
            readings=telemetry,
            admissible_probe_count=0,
        )
        assert ch_stop_imm.path == "stop_uncertainty_immaterial"

        # B) stop_budget_exhausted holds (spent all wall budget)
        ch_stop_budg = ib.decide_stopping_rule(
            "test:stop_budg",
            max_remaining_voi_s=50.0,
            decision_margin_s=0.1,
            all_critical_facts_known=False,
            budget_limits=limits,
            spent_wall_s=35.0,  # exceeds max_wall_s of 30.0
            spent_cpu_s=5.0,
            spent_io_bytes=1000,
            spent_lock_s=0.01,
            readings=telemetry,
            admissible_probe_count=3,
        )
        assert ch_stop_budg.path == "stop_budget_exhausted"

        # C) continue_probing_viable holds
        ch_cont = ib.decide_stopping_rule(
            "test:cont",
            max_remaining_voi_s=25.0,
            decision_margin_s=0.2,
            all_critical_facts_known=False,
            budget_limits=limits,
            spent_wall_s=5.0,
            spent_cpu_s=5.0,
            spent_io_bytes=1000,
            spent_lock_s=0.01,
            readings=telemetry,
            admissible_probe_count=2,
        )
        assert ch_cont.path == "continue_probing_viable"

        # 4. CONSERVATIVE_BASELINE_LADDER
        # A) adopt_optimistic_plan_with_proof holds
        plan_opt = ib.CandidatePlan(
            plan_id="opt_stream",
            nominal_cost_s=100.0,
            fallback_penalty_s=1000.0,
            is_conservative_baseline=False,
            prerequisite_conditions={"wal_logical": "true"},
        )
        facts_proved = {
            "wal_logical": ib.UncertainFact(
                fact_key="wal_logical",
                distribution_kind=ib.FactDistributionKind.BOOLEAN,
                prior_probabilities={"true": 1.0},
                measured_value="true",
                is_measured=True,
            )
        }
        ch_adopt = ib.decide_plan_choice("test:adopt", winning_candidate=plan_opt, facts_map=facts_proved)
        assert ch_adopt.path == "adopt_optimistic_plan_with_proof"

        # B) enforce_conservative_baseline holds
        facts_unproved = {
            "wal_logical": ib.UncertainFact(
                fact_key="wal_logical",
                distribution_kind=ib.FactDistributionKind.BOOLEAN,
                prior_probabilities={"true": 0.5, "false": 0.5},
                measured_value=None,
                is_measured=False,
            )
        }
        ch_enforce = ib.decide_plan_choice("test:enforce", winning_candidate=plan_opt, facts_map=facts_unproved)
        assert ch_enforce.path == "enforce_conservative_baseline"

    # Now verify that both holds and refuses were observed for all rungs
    for _, ladder_name, rung_name, branch_name in PROVES_RUNGS:
        branches = branch(seen, rung_name)
        assert branch_name in branches, (
            f"Rung {rung_name!r} in {ladder_name!r} failed to reach {branch_name!r}. "
            f"Observed branches: {branches}"
        )


def test_mathematical_value_of_information():
    """Verify EVSI and decision loss reduction under varying uncertainty."""
    # Fast plan requires wal_logical == true. If false, pays penalty of 500s.
    plan_fast = ib.CandidatePlan(
        plan_id="fast_cdc",
        nominal_cost_s=50.0,
        fallback_penalty_s=500.0,
        is_conservative_baseline=False,
        prerequisite_conditions={"wal_logical": "true"},
    )
    # Conservative baseline runs in 150s with 0 penalty under any state.
    plan_baseline = ib.CandidatePlan(
        plan_id="safe_bulk",
        nominal_cost_s=150.0,
        fallback_penalty_s=0.0,
        is_conservative_baseline=True,
        prerequisite_conditions={},
    )
    plans = [plan_fast, plan_baseline]

    # Prior: 50% chance wal_logical is true
    f_uncertain = ib.UncertainFact(
        fact_key="wal_logical",
        distribution_kind=ib.FactDistributionKind.BOOLEAN,
        prior_probabilities={"true": 0.5, "false": 0.5},
    )
    facts = {"wal_logical": f_uncertain}

    # Loss under prior:
    # fast plan: 0.5*50 + 0.5*(50+500) = 300s
    # baseline plan: 150s
    # Best prior plan is baseline (150s)
    best_p, base_loss = ib.compute_baseline_decision(plans, facts)
    assert best_p.plan_id == "safe_bulk"
    assert math.isclose(base_loss, 150.0)

    # VoI calculation:
    # If probed "true" (p=0.5): fast plan is 50s, baseline is 150s -> pick fast (50s).
    # If probed "false" (p=0.5): fast plan is 550s, baseline is 150s -> pick baseline (150s).
    # Expected posterior loss: 0.5 * 50 + 0.5 * 150 = 100s.
    # VoI = 150 - 100 = 50.0s!
    voi = ib.compute_value_of_information("wal_logical", plans, facts)
    assert math.isclose(voi, 50.0)

    # If the fact is an unhelpful fact that doesn't change any choice:
    f_useless = ib.UncertainFact(
        fact_key="unrelated_metric",
        distribution_kind=ib.FactDistributionKind.BOOLEAN,
        prior_probabilities={"true": 0.8, "false": 0.2},
    )
    facts_with_useless = dict(facts)
    facts_with_useless["unrelated_metric"] = f_useless
    voi_useless = ib.compute_value_of_information("unrelated_metric", plans, facts_with_useless)
    assert math.isclose(voi_useless, 0.0)


def test_scanplan_fusion_amortization():
    """Verify that scanplan fusion amortizes cost and saves CPU/IO/locks."""
    p1 = ib.ProbeCostProfile(
        probe_id="p_chk1",
        fact_key="col1_nulls",
        target_host="127.0.0.1",
        expected_duration_s=1.0,
        source_cpu_cores=1.0,
        source_io_bytes=5_000_000,
        lock_mode=ib.ProbeLockMode.SHARED_READ,
        lock_duration_s=0.05,
        money_cost_dollars=0.0,
        is_scanplan_compatible=True,
        scanplan_table="customer_accounts",
    )
    p2 = ib.ProbeCostProfile(
        probe_id="p_chk2",
        fact_key="col2_bounds",
        target_host="127.0.0.1",
        expected_duration_s=1.2,
        source_cpu_cores=1.0,
        source_io_bytes=5_000_000,
        lock_mode=ib.ProbeLockMode.SHARED_READ,
        lock_duration_s=0.05,
        money_cost_dollars=0.0,
        is_scanplan_compatible=True,
        scanplan_table="customer_accounts",
    )

    batch = ib.FusedScanplanBatch.fuse([p1, p2])
    assert batch.target_table == "customer_accounts"
    assert len(batch.probes) == 2
    # Single fused read IO equals max single table read, not sum
    assert batch.fused_io_bytes == 5_000_000
    assert batch.fused_duration_s < (p1.expected_duration_s + p2.expected_duration_s)


def test_universal_typed_durable_resume(tmp_path: Path):
    """Verify O(1) typed durable checkpoint resume with datetime, Decimal, UUID, and tuples."""
    store_file = tmp_path / "info_budget_checkpoint.json"
    store = ib.InformationBudgetCheckpointStore(store_file)

    now_dt = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    dec_val = decimal.Decimal("12345.6789")
    uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    cursor = ("orders_table", now_dt, dec_val, uid, 42)

    rec = ib.InformationBudgetCheckpointRecord(
        fact_key="cluster_version",
        measured_value="16.4",
        measured_at_s=time.time(),
        cursor_position=cursor,
        probe_id="probe_pg_ver",
        care_badge=ib.CareBadge.PASS,
    )
    store.record_measurement(rec)

    # Reload store from disk
    store_loaded = ib.InformationBudgetCheckpointStore(store_file)
    recovered = store_loaded.get_measurement("cluster_version")
    assert recovered is not None
    assert recovered.measured_value == "16.4"
    assert recovered.care_badge == ib.CareBadge.PASS

    rec_cursor = recovered.cursor_position
    assert isinstance(rec_cursor, tuple)
    assert rec_cursor[0] == "orders_table"
    assert isinstance(rec_cursor[1], datetime.datetime)
    assert rec_cursor[1] == now_dt
    assert isinstance(rec_cursor[2], decimal.Decimal)
    assert rec_cursor[2] == dec_val
    assert isinstance(rec_cursor[3], uuid.UUID)
    assert rec_cursor[3] == uid
    assert rec_cursor[4] == 42


def test_active_verification_lease(tmp_path: Path):
    """Verify that active verification lease lock provides mutual exclusion."""
    lock_file = tmp_path / ".run.lock"
    lease1 = ib.ActiveVerificationLease(lock_file, holder_id="agent_1")
    lease1.acquire()
    lease1.heartbeat()

    # Second concurrent acquire must fail
    lease2 = ib.ActiveVerificationLease(lock_file, holder_id="agent_2")
    with pytest.raises(ib.VerificationLeaseError):
        lease2.acquire()

    lease1.release()

    # After release, second lease can be acquired cleanly
    lease2.acquire()
    lease2.release()


def test_aimd_batch_controller():
    """Verify adaptive AIMD batch controller dynamics."""
    aimd = ib.AIMDProbeController(
        current_batch_size=5000,
        min_batch_size=1000,
        max_batch_size=20000,
        additive_step=1000,
        multiplicative_factor=0.5,
    )
    # Success within target latency increases batch size additively
    aimd.on_probe_success(duration_s=0.2, target_latency_s=0.5)
    assert aimd.current_batch_size == 6000

    # Contention or spike cuts batch size multiplicatively
    aimd.on_probe_contention_or_spike()
    assert aimd.current_batch_size == 3000

    # Multiple cuts respect minimum
    aimd.on_probe_contention_or_spike()
    aimd.on_probe_contention_or_spike()
    assert aimd.current_batch_size == 1000


def test_cold_start_byte_weighted_eta():
    """Verify cold-start byte-weighted ETA calculation."""
    telemetry = _make_sample_telemetry(disk_read_bps=100_000_000.0)
    # Instant cold start without prior bytes uses headroom
    eta_cold = ib.calculate_cold_start_byte_weighted_eta(
        total_bytes=150_000_000,
        bytes_probed=0,
        elapsed_probe_s=0.0,
        telemetry=telemetry,
    )
    # 150MB at 75MB/s headroom = 2.0s
    assert math.isclose(eta_cold, 2.0)

    # With observed sample rate
    eta_warm = ib.calculate_cold_start_byte_weighted_eta(
        total_bytes=200_000_000,
        bytes_probed=50_000_000,
        elapsed_probe_s=1.0,  # 50MB/s observed
        telemetry=telemetry,
    )
    assert eta_warm > 0.0


def test_normalized_tool_wrapper():
    """Verify external tool wrapper emits CareBadges and UNIX log format."""
    def clean_run():
        return 1000, 0, 0.45, 0

    res_clean = ib.execute_normalized_tool_wrapper("datafold", "accounts", clean_run)
    assert res_clean.care_badge == ib.CareBadge.PASS
    log_str = res_clean.to_unix_log()
    assert "[PASS]" in log_str
    assert "MISMATCHES: 0" in log_str

    def diff_run():
        return 1000, 3, 0.52, 1

    res_diff = ib.execute_normalized_tool_wrapper("dvt", "accounts", diff_run)
    assert res_diff.care_badge == ib.CareBadge.ACTION
    assert "[ACTION]" in res_diff.to_unix_log()


def test_safe_endpoint_validation():
    """Verify that only 127.0.0.1, localhost, ::1, and 10.0.0.x hosts are accepted."""
    assert ib.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert ib.validate_safe_endpoint("localhost") == "localhost"
    assert ib.validate_safe_endpoint("10.0.0.45") == "10.0.0.45"

    with pytest.raises(ib.UnsafeEndpointHostError):
        ib.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(ib.UnsafeEndpointHostError):
        ib.validate_safe_endpoint("8.8.8.8")

    with pytest.raises(ib.UnsafeEndpointHostError):
        ib.validate_safe_endpoint("")


def test_end_to_end_information_budget_decision_loop(tmp_path: Path):
    """Verify end-to-end loop: selects high-VoI probe, skips useless, proves plan."""
    telemetry = _make_sample_telemetry()
    limits = _make_sample_limits(max_wall_s=10.0)
    ckpt_file = tmp_path / "loop_checkpoint.json"
    ckpt_store = ib.InformationBudgetCheckpointStore(ckpt_file)

    plan_fast = ib.CandidatePlan(
        plan_id="cdc_stream",
        nominal_cost_s=60.0,
        fallback_penalty_s=800.0,
        is_conservative_baseline=False,
        prerequisite_conditions={"cdc_supported": "true"},
    )
    plan_safe = ib.CandidatePlan(
        plan_id="chunk_cursor",
        nominal_cost_s=200.0,
        fallback_penalty_s=0.0,
        is_conservative_baseline=True,
        prerequisite_conditions={},
    )
    plans = [plan_fast, plan_safe]

    facts = {
        "cdc_supported": ib.UncertainFact(
            fact_key="cdc_supported",
            distribution_kind=ib.FactDistributionKind.BOOLEAN,
            prior_probabilities={"true": 0.5, "false": 0.5},
        ),
        "unrelated_setting": ib.UncertainFact(
            fact_key="unrelated_setting",
            distribution_kind=ib.FactDistributionKind.BOOLEAN,
            prior_probabilities={"v1": 0.5, "v2": 0.5},
        ),
    }

    probes = [
        ib.ProbeCostProfile(
            probe_id="probe_cdc",
            fact_key="cdc_supported",
            target_host="127.0.0.1",
            expected_duration_s=0.5,
            source_cpu_cores=0.5,
            source_io_bytes=1000,
            lock_mode=ib.ProbeLockMode.NONE,
            lock_duration_s=0.0,
            money_cost_dollars=0.0,
        ),
        ib.ProbeCostProfile(
            probe_id="probe_unrelated",
            fact_key="unrelated_setting",
            target_host="127.0.0.1",
            expected_duration_s=0.5,
            source_cpu_cores=0.5,
            source_io_bytes=1000,
            lock_mode=ib.ProbeLockMode.NONE,
            lock_duration_s=0.0,
            money_cost_dollars=0.0,
        ),
    ]

    report = ib.run_information_budget_decision_loop(
        plans=plans,
        facts=facts,
        candidate_probes=probes,
        budget_limits=limits,
        telemetry=telemetry,
        checkpoint_store=ckpt_store,
        execute_probe_fn=lambda p: "true" if p.probe_id == "probe_cdc" else "v1",
    )

    assert report.winning_plan_id == "cdc_stream"
    assert report.selected_rung_path == "adopt_optimistic_plan_with_proof"
    assert report.care_badge == ib.CareBadge.PASS
    assert "probe_cdc" in report.probes_executed
    assert report.total_spent_wall_s <= limits.max_probe_wall_s

    # In a second run, checkpoint store provides measurement in O(1) without re-executing probe
    executed_probes_2: List[str] = []
    report2 = ib.run_information_budget_decision_loop(
        plans=plans,
        facts=facts,
        candidate_probes=probes,
        budget_limits=limits,
        telemetry=telemetry,
        checkpoint_store=ckpt_store,
        execute_probe_fn=lambda p: executed_probes_2.append(p.probe_id) or "true",
    )
    assert report2.winning_plan_id == "cdc_stream"
    assert len(executed_probes_2) == 0  # Zero re-probes needed!
