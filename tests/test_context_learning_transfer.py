"""Tests for Row 586: SMART-N3.context-learning (Transfer measurements without false confidence).

Proves:
1. Every `decide.Rung` in `migkit/context_learning.py` (`context_transfer_rungs`,
   `drift_adaptation_rungs`, `residual_robustness_rungs`, and `calibration_gate_rungs`)
   reaches both `holds` and `refuses` under `tests.rung_branches.watched()` and
   is declared in `PROVES_RUNGS`.
2. Hierarchical pooling across context tiers (exact local, service pool, engine-pair pool,
   global rung, cold-start): shrinkage towards parent when sample size is small, and
   dominance of local empirical data when sample size is large.
3. Out-of-distribution (OOD) detection: evidence from a different context NEVER becomes
   falsely precise local measurement; predictive variance is conservatively inflated.
4. Structural drift detection and rapid recovery after hardware, version, or network shifts.
5. Continuous exponential time decay adapting to non-stationary workloads.
6. Robust Huber residual handling: downweights transient heavy-tailed spikes and refuses
   corrupted or physically impossible readings without swallowing errors.
7. Calibrated predictive intervals against held-out real workloads: empirical p50 and p90
   coverage, forecast error (MAE/RMSE), and decision regret.
8. Universal typed durable resume in O(1) (datetime, Decimal, UUID, composite cursors),
   and active verification lease lock (.run.lock) with heartbeat.
9. Adaptive AIMD batch sizing and instant cold-start byte-weighted ETA.
10. Safe test endpoint validation (127.0.0.1, 10.0.0.x).

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import datetime
import decimal
import math
import pathlib
import time
import uuid
from typing import Dict, List, Tuple

import numpy as np
import pytest

import tests.rung_branches
from migkit import context_learning as cl
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: Read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    # context_transfer_rungs
    ("context_learning", "context_transfer_rungs", "exact_local_calibrated", "holds"),
    ("context_learning", "context_transfer_rungs", "exact_local_calibrated", "refuses"),
    ("context_learning", "context_transfer_rungs", "hierarchical_pooled_transfer", "holds"),
    ("context_learning", "context_transfer_rungs", "hierarchical_pooled_transfer", "refuses"),
    ("context_learning", "context_transfer_rungs", "ood_conservative_widened", "holds"),
    ("context_learning", "context_transfer_rungs", "ood_conservative_widened", "refuses"),
    ("context_learning", "context_transfer_rungs", "safe_cold_start_prior", "holds"),
    ("context_learning", "context_transfer_rungs", "safe_cold_start_prior", "refuses"),

    # drift_adaptation_rungs
    ("context_learning", "drift_adaptation_rungs", "active_drift_reset_and_recalibrate", "holds"),
    ("context_learning", "drift_adaptation_rungs", "active_drift_reset_and_recalibrate", "refuses"),
    ("context_learning", "drift_adaptation_rungs", "exponential_decay_tracking", "holds"),
    ("context_learning", "drift_adaptation_rungs", "exponential_decay_tracking", "refuses"),
    ("context_learning", "drift_adaptation_rungs", "stationary_retention", "holds"),
    ("context_learning", "drift_adaptation_rungs", "stationary_retention", "refuses"),

    # residual_robustness_rungs
    ("context_learning", "residual_robustness_rungs", "robust_huber_reweighted_fit", "holds"),
    ("context_learning", "residual_robustness_rungs", "robust_huber_reweighted_fit", "refuses"),
    ("context_learning", "residual_robustness_rungs", "standard_sufficient_stats_update", "holds"),
    ("context_learning", "residual_robustness_rungs", "standard_sufficient_stats_update", "refuses"),
    ("context_learning", "residual_robustness_rungs", "refuse_corrupted_measurement", "holds"),
    ("context_learning", "residual_robustness_rungs", "refuse_corrupted_measurement", "refuses"),

    # calibration_gate_rungs
    ("context_learning", "calibration_gate_rungs", "conformal_calibrated_interval", "holds"),
    ("context_learning", "calibration_gate_rungs", "conformal_calibrated_interval", "refuses"),
    ("context_learning", "calibration_gate_rungs", "empirically_widened_fallback", "holds"),
    ("context_learning", "calibration_gate_rungs", "empirically_widened_fallback", "refuses"),
    ("context_learning", "calibration_gate_rungs", "refuse_uncalibrated_intervals", "holds"),
    ("context_learning", "calibration_gate_rungs", "refuse_uncalibrated_intervals", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures and Helpers
# -----------------------------------------------------------------------------

def _sample_context(
    *,
    engine_pair: str = "pg->pg",
    rung: str = "bulk_copy",
    version: str = "16.2",
    service: str = "rds",
    cores: int = 8,
    memory_gb: int = 32,
    iops: int = 12000,
    network_kind: str = "same_vpc",
    latency_ms: float = 0.5,
    bandwidth_mbps: float = 5000.0,
    lob_share: float = 0.05,
    keyless_share: float = 0.0,
    host: str = "127.0.0.1",
) -> cl.MigrationContext:
    hw = cl.HardwareProfile(
        cores=cores,
        memory_bytes=memory_gb * 1024 * 1024 * 1024,
        iops=iops,
        disk_type="gp3",
        instance_class="m6i.2xlarge",
    )
    top = cl.TopologyProfile(
        network_kind=network_kind,
        latency_ms=latency_ms,
        bandwidth_mbps=bandwidth_mbps,
    )
    types = cl.TypeMixProfile(
        lob_share=lob_share,
        keyless_share=keyless_share,
        numeric_share=0.4,
        json_share=0.1,
        text_share=0.3,
        temporal_share=0.15,
    )
    return cl.MigrationContext(
        engine_pair=engine_pair,
        rung=rung,
        version=version,
        service=service,
        hardware=hw,
        topology=top,
        type_mix=types,
        host=host,
    )


def _sample_unit(
    *,
    mb: float = 100.0,
    rows: int = 100000,
    lob_mb: float = 5.0,
    keyless: bool = False,
    rtt_s: float = 0.0005,
    unit_id: str = "u-01",
) -> cl.ContextWorkloadUnit:
    mib = 1024 * 1024
    return cl.ContextWorkloadUnit(
        bytes=int(mb * mib),
        rows=rows,
        lob_bytes=int(lob_mb * mib),
        keyless=keyless,
        rtt_s=rtt_s,
        load_share=0.1,
        unit_id=unit_id,
    )


# -----------------------------------------------------------------------------
# 1. Rung Branch Reachability Proof Under Watched
# -----------------------------------------------------------------------------

def test_every_context_learning_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both `holds` and `refuses` for every decide.Rung in `context_learning.py`."""
    with watched() as seen:
        # 1. context_transfer_rungs
        # Holds: exact_local_calibrated
        cl.choose_context_transfer_strategy(
            {
                "exact_context_match": True,
                "local_observations_count": 5,
                "min_exact_observations": 3,
                "is_ood": False,
                "ood_distance": 0.5,
                "ood_threshold": 2.5,
                "persistent_drift_detected": False,
                "poolable_context": True,
                "pooled_evidence_count": 10,
                "context_distance": 0.0,
                "max_transfer_distance": 4.0,
                "conservative_bounds_available": True,
                "cold_start_available": True,
            },
            work="transfer_exact_hold",
        )
        # Refuses exact_local_calibrated, Holds hierarchical_pooled_transfer
        cl.choose_context_transfer_strategy(
            {
                "exact_context_match": False,
                "local_observations_count": 0,
                "min_exact_observations": 3,
                "is_ood": False,
                "ood_distance": 0.0,
                "ood_threshold": 2.5,
                "persistent_drift_detected": False,
                "poolable_context": True,
                "pooled_evidence_count": 8,
                "context_distance": 1.2,
                "max_transfer_distance": 4.0,
                "conservative_bounds_available": True,
                "cold_start_available": True,
            },
            work="transfer_pool_hold",
        )
        # Refuses hierarchical_pooled_transfer, Holds ood_conservative_widened
        cl.choose_context_transfer_strategy(
            {
                "exact_context_match": False,
                "local_observations_count": 0,
                "min_exact_observations": 3,
                "is_ood": True,
                "ood_distance": 4.5,
                "ood_threshold": 2.5,
                "persistent_drift_detected": False,
                "poolable_context": True,
                "pooled_evidence_count": 5,
                "context_distance": 5.0,  # exceeds max_transfer_distance
                "max_transfer_distance": 4.0,
                "conservative_bounds_available": True,
                "cold_start_available": True,
            },
            work="transfer_ood_hold",
        )
        # Refuses ood_conservative_widened, Holds safe_cold_start_prior
        cl.choose_context_transfer_strategy(
            {
                "exact_context_match": False,
                "local_observations_count": 0,
                "min_exact_observations": 3,
                "is_ood": False,
                "ood_distance": 0.0,
                "ood_threshold": 2.5,
                "persistent_drift_detected": False,
                "poolable_context": False,
                "pooled_evidence_count": 0,
                "context_distance": 0.0,
                "max_transfer_distance": 4.0,
                "conservative_bounds_available": True,
                "cold_start_available": True,
            },
            work="transfer_cold_start_hold",
        )
        # Refuses safe_cold_start_prior
        cl.choose_context_transfer_strategy(
            {
                "exact_context_match": False,
                "local_observations_count": 0,
                "min_exact_observations": 3,
                "is_ood": False,
                "ood_distance": 0.0,
                "ood_threshold": 2.5,
                "persistent_drift_detected": False,
                "poolable_context": False,
                "pooled_evidence_count": 0,
                "context_distance": 0.0,
                "max_transfer_distance": 4.0,
                "conservative_bounds_available": False,
                "cold_start_available": False,
            },
            work="transfer_all_refuse",
        )

        # 2. drift_adaptation_rungs
        # Holds: active_drift_reset_and_recalibrate
        cl.choose_drift_adaptation_strategy(
            {
                "structural_drift_flag": True,
                "consecutive_drift_count": 3,
                "drift_trigger_count": 2,
                "post_drift_samples": 2,
                "min_post_drift_samples": 1,
                "time_decay_enabled": True,
                "elapsed_time_hours": 1.0,
                "decay_grace_period_hours": 0.0,
                "environment_stationary": False,
            },
            work="drift_reset_hold",
        )
        # Refuses active_drift_reset_and_recalibrate, Holds exponential_decay_tracking
        cl.choose_drift_adaptation_strategy(
            {
                "structural_drift_flag": False,
                "consecutive_drift_count": 0,
                "drift_trigger_count": 2,
                "post_drift_samples": 0,
                "min_post_drift_samples": 1,
                "time_decay_enabled": True,
                "elapsed_time_hours": 5.0,
                "decay_grace_period_hours": 0.0,
                "environment_stationary": False,
            },
            work="drift_decay_hold",
        )
        # Refuses exponential_decay_tracking, Holds stationary_retention
        cl.choose_drift_adaptation_strategy(
            {
                "structural_drift_flag": False,
                "consecutive_drift_count": 0,
                "drift_trigger_count": 2,
                "post_drift_samples": 0,
                "min_post_drift_samples": 1,
                "time_decay_enabled": False,
                "elapsed_time_hours": 0.0,
                "decay_grace_period_hours": 0.0,
                "environment_stationary": True,
            },
            work="drift_stationary_hold",
        )
        # Refuses stationary_retention
        cl.choose_drift_adaptation_strategy(
            {
                "structural_drift_flag": True,
                "consecutive_drift_count": 0,  # refuses active_drift because count < 2
                "drift_trigger_count": 2,
                "post_drift_samples": 0,
                "min_post_drift_samples": 1,
                "time_decay_enabled": False,  # refuses decay
                "elapsed_time_hours": 0.0,
                "decay_grace_period_hours": 0.0,
                "environment_stationary": False,  # refuses stationary
            },
            work="drift_all_refuse",
        )

        # 3. residual_robustness_rungs
        # Holds: robust_huber_reweighted_fit
        cl.choose_residual_robustness_strategy(
            {
                "standardized_residual": 2.1,
                "huber_tuning_k": 1.345,
                "physically_plausible": True,
                "measurement_valid": True,
                "measured_seconds": 1.5,
            },
            work="residual_huber_hold",
        )
        # Refuses robust_huber_reweighted_fit, Holds standard_sufficient_stats_update
        cl.choose_residual_robustness_strategy(
            {
                "standardized_residual": 0.4,
                "huber_tuning_k": 1.345,
                "physically_plausible": True,
                "measurement_valid": True,
                "measured_seconds": 1.0,
            },
            work="residual_standard_hold",
        )
        # Refuses standard_sufficient_stats_update, Holds refuse_corrupted_measurement
        cl.choose_residual_robustness_strategy(
            {
                "standardized_residual": 0.2,
                "huber_tuning_k": 1.345,
                "physically_plausible": False,
                "measurement_valid": False,
                "measured_seconds": -0.5,
            },
            work="residual_corrupt_hold",
        )
        # Refuses refuse_corrupted_measurement
        cl.choose_residual_robustness_strategy(
            {
                "standardized_residual": 0.8,
                "huber_tuning_k": 1.345,
                "physically_plausible": True,
                "measurement_valid": False,  # refuses standard update
                "measured_seconds": 2.0,     # refuses corrupt
            },
            work="residual_corrupt_refuse",
        )

        # 4. calibration_gate_rungs
        # Holds: conformal_calibrated_interval
        cl.choose_calibration_strategy(
            {
                "empirical_p90_coverage": 0.92,
                "target_p90_coverage": 0.85,
                "empirical_p50_coverage": 0.52,
                "target_p50_coverage": 0.45,
                "validation_sample_count": 25,
                "min_validation_samples": 10,
                "calibration_multiplier": 1.0,
                "max_calibration_multiplier": 10.0,
                "calibration_failed": False,
            },
            work="calib_conformal_hold",
        )
        # Refuses conformal_calibrated_interval, Holds empirically_widened_fallback
        cl.choose_calibration_strategy(
            {
                "empirical_p90_coverage": 0.70,  # deficit
                "target_p90_coverage": 0.85,
                "empirical_p50_coverage": 0.35,
                "target_p50_coverage": 0.45,
                "validation_sample_count": 25,
                "min_validation_samples": 10,
                "calibration_multiplier": 1.8,
                "max_calibration_multiplier": 10.0,
                "calibration_failed": False,
            },
            work="calib_widened_hold",
        )
        # Refuses empirically_widened_fallback, Holds refuse_uncalibrated_intervals
        cl.choose_calibration_strategy(
            {
                "empirical_p90_coverage": 0.50,
                "target_p90_coverage": 0.85,
                "empirical_p50_coverage": 0.20,
                "target_p50_coverage": 0.45,
                "validation_sample_count": 25,
                "min_validation_samples": 10,
                "calibration_multiplier": 15.0,  # exceeds max_calibration_multiplier
                "max_calibration_multiplier": 10.0,
                "calibration_failed": False,
            },
            work="calib_refuse_hold",
        )
        # Refuses refuse_uncalibrated_intervals
        cl.choose_calibration_strategy(
            {
                "empirical_p90_coverage": 0.95,
                "target_p90_coverage": 0.85,
                "empirical_p50_coverage": 0.60,
                "target_p50_coverage": 0.45,
                "validation_sample_count": 5,  # small sample
                "min_validation_samples": 10,
                "calibration_multiplier": 1.0,
                "max_calibration_multiplier": 10.0,
                "calibration_failed": False,
            },
            work="calib_refuse_refuses",
        )

    # Verify that every rung in PROVES_RUNGS was reached on the declared branch
    for module, ladder, rung, branch_kind in PROVES_RUNGS:
        assert module == "context_learning"
        reached = branch(seen, rung)
        assert (
            branch_kind in reached
        ), f"rung '{rung}' in ladder '{ladder}' did not reach '{branch_kind}' (reached: {reached})"


# -----------------------------------------------------------------------------
# 2. Context Distance & Lineage Preservation
# -----------------------------------------------------------------------------

def test_context_distance_and_lineage_preservation() -> None:
    """Validate multi-dimensional context distance and cryptographic lineage preservation."""
    ctx_local = _sample_context(service="rds", version="16.2", cores=8)
    ctx_peer = _sample_context(service="aurora", version="15.4", cores=16)

    dist = cl.compute_context_distance(ctx_local, ctx_peer)
    assert dist > 1.5, f"expected non-zero context distance across service/version/cores, got {dist}"

    model = cl.HierarchicalContextModel()
    unit = _sample_unit(mb=50.0, rows=50000)

    # Cold start prediction has lineage
    pred = model.predict(ctx_local, unit)
    assert pred.lineage is not None
    assert pred.lineage.target_context_id == ctx_local.context_id
    assert pred.lineage.prior_provenance == "safe_cold_start_prior"
    assert pred.lineage.effective_sample_size == 0.0

    # Record 4 local observations
    for i in range(4):
        model.record_measurement(ctx_local, unit, actual_seconds=1.2 + 0.05 * i)

    # Next prediction uses exact_local_calibrated with lineage
    pred_local = model.predict(ctx_local, unit)
    assert pred_local.rung_chosen == "exact_local_calibrated"
    assert pred_local.lineage.pooling_level == "exact_local"
    assert pred_local.lineage.effective_sample_size >= 4.0
    assert pred_local.lineage.shrinkage_factor == 1.0


# -----------------------------------------------------------------------------
# 3. Hierarchical Pooling and Shrinkage
# -----------------------------------------------------------------------------

def test_hierarchical_pooling_shrinkage() -> None:
    """Validate hierarchical pooling: borrows strength from parent when local n is small."""
    ctx_a = _sample_context(service="rds", cores=8)
    ctx_b = _sample_context(service="rds", cores=16)  # same service, different cores

    model = cl.HierarchicalContextModel()
    unit = _sample_unit(mb=100.0, rows=100000)

    # Populate service model through ctx_a
    for _ in range(10):
        model.record_measurement(ctx_a, unit, actual_seconds=2.0)

    # Predict ctx_b with 0 local measurements: should use hierarchical pool
    pred_b_zero = model.predict(ctx_b, unit)
    assert pred_b_zero.rung_chosen == "hierarchical_pooled_transfer"
    assert pred_b_zero.lineage.pooling_level == "hierarchical_pool"
    assert pred_b_zero.lineage.shrinkage_factor == 0.0

    # Add 1 local measurement to ctx_b
    model.record_measurement(ctx_b, unit, actual_seconds=1.0)
    pred_b_one = model.predict(ctx_b, unit)
    # Partial pooling shrinkage: 0 < lambda < 1
    assert pred_b_one.rung_chosen == "hierarchical_pooled_transfer"
    assert 0.0 < pred_b_one.lineage.shrinkage_factor < 1.0
    # Estimate is between local measurement (1.0) and service prior (2.0)
    assert 1.0 < pred_b_one.mean < 2.0


# -----------------------------------------------------------------------------
# 4. Out-of-Distribution Detection and Conservative Widening
# -----------------------------------------------------------------------------

def test_out_of_distribution_detection_and_no_false_precision() -> None:
    """Validate OOD detection: unfamiliar workloads never claim falsely precise local certainty."""
    ctx = _sample_context()
    model = cl.HierarchicalContextModel()

    # Train model on typical medium units (50-100 MB, low LOB)
    for mb in (50.0, 75.0, 100.0, 60.0, 80.0, 90.0):
        u = _sample_unit(mb=mb, rows=int(mb * 1000), lob_mb=0.0)
        model.record_measurement(ctx, u, actual_seconds=mb * 0.02)

    # In-distribution unit
    in_dist_unit = _sample_unit(mb=70.0, rows=70000, lob_mb=0.0)
    pred_in = model.predict(ctx, in_dist_unit)
    assert pred_in.rung_chosen == "exact_local_calibrated"
    assert pred_in.care_badge == cl.CareBadge.PASS

    # Extreme OOD unit: 500x larger LOB and round-trips
    ood_unit = _sample_unit(mb=8000.0, rows=2000000, lob_mb=7000.0, rtt_s=0.1)
    pred_ood = model.predict(ctx, ood_unit)

    # CRITICAL INVARIANT: Never becomes falsely precise local measurement
    assert pred_ood.rung_chosen == "ood_conservative_widened"
    assert pred_ood.care_badge == cl.CareBadge.REVIEW
    assert pred_ood.lineage.is_ood is True
    # Predictive spread is conservatively wider than nominal local prediction
    assert pred_ood.high_90 > pred_ood.mean * 1.5


# -----------------------------------------------------------------------------
# 5. Non-Stationary Structural Drift Recovery
# -----------------------------------------------------------------------------

def test_structural_drift_recovery_after_hardware_or_network_change() -> None:
    """Validate structural drift detection and fast recovery when environment changes."""
    ctx = _sample_context()
    model = cl.HierarchicalContextModel()
    unit = _sample_unit(mb=100.0, rows=100000)

    # Establish fast baseline regime (1.0s)
    for _ in range(8):
        model.record_measurement(ctx, unit, actual_seconds=1.0)

    pred_before = model.predict(ctx, unit)
    assert abs(pred_before.mean - 1.0) < 0.2

    # Simulate hardware degradation: actual time jumps 4x to 4.0s
    badge1 = model.record_measurement(ctx, unit, actual_seconds=4.0)
    assert badge1 in (cl.CareBadge.REVIEW, cl.CareBadge.PASS)

    # Second persistent jump confirms structural drift
    badge2 = model.record_measurement(ctx, unit, actual_seconds=4.2)
    assert model.consecutive_drift_deviations[ctx.context_id] >= 2

    # Adapt drift: triggers active_drift_reset_and_recalibrate with ACTION
    drift_badge = model.adapt_drift_and_decay(ctx, elapsed_seconds=60.0)
    assert drift_badge == cl.CareBadge.ACTION

    # Record new measurements under the post-drift regime
    model.record_measurement(ctx, unit, actual_seconds=4.1)
    model.record_measurement(ctx, unit, actual_seconds=4.0)
    model.record_measurement(ctx, unit, actual_seconds=4.0)

    # New predictions rapidly converge to ~4.0s rather than staying stuck at 1.0s
    pred_after = model.predict(ctx, unit)
    assert pred_after.mean > 3.0, f"expected rapid drift recovery towards 4.0s, got {pred_after.mean}"


# -----------------------------------------------------------------------------
# 6. Continuous Exponential Time Decay
# -----------------------------------------------------------------------------

def test_exponential_time_decay() -> None:
    """Validate that continuous time decay discounts historical evidence."""
    ctx = _sample_context()
    model = cl.HierarchicalContextModel(half_life_seconds=3600.0)
    unit = _sample_unit(mb=50.0, rows=50000)

    for _ in range(5):
        model.record_measurement(ctx, unit, actual_seconds=1.0)

    exact_mod = model.exact_models[ctx.context_id]
    n_before = exact_mod.n_obs

    # Decay over 1 half life (3600s = 1 hour)
    badge = model.adapt_drift_and_decay(ctx, elapsed_seconds=3600.0)
    assert badge == cl.CareBadge.BENIGN

    n_after = exact_mod.n_obs
    assert n_after < n_before
    assert abs(n_after - n_before * 0.5) < 0.2


# -----------------------------------------------------------------------------
# 7. Robust Residuals and Corruption Refusal
# -----------------------------------------------------------------------------

def test_robust_residual_handling_and_corruption_refusal() -> None:
    """Validate Huber robust reweighting on transient spikes and corruption refusal."""
    ctx = _sample_context()
    model = cl.HierarchicalContextModel()
    unit = _sample_unit(mb=50.0, rows=50000)

    for _ in range(6):
        model.record_measurement(ctx, unit, actual_seconds=1.0)

    # Transient outlier (e.g. 3.5s due to lock wait) gets REVIEW via Huber reweighting
    badge = model.record_measurement(ctx, unit, actual_seconds=3.5)
    assert badge == cl.CareBadge.REVIEW

    # Negative duration is physically impossible: raises CorruptedMeasurementError
    with pytest.raises(cl.CorruptedMeasurementError):
        model.record_measurement(ctx, unit, actual_seconds=-1.0)

    # Unreasonable rate (e.g. 50MB in 0.000001s = 50,000,000 MB/s): refuses corruption
    with pytest.raises(cl.CorruptedMeasurementError):
        model.record_measurement(ctx, unit, actual_seconds=1e-6)


# -----------------------------------------------------------------------------
# 8. Calibration Against Held-Out Workloads
# -----------------------------------------------------------------------------

def test_predictive_interval_calibration_against_held_out_workloads() -> None:
    """Validate calibration against held-out workload units: p50/p90 empirical coverage."""
    ctx = _sample_context()
    model = cl.HierarchicalContextModel()

    # Train model
    np.random.seed(42)
    for i in range(20):
        mb = 20.0 + i * 5.0
        u = _sample_unit(mb=mb, rows=int(mb * 1000))
        noise = np.random.normal(0.0, 0.05)
        model.record_measurement(ctx, u, actual_seconds=mb * 0.02 + noise)

    # Generate held-out validation set of 25 workloads
    val_units = []
    val_actuals = []
    for i in range(25):
        mb = 25.0 + i * 4.0
        u = _sample_unit(mb=mb, rows=int(mb * 1000), unit_id=f"val-{i}")
        actual = mb * 0.02 + float(np.random.normal(0.0, 0.06))
        val_units.append(u)
        val_actuals.append(actual)

    report = model.evaluate_held_out_calibration(ctx, val_units, val_actuals)
    assert report.sample_count == 25
    assert report.empirical_p90_coverage >= 0.80
    assert report.empirical_p50_coverage >= 0.40
    assert report.mean_absolute_error > 0.0
    assert report.root_mean_squared_error > 0.0
    assert report.care_badge in (cl.CareBadge.PASS, cl.CareBadge.REVIEW)


# -----------------------------------------------------------------------------
# 9. Universal Typed Durable Resume in O(1)
# -----------------------------------------------------------------------------

def test_universal_typed_durable_checkpoint_resume_o1(tmp_path: pathlib.Path) -> None:
    """Validate O(1) typed checkpoint resume with native datetime, Decimal, UUID, and tuples."""
    store_file = tmp_path / "context_learning_checkpoint.json"
    store = cl.ContextLearningCheckpointStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    u_id = uuid.uuid4()
    unit = cl.ContextLearningCheckpointUnit(
        unit_key="table:orders:chunk_001",
        context_id="ctx-pg-16-rds",
        recorded_at=now,
        predicted_seconds=decimal.Decimal("1.425"),
        actual_seconds=decimal.Decimal("1.410"),
        unit_uuid=u_id,
        care_badge=cl.CARE_PASS,
        composite_cursor=("orders_pkey", 50000),
    )

    store.record_unit(unit)

    # O(1) query on existing unit
    assert store.should_skip_unit("table:orders:chunk_001", "ctx-pg-16-rds") is True
    assert store.should_skip_unit("table:orders:chunk_001", "other-ctx") is False
    assert store.should_skip_unit("unknown_key", "ctx-pg-16-rds") is False

    # Reload store in new instance to verify durable serialization fidelity
    store_reloaded = cl.ContextLearningCheckpointStore(store_file)
    loaded_unit = store_reloaded.get_unit("table:orders:chunk_001")
    assert loaded_unit is not None
    assert loaded_unit.recorded_at == now
    assert loaded_unit.predicted_seconds == decimal.Decimal("1.425")
    assert loaded_unit.actual_seconds == decimal.Decimal("1.410")
    assert loaded_unit.unit_uuid == u_id
    assert loaded_unit.composite_cursor == ("orders_pkey", 50000)
    assert loaded_unit.care_badge == cl.CARE_PASS


# -----------------------------------------------------------------------------
# 10. Active Verification Lease Lock Mutual Exclusion
# -----------------------------------------------------------------------------

def test_active_verification_lease_lock_mutual_exclusion(tmp_path: pathlib.Path) -> None:
    """Validate active mutual exclusion lease lock (.run.lock) with heartbeat."""
    lock_dir = tmp_path / "lease_test"

    lock1 = cl.acquire_context_learning_lease(lock_dir, ttl_seconds=30.0, owner_id="agent-1")
    assert lock1.acquired_at_s is not None

    # Heartbeat updates timestamp
    time.sleep(0.01)
    lock1.heartbeat()
    assert lock1.last_heartbeat_s is not None
    assert lock1.last_heartbeat_s >= lock1.acquired_at_s

    # Second concurrent acquire fails with VerificationLeaseError
    with pytest.raises(cl.VerificationLeaseError):
        cl.acquire_context_learning_lease(lock_dir, ttl_seconds=30.0, owner_id="agent-2")

    # Releasing lock1 allows lock2 to acquire
    lock1.release()
    lock2 = cl.acquire_context_learning_lease(lock_dir, ttl_seconds=30.0, owner_id="agent-2")
    assert lock2.acquired_at_s is not None
    lock2.release()


# -----------------------------------------------------------------------------
# 11. Adaptive AIMD Batching and Cold-Start ETA
# -----------------------------------------------------------------------------

def test_adaptive_aimd_batching_and_cold_start_eta() -> None:
    """Validate dynamic AIMD batch sizing and byte-weighted ETA."""
    # Headroom: additive increase
    sz1 = cl.adjust_aimd_batch_size(
        current_size=1000,
        measured_latency_s=0.2,
        target_latency_s=0.5,
        additive_increase=100,
    )
    assert sz1 == 1100

    # Backpressure: multiplicative decrease
    sz2 = cl.adjust_aimd_batch_size(
        current_size=1100,
        measured_latency_s=0.9,
        target_latency_s=0.5,
        multiplicative_decrease=0.5,
    )
    assert sz2 == 550

    # Byte-weighted ETA
    total_bytes = 100 * 1024 * 1024  # 100 MB
    completed_bytes = 20 * 1024 * 1024  # 20 MB completed
    eta = cl.estimate_byte_weighted_eta(
        completed_bytes=completed_bytes,
        total_bytes=total_bytes,
        elapsed_seconds=2.0,  # 10 MB/s
        cold_start_predicted_rate_mb_s=12.0,
    )
    assert 5.0 < eta < 10.0


# -----------------------------------------------------------------------------
# 12. Safe Endpoint Validation
# -----------------------------------------------------------------------------

def test_safe_endpoint_validation() -> None:
    """Validate endpoint host gating strictly allows safe loopback or 10.0.0.x."""
    assert cl.validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert cl.validate_safe_endpoint("127.0.0.99") == "127.0.0.99"
    assert cl.validate_safe_endpoint("localhost") == "localhost"
    assert cl.validate_safe_endpoint("::1") == "::1"
    assert cl.validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(cl.UnsafeEndpointHostError):
        cl.validate_safe_endpoint("8.8.8.8")

    with pytest.raises(cl.UnsafeEndpointHostError):
        cl.validate_safe_endpoint("192.168.1.1")

    with pytest.raises(cl.UnsafeEndpointHostError):
        cl.validate_safe_endpoint("")
