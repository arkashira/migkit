"""Tests for query regression and behavioral performance envelope gating (RCH23, row 631).

Verifies representative approved workloads, parameter distributions, concurrency,
app roles on target sandbox, separating cold-cache effects and noisy resource changes,
candidate index/statistics fixes verification and pricing, gating rules where identical
rows with changed collation, missing index, or regressed execution plan cannot receive
unconditional app-ready verdicts, prohibition of labelling synthetic benchmarks as
production performance, pure UNIX formatting, 100% pure ASCII, safe test hosts,
universal typed durable resume in O(1), and mutual exclusion lease locks.
"""

import datetime
import decimal
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path

import pytest

from migkit.query_regression import (
    APP_NOT_READY,
    APP_READY_CONDITIONAL,
    APP_READY_UNCONDITIONAL,
    BENCHMARK_TYPE_SANDBOX,
    BENCHMARK_TYPE_SYNTHETIC,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    LABEL_PRODUCTION_PERFORMANCE,
    BehavioralParityVerifier,
    CandidateFixPricing,
    CandidateRemediation,
    DurableQueryRegressionCheckpoint,
    ExecutionPlanAnalysis,
    ParameterDistribution,
    PerformanceEnvelope,
    PerformanceEnvelopeCalculator,
    PlanRegressionAnalyzer,
    QueryExecutionResult,
    QueryRegressionError,
    QueryRegressionGate,
    QueryRegressionVerdict,
    RemediationPricer,
    SemanticParityResult,
    SyntheticBenchmarkLabelError,
    UnsafeEndpointError,
    WorkloadNotApprovedError,
    WorkloadProfile,
    WorkloadQuery,
    is_safe_test_host,
    RegressionThresholds,
    decide_query_regression_envelope,
    query_regression_envelope_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("query_regression", "query_regression_envelope_rungs", "certified_within_envelope", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "certified_within_envelope", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "p99_tail_latency_review", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "p99_tail_latency_review", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "explain_cost_increase_review", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "explain_cost_increase_review", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "implicit_cast_hazard_review", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "implicit_cast_hazard_review", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "unpriced_index_remediation_review", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "unpriced_index_remediation_review", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "missing_baseline_review", "holds"),
    ("query_regression", "query_regression_envelope_rungs", "missing_baseline_review", "refuses"),
    ("query_regression", "query_regression_envelope_rungs", "severe_regression_action", "holds"),
]



@pytest.fixture
def tmp_dir():
    d = Path(tempfile.mkdtemp(prefix="test_query_regression_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_safe_test_host_validation():
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("localhost") is True
    assert is_safe_test_host("0.0.0.0") is True
    assert is_safe_test_host("::1") is True
    assert is_safe_test_host("10.0.0.1") is True
    assert is_safe_test_host("10.0.0.254") is True

    assert is_safe_test_host("192.168.1.1") is False
    assert is_safe_test_host("8.8.8.8") is False
    assert is_safe_test_host("10.0.1.1") is False  # dummy unsafe IP
    assert is_safe_test_host("prod-db.internal") is False


def test_parameter_distributions():
    # Uniform
    dist_u = ParameterDistribution(distribution_type="uniform", min_val=10, max_val=20)
    for i in range(10):
        val = dist_u.sample(step=i)
        assert 10 <= val <= 20

    # Sequential
    dist_s = ParameterDistribution(distribution_type="sequential", min_val=1, max_val=5)
    seq = [dist_s.sample(step=i) for i in range(5)]
    assert seq == [1, 2, 3, 4, 5]

    # Zipfian / skewed
    dist_z = ParameterDistribution(distribution_type="zipfian", cardinality=100, min_val=1, max_val=100)
    samples = [dist_z.sample(step=i) for i in range(50)]
    assert len(samples) == 50
    assert all(1 <= x <= 100 for x in samples)

    # Timestamp range
    base_t = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    dist_t = ParameterDistribution(
        distribution_type="timestamp_range",
        date_start=base_t,
        date_step_seconds=3600.0,
    )
    t1 = dist_t.sample(step=1)
    assert t1 == base_t + datetime.timedelta(seconds=3600)

    # Composite types (preserving Decimals and UUIDs)
    dist_c = ParameterDistribution(
        distribution_type="composite",
        composite_specs=[
            {"name": "id", "type": "uuid"},
            {"name": "amount", "type": "decimal"},
        ],
    )
    comp = dist_c.sample(step=0)
    assert isinstance(comp["id"], uuid.UUID)
    assert isinstance(comp["amount"], decimal.Decimal)


def test_workload_profile_validation_guards():
    # Unapproved workload raises
    w_unapproved = WorkloadProfile(
        workload_id="wl_orders_v1",
        name="Orders Baseline",
        queries=[],
        approved=False,
        target_sandbox_host="127.0.0.1",
    )
    with pytest.raises(WorkloadNotApprovedError):
        w_unapproved.validate()

    # Unsafe target host raises
    w_unsafe = WorkloadProfile(
        workload_id="wl_orders_v2",
        name="Orders Baseline",
        queries=[],
        approved=True,
        target_sandbox_host="192.168.1.100",
    )
    with pytest.raises(UnsafeEndpointError):
        w_unsafe.validate()

    # Labelled as production performance raises
    w_prod = WorkloadProfile(
        workload_id="wl_orders_v3",
        name="Orders Baseline",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
        benchmark_label=LABEL_PRODUCTION_PERFORMANCE,
    )
    with pytest.raises(SyntheticBenchmarkLabelError):
        w_prod.validate()

    w_prod_flag = WorkloadProfile(
        workload_id="wl_orders_v4",
        name="Orders Baseline",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
        is_production_claimed=True,
    )
    with pytest.raises(SyntheticBenchmarkLabelError):
        w_prod_flag.validate()

    # Valid sandbox workload passes
    w_valid = WorkloadProfile(
        workload_id="wl_orders_ok",
        name="Orders Approved Workload",
        queries=[
            WorkloadQuery(
                query_id="q1",
                sql_template="SELECT * FROM orders WHERE id = :id",
                app_role="app_user",
            )
        ],
        concurrency_levels=[1, 4, 16],
        approved=True,
        target_sandbox_host="10.0.0.5",
        benchmark_label=BENCHMARK_TYPE_SANDBOX,
    )
    w_valid.validate()


def test_performance_envelope_cold_cache_and_noise_separation():
    # Create cold-cache runs (high latency) and warm-cache runs (fast, steady latency)
    results = []
    # 5 cold runs
    for i in range(5):
        results.append(
            QueryExecutionResult(
                query_id="q_select",
                app_role="app_user",
                concurrency=4,
                is_warm=False,
                execution_time_ms=150.0 + i * 5.0,
                buffer_hit_ratio=0.10,
            )
        )
    # 20 warm runs
    for i in range(20):
        results.append(
            QueryExecutionResult(
                query_id="q_select",
                app_role="app_user",
                concurrency=4,
                is_warm=True,
                execution_time_ms=10.0 + (i % 3) * 0.5,
                buffer_hit_ratio=0.99,
            )
        )

    env = PerformanceEnvelopeCalculator.calculate("q_select", results)
    assert env.sample_count == 25
    assert env.cold_sample_count == 5
    assert env.warm_sample_count == 20
    assert 9.0 <= env.p50_latency_ms <= 12.0
    assert 10.0 <= env.warm_cache_p95_ms <= 12.0
    assert env.cold_cache_p95_ms >= 150.0
    assert env.cold_cache_penalty_ratio > 10.0
    assert env.throughput_qps > 0.0
    assert env.resource_jitter_detected is False


def test_performance_envelope_environmental_noise_separation():
    # Simulate a noisy resource run with throttle flags and high variance
    results = []
    for i in range(20):
        lat = 10.0 if (i % 4 != 0) else 120.0  # sporadic resource throttle spikes
        results.append(
            QueryExecutionResult(
                query_id="q_noisy",
                app_role="reporting_role",
                concurrency=1,
                is_warm=True,
                execution_time_ms=lat,
                resource_throttle_detected=(i % 4 == 0),
            )
        )

    env = PerformanceEnvelopeCalculator.calculate("q_noisy", results)
    assert env.resource_jitter_detected is True
    assert env.noise_variance_coefficient > 0.45


def test_behavioral_parity_changed_collation_blocks_unconditional_ready():
    # Identical set of rows, but different order due to changed collation
    # e.g. ["Apple", "banana", "Cherry"] vs ["banana", "Apple", "Cherry"]
    src_data = {
        "row_count": 3,
        "row_hash": "hash_abc_123",
        "result_ordering_sample": ["Apple", "banana", "Cherry"],
    }
    tgt_data = {
        "row_count": 3,
        "row_hash": "hash_abc_123",
        "result_ordering_sample": ["banana", "Apple", "Cherry"],  # case-insensitive or different collation sort
    }

    parity = BehavioralParityVerifier.verify("q_collate", src_data, tgt_data)
    assert parity.changed_collation_detected is True
    assert parity.ordering_mismatch_detected is True
    assert parity.care_badge == CARE_REVIEW

    # Gate verification
    workload = WorkloadProfile(
        workload_id="wl_collation",
        name="Collation Test",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
    )
    gate = QueryRegressionGate(workload)

    base_env = PerformanceEnvelopeCalculator.calculate(
        "q_collate",
        [QueryExecutionResult("q_collate", "app_user", 1, True, 10.0)]
    )
    tgt_env = PerformanceEnvelopeCalculator.calculate(
        "q_collate",
        [QueryExecutionResult("q_collate", "app_user", 1, True, 10.5)]
    )
    plan_analysis = PlanRegressionAnalyzer.analyze(
        "q_collate",
        {"node_type": "Index Scan", "plan_cost": 10.0},
        {"node_type": "Index Scan", "plan_cost": 10.0},
    )

    verdict = gate.evaluate_readiness(
        baseline_envelopes={"q_collate": base_env},
        target_envelopes={"q_collate": tgt_env},
        semantic_parities={"q_collate": parity},
        plan_analyses={"q_collate": plan_analysis},
    )

    # RULE: Identical rows with a changed collation CANNOT receive an unconditional app-ready verdict!
    assert verdict.unconditional_ready_permitted is False
    assert verdict.app_readiness != APP_READY_UNCONDITIONAL
    assert verdict.care_badge in (CARE_REVIEW, CARE_ACTION)
    assert "changed collation" in verdict.reasons[0].lower()


def test_missing_index_and_regressed_plan_must_be_verified_and_priced():
    # Query with missing index causing Seq Scan
    plan_src = {"node_type": "Index Scan", "plan_cost": 15.0, "table": "orders"}
    plan_tgt = {
        "node_type": "Seq Scan",
        "plan_cost": 1200.0,
        "table": "orders",
        "has_seq_scan": True,
        "filter_columns": ["customer_id", "status"],
    }

    plan_analysis = PlanRegressionAnalyzer.analyze("q_orders", plan_src, plan_tgt, latency_ratio=4.5)
    assert plan_analysis.missing_index_detected is True
    assert plan_analysis.has_seq_scan is True
    assert plan_analysis.care_badge == CARE_ACTION

    workload = WorkloadProfile(
        workload_id="wl_orders",
        name="Orders Regression",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
    )
    gate = QueryRegressionGate(workload)

    base_env = PerformanceEnvelopeCalculator.calculate(
        "q_orders",
        [QueryExecutionResult("q_orders", "app_user", 1, True, 15.0)]
    )
    tgt_env = PerformanceEnvelopeCalculator.calculate(
        "q_orders",
        [QueryExecutionResult("q_orders", "app_user", 1, True, 70.0)]
    )
    parity = SemanticParityResult(
        query_id="q_orders",
        identical_rows=True,
        row_count_match=True,
        row_hash_match=True,
        changed_collation_detected=False,
    )

    # 1. Without candidate remediations: must NOT receive app-ready verdict
    verdict1 = gate.evaluate_readiness(
        baseline_envelopes={"q_orders": base_env},
        target_envelopes={"q_orders": tgt_env},
        semantic_parities={"q_orders": parity},
        plan_analyses={"q_orders": plan_analysis},
        candidate_remediations=[],
    )
    assert verdict1.unconditional_ready_permitted is False
    assert verdict1.app_readiness == APP_NOT_READY
    assert verdict1.care_badge == CARE_ACTION

    # 2. Generate candidate remediation and price it
    remediation = RemediationPricer.generate_and_price(
        query_id="q_orders",
        table="orders",
        columns=["customer_id", "status"],
        fix_type="CREATE_INDEX",
        table_row_count=500000,
    )
    assert remediation.pricing.is_priced is True
    assert remediation.pricing.index_size_bytes > 0
    assert remediation.pricing.estimated_build_time_sec > 0
    assert remediation.pricing.write_amplification_pct > 0
    assert remediation.verified_on_sandbox is False

    # Still unverified -> must NOT receive app-ready verdict
    verdict2 = gate.evaluate_readiness(
        baseline_envelopes={"q_orders": base_env},
        target_envelopes={"q_orders": tgt_env},
        semantic_parities={"q_orders": parity},
        plan_analyses={"q_orders": plan_analysis},
        candidate_remediations=[remediation],
    )
    assert verdict2.app_readiness == APP_NOT_READY
    assert verdict2.unverified_remediations_count == 1

    # 3. Verify fix on sandbox
    def mock_sandbox_verifier(fix: CandidateRemediation):
        return True, 82.5, "Index Scan"

    verified_remediation = RemediationPricer.verify_on_sandbox(remediation, mock_sandbox_verifier)
    assert verified_remediation.verified_on_sandbox is True
    assert verified_remediation.verified_latency_reduction_pct == 82.5
    assert verified_remediation.care_badge == CARE_PASS

    # Now that it's verified and priced, it can receive CONDITIONAL app-ready (never unconditional)
    # Target latency post-fix simulated
    fixed_tgt_env = PerformanceEnvelopeCalculator.calculate(
        "q_orders",
        [QueryExecutionResult("q_orders", "app_user", 1, True, 16.0)]
    )
    verdict3 = gate.evaluate_readiness(
        baseline_envelopes={"q_orders": base_env},
        target_envelopes={"q_orders": fixed_tgt_env},
        semantic_parities={"q_orders": parity},
        plan_analyses={"q_orders": plan_analysis},
        candidate_remediations=[verified_remediation],
    )
    assert verdict3.unconditional_ready_permitted is False
    assert verdict3.app_readiness == APP_READY_CONDITIONAL
    assert verdict3.care_badge == CARE_REVIEW
    assert verdict3.unverified_remediations_count == 0
    assert verdict3.unpriced_remediations_count == 0


def test_statistics_remediation_pricing_and_verification():
    stats_remediation = RemediationPricer.generate_and_price(
        query_id="q_stats",
        table="inventory",
        columns=["warehouse_id", "product_id"],
        fix_type="ANALYZE_STATISTICS",
        table_row_count=100000,
    )
    assert stats_remediation.pricing.is_priced is True
    assert stats_remediation.pricing.write_amplification_pct == 0.0
    assert stats_remediation.ddl_statement.startswith("ANALYZE inventory")

    verified = RemediationPricer.verify_on_sandbox(
        stats_remediation,
        lambda r: (True, 45.0, "Bitmap Index Scan")
    )
    assert verified.verified_on_sandbox is True
    assert verified.verified_target_plan == "Bitmap Index Scan"


def test_universal_typed_durable_resume_o1(tmp_dir):
    workload = WorkloadProfile(
        workload_id="wl_resume",
        name="Resume Workload",
        queries=[],
        approved=True,
        target_sandbox_host="10.0.0.12",
    )
    gate = QueryRegressionGate(workload)

    # Checkpoint with rich native types
    now = datetime.datetime.now(datetime.timezone.utc)
    dec = decimal.Decimal("12345.67")
    uid = uuid.uuid4()
    cursors = {
        "last_seen_date": now,
        "max_balance": dec,
        "tx_id": uid,
        "composite_key": ("region_east", 42),
    }

    chk = gate.checkpoint(
        checkpoint_dir=tmp_dir,
        completed_query_ids=["q1", "q2", "q3"],
        typed_cursors=cursors,
        interim_envelopes={"q1": {"p95": 12.0}},
    )
    assert chk.checkpoint_id != ""

    # Load checkpoint in O(1)
    loaded = gate.load_checkpoint(tmp_dir)
    assert loaded is not None
    assert loaded.workload_id == "wl_resume"
    assert loaded.completed_query_ids == ["q1", "q2", "q3"]

    # Verify native type restoration
    assert isinstance(loaded.typed_cursors["last_seen_date"], datetime.datetime)
    assert loaded.typed_cursors["last_seen_date"] == now
    assert isinstance(loaded.typed_cursors["max_balance"], decimal.Decimal)
    assert loaded.typed_cursors["max_balance"] == dec
    assert isinstance(loaded.typed_cursors["tx_id"], uuid.UUID)
    assert loaded.typed_cursors["tx_id"] == uid
    assert isinstance(loaded.typed_cursors["composite_key"], tuple)
    assert loaded.typed_cursors["composite_key"] == ("region_east", 42)


def test_mutual_exclusion_lease_lock(tmp_dir):
    workload = WorkloadProfile(
        workload_id="wl_lease",
        name="Lease Test",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
    )
    gate1 = QueryRegressionGate(workload)
    gate2 = QueryRegressionGate(workload)

    # Gate 1 acquires lease
    assert gate1.acquire_lease(tmp_dir, timeout_sec=0.5) is True
    gate1.renew_lease()

    # Second runner attempting to acquire fails
    assert gate2.acquire_lease(tmp_dir, timeout_sec=0.2) is False

    # Gate 1 releases lease
    gate1.release_lease()

    # Gate 2 can now acquire lease
    assert gate2.acquire_lease(tmp_dir, timeout_sec=0.5) is True
    gate2.release_lease()


def test_pure_unix_terse_report_and_care_badges():
    workload = WorkloadProfile(
        workload_id="wl_report",
        name="Report Test",
        queries=[],
        approved=True,
        target_sandbox_host="127.0.0.1",
    )
    gate = QueryRegressionGate(workload)

    env = PerformanceEnvelopeCalculator.calculate(
        "q1",
        [QueryExecutionResult("q1", "app_user", 1, True, 10.0)]
    )
    parity = SemanticParityResult(
        query_id="q1",
        identical_rows=True,
        row_count_match=True,
        row_hash_match=True,
        changed_collation_detected=False,
    )
    plan = ExecutionPlanAnalysis(
        query_id="q1",
        source_plan_node="Index Scan",
        target_plan_node="Index Scan",
        has_seq_scan=False,
        has_nested_loop_spill=False,
        missing_index_detected=False,
        stale_statistics_detected=False,
        cost_regression_ratio=1.0,
        affected_table="items",
    )

    verdict = gate.evaluate_readiness(
        baseline_envelopes={"q1": env},
        target_envelopes={"q1": env},
        semantic_parities={"q1": parity},
        plan_analyses={"q1": plan},
    )

    assert verdict.app_readiness == APP_READY_UNCONDITIONAL
    assert verdict.care_badge == CARE_PASS

    report = verdict.format_terse_report()
    # Check pure ASCII and UNIX format
    assert report.isascii()
    assert "[PASS]" in report
    assert "QUERY-REGRESSION:" in report
    assert "readiness=APP_READY_UNCONDITIONAL" in report
    # No non-ASCII characters or emojis
    assert all(ord(c) < 128 for c in report)


def test_query_regression_envelope_rungs_coverage():
    with watched() as seen:
        # 1. certified_within_envelope holds
        f1 = {
            "executed": True,
            "cold_cache_tested": True,
            "collation_mismatch_count": 0,
            "latency_p95_ratio": 1.0,
            "latency_p99_ratio": 1.0,
            "cost_ratio": 1.0,
            "became_seq_scan": False,
            "cardinality_divergence": 0,
            "temp_disk_spills": 0,
            "has_baseline": True,
            "needs_unpriced_index": False,
            "implicit_casts": 0,
        }
        c1 = decide_query_regression_envelope(f1)
        assert c1.rung.name == "certified_within_envelope"

        # 2. certified_within_envelope refuses (collation_mismatch_count=1),
        # all review rungs refuse (they require collation_mismatch_count == 0)
        # severe_regression_action holds
        f2 = {
            "executed": True,
            "collation_mismatch_count": 1,
            "latency_p99_ratio": 3.0,
            "cost_ratio": 2.0,
            "implicit_casts": 1,
            "needs_unpriced_index": True,
            "has_baseline": False,
        }
        c2 = decide_query_regression_envelope(f2)
        assert c2.rung.name == "severe_regression_action"

        # 3. p99_tail_latency_review holds
        f3 = {
            "executed": True,
            "collation_mismatch_count": 0,
            "latency_p99_ratio": 3.0,
            "max_p99_ratio": 2.0,
        }
        c3 = decide_query_regression_envelope(f3)
        assert c3.rung.name == "p99_tail_latency_review"

        # 4. explain_cost_increase_review holds
        f4 = {
            "executed": True,
            "collation_mismatch_count": 0,
            "latency_p99_ratio": 1.0,
            "cost_ratio": 2.5,
            "max_cost_ratio": 1.5,
        }
        c4 = decide_query_regression_envelope(f4)
        assert c4.rung.name == "explain_cost_increase_review"

        # 5. implicit_cast_hazard_review holds
        f5 = {
            "executed": True,
            "collation_mismatch_count": 0,
            "latency_p99_ratio": 1.0,
            "cost_ratio": 1.0,
            "implicit_casts": 2,
        }
        c5 = decide_query_regression_envelope(f5)
        assert c5.rung.name == "implicit_cast_hazard_review"

        # 6. unpriced_index_remediation_review holds
        f6 = {
            "executed": True,
            "collation_mismatch_count": 0,
            "latency_p99_ratio": 1.0,
            "cost_ratio": 1.0,
            "implicit_casts": 0,
            "needs_unpriced_index": True,
        }
        c6 = decide_query_regression_envelope(f6)
        assert c6.rung.name == "unpriced_index_remediation_review"

        # 7. missing_baseline_review holds
        f7 = {
            "executed": True,
            "collation_mismatch_count": 0,
            "latency_p99_ratio": 1.0,
            "cost_ratio": 1.0,
            "implicit_casts": 0,
            "needs_unpriced_index": False,
            "has_baseline": False,
        }
        c7 = decide_query_regression_envelope(f7)
        assert c7.rung.name == "missing_baseline_review"


def test_application_readiness_query_regression_wiring():
    from migkit import application_readiness
    facts = {
        "executed": True,
        "cold_cache_tested": True,
        "collation_mismatch_count": 0,
        "latency_p95_ratio": 1.0,
        "latency_p99_ratio": 1.0,
        "cost_ratio": 1.0,
        "became_seq_scan": False,
        "cardinality_divergence": 0,
        "temp_disk_spills": 0,
        "has_baseline": True,
        "needs_unpriced_index": False,
        "implicit_casts": 0,
    }
    choice = application_readiness.verify_query_regression_envelope(facts)
    assert choice.rung.name == "certified_within_envelope"

