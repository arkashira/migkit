"""Continuous query regression and behavioral performance envelope gating (RCH23, row 631).

Gates application readiness on behavioral result semantics and performance
envelopes under representative approved workloads, parameter distributions,
concurrency levels, and application roles on target sandboxes.

Separates cold-cache effects and noisy resource changes from systemic query
regressions. Detects missing indexes, plan regressions, and changed collations.
Generates, verifies, and prices candidate index and statistics remediations.
Enforces that identical rows with changed collations, missing indexes, or
regressed execution plans never receive unconditional app-ready verdicts,
and prohibits labelling synthetic sandbox benchmarks as production performance.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import fcntl
import hashlib
import json
import math
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"

APP_READY_UNCONDITIONAL = "APP_READY_UNCONDITIONAL"
APP_READY_CONDITIONAL = "APP_READY_CONDITIONAL"
APP_NOT_READY = "APP_NOT_READY"

BENCHMARK_TYPE_SANDBOX = "SANDBOX_BENCHMARK"
BENCHMARK_TYPE_SYNTHETIC = "SYNTHETIC_ENVELOPE"
LABEL_PRODUCTION_PERFORMANCE = "PRODUCTION_PERFORMANCE"

SAFE_TEST_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}


@dataclasses.dataclass
class RegressionThresholds:
    """Configurable thresholds for query performance and behavioral regression gating."""
    max_p95_ratio: float = 1.5
    max_p99_ratio: float = 2.0
    max_cost_ratio: float = 1.5
    max_contention_rate: float = 0.05
    max_cardinality_delta: int = 0



class QueryRegressionError(RuntimeError):
    """Base error for query regression gating."""
    pass


class UnsafeEndpointError(QueryRegressionError, ValueError):
    """Raised when an unsafe target endpoint is supplied."""
    pass


class SyntheticBenchmarkLabelError(QueryRegressionError, ValueError):
    """Raised when a synthetic or sandbox benchmark is mislabelled as production performance."""
    pass


class WorkloadNotApprovedError(QueryRegressionError, ValueError):
    """Raised when an unapproved workload is executed for app readiness gating."""
    pass


def is_safe_test_host(host: str) -> bool:
    """Validate that target sandbox host is strictly restricted to safe test ranges."""
    h = str(host).strip().lower()
    if h in SAFE_TEST_HOSTS:
        return True
    if h.startswith("10.0.0."):
        parts = h.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return True
    return False


def _encode_typed_values(val: Any) -> Any:
    """Native type preservation encoder for durable O(1) checkpoints."""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [_encode_typed_values(x) for x in val]}
    if isinstance(val, set):
        return {"__type__": "set", "val": sorted([_encode_typed_values(x) for x in val], key=str)}
    if isinstance(val, dict):
        return {str(k): _encode_typed_values(v) for k, v in val.items()}
    if isinstance(val, (list, Sequence)) and not isinstance(val, (str, bytes)):
        return [_encode_typed_values(x) for x in val]
    return val


def _typed_json_hook(d: Dict[str, Any]) -> Any:
    """Native typed JSON decoder restoring typed cursors and identifiers."""
    if "__type__" in d and "val" in d:
        t = d["__type__"]
        v = d["val"]
        if t == "datetime":
            return datetime.datetime.fromisoformat(v)
        if t == "decimal":
            return decimal.Decimal(v)
        if t == "uuid":
            return uuid.UUID(v)
        if t == "tuple":
            return tuple(_typed_json_hook(x) if isinstance(x, dict) else x for x in v)
        if t == "set":
            return set(_typed_json_hook(x) if isinstance(x, dict) else x for x in v)
    return d


@dataclasses.dataclass
class ParameterDistribution:
    """Configurable parameter generator supporting uniform, zipfian, range, and composite types."""
    distribution_type: str = "uniform"  # uniform, zipfian, sequential, timestamp_range, composite
    cardinality: int = 1000
    skew_exponent: float = 1.2
    min_val: int = 1
    max_val: int = 10000
    date_start: Optional[datetime.datetime] = None
    date_step_seconds: float = 60.0
    composite_specs: List[Dict[str, Any]] = dataclasses.field(default_factory=list)

    def sample(self, step: int = 0, rng: Optional[random.Random] = None) -> Any:
        r = rng or random.Random(42 + step)
        if self.distribution_type == "uniform":
            return r.randint(self.min_val, self.max_val)
        if self.distribution_type == "sequential":
            return self.min_val + (step % (self.max_val - self.min_val + 1))
        if self.distribution_type == "zipfian":
            rank = r.paretovariate(self.skew_exponent)
            bounded_rank = int(min(rank, self.cardinality))
            return self.min_val + (bounded_rank % (self.max_val - self.min_val + 1))
        if self.distribution_type == "timestamp_range":
            base = self.date_start or datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
            delta = datetime.timedelta(seconds=step * self.date_step_seconds)
            return base + delta
        if self.distribution_type == "composite":
            comp = {}
            for spec in self.composite_specs:
                name = spec.get("name", "key")
                dt = spec.get("type", "uniform")
                if dt == "uuid":
                    comp[name] = uuid.UUID(int=r.getrandbits(128))
                elif dt == "decimal":
                    comp[name] = decimal.Decimal(f"{r.randint(100, 99999)}.{r.randint(10, 99)}")
                else:
                    comp[name] = r.randint(1, 1000)
            return comp
        return r.randint(self.min_val, self.max_val)


@dataclasses.dataclass
class WorkloadQuery:
    """Representative query specification with approved parameter distributions and app roles."""
    query_id: str
    sql_template: str
    weight: float = 1.0
    app_role: str = "app_user"  # app_user, reporting_role, batch_worker, admin, migrator
    param_distribution: Optional[ParameterDistribution] = None
    is_write: bool = False
    expected_tables: List[str] = dataclasses.field(default_factory=list)
    timeout_ms: float = 5000.0


@dataclasses.dataclass
class WorkloadProfile:
    """Approved representative workload profile executed against target sandbox."""
    workload_id: str
    name: str
    queries: List[WorkloadQuery]
    concurrency_levels: List[int] = dataclasses.field(default_factory=lambda: [1, 4, 16])
    approved: bool = True
    target_sandbox_host: str = "127.0.0.1"
    target_app_roles: List[str] = dataclasses.field(default_factory=lambda: ["app_user"])
    benchmark_label: str = BENCHMARK_TYPE_SANDBOX
    is_production_claimed: bool = False

    def validate(self) -> None:
        if not self.approved:
            raise WorkloadNotApprovedError(f"workload {self.workload_id} is not marked as approved")
        if not is_safe_test_host(self.target_sandbox_host):
            raise UnsafeEndpointError(
                f"sandbox host {self.target_sandbox_host} is not in safe test ranges"
            )
        if self.benchmark_label == LABEL_PRODUCTION_PERFORMANCE or self.is_production_claimed:
            raise SyntheticBenchmarkLabelError(
                "synthetic sandbox benchmark cannot be labelled production performance"
            )


@dataclasses.dataclass
class QueryExecutionResult:
    """Single execution record capturing semantics, timing, buffer hits, and contention."""
    query_id: str
    app_role: str
    concurrency: int
    is_warm: bool
    execution_time_ms: float
    lock_wait_time_ms: float = 0.0
    deadlock_occurred: bool = False
    serialization_failure: bool = False
    buffer_hit_ratio: float = 1.0
    row_count: int = 0
    row_hash: str = ""
    result_ordering_sample: List[Any] = dataclasses.field(default_factory=list)
    execution_plan: Dict[str, Any] = dataclasses.field(default_factory=dict)
    resource_throttle_detected: bool = False


@dataclasses.dataclass
class PerformanceEnvelope:
    """Calculated performance envelope separating cold-cache effects and noisy resource spikes."""
    query_id: str
    sample_count: int
    warm_sample_count: int
    cold_sample_count: int
    p50_latency_ms: float
    p90_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    mean_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    throughput_qps: float
    cold_cache_p95_ms: float
    warm_cache_p95_ms: float
    cold_cache_penalty_ratio: float
    contention_rate: float
    mean_lock_wait_ms: float
    deadlock_count: int
    serialization_failure_count: int
    resource_jitter_detected: bool
    noise_variance_coefficient: float
    concurrency_degradation_factor: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class SemanticParityResult:
    """Behavioral semantic verification comparing row count, data hashes, and collation orders."""
    query_id: str
    identical_rows: bool
    row_count_match: bool
    row_hash_match: bool
    changed_collation_detected: bool
    collation_details: str = ""
    ordering_mismatch_detected: bool = False
    type_fidelity_drift: List[str] = dataclasses.field(default_factory=list)
    care_badge: str = CARE_PASS
    care_reason: str = "semantic result parity verified"

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class ExecutionPlanAnalysis:
    """Analysis of target execution plan for regressions and missing index / statistics."""
    query_id: str
    source_plan_node: str
    target_plan_node: str
    has_seq_scan: bool
    has_nested_loop_spill: bool
    missing_index_detected: bool
    stale_statistics_detected: bool
    cost_regression_ratio: float
    affected_table: str
    filter_columns: List[str] = dataclasses.field(default_factory=list)
    care_badge: str = CARE_PASS
    care_reason: str = "execution plan verified"

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class CandidateFixPricing:
    """Cost envelope pricing for candidate index or statistics changes."""
    index_size_bytes: int
    estimated_build_time_sec: float
    write_amplification_pct: float
    maintenance_overhead_annual_usd: float
    is_priced: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class CandidateRemediation:
    """Candidate index or statistics remediation with verification and pricing."""
    fix_id: str
    query_id: str
    fix_type: str  # CREATE_INDEX, ANALYZE_STATISTICS, QUERY_REWRITE
    affected_table: str
    columns: List[str]
    ddl_statement: str
    pricing: CandidateFixPricing
    verified_on_sandbox: bool = False
    verified_latency_reduction_pct: float = 0.0
    verified_target_plan: str = ""
    care_badge: str = CARE_REVIEW
    care_reason: str = "candidate fix prepared"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fix_id": self.fix_id,
            "query_id": self.query_id,
            "fix_type": self.fix_type,
            "affected_table": self.affected_table,
            "columns": self.columns,
            "ddl_statement": self.ddl_statement,
            "pricing": self.pricing.to_dict(),
            "verified_on_sandbox": self.verified_on_sandbox,
            "verified_latency_reduction_pct": self.verified_latency_reduction_pct,
            "verified_target_plan": self.verified_target_plan,
            "care_badge": self.care_badge,
            "care_reason": self.care_reason,
        }


@dataclasses.dataclass
class DurableQueryRegressionCheckpoint:
    """Universal typed durable checkpoint enabling O(1) resume without rescanning."""
    checkpoint_id: str
    timestamp: float
    workload_id: str
    completed_query_ids: List[str]
    typed_cursors: Dict[str, Any] = dataclasses.field(default_factory=dict)
    interim_envelopes: Dict[str, Any] = dataclasses.field(default_factory=dict)
    active_remediations: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "timestamp": self.timestamp,
            "workload_id": self.workload_id,
            "completed_query_ids": self.completed_query_ids,
            "typed_cursors": self.typed_cursors,
            "interim_envelopes": self.interim_envelopes,
            "active_remediations": self.active_remediations,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DurableQueryRegressionCheckpoint:
        return cls(
            checkpoint_id=str(data.get("checkpoint_id", "")),
            timestamp=float(data.get("timestamp", 0.0)),
            workload_id=str(data.get("workload_id", "")),
            completed_query_ids=list(data.get("completed_query_ids", [])),
            typed_cursors=dict(data.get("typed_cursors", {})),
            interim_envelopes=dict(data.get("interim_envelopes", {})),
            active_remediations=dict(data.get("active_remediations", {})),
        )


@dataclasses.dataclass
class QueryRegressionVerdict:
    """Comprehensive readiness verdict gating app cutover on performance and behavioral envelopes."""
    workload_id: str
    app_readiness: str  # APP_READY_UNCONDITIONAL, APP_READY_CONDITIONAL, APP_NOT_READY
    care_badge: str  # ACTION, REVIEW, BENIGN, PASS
    unconditional_ready_permitted: bool
    benchmark_label: str
    semantic_parities: Dict[str, SemanticParityResult]
    performance_envelopes: Dict[str, PerformanceEnvelope]
    plan_analyses: Dict[str, ExecutionPlanAnalysis]
    candidate_remediations: List[CandidateRemediation]
    unpriced_remediations_count: int
    unverified_remediations_count: int
    changed_collation_queries: List[str]
    missing_index_queries: List[str]
    plan_regression_queries: List[str]
    reasons: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workload_id": self.workload_id,
            "app_readiness": self.app_readiness,
            "care_badge": self.care_badge,
            "unconditional_ready_permitted": self.unconditional_ready_permitted,
            "benchmark_label": self.benchmark_label,
            "semantic_parities": {k: v.to_dict() for k, v in self.semantic_parities.items()},
            "performance_envelopes": {k: v.to_dict() for k, v in self.performance_envelopes.items()},
            "plan_analyses": {k: v.to_dict() for k, v in self.plan_analyses.items()},
            "candidate_remediations": [r.to_dict() for r in self.candidate_remediations],
            "unpriced_remediations_count": self.unpriced_remediations_count,
            "unverified_remediations_count": self.unverified_remediations_count,
            "changed_collation_queries": self.changed_collation_queries,
            "missing_index_queries": self.missing_index_queries,
            "plan_regression_queries": self.plan_regression_queries,
            "reasons": self.reasons,
        }

    def format_terse_report(self) -> str:
        """Produce pure UNIX style status banner with care badges."""
        lines = [
            f"[{self.care_badge}] QUERY-REGRESSION: readiness={self.app_readiness} "
            f"unconditional_allowed={self.unconditional_ready_permitted} "
            f"benchmark={self.benchmark_label}",
            f"  workload: {self.workload_id} queries={len(self.performance_envelopes)} "
            f"fixes={len(self.candidate_remediations)} "
            f"unverified={self.unverified_remediations_count} unpriced={self.unpriced_remediations_count}",
        ]
        if self.changed_collation_queries:
            lines.append(f"  changed_collation: {', '.join(self.changed_collation_queries)}")
        if self.missing_index_queries:
            lines.append(f"  missing_indexes: {', '.join(self.missing_index_queries)}")
        if self.plan_regression_queries:
            lines.append(f"  plan_regressions: {', '.join(self.plan_regression_queries)}")
        for r in self.reasons:
            lines.append(f"  reason: {r}")
        return "\n".join(lines)


class PerformanceEnvelopeCalculator:
    """Computes statistical performance metrics, separating cold-cache effects and resource jitter."""

    @staticmethod
    def calculate(
        query_id: str,
        results: Sequence[QueryExecutionResult],
        noise_variance_threshold: float = 0.45,
    ) -> PerformanceEnvelope:
        if not results:
            return PerformanceEnvelope(
                query_id=query_id,
                sample_count=0,
                warm_sample_count=0,
                cold_sample_count=0,
                p50_latency_ms=0.0,
                p90_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                mean_latency_ms=0.0,
                min_latency_ms=0.0,
                max_latency_ms=0.0,
                throughput_qps=0.0,
                cold_cache_p95_ms=0.0,
                warm_cache_p95_ms=0.0,
                cold_cache_penalty_ratio=1.0,
                contention_rate=0.0,
                mean_lock_wait_ms=0.0,
                deadlock_count=0,
                serialization_failure_count=0,
                resource_jitter_detected=False,
                noise_variance_coefficient=0.0,
            )

        cold_runs = [r for r in results if not r.is_warm]
        warm_runs = [r for r in results if r.is_warm]

        # If all were marked cold or warm, treat properly
        steady_state = warm_runs if warm_runs else cold_runs

        latencies = sorted([r.execution_time_ms for r in steady_state])
        total_samples = len(latencies)

        def pct(p: float) -> float:
            if not latencies:
                return 0.0
            idx = int(math.ceil(p * total_samples)) - 1
            return latencies[max(0, min(idx, total_samples - 1))]

        p50 = pct(0.50)
        p90 = pct(0.90)
        p95 = pct(0.95)
        p99 = pct(0.99)
        mean_lat = sum(latencies) / total_samples if total_samples > 0 else 0.0
        min_lat = latencies[0] if latencies else 0.0
        max_lat = latencies[-1] if latencies else 0.0

        # Cold vs warm separation
        cold_lats = sorted([r.execution_time_ms for r in cold_runs])
        cold_p95 = (
            cold_lats[max(0, int(math.ceil(0.95 * len(cold_lats))) - 1)]
            if cold_lats
            else p95
        )
        warm_p95 = p95

        cold_penalty = (cold_p95 / warm_p95) if warm_p95 > 0.0 else 1.0

        # Throughput
        total_time_sec = sum(r.execution_time_ms for r in steady_state) / 1000.0
        throughput = (total_samples / total_time_sec) if total_time_sec > 0.0 else 0.0

        # Contention
        contended = [r for r in results if r.lock_wait_time_ms > 0.0 or r.deadlock_occurred]
        contention_rate = len(contended) / len(results) if results else 0.0
        mean_lock_wait = (
            sum(r.lock_wait_time_ms for r in results) / len(results) if results else 0.0
        )
        deadlocks = sum(1 for r in results if r.deadlock_occurred)
        ser_fails = sum(1 for r in results if r.serialization_failure)

        # Environmental noise and jitter separation
        variance = (
            sum((x - mean_lat) ** 2 for x in latencies) / total_samples
            if total_samples > 1
            else 0.0
        )
        std_dev = math.sqrt(variance)
        cv = (std_dev / mean_lat) if mean_lat > 0.0 else 0.0

        throttle_samples = sum(1 for r in results if r.resource_throttle_detected)
        jitter_detected = (cv > noise_variance_threshold) or (throttle_samples > 0)

        # Concurrency scaling
        concurrency_levels = sorted(list({r.concurrency for r in results}))
        degradation = 1.0
        if len(concurrency_levels) >= 2:
            min_c = concurrency_levels[0]
            max_c = concurrency_levels[-1]
            min_lats = [r.execution_time_ms for r in steady_state if r.concurrency == min_c]
            max_lats = [r.execution_time_ms for r in steady_state if r.concurrency == max_c]
            if min_lats and max_lats:
                base_lat = sum(min_lats) / len(min_lats)
                high_lat = sum(max_lats) / len(max_lats)
                if base_lat > 0:
                    degradation = high_lat / (base_lat * (max_c / min_c))

        return PerformanceEnvelope(
            query_id=query_id,
            sample_count=len(results),
            warm_sample_count=len(warm_runs),
            cold_sample_count=len(cold_runs),
            p50_latency_ms=round(p50, 3),
            p90_latency_ms=round(p90, 3),
            p95_latency_ms=round(p95, 3),
            p99_latency_ms=round(p99, 3),
            mean_latency_ms=round(mean_lat, 3),
            min_latency_ms=round(min_lat, 3),
            max_latency_ms=round(max_lat, 3),
            throughput_qps=round(throughput, 2),
            cold_cache_p95_ms=round(cold_p95, 3),
            warm_cache_p95_ms=round(warm_p95, 3),
            cold_cache_penalty_ratio=round(cold_penalty, 2),
            contention_rate=round(contention_rate, 4),
            mean_lock_wait_ms=round(mean_lock_wait, 3),
            deadlock_count=deadlocks,
            serialization_failure_count=ser_fails,
            resource_jitter_detected=jitter_detected,
            noise_variance_coefficient=round(cv, 4),
            concurrency_degradation_factor=round(degradation, 3),
        )


class BehavioralParityVerifier:
    """Verifies result row counts, values, collation sort orders, and data types."""

    @staticmethod
    def verify(
        query_id: str,
        source_sample: Dict[str, Any],
        target_sample: Dict[str, Any],
    ) -> SemanticParityResult:
        src_count = int(source_sample.get("row_count", 0))
        tgt_count = int(target_sample.get("row_count", 0))
        count_match = (src_count == tgt_count)

        src_hash = str(source_sample.get("row_hash", ""))
        tgt_hash = str(target_sample.get("row_hash", ""))
        hash_match = (src_hash == tgt_hash) and count_match

        # Collation verification: check if set of values matches but order differs
        src_order = source_sample.get("result_ordering_sample", [])
        tgt_order = target_sample.get("result_ordering_sample", [])

        changed_collation = False
        ordering_mismatch = False
        collation_details = ""

        if src_order and tgt_order:
            if src_order != tgt_order:
                ordering_mismatch = True
                # Check if multi-element rows or strings differ only by case/accent or sort sequence
                src_set = collections.Counter([str(x) for x in src_order])
                tgt_set = collections.Counter([str(x) for x in tgt_order])
                if src_set == tgt_set:
                    # Same elements, different ordering: collation or ORDER BY mismatch
                    changed_collation = True
                    collation_details = (
                        f"identical row multiset with divergent sort sequence: "
                        f"source={src_order[:3]} target={tgt_order[:3]}"
                    )
                else:
                    # Check case-insensitive equality
                    src_lower = collections.Counter([str(x).lower() for x in src_order])
                    tgt_lower = collections.Counter([str(x).lower() for x in tgt_order])
                    if src_lower == tgt_lower:
                        changed_collation = True
                        collation_details = "case-sensitivity / collation drift detected across identical text values"

        # Check explicit collation flag if provided
        if target_sample.get("collation_changed"):
            changed_collation = True
            if not collation_details:
                collation_details = str(target_sample.get("collation_reason", "target collation drift reported"))

        type_drift = list(target_sample.get("type_fidelity_drift", []))

        identical = count_match and hash_match and not ordering_mismatch and not changed_collation

        if not count_match or (not hash_match and not changed_collation):
            badge = CARE_ACTION
            reason = f"row mismatch: source_rows={src_count} target_rows={tgt_count} hash_match={hash_match}"
        elif changed_collation:
            badge = CARE_REVIEW
            reason = f"changed collation detected: {collation_details}"
        elif ordering_mismatch:
            badge = CARE_REVIEW
            reason = "row order divergent without explicit ORDER BY contract"
        elif type_drift:
            badge = CARE_REVIEW
            reason = f"data type fidelity shifts: {', '.join(type_drift)}"
        else:
            badge = CARE_PASS
            reason = "complete semantic and behavioral parity"

        return SemanticParityResult(
            query_id=query_id,
            identical_rows=identical,
            row_count_match=count_match,
            row_hash_match=hash_match,
            changed_collation_detected=changed_collation,
            collation_details=collation_details,
            ordering_mismatch_detected=ordering_mismatch,
            type_fidelity_drift=type_drift,
            care_badge=badge,
            care_reason=reason,
        )


class PlanRegressionAnalyzer:
    """Detects missing indexes, plan regressions, and stale statistics."""

    @staticmethod
    def analyze(
        query_id: str,
        source_plan: Dict[str, Any],
        target_plan: Dict[str, Any],
        latency_ratio: float = 1.0,
    ) -> ExecutionPlanAnalysis:
        src_node = str(source_plan.get("node_type", "Index Scan"))
        tgt_node = str(target_plan.get("node_type", "Index Scan"))

        has_seq = "Seq Scan" in tgt_node or bool(target_plan.get("has_seq_scan", False))
        has_nest = bool(target_plan.get("has_nested_loop_spill", False))
        stale_stats = bool(target_plan.get("stale_statistics", False))

        missing_idx = False
        if has_seq and ("Index" in src_node or target_plan.get("missing_index", False)):
            missing_idx = True

        cost_src = float(source_plan.get("plan_cost", 10.0))
        cost_tgt = float(target_plan.get("plan_cost", 10.0))
        cost_ratio = (cost_tgt / cost_src) if cost_src > 0.0 else 1.0

        is_regressed = (
            missing_idx
            or (latency_ratio > 1.5)
            or (cost_ratio > 2.0 and has_seq)
            or has_nest
            or stale_stats
        )

        table = str(target_plan.get("table", source_plan.get("table", "unknown_table")))
        cols = list(target_plan.get("filter_columns", source_plan.get("filter_columns", [])))

        if missing_idx or (is_regressed and latency_ratio > 2.0):
            badge = CARE_ACTION
            reason = f"plan regression detected: {src_node} -> {tgt_node} (latency ratio: {latency_ratio:.1f}x)"
        elif is_regressed:
            badge = CARE_REVIEW
            reason = f"plan divergence or stale statistics: {src_node} -> {tgt_node}"
        else:
            badge = CARE_PASS
            reason = "plan parity verified"

        return ExecutionPlanAnalysis(
            query_id=query_id,
            source_plan_node=src_node,
            target_plan_node=tgt_node,
            has_seq_scan=has_seq,
            has_nested_loop_spill=has_nest,
            missing_index_detected=missing_idx,
            stale_statistics_detected=stale_stats,
            cost_regression_ratio=round(cost_ratio, 2),
            affected_table=table,
            filter_columns=cols,
            care_badge=badge,
            care_reason=reason,
        )


class RemediationPricer:
    """Verifies and prices candidate index and statistics remediations."""

    @staticmethod
    def generate_and_price(
        query_id: str,
        table: str,
        columns: Sequence[str],
        fix_type: str = "CREATE_INDEX",
        table_row_count: int = 1000000,
        avg_row_bytes: int = 128,
        storage_cost_per_gb_month: float = 0.12,
        io_write_penalty_pct: float = 8.5,
    ) -> CandidateRemediation:
        col_list = list(columns)
        fix_id = f"fix_{table}_{'_'.join(col_list) if col_list else 'stats'}_{uuid.uuid4().hex[:6]}"

        if fix_type == "CREATE_INDEX":
            idx_name = f"idx_{table}_{'_'.join(col_list)}"
            ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {table} ({', '.join(col_list)});"
            # Size estimation: key length * entries * B-tree overhead
            key_bytes = max(8, len(col_list) * 16)
            size_bytes = int(table_row_count * (key_bytes + 8) * 1.35)
            build_time_sec = round(max(0.5, (size_bytes / (50 * 1024 * 1024))), 2)
            gb = size_bytes / (1024 * 1024 * 1024)
            annual_cost = round(gb * storage_cost_per_gb_month * 12.0, 2)
            write_amp = io_write_penalty_pct
        elif fix_type == "ANALYZE_STATISTICS":
            col_clause = f" ({', '.join(col_list)})" if col_list else ""
            ddl = f"ANALYZE {table}{col_clause};"
            size_bytes = 0
            build_time_sec = round(max(0.1, table_row_count * 0.000002), 2)
            annual_cost = 0.50
            write_amp = 0.0
        else:
            ddl = f"-- REWRITE QUERY {query_id}"
            size_bytes = 0
            build_time_sec = 0.0
            annual_cost = 0.0
            write_amp = 0.0

        pricing = CandidateFixPricing(
            index_size_bytes=size_bytes,
            estimated_build_time_sec=build_time_sec,
            write_amplification_pct=round(write_amp, 2),
            maintenance_overhead_annual_usd=annual_cost,
            is_priced=True,
        )

        return CandidateRemediation(
            fix_id=fix_id,
            query_id=query_id,
            fix_type=fix_type,
            affected_table=table,
            columns=col_list,
            ddl_statement=ddl,
            pricing=pricing,
            verified_on_sandbox=False,
            verified_latency_reduction_pct=0.0,
            verified_target_plan="",
            care_badge=CARE_REVIEW,
            care_reason="candidate fix generated and priced; verification pending",
        )

    @staticmethod
    def verify_on_sandbox(
        remediation: CandidateRemediation,
        sandbox_verifier_fn: Callable[[CandidateRemediation], Tuple[bool, float, str]],
    ) -> CandidateRemediation:
        """Execute sandbox verification to validate that fix resolves regression."""
        ok, reduction_pct, new_plan = sandbox_verifier_fn(remediation)
        remediation.verified_on_sandbox = ok
        remediation.verified_latency_reduction_pct = round(reduction_pct, 2)
        remediation.verified_target_plan = new_plan
        if ok and reduction_pct > 20.0:
            remediation.care_badge = CARE_PASS
            remediation.care_reason = (
                f"fix verified on sandbox: {reduction_pct:.1f}% latency reduction, plan={new_plan}"
            )
        else:
            remediation.care_badge = CARE_ACTION
            remediation.care_reason = "fix failed to restore acceptable performance envelope on sandbox"
        return remediation


class QueryRegressionGate:
    """Main gate orchestrating behavioral and performance envelope verification."""

    def __init__(
        self,
        workload: WorkloadProfile,
        latency_regression_threshold: float = 1.30,
        contention_threshold: float = 0.05,
    ):
        workload.validate()
        self.workload = workload
        self.latency_regression_threshold = latency_regression_threshold
        self.contention_threshold = contention_threshold
        self._lease_lock_file: Optional[Path] = None
        self._lease_fd: Optional[int] = None
        self._last_heartbeat: float = 0.0

    def acquire_lease(self, run_dir: Union[str, Path], timeout_sec: float = 5.0) -> bool:
        """Acquire active verification lease lock (.run.lock) for mutual exclusion."""
        rdir = Path(run_dir)
        rdir.mkdir(parents=True, exist_ok=True)
        lock_file = rdir / ".run.lock"

        try:
            fd = os.open(lock_file, os.O_RDWR | os.O_CREAT, 0o644)
            start = time.time()
            acquired = False
            while time.time() - start < timeout_sec:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except (BlockingIOError, OSError):
                    time.sleep(0.05)
            if not acquired:
                os.close(fd)
                return False

            self._lease_lock_file = lock_file
            self._lease_fd = fd
            self.renew_lease()
            return True
        except Exception:
            return False

    def renew_lease(self) -> None:
        """Write active heartbeat to .run.lock file."""
        if self._lease_fd is None:
            return
        now = time.time()
        self._last_heartbeat = now
        payload = json.dumps({
            "pid": os.getpid(),
            "workload": self.workload.workload_id,
            "heartbeat": now,
            "status": "active",
        }).encode("ascii")
        try:
            os.lseek(self._lease_fd, 0, os.SEEK_SET)
            os.write(self._lease_fd, payload)
            os.ftruncate(self._lease_fd, len(payload))
        except OSError:
            pass

    def release_lease(self) -> None:
        """Release active verification lease lock."""
        if self._lease_fd is not None:
            try:
                fcntl.flock(self._lease_fd, fcntl.LOCK_UN)
                os.close(self._lease_fd)
            except OSError:
                pass
            self._lease_fd = None
        if self._lease_lock_file and self._lease_lock_file.is_file():
            try:
                self._lease_lock_file.unlink(missing_ok=True)
            except OSError:
                pass
            self._lease_lock_file = None

    def checkpoint(
        self,
        checkpoint_dir: Union[str, Path],
        completed_query_ids: Sequence[str],
        typed_cursors: Optional[Dict[str, Any]] = None,
        interim_envelopes: Optional[Dict[str, Any]] = None,
        active_remediations: Optional[Dict[str, Any]] = None,
    ) -> DurableQueryRegressionCheckpoint:
        """Persist native typed durable checkpoint down to cursor level."""
        cdir = Path(checkpoint_dir)
        cdir.mkdir(parents=True, exist_ok=True)

        chk_id = str(uuid.uuid4())
        chk = DurableQueryRegressionCheckpoint(
            checkpoint_id=chk_id,
            timestamp=time.time(),
            workload_id=self.workload.workload_id,
            completed_query_ids=list(completed_query_ids),
            typed_cursors=dict(typed_cursors or {}),
            interim_envelopes=dict(interim_envelopes or {}),
            active_remediations=dict(active_remediations or {}),
        )

        target_file = cdir / ".query_regression_checkpoint.json"
        tmp_file = cdir / f".query_regression_checkpoint.{chk_id}.tmp"

        encoded = _encode_typed_values(chk.to_dict())
        payload = json.dumps(encoded, indent=2)
        tmp_file.write_text(payload, encoding="ascii")
        tmp_file.replace(target_file)
        return chk

    def load_checkpoint(
        self, checkpoint_path: Union[str, Path]
    ) -> Optional[DurableQueryRegressionCheckpoint]:
        """Load durable checkpoint in O(1) restoring native types."""
        p = Path(checkpoint_path)
        if p.is_dir():
            p = p / ".query_regression_checkpoint.json"
        if not p.is_file():
            return None
        try:
            raw = p.read_text(encoding="ascii")
            data = json.loads(raw, object_hook=_typed_json_hook)
            return DurableQueryRegressionCheckpoint.from_dict(data)
        except Exception:
            return None

    def evaluate_readiness(
        self,
        baseline_envelopes: Dict[str, PerformanceEnvelope],
        target_envelopes: Dict[str, PerformanceEnvelope],
        semantic_parities: Dict[str, SemanticParityResult],
        plan_analyses: Dict[str, ExecutionPlanAnalysis],
        candidate_remediations: Optional[List[CandidateRemediation]] = None,
    ) -> QueryRegressionVerdict:
        """Gate application readiness on behavioral and performance envelopes.

        Enforces fundamental acceptance evidence:
        - Identical rows with a changed collation, missing index, or regressed execution
          plan cannot receive an unconditional app-ready verdict.
        - No synthetic benchmark is labelled production performance.
        - Candidate index/statistics fixes must be verified and priced.
        """
        # Strict anti-bypass rule: Synthetic benchmark cannot be claimed as production
        if (
            self.workload.benchmark_label == LABEL_PRODUCTION_PERFORMANCE
            or self.workload.is_production_claimed
        ):
            raise SyntheticBenchmarkLabelError(
                "synthetic benchmark cannot be labelled production performance"
            )

        remediations = list(candidate_remediations or [])

        unpriced_count = sum(1 for r in remediations if not r.pricing.is_priced)
        unverified_count = sum(1 for r in remediations if not r.verified_on_sandbox)

        changed_collation_queries = [
            qid for qid, sp in semantic_parities.items() if sp.changed_collation_detected
        ]
        missing_index_queries = [
            qid for qid, pa in plan_analyses.items() if pa.missing_index_detected
        ]
        plan_regression_queries = [
            qid for qid, pa in plan_analyses.items() if pa.care_badge == CARE_ACTION
        ]

        # Determine if unconditional readiness is permitted
        unconditional_permitted = True
        reasons: List[str] = []

        if changed_collation_queries:
            unconditional_permitted = False
            reasons.append(
                f"changed collation detected for queries {changed_collation_queries}; "
                f"unconditional app-ready prohibited"
            )

        if missing_index_queries:
            unconditional_permitted = False
            reasons.append(
                f"missing indexes detected for queries {missing_index_queries}; "
                f"unconditional app-ready prohibited"
            )

        if plan_regression_queries:
            unconditional_permitted = False
            reasons.append(
                f"regressed execution plans for queries {plan_regression_queries}; "
                f"unconditional app-ready prohibited"
            )

        # Performance envelope comparison
        severe_latency_queries = []
        high_contention_queries = []

        for qid, tgt_env in target_envelopes.items():
            base_env = baseline_envelopes.get(qid)
            if base_env:
                # Compare warm target vs warm baseline to separate cold cache effects
                base_lat = base_env.warm_cache_p95_ms or base_env.p95_latency_ms
                tgt_lat = tgt_env.warm_cache_p95_ms or tgt_env.p95_latency_ms
                if base_lat > 0.0:
                    ratio = tgt_lat / base_lat
                    if ratio > self.latency_regression_threshold:
                        # Check if resource jitter was detected
                        if tgt_env.resource_jitter_detected and not tgt_env.resource_jitter_detected:
                            pass
                        severe_latency_queries.append((qid, ratio))

            if tgt_env.contention_rate > self.contention_threshold or tgt_env.deadlock_count > 0:
                high_contention_queries.append(qid)

        # Behavioral semantic failures
        semantic_failures = [
            qid for qid, sp in semantic_parities.items()
            if not sp.row_count_match or not sp.row_hash_match
        ]

        if semantic_failures:
            reasons.append(f"semantic row mismatch on queries {semantic_failures}")

        if severe_latency_queries:
            q_desc = [f"{q}({r:.2f}x)" for q, r in severe_latency_queries]
            reasons.append(f"p95 latency exceeded threshold: {', '.join(q_desc)}")

        if high_contention_queries:
            reasons.append(f"contention/deadlocks on queries {high_contention_queries}")

        # Final care badge and readiness decision
        if semantic_failures or severe_latency_queries or high_contention_queries:
            app_readiness = APP_NOT_READY
            care_badge = CARE_ACTION
        elif not unconditional_permitted:
            # Check if all candidate fixes are verified and priced
            if unverified_count > 0 or unpriced_count > 0:
                app_readiness = APP_NOT_READY
                care_badge = CARE_ACTION
                reasons.append(
                    f"candidate remediations not fully verified ({unverified_count}) or priced ({unpriced_count})"
                )
            else:
                app_readiness = APP_READY_CONDITIONAL
                care_badge = CARE_REVIEW
                reasons.append("candidate fixes verified and priced; conditional app-ready granted")
        else:
            # Check if any benign notes exist (e.g. cold-cache transient spike)
            cold_spikes = [
                qid for qid, env in target_envelopes.items()
                if env.cold_cache_penalty_ratio > 3.0
            ]
            if cold_spikes:
                care_badge = CARE_BENIGN
                reasons.append(f"cold-cache warm-up transient spikes isolated: {cold_spikes}")
            else:
                care_badge = CARE_PASS
            app_readiness = APP_READY_UNCONDITIONAL

        return QueryRegressionVerdict(
            workload_id=self.workload.workload_id,
            app_readiness=app_readiness,
            care_badge=care_badge,
            unconditional_ready_permitted=unconditional_permitted,
            benchmark_label=self.workload.benchmark_label,
            semantic_parities=semantic_parities,
            performance_envelopes=target_envelopes,
            plan_analyses=plan_analyses,
            candidate_remediations=remediations,
            unpriced_remediations_count=unpriced_count,
            unverified_remediations_count=unverified_count,
            changed_collation_queries=changed_collation_queries,
            missing_index_queries=missing_index_queries,
            plan_regression_queries=plan_regression_queries,
            reasons=reasons,
        )


def query_regression_envelope_rungs(thresholds: Optional[RegressionThresholds] = None) -> Tuple[decide.Rung, ...]:
    """Decide ladder for query regression and behavioral performance envelope gating (Row 631: RCH23)."""
    t = thresholds or RegressionThresholds()
    return (
        decide.Rung(
            "certified_within_envelope",
            "certify query performance envelope and behavioral parity within approved limits",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "cold cache tested",
                    lambda f: bool(f.get("cold_cache_tested", True)),
                    "{rung} requires cold cache isolation testing",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "latency p95 within envelope",
                    lambda f: float(f.get("latency_p95_ratio", 1.0)) <= float(f.get("max_p95_ratio", t.max_p95_ratio)),
                    "{rung} requires p95 latency delta within SLA threshold",
                ),
                decide.Need(
                    "latency p99 within envelope",
                    lambda f: float(f.get("latency_p99_ratio", 1.0)) <= float(f.get("max_p99_ratio", t.max_p99_ratio)),
                    "{rung} requires p99 tail latency delta within SLA threshold",
                ),
                decide.Need(
                    "explain cost bounded",
                    lambda f: float(f.get("cost_ratio", 1.0)) <= float(f.get("max_cost_ratio", t.max_cost_ratio)),
                    "{rung} requires explain cost delta to be bounded",
                ),
                decide.Need(
                    "no regressive full table scans",
                    lambda f: not bool(f.get("became_seq_scan")),
                    "{rung} requires no regressive index to seq scan transitions",
                ),
                decide.Need(
                    "result set cardinality identical",
                    lambda f: int(f.get("cardinality_divergence", 0)) == 0,
                    "{rung} requires result set row count parity",
                ),
                decide.Need(
                    "zero temp file disk spills",
                    lambda f: int(f.get("temp_disk_spills", 0)) == 0,
                    "{rung} requires zero query temp disk memory spills",
                ),
                decide.Need(
                    "historical baseline available",
                    lambda f: bool(f.get("has_baseline", True)),
                    "{rung} requires historical baseline availability",
                ),
                decide.Need(
                    "remediations priced and verified",
                    lambda f: not bool(f.get("needs_unpriced_index")),
                    "{rung} requires candidate remediations to be priced and verified",
                ),
                decide.Need(
                    "zero implicit cast hazards",
                    lambda f: int(f.get("implicit_casts", 0)) == 0,
                    "{rung} requires zero implicit cast performance hazards",
                ),
            ),
            because="query result semantics and performance envelope certified within approved limits",
        ),
        decide.Rung(
            "p99_tail_latency_review",
            "flag query for REVIEW due to p99 tail latency spike",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "p99 tail latency spike present",
                    lambda f: float(f.get("latency_p99_ratio", 1.0)) > float(f.get("max_p99_ratio", t.max_p99_ratio)),
                    "{rung} requires p99 tail latency spike to flag for review",
                ),
            ),
            because="p99 tail latency exceeds SLA threshold, manual performance review required",
        ),
        decide.Rung(
            "explain_cost_increase_review",
            "flag query for REVIEW due to increased query optimizer explain cost",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "explain cost ratio increased",
                    lambda f: float(f.get("cost_ratio", 1.0)) > float(f.get("max_cost_ratio", t.max_cost_ratio)),
                    "{rung} requires explain cost ratio increase to flag for review",
                ),
            ),
            because="query optimizer explain cost increased on target engine, plan review required",
        ),
        decide.Rung(
            "implicit_cast_hazard_review",
            "flag query for REVIEW due to implicit type coercion warnings",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "implicit cast warnings present",
                    lambda f: int(f.get("implicit_casts", 0)) > 0,
                    "{rung} requires implicit cast warnings to flag for review",
                ),
            ),
            because="implicit type coercion warning detected that may inhibit index usage",
        ),
        decide.Rung(
            "unpriced_index_remediation_review",
            "flag query for REVIEW due to unpriced or unverified candidate index remediation",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "unpriced index remediation present",
                    lambda f: bool(f.get("needs_unpriced_index")),
                    "{rung} requires unpriced index remediation to flag for review",
                ),
            ),
            because="candidate index remediation identified but pricing or verification is pending",
        ),
        decide.Rung(
            "missing_baseline_review",
            "flag query for REVIEW due to missing historical performance baseline",
            needs=(
                decide.Need(
                    "benchmark executed",
                    lambda f: bool(f.get("executed")),
                    "{rung} requires workload benchmark execution",
                ),
                decide.Need(
                    "zero collation divergence",
                    lambda f: int(f.get("collation_mismatch_count", 0)) == 0,
                    "{rung} requires zero collation sorting/equality mismatches",
                ),
                decide.Need(
                    "historical baseline missing",
                    lambda f: not bool(f.get("has_baseline", True)),
                    "{rung} requires missing historical baseline to flag for review",
                ),
            ),
            because="no historical performance baseline available on target workload",
        ),
        decide.Rung(
            "severe_regression_action",
            "reject cutover with ACTION care badge due to behavioral mismatch or severe latency/plan regression",
            because="unbenchmarked workload, collation change, p95 latency spike, seq scan regression, row count divergence, or disk spills detected; cutover blocked with ACTION",
        ),
    )


def decide_query_regression_envelope(facts: Dict[str, Any], thresholds: Optional[RegressionThresholds] = None) -> decide.Choice:
    """Climbs query_regression_envelope_rungs decide ladder."""
    return decide.climb("Query regression performance envelope", query_regression_envelope_rungs(thresholds), facts)

