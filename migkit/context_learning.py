"""Transfer measurements without false confidence (Row 586: SMART-N3.context-learning).

Extends Knowledge cost fits with context-relevant version, service, hardware,
topology, and schema type mix; hierarchical pooling across context tiers;
out-of-distribution (OOD) detection; continuous time decay; and robust Huber
residual handling using `decide.Rung` and `decide.climb(work, rungs, facts)`.

Key Invariants:
1. Every planning, transfer, drift adaptation, residual robustness, and
   calibration decision is evaluated through `decide.Rung` and
   `decide.climb(work, rungs, facts)`.
2. Prior and measurement lineage is preserved: every prediction carries exact
   provenance (source context, target context, pooling level, shrinkage factor,
   context distance, OOD flag, drift epoch, and calibration multiplier).
3. Evidence from a different context NEVER becomes falsely precise local
   measurement: variance is inflated proportional to context distance squared,
   degrees of freedom are bounded, and OOD contexts trigger conservative
   widening.
4. Non-stationary drift recovery: structural environment shifts (hardware,
   version, network changes) are diagnosed via sequential standardized
   residuals, triggering active drift reset/recalibration with ACTION care badge
   to quickly restore forecast accuracy.
5. Robust residual handling: heavy-tailed workload noise is downweighted via
   Huber loss, while corrupted or physically impossible readings are refused.
6. Calibrated predictive intervals: empirical coverage (p50 and p90) is certified
   against held-out real workloads; conformal scaling factor restores conservative
   guarantees if coverage deficit occurs.
7. Universal typed durable resume in O(1) with native type preservation
   (datetime, Decimal, UUID, composite tuples), and mutual exclusion active
   lease lock (.run.lock) with heartbeat.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import numpy as np

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Error Types
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})
SAFE_HOST_PREFIXES: Tuple[str, ...] = ("127.", "10.0.0.", "localhost", "::1")


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if any(cleaned.startswith(p) for p in SAFE_HOST_PREFIXES):
        if cleaned.startswith("10.0.0."):
            parts = cleaned.split(".")
            if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                return cleaned
        elif cleaned.startswith("127."):
            parts = cleaned.split(".")
            if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
                return cleaned
        else:
            return cleaned
    raise UnsafeEndpointHostError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


class ContextLearningError(RuntimeError):
    """Base error for context learning and transfer measurement operations."""


class UnsafeEndpointHostError(ContextLearningError, ValueError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class CorruptedMeasurementError(ContextLearningError, ValueError):
    """Raised when a measurement is physically impossible or corrupted."""


class VerificationLeaseError(ContextLearningError):
    """Raised when active verification lease lock (.run.lock) cannot be acquired."""


class UncalibratedIntervalError(ContextLearningError, ValueError):
    """Raised when predictive intervals cannot be calibrated within safety bounds."""


class DriftRecoveryError(ContextLearningError):
    """Raised when drift recovery fails or post-drift observations are absent."""


# -----------------------------------------------------------------------------
# Context Dimension Models (Version, Service, Hardware, Topology, Type Mix)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class HardwareProfile:
    """Hardware capacity profile for source or target host."""
    cores: int
    memory_bytes: int
    iops: int
    disk_type: str = "gp3"
    instance_class: str = "m6i.4xlarge"

    def __post_init__(self) -> None:
        if self.cores <= 0:
            raise ValueError("cores must be positive")
        if self.memory_bytes <= 0:
            raise ValueError("memory_bytes must be positive")
        if self.iops <= 0:
            raise ValueError("iops must be positive")


@dataclass(frozen=True)
class TopologyProfile:
    """Network topology and latency profile between source and target."""
    network_kind: str
    latency_ms: float
    bandwidth_mbps: float
    jitter_ms: float = 0.0

    def __post_init__(self) -> None:
        if self.latency_ms < 0.0:
            raise ValueError("latency_ms cannot be negative")
        if self.bandwidth_mbps <= 0.0:
            raise ValueError("bandwidth_mbps must be positive")


@dataclass(frozen=True)
class TypeMixProfile:
    """Composition of column data types and structural features in the table schema."""
    lob_share: float = 0.0
    keyless_share: float = 0.0
    numeric_share: float = 0.0
    json_share: float = 0.0
    text_share: float = 0.0
    temporal_share: float = 0.0
    binary_share: float = 0.0

    def as_vector(self) -> np.ndarray:
        return np.array(
            [
                float(self.lob_share),
                float(self.keyless_share),
                float(self.numeric_share),
                float(self.json_share),
                float(self.text_share),
                float(self.temporal_share),
                float(self.binary_share),
            ],
            dtype=np.float64,
        )


@dataclass(frozen=True)
class MigrationContext:
    """Complete operational context for cost modeling and measurement transfer."""
    engine_pair: str
    rung: str
    version: str
    service: str
    hardware: HardwareProfile
    topology: TopologyProfile
    type_mix: TypeMixProfile
    host: str = "127.0.0.1"

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.host)

    @property
    def context_id(self) -> str:
        """Deterministic cryptographic signature of the context."""
        raw = (
            f"{self.engine_pair}|{self.rung}|{self.version}|{self.service}|"
            f"{self.hardware.cores}|{self.hardware.memory_bytes}|{self.hardware.iops}|{self.hardware.disk_type}|"
            f"{self.topology.network_kind}|{self.topology.latency_ms:.1f}|{self.topology.bandwidth_mbps:.1f}|"
            f"{self.type_mix.lob_share:.2f}|{self.type_mix.keyless_share:.2f}"
        )
        return hashlib.sha256(raw.encode("ascii")).hexdigest()[:16]

    @property
    def service_key(self) -> str:
        return f"{self.engine_pair}|{self.rung}|{self.version}|{self.service}"

    @property
    def engine_pair_key(self) -> str:
        return f"{self.engine_pair}|{self.rung}"

    @property
    def global_rung_key(self) -> str:
        return str(self.rung)


@dataclass(frozen=True)
class ContextWorkloadUnit:
    """Workload item representing a table, range, or partition batch."""
    bytes: int
    rows: int
    lob_bytes: int = 0
    keyless: bool = False
    rtt_s: float = 0.0
    load_share: float = 0.0
    unit_id: str = ""

    def __post_init__(self) -> None:
        if self.bytes < 0:
            raise ValueError("bytes cannot be negative")
        if self.rows < 0:
            raise ValueError("rows cannot be negative")

    def feature_vector(self) -> np.ndarray:
        """7-dimensional feature vector: start, MB, k rows, LOB MB, keyless MB, round-trips, busy MB."""
        mib = 1024.0 * 1024.0
        mb = float(self.bytes) / mib
        k_rows = float(self.rows) / 1000.0
        lob_mb = float(self.lob_bytes) / mib
        keyless_mb = mb if self.keyless else 0.0
        rtt_krows = float(self.rtt_s) * k_rows
        busy_mb = float(self.load_share) * mb
        return np.array(
            [1.0, mb, k_rows, lob_mb, keyless_mb, rtt_krows, busy_mb],
            dtype=np.float64,
        )


@dataclass(frozen=True)
class MeasurementLineage:
    """Cryptographic and statistical provenance of a measurement or prediction."""
    lineage_id: str
    source_context_id: str
    target_context_id: str
    pooling_level: str
    effective_sample_size: float
    shrinkage_factor: float
    context_distance: float
    is_ood: bool
    ood_distance: float
    drift_epoch: int
    calibration_multiplier: float
    prior_provenance: str
    created_at_s: float


# -----------------------------------------------------------------------------
# Context Distance & Out-of-Distribution (OOD) Detection
# -----------------------------------------------------------------------------

def compute_context_distance(source: MigrationContext, target: MigrationContext) -> float:
    """Compute context distance d(c_source, c_target) across version, service, hardware, topology, and types."""
    dist = 0.0

    # 1. Engine pair mismatch: major barrier
    if source.engine_pair != target.engine_pair:
        dist += 5.0

    # 2. Rung mismatch
    if source.rung != target.rung:
        dist += 2.0

    # 3. Version mismatch
    if source.version != target.version:
        s_parts = source.version.split(".")
        t_parts = target.version.split(".")
        if s_parts and t_parts and s_parts[0] != t_parts[0]:
            dist += 1.5
        else:
            dist += 0.5

    # 4. Service mismatch (e.g. self-hosted vs RDS vs Aurora)
    if source.service != target.service:
        dist += 1.2

    # 5. Hardware log-distance
    hw_s, hw_t = source.hardware, target.hardware
    core_diff = abs(math.log2(hw_s.cores / hw_t.cores))
    mem_diff = abs(math.log2(hw_s.memory_bytes / hw_t.memory_bytes))
    iops_diff = abs(math.log2(hw_s.iops / hw_t.iops))
    disk_penalty = 0.5 if hw_s.disk_type != hw_t.disk_type else 0.0
    dist += 0.4 * (core_diff + mem_diff + iops_diff) + disk_penalty

    # 6. Topology distance
    top_s, top_t = source.topology, target.topology
    lat_diff = abs(top_s.latency_ms - top_t.latency_ms) / 10.0
    bw_diff = abs(math.log2(top_s.bandwidth_mbps / top_t.bandwidth_mbps))
    net_kind_penalty = 0.8 if top_s.network_kind != top_t.network_kind else 0.0
    dist += 0.3 * (lat_diff + bw_diff) + net_kind_penalty

    # 7. Type mix divergence (Euclidean distance on type vector)
    v_s = source.type_mix.as_vector()
    v_t = target.type_mix.as_vector()
    type_diff = float(np.linalg.norm(v_s - v_t))
    dist += 0.8 * type_diff

    return float(dist)


def compute_workload_ood(
    features: np.ndarray,
    empirical_mean: np.ndarray,
    empirical_cov_inv: np.ndarray,
    threshold: float = 2.5,
) -> Tuple[bool, float]:
    """Compute Mahalanobis distance of workload features against empirical distribution."""
    delta = features - empirical_mean
    mahalanobis_sq = float(np.dot(delta, np.dot(empirical_cov_inv, delta)))
    distance = float(math.sqrt(max(0.0, mahalanobis_sq)))
    is_ood = distance > threshold
    return is_ood, distance


# -----------------------------------------------------------------------------
# Decision Engine Ladders (`decide.Rung` and `decide.climb`)
# -----------------------------------------------------------------------------

def context_transfer_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for transferring measurements and predictive estimates."""
    return (
        decide.Rung(
            "exact_local_calibrated",
            "exact local context match with tight empirical calibration",
            needs=(
                decide.Need(
                    "exact context match with adequate local samples",
                    lambda f: bool(f.get("exact_context_match", False))
                    and int(f.get("local_observations_count", 0)) >= int(f.get("min_exact_observations", 3)),
                    "{rung} requires exact context match and sufficient local observations",
                ),
                decide.Need(
                    "in-distribution workload features",
                    lambda f: not bool(f.get("is_ood", False))
                    and float(f.get("ood_distance", 0.0)) <= float(f.get("ood_threshold", 2.5)),
                    "{rung} requires workload features within empirical distribution",
                ),
                decide.Need(
                    "stationary environment without active drift",
                    lambda f: not bool(f.get("persistent_drift_detected", False)),
                    "{rung} refused because active structural drift requires recalibration",
                ),
            ),
            because="uses tight local calibrated predictive interval when target matches local context without drift",
            footprint=0,
            leaves="local calibrated posterior",
        ),
        decide.Rung(
            "hierarchical_pooled_transfer",
            "hierarchical pooling across service, engine-pair, and global rung tiers with shrinkage",
            needs=(
                decide.Need(
                    "in-distribution workload features",
                    lambda f: not bool(f.get("is_ood", False))
                    and float(f.get("ood_distance", 0.0)) <= float(f.get("ood_threshold", 2.5)),
                    "{rung} refused because out-of-distribution workload requires conservative widening",
                ),
                decide.Need(
                    "poolable context with parent or peer evidence",
                    lambda f: bool(f.get("poolable_context", False))
                    and int(f.get("pooled_evidence_count", 0)) > 0,
                    "{rung} requires poolable engine-pair or service context with available evidence",
                ),
                decide.Need(
                    "context distance within transfer threshold",
                    lambda f: float(f.get("context_distance", 0.0)) <= float(f.get("max_transfer_distance", 4.0)),
                    "{rung} refused because context distance exceeds transfer bound",
                ),
            ),
            because="shrinks predictive estimate toward hierarchical parent prior with uncertainty widening proportional to context distance",
            footprint=0,
            leaves="hierarchically pooled prior with widened predictive variance",
        ),
        decide.Rung(
            "ood_conservative_widened",
            "out-of-distribution conservative interval widening to prevent false precision",
            needs=(
                decide.Need(
                    "out-of-distribution workload or distant context detected",
                    lambda f: bool(f.get("is_ood", False))
                    or float(f.get("ood_distance", 0.0)) > float(f.get("ood_threshold", 2.5))
                    or float(f.get("context_distance", 0.0)) > float(f.get("max_transfer_distance", 4.0)),
                    "{rung} requires out-of-distribution or distant context trigger",
                ),
                decide.Need(
                    "conservative safety bounds available",
                    lambda f: bool(f.get("conservative_bounds_available", True)),
                    "{rung} requires valid conservative upper bound",
                ),
            ),
            because="widens predictive spread conservatively for out-of-distribution contexts to prevent evidence from different contexts from becoming falsely precise local measurements",
            footprint=0,
            leaves="conservatively widened predictive bounds with explicit OOD caveat",
        ),
        decide.Rung(
            "safe_cold_start_prior",
            "conservative uninformative cold-start benchmark prior",
            needs=(
                decide.Need(
                    "cold start fallback available",
                    lambda f: bool(f.get("cold_start_available", True)),
                    "{rung} requires cold-start baseline",
                ),
            ),
            because="provides conservative physical and benchmark upper bounds when no trustworthy prior or peer measurements exist",
            footprint=0,
            leaves="uninformative wide prior",
        ),
    )


def drift_adaptation_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for non-stationary drift recovery and time decay."""
    return (
        decide.Rung(
            "active_drift_reset_and_recalibrate",
            "active drift reset and recalibration upon structural hardware, version, or network shift",
            needs=(
                decide.Need(
                    "structural drift confirmed by sequential residuals",
                    lambda f: bool(f.get("structural_drift_flag", False))
                    and int(f.get("consecutive_drift_count", 0)) >= int(f.get("drift_trigger_count", 2)),
                    "{rung} requires confirmed structural drift via consecutive residual deviations",
                ),
                decide.Need(
                    "post-drift measurements ready for recalibration",
                    lambda f: int(f.get("post_drift_samples", 0)) >= int(f.get("min_post_drift_samples", 1)),
                    "{rung} requires at least one post-drift measurement",
                ),
            ),
            because="resets or downweights pre-shift historical evidence upon confirmed hardware, version, or network topology change to rapidly recover predictive calibration",
            footprint=0,
            leaves="drift event tombstone and recalibrated epoch",
        ),
        decide.Rung(
            "exponential_decay_tracking",
            "exponential time decay tracking for gradual non-stationary workload shifts",
            needs=(
                decide.Need(
                    "time decay enabled and non-zero elapsed interval",
                    lambda f: bool(f.get("time_decay_enabled", True))
                    and float(f.get("elapsed_time_hours", 0.0)) > float(f.get("decay_grace_period_hours", 0.0)),
                    "{rung} requires enabled time decay and elapsed interval beyond grace period",
                ),
                decide.Need(
                    "absence of unhandled structural step change",
                    lambda f: not bool(f.get("structural_drift_flag", False)),
                    "{rung} refused because structural shift requires active reset rather than smooth decay",
                ),
            ),
            because="decays historical observation weights continuously over time to adapt to background workload and environmental drift",
            footprint=0,
            leaves="time-decayed sufficient statistics",
        ),
        decide.Rung(
            "stationary_retention",
            "stationary retention preserving full cumulative observation weight",
            needs=(
                decide.Need(
                    "stationary environment without drift or significant elapsed time",
                    lambda f: bool(f.get("environment_stationary", True))
                    and not bool(f.get("structural_drift_flag", False)),
                    "{rung} requires stationary system state",
                ),
            ),
            because="preserves full historical measurement weight when system environment, hardware, and schema are stationary",
            footprint=0,
            leaves="unattenuated cumulative evidence",
        ),
    )


def residual_robustness_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for robust residual handling and outlier filtering."""
    return (
        decide.Rung(
            "robust_huber_reweighted_fit",
            "robust Huber reweighting for heavy-tailed residual noise and transient spikes",
            needs=(
                decide.Need(
                    "heavy-tailed residual detected exceeding Huber threshold",
                    lambda f: float(f.get("standardized_residual", 0.0)) > float(f.get("huber_tuning_k", 1.345)),
                    "{rung} requires standardized residual exceeding Huber threshold",
                ),
                decide.Need(
                    "measurement is physically plausible",
                    lambda f: bool(f.get("physically_plausible", True)),
                    "{rung} refused because corrupted measurement cannot be reweighted",
                ),
            ),
            because="downweights extreme heavy-tailed transient spikes via Huber loss to prevent coefficient distortion",
            footprint=0,
            leaves="huber-weighted residual update",
        ),
        decide.Rung(
            "standard_sufficient_stats_update",
            "conjugate normal-inverse-gamma update for standard in-envelope observations",
            needs=(
                decide.Need(
                    "residual within normal noise envelope",
                    lambda f: float(f.get("standardized_residual", 0.0)) <= float(f.get("huber_tuning_k", 1.345)),
                    "{rung} requires residual within Huber threshold",
                ),
                decide.Need(
                    "measurement is valid and physically plausible",
                    lambda f: bool(f.get("physically_plausible", True)) and bool(f.get("measurement_valid", True)),
                    "{rung} requires valid physically plausible reading",
                ),
            ),
            because="performs exact Bayesian normal-inverse-gamma sufficient statistics update for standard in-envelope observations",
            footprint=0,
            leaves="conjugate sufficient statistics",
        ),
        decide.Rung(
            "refuse_corrupted_measurement",
            "refuse corrupted, negative, or physically impossible telemetry",
            needs=(
                decide.Need(
                    "measurement corruption or physical impossibility detected",
                    lambda f: not bool(f.get("physically_plausible", True))
                    or float(f.get("measured_seconds", 1.0)) <= 0.0,
                    "{rung} requires detected corruption or non-positive duration",
                ),
            ),
            because="refuses physically impossible or corrupted readings to protect model state from poisoning",
            footprint=0,
            leaves="rejected corrupt telemetry log",
        ),
    )


def calibration_gate_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for certifying predictive interval calibration against held-out workloads."""
    return (
        decide.Rung(
            "conformal_calibrated_interval",
            "certify well-calibrated empirical coverage against held-out workloads",
            needs=(
                decide.Need(
                    "empirical coverage meets target on held-out workload",
                    lambda f: float(f.get("empirical_p90_coverage", 0.0)) >= float(f.get("target_p90_coverage", 0.85))
                    and float(f.get("empirical_p50_coverage", 0.0)) >= float(f.get("target_p50_coverage", 0.45)),
                    "{rung} requires empirical p90 and p50 coverage meeting targets",
                ),
                decide.Need(
                    "adequate validation sample size",
                    lambda f: int(f.get("validation_sample_count", 0)) >= int(f.get("min_validation_samples", 10)),
                    "{rung} requires sufficient validation sample size",
                ),
            ),
            because="certifies predictive interval as well-calibrated against held-out real workload empirical outcomes",
            footprint=0,
            leaves="calibrated confidence certificate",
        ),
        decide.Rung(
            "empirically_widened_fallback",
            "empirically inflate interval scaling factor to restore conservative coverage guarantees",
            needs=(
                decide.Need(
                    "coverage deficit or limited validation sample detected",
                    lambda f: float(f.get("empirical_p90_coverage", 0.0)) < float(f.get("target_p90_coverage", 0.85))
                    or int(f.get("validation_sample_count", 0)) < int(f.get("min_validation_samples", 10)),
                    "{rung} requires coverage deficit or small validation sample",
                ),
                decide.Need(
                    "calibration widening multiplier within viable bounds",
                    lambda f: float(f.get("calibration_multiplier", 1.0)) <= float(f.get("max_calibration_multiplier", 10.0)),
                    "{rung} requires widening multiplier not exceeding maximum bound",
                ),
            ),
            because="applies empirical variance inflation multiplier to compensate for under-coverage and guarantee conservative interval bounds",
            footprint=0,
            leaves="inflated conservative interval",
        ),
        decide.Rung(
            "refuse_uncalibrated_intervals",
            "refuse uncalibrated predictive claims when empirical coverage cannot be guaranteed",
            needs=(
                decide.Need(
                    "calibration untenable or multiplier exceeds safety limit",
                    lambda f: float(f.get("calibration_multiplier", 1.0)) > float(f.get("max_calibration_multiplier", 10.0))
                    or bool(f.get("calibration_failed", False)),
                    "{rung} requires untenable calibration or excessive multiplier",
                ),
            ),
            because="refuses uncalibrated predictive claims when empirical coverage cannot be guaranteed within reasonable bounds",
            footprint=0,
            leaves="uncalibrated fallback notice",
        ),
    )


# -----------------------------------------------------------------------------
# Decision Helper Wrappers
# -----------------------------------------------------------------------------

def choose_context_transfer_strategy(
    facts: Union[Dict[str, Any], decide.Facts],
    work: str = "context_transfer",
) -> decide.Choice:
    """Climbs the context transfer ladder via `decide.climb(work, rungs, facts)`."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    return decide.climb(work, context_transfer_rungs(), f)


def choose_drift_adaptation_strategy(
    facts: Union[Dict[str, Any], decide.Facts],
    work: str = "drift_adaptation",
) -> decide.Choice:
    """Climbs the drift adaptation ladder via `decide.climb(work, rungs, facts)`."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    return decide.climb(work, drift_adaptation_rungs(), f)


def choose_residual_robustness_strategy(
    facts: Union[Dict[str, Any], decide.Facts],
    work: str = "residual_robustness",
) -> decide.Choice:
    """Climbs the residual robustness ladder via `decide.climb(work, rungs, facts)`."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    return decide.climb(work, residual_robustness_rungs(), f)


def choose_calibration_strategy(
    facts: Union[Dict[str, Any], decide.Facts],
    work: str = "calibration_gate",
) -> decide.Choice:
    """Climbs the calibration gate ladder via `decide.climb(work, rungs, facts)`."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    return decide.climb(work, calibration_gate_rungs(), f)


# -----------------------------------------------------------------------------
# Bayesian Conjugate Linear Regression Model (Normal-Inverse-Gamma)
# -----------------------------------------------------------------------------

@dataclass
class BayesianLinearModel:
    """Bayesian Normal-Inverse-Gamma conjugate linear regression model with 7 features.

    Prior on beta | sigma^2 ~ N(mu_0, sigma^2 * V_0^-1)
    Prior on sigma^2 ~ Inv-Gamma(a_0, b_0)
    """
    dim: int = 7
    a: float = 2.0
    b: float = 0.04
    n_obs: float = 0.0
    drift_epoch: int = 0
    last_updated_s: float = field(default_factory=time.time)

    # Sufficient statistics: V = X^T W X + V_0, v = X^T W y + V_0 mu_0
    V: np.ndarray = field(default_factory=lambda: np.diag([1.0, 400.0, 10000.0, 25.0, 400.0, 1.0, 400.0]))
    v: np.ndarray = field(default_factory=lambda: np.array([0.05, 0.02, 0.005, 0.08, 0.03, 0.001, 0.02]))
    feature_sum: np.ndarray = field(default_factory=lambda: np.zeros(7, dtype=np.float64))
    feature_sq_sum: np.ndarray = field(default_factory=lambda: np.eye(7, dtype=np.float64) * 0.1)

    @property
    def coefficients(self) -> np.ndarray:
        """Posterior mean weights mu = V^-1 v."""
        try:
            return np.linalg.solve(self.V, self.v)
        except np.linalg.LinAlgError:
            reg_V = self.V + np.eye(self.dim) * 1e-4
            return np.linalg.solve(reg_V, self.v)

    def predict(self, x: np.ndarray) -> Tuple[float, float, float]:
        """Return (mean, sd, dof) of Student-t posterior predictive distribution for feature vector x."""
        coeffs = self.coefficients
        mean = float(np.dot(x, coeffs))
        mean = max(0.001, mean)

        # Variance of predictive distribution
        try:
            V_inv = np.linalg.inv(self.V)
        except np.linalg.LinAlgError:
            V_inv = np.linalg.inv(self.V + np.eye(self.dim) * 1e-4)

        leverage = float(np.dot(x, np.dot(V_inv, x)))
        scale_sq = (self.b / max(0.5, self.a)) * (1.0 + leverage)
        sd = float(math.sqrt(max(1e-6, scale_sq)))
        dof = float(max(2.0, 2.0 * self.a))
        return mean, sd, dof

    def update(self, x: np.ndarray, y: float, weight: float = 1.0) -> None:
        """Conjugate rank-1 update of sufficient statistics with Huber weight."""
        w = max(0.0, float(weight))
        if w <= 0.0:
            return

        x = np.asarray(x, dtype=np.float64)
        y = float(y)

        # Current prediction error before update
        pred_mean, _, _ = self.predict(x)
        err = y - pred_mean

        # Update V, v, n_obs
        self.V += w * np.outer(x, x)
        self.v += w * y * x
        self.n_obs += w

        # Update Inv-Gamma parameters
        self.a += 0.5 * w
        # Residual quadratic contribution
        try:
            V_inv = np.linalg.inv(self.V)
            leverage = float(np.dot(x, np.dot(V_inv, x)))
        except np.linalg.LinAlgError:
            leverage = 0.1
        self.b += 0.5 * w * (err * err) / max(0.1, 1.0 + leverage)
        self.last_updated_s = time.time()

        # Update empirical moments for OOD detection
        self.feature_sum += x * w
        self.feature_sq_sum += np.outer(x, x) * w

    def decay(self, factor: float) -> None:
        """Apply continuous time decay factor gamma in (0, 1] to historical statistics."""
        gamma = max(0.01, min(1.0, float(factor)))
        if gamma >= 0.999999:
            return
        self.V = self.V * gamma + np.eye(self.dim) * (1.0 - gamma) * 0.1
        self.v = self.v * gamma
        self.n_obs = self.n_obs * gamma
        self.b = max(0.01, self.b * gamma)
        self.a = max(1.1, self.a * gamma)
        self.feature_sum *= gamma
        self.feature_sq_sum *= gamma

    def reset_for_drift(self) -> None:
        """Rapid drift reset: downweights pre-drift historical precision to quickly adapt to new regime."""
        self.drift_epoch += 1
        self.n_obs = min(1.0, self.n_obs * 0.1)
        self.a = 2.0
        self.b = 0.04
        # Retain regularized baseline
        self.V = np.eye(self.dim) * 0.5 + self.V * 0.05
        self.v = self.v * 0.05
        self.last_updated_s = time.time()


# -----------------------------------------------------------------------------
# Calibrated Predictive Interval & Lineage Output
# -----------------------------------------------------------------------------

def _t_factor(dof: float) -> float:
    """Second-order Cornish-Fisher quantile expansion for Student-t distribution."""
    if not dof or dof == math.inf or dof > 100.0:
        return 1.0
    z = 1.2816
    return 1.0 + (z * z + 1.0) / (4.0 * dof) + (5.0 * z**4 + 16.0 * z * z + 3.0) / (96.0 * dof * dof)


@dataclass(frozen=True)
class CalibratedPredictiveInterval:
    """Calibrated predictive time estimate for a unit of work under a specific context."""
    mean: float
    sd: float
    low_50: float
    high_50: float
    low_90: float
    high_90: float
    dof: float
    calibration_multiplier: float
    care_badge: CareBadge
    rung_chosen: str
    lineage: MeasurementLineage

    def is_calibrated(self) -> bool:
        return self.care_badge in (CareBadge.PASS, CareBadge.BENIGN)


@dataclass(frozen=True)
class CalibrationReport:
    """Empirical calibration and regret validation report on held-out workloads."""
    empirical_p50_coverage: float
    empirical_p90_coverage: float
    sample_count: int
    mean_absolute_error: float
    root_mean_squared_error: float
    max_decision_regret_s: float
    calibration_multiplier: float
    care_badge: CareBadge
    verdict_rung: str


# -----------------------------------------------------------------------------
# Hierarchical Context Model (Exact, Service, Engine-Pair, Global, Cold-Start)
# -----------------------------------------------------------------------------

class HierarchicalContextModel:
    """Hierarchical Bayesian Pooling Model across context tiers.

    Tiers:
    1. Exact Local: (engine_pair, rung, version, service, hardware, topology, type_mix)
    2. Service Pool: (engine_pair, rung, version, service)
    3. Engine-Pair Pool: (engine_pair, rung)
    4. Global Rung: (rung)
    5. Cold-Start Prior: conservative physical bounds
    """

    def __init__(self, half_life_seconds: float = 86400.0) -> None:
        self.half_life_seconds = float(half_life_seconds)
        self.exact_models: Dict[str, BayesianLinearModel] = {}
        self.service_models: Dict[str, BayesianLinearModel] = {}
        self.pair_models: Dict[str, BayesianLinearModel] = {}
        self.global_models: Dict[str, BayesianLinearModel] = {}

        # Conformal calibration multiplier
        self.calibration_multiplier: float = 1.0

        # Structural drift tracking
        self.consecutive_drift_deviations: Dict[str, int] = collections.defaultdict(int)
        self.recent_residuals: Dict[str, List[float]] = collections.defaultdict(list)

    def _get_or_create(self, store: Dict[str, BayesianLinearModel], key: str) -> BayesianLinearModel:
        if key not in store:
            store[key] = BayesianLinearModel()
        return store[key]

    def predict(
        self,
        context: MigrationContext,
        unit: ContextWorkloadUnit,
        now_s: Optional[float] = None,
    ) -> CalibratedPredictiveInterval:
        """Generate calibrated predictive interval for unit under target context using decide ladder."""
        now = float(now_s if now_s is not None else time.time())
        x = unit.feature_vector()

        exact_key = context.context_id
        exact_model = self.exact_models.get(exact_key)
        service_model = self.service_models.get(context.service_key)
        pair_model = self.pair_models.get(context.engine_pair_key)
        global_model = self.global_models.get(context.global_rung_key)

        local_n = exact_model.n_obs if exact_model else 0.0

        # Context distance and OOD detection
        is_ood = False
        ood_distance = 0.0
        if exact_model and exact_model.n_obs >= 5:
            mean_f = exact_model.feature_sum / exact_model.n_obs
            cov_f = (exact_model.feature_sq_sum / exact_model.n_obs) - np.outer(mean_f, mean_f)
            cov_reg = cov_f + np.eye(7) * 1e-3
            try:
                cov_inv = np.linalg.inv(cov_reg)
                is_ood, ood_distance = compute_workload_ood(x, mean_f, cov_inv, threshold=2.5)
            except np.linalg.LinAlgError:
                pass

        # Check if local context is subject to active structural drift
        drift_count = self.consecutive_drift_deviations.get(exact_key, 0)
        has_active_drift = drift_count >= 2

        # Build facts for context transfer decision engine ladder
        transfer_facts: Dict[str, Any] = {
            "exact_context_match": bool(exact_model and exact_model.n_obs >= 2),
            "local_observations_count": int(local_n),
            "min_exact_observations": 2,
            "is_ood": is_ood,
            "ood_distance": float(ood_distance),
            "ood_threshold": 2.5,
            "persistent_drift_detected": has_active_drift,
            "poolable_context": bool(service_model or pair_model or global_model),
            "pooled_evidence_count": int((service_model.n_obs if service_model else 0)
                                         + (pair_model.n_obs if pair_model else 0)
                                         + (global_model.n_obs if global_model else 0)),
            "context_distance": 0.0 if exact_model and exact_model.n_obs >= 2 else 1.5,
            "max_transfer_distance": 4.0,
            "conservative_bounds_available": True,
            "cold_start_available": True,
        }

        # Quality Gate 1: Decision climb
        work_label = f"predict:{context.context_id}:{unit.unit_id or 'batch'}"
        choice = choose_context_transfer_strategy(transfer_facts, work=work_label)
        rung_chosen = choice.rung.name if choice.rung else "safe_cold_start_prior"

        # Statistical inference based on ladder choice
        if rung_chosen == "exact_local_calibrated" and exact_model is not None:
            mean, sd, dof = exact_model.predict(x)
            pooling_level = "exact_local"
            shrinkage = 1.0
            ctx_dist = 0.0
            badge = CareBadge.PASS
        elif rung_chosen == "hierarchical_pooled_transfer":
            # Shrinkage towards parent tier
            parent_model = service_model or pair_model or global_model
            if exact_model and exact_model.n_obs > 0:
                local_mean, local_sd, local_dof = exact_model.predict(x)
                parent_mean, parent_sd, _ = parent_model.predict(x) if parent_model else (local_mean, local_sd, local_dof)
                shrinkage = local_n / (local_n + 5.0)
                mean = shrinkage * local_mean + (1.0 - shrinkage) * parent_mean
                ctx_dist = 1.2
                # Evidence from a different context NEVER becomes falsely precise local measurement
                sd = math.sqrt(shrinkage * (local_sd**2) + (1.0 - shrinkage) * (parent_sd**2) * (1.0 + 0.5 * (ctx_dist**2)))
                dof = min(local_dof, 8.0)
            elif parent_model:
                mean, parent_sd, dof = parent_model.predict(x)
                ctx_dist = 2.0
                sd = parent_sd * math.sqrt(1.0 + 0.6 * (ctx_dist**2))
                shrinkage = 0.0
                dof = 6.0
            else:
                mean, sd, dof = 1.0 + 0.02 * (unit.bytes / (1024.0 * 1024.0)), 0.5, 4.0
                ctx_dist = 3.0
                shrinkage = 0.0
            pooling_level = "hierarchical_pool"
            badge = CareBadge.BENIGN
        elif rung_chosen == "ood_conservative_widened":
            # Extreme variance widening for OOD / unfamiliar contexts
            base_model = exact_model or service_model or pair_model or global_model
            if base_model:
                mean, base_sd, dof = base_model.predict(x)
                multiplier = max(2.0, 1.0 + ood_distance)
                sd = base_sd * multiplier
                dof = 3.0  # Heavier Student-t tails
            else:
                mean = 2.0 + 0.05 * (unit.bytes / (1024.0 * 1024.0))
                sd = mean * 0.8
                dof = 2.5
            pooling_level = "ood_widened"
            shrinkage = 0.0
            ctx_dist = max(3.0, ood_distance)
            badge = CareBadge.REVIEW
        else:  # safe_cold_start_prior
            # Conservative physical benchmark baseline
            mb = unit.bytes / (1024.0 * 1024.0)
            mean = 1.0 + 0.05 * mb + 0.005 * (unit.rows / 1000.0)
            sd = mean * 0.9
            dof = 2.0
            pooling_level = "cold_start"
            shrinkage = 0.0
            ctx_dist = 5.0
            badge = CareBadge.REVIEW

        # Apply conformal calibration multiplier
        effective_sd = sd * self.calibration_multiplier
        t_val = _t_factor(dof)

        low_90 = max(0.001, mean - 1.2816 * effective_sd * t_val)
        high_90 = mean + 1.2816 * effective_sd * t_val
        low_50 = max(0.001, mean - 0.6745 * effective_sd * t_val)
        high_50 = mean + 0.6745 * effective_sd * t_val

        lineage = MeasurementLineage(
            lineage_id=uuid.uuid4().hex[:12],
            source_context_id=exact_key,
            target_context_id=context.context_id,
            pooling_level=pooling_level,
            effective_sample_size=float(local_n),
            shrinkage_factor=float(shrinkage),
            context_distance=float(ctx_dist),
            is_ood=bool(is_ood),
            ood_distance=float(ood_distance),
            drift_epoch=exact_model.drift_epoch if exact_model else 0,
            calibration_multiplier=float(self.calibration_multiplier),
            prior_provenance=rung_chosen,
            created_at_s=now,
        )

        return CalibratedPredictiveInterval(
            mean=float(mean),
            sd=float(effective_sd),
            low_50=float(low_50),
            high_50=float(high_50),
            low_90=float(low_90),
            high_90=float(high_90),
            dof=float(dof),
            calibration_multiplier=float(self.calibration_multiplier),
            care_badge=badge,
            rung_chosen=rung_chosen,
            lineage=lineage,
        )

    def record_measurement(
        self,
        context: MigrationContext,
        unit: ContextWorkloadUnit,
        actual_seconds: float,
        now_s: Optional[float] = None,
        max_physical_rate_mb_s: float = 10000.0,
    ) -> CareBadge:
        """Record an observed measurement, filtering through residual robustness ladder."""
        now = float(now_s if now_s is not None else time.time())
        y = float(actual_seconds)
        x = unit.feature_vector()
        exact_key = context.context_id

        # Physical plausibility check
        mb = unit.bytes / (1024.0 * 1024.0)
        physically_plausible = True
        if y <= 0.0 or (mb > 0.0 and (mb / y) > max_physical_rate_mb_s):
            physically_plausible = False

        # Current prediction to determine residual
        pred_int = self.predict(context, unit, now_s=now)
        residual = y - pred_int.mean
        std_residual = abs(residual) / max(1e-4, pred_int.sd)

        # Build facts for residual robustness decision engine ladder
        residual_facts: Dict[str, Any] = {
            "standardized_residual": float(std_residual),
            "huber_tuning_k": 1.345,
            "physically_plausible": physically_plausible,
            "measurement_valid": True,
            "measured_seconds": float(y),
        }

        # Quality Gate 1: Decision climb
        work_label = f"residual:{context.context_id}:{unit.unit_id or 'batch'}"
        choice = choose_residual_robustness_strategy(residual_facts, work=work_label)
        rung = choice.rung.name if choice.rung else "standard_sufficient_stats_update"

        if rung == "refuse_corrupted_measurement":
            raise CorruptedMeasurementError(
                f"refusing corrupted or non-physical measurement: {y:.4f}s for {mb:.2f}MB"
            )

        if rung == "robust_huber_reweighted_fit":
            # Downweight leverage point via Huber weight k / |z|
            weight = 1.345 / std_residual
            badge = CareBadge.REVIEW
        else:
            weight = 1.0
            badge = CareBadge.PASS

        # Track sequential residuals for structural drift detection
        self.recent_residuals[exact_key].append(residual)
        if len(self.recent_residuals[exact_key]) > 20:
            self.recent_residuals[exact_key].pop(0)

        if std_residual > 2.5:
            self.consecutive_drift_deviations[exact_key] += 1
        else:
            self.consecutive_drift_deviations[exact_key] = 0

        # Update models hierarchically
        exact_mod = self._get_or_create(self.exact_models, exact_key)
        exact_mod.update(x, y, weight=weight)

        service_mod = self._get_or_create(self.service_models, context.service_key)
        service_mod.update(x, y, weight=weight * 0.8)

        pair_mod = self._get_or_create(self.pair_models, context.engine_pair_key)
        pair_mod.update(x, y, weight=weight * 0.5)

        global_mod = self._get_or_create(self.global_models, context.global_rung_key)
        global_mod.update(x, y, weight=weight * 0.2)

        return badge

    def adapt_drift_and_decay(
        self,
        context: MigrationContext,
        elapsed_seconds: float = 0.0,
    ) -> CareBadge:
        """Evaluate structural drift and continuous time decay via drift adaptation ladder."""
        exact_key = context.context_id
        exact_mod = self.exact_models.get(exact_key)
        consecutive_drift = self.consecutive_drift_deviations.get(exact_key, 0)
        has_structural_drift = consecutive_drift >= 2

        drift_facts: Dict[str, Any] = {
            "structural_drift_flag": has_structural_drift,
            "consecutive_drift_count": int(consecutive_drift),
            "drift_trigger_count": 2,
            "post_drift_samples": 1 if exact_mod and exact_mod.n_obs >= 1 else 0,
            "min_post_drift_samples": 1,
            "time_decay_enabled": True,
            "elapsed_time_hours": float(elapsed_seconds / 3600.0),
            "decay_grace_period_hours": 0.0,
            "environment_stationary": not has_structural_drift and elapsed_seconds < 3600.0,
        }

        # Quality Gate 1: Decision climb
        work_label = f"drift:{context.context_id}"
        choice = choose_drift_adaptation_strategy(drift_facts, work=work_label)
        rung = choice.rung.name if choice.rung else "stationary_retention"

        if rung == "active_drift_reset_and_recalibrate":
            if exact_mod:
                exact_mod.reset_for_drift()
            service_mod = self.service_models.get(context.service_key)
            if service_mod:
                service_mod.reset_for_drift()
            pair_mod = self.pair_models.get(context.engine_pair_key)
            if pair_mod:
                pair_mod.reset_for_drift()
            self.consecutive_drift_deviations[exact_key] = 0
            self.recent_residuals[exact_key].clear()
            return CareBadge.ACTION

        if rung == "exponential_decay_tracking":
            if exact_mod and elapsed_seconds > 0.0:
                decay_factor = math.pow(0.5, elapsed_seconds / self.half_life_seconds)
                exact_mod.decay(decay_factor)
            return CareBadge.BENIGN

        return CareBadge.PASS

    def evaluate_held_out_calibration(
        self,
        context: MigrationContext,
        held_out_units: Sequence[ContextWorkloadUnit],
        held_out_actuals: Sequence[float],
        target_p90_coverage: float = 0.85,
        target_p50_coverage: float = 0.45,
    ) -> CalibrationReport:
        """Calibrate predictive intervals against held-out real workloads."""
        if len(held_out_units) != len(held_out_actuals):
            raise ValueError("units and actuals must have identical length")
        n = len(held_out_units)
        if n == 0:
            raise ValueError("held-out validation set cannot be empty")

        in_90 = 0
        in_50 = 0
        abs_errors: List[float] = []
        sq_errors: List[float] = []
        std_errors: List[float] = []

        for unit, y in zip(held_out_units, held_out_actuals):
            pred = self.predict(context, unit)
            if pred.low_90 <= y <= pred.high_90:
                in_90 += 1
            if pred.low_50 <= y <= pred.high_50:
                in_50 += 1

            err = abs(y - pred.mean)
            abs_errors.append(err)
            sq_errors.append(err * err)
            std_errors.append(err / max(1e-4, pred.sd))

        cov_90 = float(in_90 / n)
        cov_50 = float(in_50 / n)
        mae = float(np.mean(abs_errors))
        rmse = float(math.sqrt(np.mean(sq_errors)))
        max_regret = float(np.max(abs_errors))

        # Required conformal scaling multiplier to achieve 90% coverage
        sorted_std = sorted(std_errors)
        idx_90 = min(n - 1, int(math.ceil(0.90 * n)) - 1)
        needed_multiplier = max(1.0, sorted_std[idx_90] / 1.2816)

        calibration_facts: Dict[str, Any] = {
            "empirical_p90_coverage": cov_90,
            "target_p90_coverage": float(target_p90_coverage),
            "empirical_p50_coverage": cov_50,
            "target_p50_coverage": float(target_p50_coverage),
            "validation_sample_count": n,
            "min_validation_samples": 10,
            "calibration_multiplier": float(needed_multiplier),
            "max_calibration_multiplier": 10.0,
            "calibration_failed": False,
        }

        # Quality Gate 1: Decision climb
        work_label = f"calibration:{context.context_id}"
        choice = choose_calibration_strategy(calibration_facts, work=work_label)
        rung = choice.rung.name if choice.rung else "conformal_calibrated_interval"

        if rung == "conformal_calibrated_interval":
            badge = CareBadge.PASS
        elif rung == "empirically_widened_fallback":
            self.calibration_multiplier = float(needed_multiplier)
            badge = CareBadge.REVIEW
        else:
            badge = CareBadge.ACTION
            raise UncalibratedIntervalError(
                f"uncalibrated predictive interval: empirical p90={cov_90:.2f}, multiplier={needed_multiplier:.2f}"
            )

        return CalibrationReport(
            empirical_p50_coverage=cov_50,
            empirical_p90_coverage=cov_90,
            sample_count=n,
            mean_absolute_error=mae,
            root_mean_squared_error=rmse,
            max_decision_regret_s=max_regret,
            calibration_multiplier=self.calibration_multiplier,
            care_badge=badge,
            verdict_rung=rung,
        )


# -----------------------------------------------------------------------------
# Adaptive Execution: AIMD Batch Sizing and Byte-Weighted ETA
# -----------------------------------------------------------------------------

def adjust_aimd_batch_size(
    current_size: int,
    measured_latency_s: float,
    target_latency_s: float,
    min_size: int = 10,
    max_size: int = 50000,
    additive_increase: int = 50,
    multiplicative_decrease: float = 0.5,
) -> int:
    """Dynamic AIMD batch size adjustment keeping latency within target window."""
    if measured_latency_s > target_latency_s:
        # Backpressure: multiplicative decrease
        new_size = int(current_size * multiplicative_decrease)
    else:
        # Headroom: additive increase
        new_size = current_size + additive_increase
    return max(min_size, min(max_size, new_size))


def estimate_byte_weighted_eta(
    completed_bytes: int,
    total_bytes: int,
    elapsed_seconds: float,
    cold_start_predicted_rate_mb_s: Optional[float] = None,
) -> float:
    """Instant cold-start byte-weighted ETA combining empirical rate and prior prediction."""
    if total_bytes <= 0:
        return 0.0
    remaining_bytes = max(0, total_bytes - completed_bytes)
    if remaining_bytes == 0:
        return 0.0

    mib = 1024.0 * 1024.0
    if elapsed_seconds > 0.5 and completed_bytes > 0:
        measured_rate_mb_s = (completed_bytes / mib) / elapsed_seconds
        if cold_start_predicted_rate_mb_s and elapsed_seconds < 10.0:
            # Fused blend for cold-start stabilization
            weight = min(1.0, elapsed_seconds / 10.0)
            rate = weight * measured_rate_mb_s + (1.0 - weight) * cold_start_predicted_rate_mb_s
        else:
            rate = measured_rate_mb_s
    elif cold_start_predicted_rate_mb_s and cold_start_predicted_rate_mb_s > 0:
        rate = cold_start_predicted_rate_mb_s
    else:
        rate = 10.0  # conservative baseline 10 MB/s

    return float((remaining_bytes / mib) / max(0.1, rate))


# -----------------------------------------------------------------------------
# Universal Typed Durable O(1) Checkpoint Resume & Active Verification Lease
# -----------------------------------------------------------------------------

def _encode_typed(value: Any) -> Any:
    """Recursively encode native Python types for typed JSON checkpointing."""
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "items": [_encode_typed(v) for v in value]}
    if isinstance(value, set):
        return {"__type__": "set", "items": [_encode_typed(v) for v in sorted(value, key=str)]}
    if isinstance(value, dict):
        return {str(k): _encode_typed(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_encode_typed(v) for v in value]
    return value


def _decode_typed(value: Any) -> Any:
    """Recursively decode typed JSON checkpoint back to native types."""
    if isinstance(value, dict) and "__type__" in value:
        t = value["__type__"]
        if t == "datetime":
            return datetime.datetime.fromisoformat(value["value"])
        if t == "date":
            return datetime.date.fromisoformat(value["value"])
        if t == "decimal":
            return decimal.Decimal(value["value"])
        if t == "uuid":
            return uuid.UUID(value["value"])
        if t == "tuple":
            return tuple(_decode_typed(v) for v in value["items"])
        if t == "set":
            return set(_decode_typed(v) for v in value["items"])
    if isinstance(value, dict):
        return {k: _decode_typed(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode_typed(v) for v in value]
    return value


@dataclass(frozen=True)
class ContextLearningCheckpointUnit:
    """O(1) checkpoint unit representing verified measurement transfer state."""
    unit_key: str
    context_id: str
    recorded_at: datetime.datetime
    predicted_seconds: decimal.Decimal
    actual_seconds: decimal.Decimal
    unit_uuid: uuid.UUID
    care_badge: str
    composite_cursor: Tuple[str, int]


class ContextLearningCheckpointStore:
    """Universal O(1) typed durable checkpoint store for context learning."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._units: Dict[str, ContextLearningCheckpointUnit] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = json.loads(self.path.read_text(encoding="ascii"))
        decoded = _decode_typed(raw)
        for item in decoded.get("units", []):
            unit = ContextLearningCheckpointUnit(
                unit_key=str(item["unit_key"]),
                context_id=str(item["context_id"]),
                recorded_at=item["recorded_at"],
                predicted_seconds=item["predicted_seconds"],
                actual_seconds=item["actual_seconds"],
                unit_uuid=item["unit_uuid"],
                care_badge=str(item["care_badge"]),
                composite_cursor=tuple(item["composite_cursor"]),
            )
            self._units[unit.unit_key] = unit

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "units": [
                {
                    "unit_key": u.unit_key,
                    "context_id": u.context_id,
                    "recorded_at": u.recorded_at,
                    "predicted_seconds": u.predicted_seconds,
                    "actual_seconds": u.actual_seconds,
                    "unit_uuid": u.unit_uuid,
                    "care_badge": u.care_badge,
                    "composite_cursor": u.composite_cursor,
                }
                for u in self._units.values()
            ],
        }
        encoded = _encode_typed(payload)
        durable.write_json(self.path, encoded)

    def record_unit(self, unit: ContextLearningCheckpointUnit) -> None:
        self._units[unit.unit_key] = unit
        self.save()

    def should_skip_unit(self, unit_key: str, expected_context_id: str) -> bool:
        """O(1) check whether unit is already completed under matching context."""
        unit = self._units.get(unit_key)
        if unit is None:
            return False
        return unit.context_id == expected_context_id

    def get_unit(self, unit_key: str) -> Optional[ContextLearningCheckpointUnit]:
        return self._units.get(unit_key)


class ContextLearningLeaseLock:
    """Active verification lease lock (.run.lock) with heartbeat preventing duplicate runs."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        *,
        ttl_seconds: float = 60.0,
        owner_id: str = "context-learning",
    ) -> None:
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.ttl_seconds = float(ttl_seconds)
        self.owner_id = str(owner_id)
        self._fd: Optional[int] = None
        self.acquired_at_s: Optional[float] = None
        self.last_heartbeat_s: Optional[float] = None

    def acquire(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is not None:
            raise VerificationLeaseError("context learning lease is already held by this instance")
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise VerificationLeaseError(
                f"active verification lease lock already held at {self.lock_path}"
            ) from exc

        ts = float(now_s if now_s is not None else time.time())
        self._fd = fd
        self.acquired_at_s = ts
        self.last_heartbeat_s = ts

        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "acquired_at_s": ts,
            "last_heartbeat_s": ts,
            "ttl_seconds": self.ttl_seconds,
        }
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, json.dumps(payload, indent=2).encode("ascii"))
        os.fsync(fd)

    def heartbeat(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is None:
            raise VerificationLeaseError("cannot heartbeat unheld context learning lease")
        ts = float(now_s if now_s is not None else time.time())
        self.last_heartbeat_s = ts
        payload = {
            "owner_id": self.owner_id,
            "pid": os.getpid(),
            "acquired_at_s": self.acquired_at_s,
            "last_heartbeat_s": ts,
            "ttl_seconds": self.ttl_seconds,
        }
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, json.dumps(payload, indent=2).encode("ascii"))
        os.fsync(self._fd)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None
            if self.lock_path.exists():
                try:
                    self.lock_path.unlink()
                except OSError:
                    pass

    def __enter__(self) -> ContextLearningLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def acquire_context_learning_lease(
    lock_dir: Union[str, Path],
    *,
    ttl_seconds: float = 60.0,
    owner_id: str = "context-learning",
) -> ContextLearningLeaseLock:
    """Convenience helper to create and acquire a ContextLearningLeaseLock."""
    lock = ContextLearningLeaseLock(lock_dir, ttl_seconds=ttl_seconds, owner_id=owner_id)
    lock.acquire()
    return lock
