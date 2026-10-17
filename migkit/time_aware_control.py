"""Plan around expiring resources and changing windows (Row 602: SMART-N19.time-aware-control).

Extends jointplan, catch-up/session forecasts and credential renewal using `decide.Rung`
and `decide.climb(work, rungs, facts)`.

Key mechanisms:
1. Maintenance Windows & Blackouts:
   - Models allowed execution windows, blackout intervals, and window close horizons.
   - Strictly enforces window boundaries: missed windows trigger a fresh admissible plan
     without guessing switch consent outside maintenance windows.
2. Traffic Cycles & Diurnal Load:
   - Models cyclical traffic variations (peak vs. off-peak hours, diurnal multipliers).
   - Dynamically throttles or schedules heavy staging during observed off-peak dips.
3. Burst Credits & Quota Reset:
   - Models cloud/storage burst credit accumulation and depletion dynamics (e.g., EBS IOPS).
   - Detects burst exhaustion and paces throughput down to baseline refill rates.
   - Aligns phase scheduling with cloud API and egress quota reset horizons.
4. Token & Certificate Lifetime (Lease Management):
   - Models ephemeral auth tokens, mTLS certificates, and database lease expiration.
   - Enforces zero optimistic execution beyond nonrenewable leases: hard failure if phase
     p95 tail breaches nonrenewable expiry.
   - Schedules proactive prerequisite renewals with lead-time uncertainty buffers for
     renewable leases.
5. Log Retention & Snapshot Expiry:
   - Models source WAL/binlog retention horizons vs. CDC replication lag.
   - Enforces hard stop if projected replication lag breaches retention horizons (truncation risk).
   - Monitors consistent snapshot TTL and schedules snapshot refreshes or rejects execution.
6. Cutover Slack & Emergency Abort Leeway:
   - Budgets drain time, lock acquisition, final verification, and emergency abort leeway.
   - Rejects cutover attempt if remaining window is smaller than required slack margin.
7. Universal migkit resilient operations:
   - Pure UNIX style, 100% ASCII, zero emojis/decorative glyphs.
   - 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
   - Universal O(1) typed durable checkpoint resume with native Python types.
   - Active mutual-exclusion lease lock (.run.lock) with heartbeat.
   - Adaptive AIMD window pacing and cold-start byte-weighted ETA.
   - Safe test endpoint validation (127.0.0.1, localhost, 10.0.0.x).
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import math
import pathlib
import time
import uuid
from dataclasses import dataclass, field
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

from . import checkpoint, decide, lease

# -----------------------------------------------------------------------------
# Care Badges & Safe Host Validation
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


class WindowState(str, enum.Enum):
    ACTIVE = "ACTIVE"
    UPCOMING = "UPCOMING"
    CLOSED = "CLOSED"
    BLACKOUT = "BLACKOUT"
    MISSED = "MISSED"


class ResourceLeaseType(str, enum.Enum):
    TOKEN = "token"
    CERTIFICATE = "certificate"
    DATABASE_LEASE = "database_lease"
    STORAGE_LOCK = "storage_lock"


SAFE_LOOPBACK_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class TimeAwareControlError(RuntimeError):
    """Base exception for time-aware control operations."""


class UnsafeEndpointHostError(TimeAwareControlError, ValueError):
    """Raised when an endpoint host is outside approved loopback or private ranges."""


class NonrenewableLeaseExpiredError(TimeAwareControlError):
    """Raised when a phase starts or runs beyond a nonrenewable lease expiration."""


class LogRetentionTruncationError(TimeAwareControlError):
    """Raised when replication lag or duration breaches source log retention."""


class SnapshotExpiredError(TimeAwareControlError):
    """Raised when consistent read snapshot TTL expires before phase completion."""


class MissedWindowConsentError(TimeAwareControlError):
    """Raised when a window is missed and switch consent outside window is absent."""


class InvalidTimeAwareFactError(TimeAwareControlError, ValueError):
    """Raised when temporal or quota facts violate mathematical or domain bounds."""


def validate_safe_endpoint(host: str) -> None:
    """Validate that endpoint host is strictly loopback (127.0.0.1) or safe private (10.0.0.x)."""
    clean = (host or "").strip().lower()
    if not clean:
        raise UnsafeEndpointHostError("Endpoint host cannot be empty")
    if clean in SAFE_LOOPBACK_HOSTS:
        return
    if clean.startswith("127."):
        return
    if clean.startswith("10.0.0."):
        return
    raise UnsafeEndpointHostError(
        f"Host '{host}' is outside approved safe test ranges (127.0.0.1, localhost, 10.0.0.x)"
    )


# -----------------------------------------------------------------------------
# Data Models: Temporal Windows, Traffic Cycles, Burst Credits & Quotas
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class TimeWindow:
    """Allowed or blackout execution window."""

    window_id: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    is_blackout: bool = False
    cutover_permitted: bool = True
    max_bandwidth_bytes_sec: float = 100_000_000.0

    def __post_init__(self) -> None:
        if not self.window_id:
            raise InvalidTimeAwareFactError("window_id cannot be empty")
        if self.end_time <= self.start_time:
            raise InvalidTimeAwareFactError("end_time must be strictly after start_time")
        if self.max_bandwidth_bytes_sec <= 0.0:
            raise InvalidTimeAwareFactError("max_bandwidth_bytes_sec must be > 0")

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def is_active(self, now: datetime.datetime) -> bool:
        return self.start_time <= now < self.end_time

    def remaining_seconds(self, now: datetime.datetime) -> float:
        if now >= self.end_time:
            return 0.0
        if now < self.start_time:
            return self.duration_seconds
        return (self.end_time - now).total_seconds()

    def seconds_until_start(self, now: datetime.datetime) -> float:
        if now >= self.start_time:
            return 0.0
        return (self.start_time - now).total_seconds()

    def is_missed(self, now: datetime.datetime, required_slack_sec: float = 0.0) -> bool:
        if required_slack_sec < 0.0:
            raise InvalidTimeAwareFactError("required_slack_sec cannot be negative")
        return (now >= self.end_time) or (self.remaining_seconds(now) < required_slack_sec)


@dataclass(frozen=True)
class TrafficCycle:
    """Diurnal or periodic traffic load profile."""

    cycle_period_seconds: float = 86400.0
    peak_hours: Tuple[int, ...] = (9, 10, 11, 14, 15, 16, 17)
    offpeak_hours: Tuple[int, ...] = (0, 1, 2, 3, 4, 5)
    peak_load_factor: float = 2.5
    offpeak_load_factor: float = 0.3
    baseline_bandwidth_bytes_sec: float = 50_000_000.0

    def __post_init__(self) -> None:
        if self.cycle_period_seconds <= 0.0:
            raise InvalidTimeAwareFactError("cycle_period_seconds must be > 0")
        if self.peak_load_factor <= 0.0 or self.offpeak_load_factor <= 0.0:
            raise InvalidTimeAwareFactError("Load factors must be > 0")
        if self.offpeak_load_factor >= self.peak_load_factor:
            raise InvalidTimeAwareFactError("offpeak_load_factor must be < peak_load_factor")
        if self.baseline_bandwidth_bytes_sec <= 0.0:
            raise InvalidTimeAwareFactError("baseline_bandwidth_bytes_sec must be > 0")

    def is_peak(self, dt: datetime.datetime) -> bool:
        return dt.hour in self.peak_hours

    def is_offpeak(self, dt: datetime.datetime) -> bool:
        return dt.hour in self.offpeak_hours

    def load_factor_at(self, dt: datetime.datetime) -> float:
        if self.is_peak(dt):
            return self.peak_load_factor
        if self.is_offpeak(dt):
            return self.offpeak_load_factor
        return 1.0

    def effective_bandwidth_bytes_sec(self, dt: datetime.datetime) -> float:
        factor = self.load_factor_at(dt)
        # Higher production load leaves less available migration bandwidth
        available_share = max(0.10, 1.0 / factor)
        return self.baseline_bandwidth_bytes_sec * available_share

    def next_offpeak_window(
        self, now: datetime.datetime
    ) -> Tuple[datetime.datetime, datetime.datetime]:
        """Compute the next off-peak interval [start, end]."""
        # Find the earliest offpeak hour at or after now
        current_day = now.date()
        sorted_offpeak = sorted(self.offpeak_hours)
        min_hour = sorted_offpeak[0]
        max_hour = sorted_offpeak[-1]

        if now.hour < min_hour:
            start_dt = datetime.datetime.combine(
                current_day, datetime.time(min_hour, 0), tzinfo=now.tzinfo
            )
            end_dt = datetime.datetime.combine(
                current_day, datetime.time(max_hour + 1, 0), tzinfo=now.tzinfo
            )
            return start_dt, end_dt

        if min_hour <= now.hour <= max_hour:
            start_dt = now
            end_dt = datetime.datetime.combine(
                current_day, datetime.time(max_hour + 1, 0), tzinfo=now.tzinfo
            )
            return start_dt, end_dt

        # Roll over to next day
        next_day = current_day + datetime.timedelta(days=1)
        start_dt = datetime.datetime.combine(
            next_day, datetime.time(min_hour, 0), tzinfo=now.tzinfo
        )
        end_dt = datetime.datetime.combine(
            next_day, datetime.time(max_hour + 1, 0), tzinfo=now.tzinfo
        )
        return start_dt, end_dt


@dataclass(frozen=True)
class BurstCreditBucket:
    """Cloud / storage burst credit model (e.g. AWS EBS IOPS burst bucket)."""

    bucket_name: str
    current_credits: float
    max_credits: float
    refill_rate_per_sec: float
    burst_consumption_per_sec: float
    baseline_consumption_per_sec: float

    def __post_init__(self) -> None:
        if not self.bucket_name:
            raise InvalidTimeAwareFactError("bucket_name cannot be empty")
        if self.max_credits <= 0.0:
            raise InvalidTimeAwareFactError("max_credits must be > 0")
        if not (0.0 <= self.current_credits <= self.max_credits):
            raise InvalidTimeAwareFactError("current_credits must be in [0, max_credits]")
        if self.refill_rate_per_sec < 0.0:
            raise InvalidTimeAwareFactError("refill_rate_per_sec must be >= 0")
        if self.burst_consumption_per_sec <= self.baseline_consumption_per_sec:
            raise InvalidTimeAwareFactError(
                "burst_consumption_per_sec must be > baseline_consumption_per_sec"
            )

    @property
    def is_depleted(self) -> bool:
        return self.current_credits <= 0.0

    @property
    def credit_fill_ratio(self) -> float:
        return self.current_credits / self.max_credits

    def time_until_depleted(self) -> float:
        net_drain = self.burst_consumption_per_sec - self.refill_rate_per_sec
        if net_drain <= 0.0:
            return float("inf")
        return self.current_credits / net_drain

    def time_until_full(self) -> float:
        if self.refill_rate_per_sec <= 0.0:
            return float("inf")
        needed = self.max_credits - self.current_credits
        return needed / self.refill_rate_per_sec

    def simulate_burn(
        self, duration_sec: float, requested_rate: float
    ) -> Tuple[float, float]:
        """Simulate credit burn over a duration; returns (effective_rate, credits_remaining)."""
        if duration_sec <= 0.0:
            return requested_rate, self.current_credits

        net_drain = requested_rate - self.refill_rate_per_sec
        if net_drain <= 0.0:
            # Refilling faster than drain
            credits_remaining = min(
                self.max_credits, self.current_credits + (-net_drain * duration_sec)
            )
            return requested_rate, credits_remaining

        depletion_time = self.current_credits / net_drain
        if duration_sec <= depletion_time:
            # Full duration supported at requested rate
            credits_remaining = self.current_credits - (net_drain * duration_sec)
            return requested_rate, credits_remaining

        # Burst depleted mid-way; pace down to baseline refill
        burst_portion = depletion_time / duration_sec
        baseline_portion = 1.0 - burst_portion
        effective_rate = (
            burst_portion * requested_rate
            + baseline_portion * self.baseline_consumption_per_sec
        )
        return effective_rate, 0.0


@dataclass(frozen=True)
class QuotaResetWindow:
    """API request or egress quota window with reset horizon."""

    quota_name: str
    remaining_units: float
    total_limit: float
    reset_horizon_seconds: float
    cost_per_phase_unit: float = 1.0

    def __post_init__(self) -> None:
        if not self.quota_name:
            raise InvalidTimeAwareFactError("quota_name cannot be empty")
        if self.total_limit <= 0.0:
            raise InvalidTimeAwareFactError("total_limit must be > 0")
        if not (0.0 <= self.remaining_units <= self.total_limit):
            raise InvalidTimeAwareFactError("remaining_units must be in [0, total_limit]")
        if self.reset_horizon_seconds < 0.0:
            raise InvalidTimeAwareFactError("reset_horizon_seconds must be >= 0")
        if self.cost_per_phase_unit <= 0.0:
            raise InvalidTimeAwareFactError("cost_per_phase_unit must be > 0")

    def can_accommodate(self, needed_units: float) -> bool:
        if needed_units < 0.0:
            raise InvalidTimeAwareFactError("needed_units cannot be negative")
        return self.remaining_units >= needed_units


@dataclass(frozen=True)
class ResourceLease:
    """Ephemeral authentication token, mTLS certificate, or database lock lease."""

    resource_id: str
    resource_type: ResourceLeaseType
    valid_until: datetime.datetime
    renewable: bool
    renewal_lead_time_seconds: float = 300.0
    max_renewals: int = 0
    renewals_performed: int = 0
    renewal_cost_seconds: float = 2.0

    def __post_init__(self) -> None:
        if not self.resource_id:
            raise InvalidTimeAwareFactError("resource_id cannot be empty")
        if self.renewal_lead_time_seconds < 0.0:
            raise InvalidTimeAwareFactError("renewal_lead_time_seconds must be >= 0")
        if self.max_renewals < 0 or self.renewals_performed < 0:
            raise InvalidTimeAwareFactError("Renewal counts must be >= 0")
        if self.renewal_cost_seconds < 0.0:
            raise InvalidTimeAwareFactError("renewal_cost_seconds must be >= 0")

    def remaining_lifetime_seconds(self, now: datetime.datetime) -> float:
        if now >= self.valid_until:
            return 0.0
        return (self.valid_until - now).total_seconds()

    def is_expired(self, now: datetime.datetime) -> bool:
        return now >= self.valid_until

    def can_renew(self) -> bool:
        return self.renewable and (self.renewals_performed < self.max_renewals)

    def needs_renewal(self, now: datetime.datetime, buffer_sec: float = 0.0) -> bool:
        threshold = self.renewal_lead_time_seconds + buffer_sec
        return self.can_renew() and (self.remaining_lifetime_seconds(now) <= threshold)

    def effective_horizon_seconds(
        self, now: datetime.datetime, single_renewal_extension_sec: float = 3600.0
    ) -> float:
        base = self.remaining_lifetime_seconds(now)
        if not self.can_renew():
            return base
        remaining_renewals = self.max_renewals - self.renewals_performed
        return base + (remaining_renewals * single_renewal_extension_sec)


@dataclass(frozen=True)
class LogRetentionWindow:
    """Database WAL, binlog, or replication slot retention limits."""

    source_engine: str
    retention_horizon_seconds: float
    max_log_bytes: int
    current_lag_bytes: int
    current_lag_seconds: float
    log_generation_rate_bytes_sec: float

    def __post_init__(self) -> None:
        if not self.source_engine:
            raise InvalidTimeAwareFactError("source_engine cannot be empty")
        if self.retention_horizon_seconds <= 0.0:
            raise InvalidTimeAwareFactError("retention_horizon_seconds must be > 0")
        if self.max_log_bytes <= 0:
            raise InvalidTimeAwareFactError("max_log_bytes must be > 0")
        if self.current_lag_bytes < 0 or self.current_lag_seconds < 0.0:
            raise InvalidTimeAwareFactError("current_lag values must be >= 0")
        if self.log_generation_rate_bytes_sec < 0.0:
            raise InvalidTimeAwareFactError("log_generation_rate_bytes_sec must be >= 0")

    def remaining_retention_seconds(self) -> float:
        return max(0.0, self.retention_horizon_seconds - self.current_lag_seconds)

    def is_at_risk_of_truncation(
        self, projected_duration_sec: float, apply_rate_bytes_sec: float
    ) -> bool:
        """Check if projected catchup duration or lag will cause log truncation."""
        if projected_duration_sec < 0.0 or apply_rate_bytes_sec <= 0.0:
            raise InvalidTimeAwareFactError("Invalid projected duration or apply rate")

        # Net lag growth rate during migration
        net_growth_rate = self.log_generation_rate_bytes_sec - apply_rate_bytes_sec
        projected_lag_bytes = self.current_lag_bytes + max(0.0, net_growth_rate * projected_duration_sec)
        if projected_lag_bytes >= self.max_log_bytes:
            return True

        projected_lag_seconds = self.current_lag_seconds + projected_duration_sec
        return projected_lag_seconds >= self.retention_horizon_seconds


@dataclass(frozen=True)
class SnapshotSpec:
    """Consistent read snapshot specification and TTL."""

    snapshot_id: str
    created_at: datetime.datetime
    ttl_seconds: float
    renewable: bool = False

    def __post_init__(self) -> None:
        if not self.snapshot_id:
            raise InvalidTimeAwareFactError("snapshot_id cannot be empty")
        if self.ttl_seconds <= 0.0:
            raise InvalidTimeAwareFactError("ttl_seconds must be > 0")

    def remaining_lifetime_seconds(self, now: datetime.datetime) -> float:
        expiry = self.created_at + datetime.timedelta(seconds=self.ttl_seconds)
        if now >= expiry:
            return 0.0
        return (expiry - now).total_seconds()

    def is_expired(self, now: datetime.datetime) -> bool:
        return self.remaining_lifetime_seconds(now) <= 0.0


@dataclass(frozen=True)
class CutoverSlackSpec:
    """Cutover slack margin, drain time, lock acquisition, and emergency abort leeway."""

    drain_seconds: float = 15.0
    lock_acquisition_seconds: float = 5.0
    final_sync_seconds: float = 30.0
    verification_seconds: float = 20.0
    abort_leeway_seconds: float = 30.0

    def __post_init__(self) -> None:
        if (
            self.drain_seconds < 0.0
            or self.lock_acquisition_seconds < 0.0
            or self.final_sync_seconds < 0.0
            or self.verification_seconds < 0.0
            or self.abort_leeway_seconds < 0.0
        ):
            raise InvalidTimeAwareFactError("Slack components must be >= 0")

    @property
    def total_slack_required_seconds(self) -> float:
        return (
            self.drain_seconds
            + self.lock_acquisition_seconds
            + self.final_sync_seconds
            + self.verification_seconds
            + self.abort_leeway_seconds
        )


@dataclass(frozen=True)
class PhaseForecast:
    """Forecast of a migration phase with uncertainty parameters."""

    phase_name: str
    nominal_duration_seconds: float
    uncertainty_factor: float = 0.20
    p90_duration_seconds: float = 0.0
    p95_duration_seconds: float = 0.0
    required_bandwidth_bytes_sec: float = 10_000_000.0
    required_quota_units: float = 100.0
    is_cutover_phase: bool = False

    def __post_init__(self) -> None:
        if not self.phase_name:
            raise InvalidTimeAwareFactError("phase_name cannot be empty")
        if self.nominal_duration_seconds <= 0.0:
            raise InvalidTimeAwareFactError("nominal_duration_seconds must be > 0")
        if self.uncertainty_factor < 0.0:
            raise InvalidTimeAwareFactError("uncertainty_factor cannot be negative")
        if self.required_bandwidth_bytes_sec <= 0.0 or self.required_quota_units < 0.0:
            raise InvalidTimeAwareFactError("Invalid bandwidth or quota facts")

    def effective_duration_seconds(self, confidence: float = 0.95) -> float:
        """Derive tail duration from facts and uncertainty factor."""
        if confidence >= 0.95 and self.p95_duration_seconds > 0.0:
            return self.p95_duration_seconds
        if confidence >= 0.90 and self.p90_duration_seconds > 0.0:
            return self.p90_duration_seconds

        # Factor in uncertainty multiplier
        # 0.90 -> 1.28 sd, 0.95 -> 1.64 sd, 0.99 -> 2.33 sd
        sd_mult = 1.64 if confidence >= 0.95 else 1.28
        std_dev = self.nominal_duration_seconds * self.uncertainty_factor
        return self.nominal_duration_seconds + (sd_mult * std_dev)


@dataclass(frozen=True)
class PrerequisiteAction:
    """A scheduled prerequisite action (e.g. credential renewal, staging)."""

    action_type: str
    resource_id: str
    scheduled_at: datetime.datetime
    lead_time_seconds: float
    estimated_duration_seconds: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimeAwarePlan:
    """Synthesized time-aware migration execution plan."""

    plan_id: str
    target_window_id: str
    scheduled_start: datetime.datetime
    projected_cutover_start: Optional[datetime.datetime]
    projected_completion: datetime.datetime
    prerequisites: Tuple[PrerequisiteAction, ...]
    switch_consent_granted: bool
    care_badge: str
    admissible: bool
    chosen_rungs: Dict[str, str]
    reasons: Tuple[str, ...]

    def summary(self) -> str:
        consent_str = "CONSENT_GRANTED" if self.switch_consent_granted else "NO_CONSENT"
        return (
            f"[{self.care_badge}] Plan {self.plan_id}: window={self.target_window_id}, "
            f"admissible={self.admissible}, switch={consent_str}, "
            f"start={self.scheduled_start.isoformat()}, end={self.projected_completion.isoformat()}, "
            f"prereqs={len(self.prerequisites)}"
        )


# -----------------------------------------------------------------------------
# Decision Engine Ladders (decide.Rung definitions)
# -----------------------------------------------------------------------------


def window_admissibility_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for maintenance window admissibility."""
    return (
        decide.Rung(
            "admit_in_active_window",
            "admit phase execution inside currently active maintenance window",
            needs=(
                decide.Need(
                    "active window open",
                    lambda f: bool(f.get("is_window_active", False)),
                    "{rung} refused because target window is not currently active",
                ),
                decide.Need(
                    "not in blackout",
                    lambda f: not bool(f.get("is_blackout", False)),
                    "{rung} refused because execution interval intersects a blackout window",
                ),
                decide.Need(
                    "fits in active window",
                    lambda f: bool(f.get("fits_in_active_window", False)),
                    "{rung} refused because phase duration plus slack exceeds active window remaining time",
                ),
            ),
            because="active window has sufficient remaining capacity and zero blackout conflict",
            footprint=0,
        ),
        decide.Rung(
            "schedule_in_upcoming_window",
            "schedule phase execution in next admissible upcoming maintenance window",
            needs=(
                decide.Need(
                    "upcoming window available",
                    lambda f: bool(f.get("has_upcoming_window", False)),
                    "{rung} refused because no admissible upcoming maintenance window exists",
                ),
                decide.Need(
                    "active window not viable",
                    lambda f: not bool(f.get("active_window_viable", True)),
                    "{rung} refused because active window remains viable",
                ),
            ),
            because="active window is missed or insufficient, but upcoming window is admissible",
            footprint=1,
        ),
        decide.Rung(
            "refuse_missed_window_no_consent",
            "refuse cutover execution because window is missed and switch consent is absent",
            needs=(
                decide.Need(
                    "window missed or expired",
                    lambda f: bool(f.get("window_missed", False)),
                    "{rung} refused because window was not missed",
                ),
                decide.Need(
                    "no switch consent outside window",
                    lambda f: not bool(f.get("switch_consent_outside_window", False)),
                    "{rung} refused because explicit switch consent was granted outside window",
                ),
            ),
            because="avoids guessing switch consent when maintenance window boundary is breached",
            footprint=0,
        ),
        decide.Rung(
            "reject_blackout_violation",
            "reject execution because active interval is within an authorized blackout window",
            needs=(
                decide.Need(
                    "current interval in blackout",
                    lambda f: bool(f.get("is_blackout", False)),
                    "{rung} refused because current interval is not in blackout",
                ),
            ),
            because="strictly protects business critical hours and blackout policies",
            footprint=0,
        ),
    )


def resource_lease_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for token, certificate, and log retention lease boundaries."""
    return (
        decide.Rung(
            "lease_safe_within_horizon",
            "admit execution because all resource leases, log retention, and snapshot TTLs hold",
            needs=(
                decide.Need(
                    "lease horizon sufficient",
                    lambda f: bool(f.get("lease_horizon_sufficient", False)),
                    "{rung} refused because resource lease expires before phase tail completion",
                ),
                decide.Need(
                    "log retention sufficient",
                    lambda f: bool(f.get("log_retention_sufficient", False)),
                    "{rung} refused because replication lag risks log retention truncation",
                ),
                decide.Need(
                    "snapshot validity sufficient",
                    lambda f: bool(f.get("snapshot_validity_sufficient", False)),
                    "{rung} refused because consistent snapshot TTL expires before phase completion",
                ),
            ),
            because="all resource leases, WAL logs, and snapshots remain valid throughout execution",
            footprint=0,
        ),
        decide.Rung(
            "schedule_prerequisite_lease_renewal",
            "schedule proactive prerequisite renewal for renewable lease expiring during phase",
            needs=(
                decide.Need(
                    "lease renewable",
                    lambda f: bool(f.get("lease_renewable", False)),
                    "{rung} refused because expiring lease is nonrenewable",
                ),
                decide.Need(
                    "lease expires during phase",
                    lambda f: bool(f.get("lease_expires_during_phase", False)),
                    "{rung} refused because lease does not expire during phase execution",
                ),
            ),
            because="renewable credentials can be refreshed proactively before expiration",
            footprint=1,
        ),
        decide.Rung(
            "reject_nonrenewable_lease_breach",
            "reject optimistic start beyond a nonrenewable lease expiration",
            needs=(
                decide.Need(
                    "lease nonrenewable",
                    lambda f: not bool(f.get("lease_renewable", True)),
                    "{rung} refused because lease is renewable",
                ),
                decide.Need(
                    "lease expires during phase",
                    lambda f: bool(f.get("lease_expires_during_phase", False)),
                    "{rung} refused because lease does not expire during phase execution",
                ),
            ),
            because="strictly forbids optimistic phase start when nonrenewable lease will expire",
            footprint=0,
        ),
        decide.Rung(
            "reject_log_retention_breach",
            "reject execution because projected duration breaches source log retention window",
            needs=(
                decide.Need(
                    "log retention insufficient",
                    lambda f: not bool(f.get("log_retention_sufficient", True)),
                    "{rung} refused because log retention window is sufficient",
                ),
            ),
            because="prevents fatal replication slot invalidation and log truncation data loss",
            footprint=0,
        ),
        decide.Rung(
            "reject_snapshot_expiry_breach",
            "reject execution because consistent snapshot TTL expires before read completion",
            needs=(
                decide.Need(
                    "snapshot validity insufficient",
                    lambda f: not bool(f.get("snapshot_validity_sufficient", True)),
                    "{rung} refused because snapshot validity holds throughout execution",
                ),
            ),
            because="prevents snapshot too old errors and inconsistent read views",
            footprint=0,
        ),
    )


def burst_and_quota_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for burst credit depletion and quota reset management."""
    return (
        decide.Rung(
            "burst_sustainable_full_speed",
            "admit phase execution at full burst rate with sufficient credits and quota",
            needs=(
                decide.Need(
                    "burst credits sufficient",
                    lambda f: bool(f.get("burst_credits_sufficient", False)),
                    "{rung} refused because burst credits are insufficient for full phase duration",
                ),
                decide.Need(
                    "quota sufficient",
                    lambda f: bool(f.get("quota_sufficient", False)),
                    "{rung} refused because API or egress quota is insufficient",
                ),
            ),
            because="burst credits and quota limits fully sustain high-speed execution",
            footprint=0,
        ),
        decide.Rung(
            "pace_to_refill_baseline",
            "pace phase throughput down to baseline refill rate due to burst credit depletion",
            needs=(
                decide.Need(
                    "burst credits depleted or low",
                    lambda f: not bool(f.get("burst_credits_sufficient", True)),
                    "{rung} refused because burst credits are fully sufficient",
                ),
                decide.Need(
                    "baseline rate acceptable",
                    lambda f: bool(f.get("baseline_rate_acceptable", False)),
                    "{rung} refused because baseline refill rate causes deadline breach",
                ),
            ),
            because="pacing throughput to refill rate avoids unexpected performance cliff",
            footprint=1,
        ),
        decide.Rung(
            "defer_for_quota_reset",
            "defer phase execution to await scheduled cloud API or egress quota reset horizon",
            needs=(
                decide.Need(
                    "quota exhausted",
                    lambda f: not bool(f.get("quota_sufficient", True)),
                    "{rung} refused because quota is sufficient",
                ),
                decide.Need(
                    "quota reset admissible",
                    lambda f: bool(f.get("quota_reset_admissible", False)),
                    "{rung} refused because quota reset horizon exceeds window or deadline",
                ),
            ),
            because="pausing until quota reset horizon avoids hard API rate-limit rejection",
            footprint=1,
        ),
        decide.Rung(
            "reject_quota_exhaustion_unresolvable",
            "reject execution because quota is exhausted and reset horizon breaches deadline",
            needs=(
                decide.Need(
                    "quota exhausted",
                    lambda f: not bool(f.get("quota_sufficient", True)),
                    "{rung} refused because quota is sufficient",
                ),
                decide.Need(
                    "quota reset inadmissible",
                    lambda f: not bool(f.get("quota_reset_admissible", True)),
                    "{rung} refused because quota reset horizon is admissible",
                ),
            ),
            because="unresolvable quota exhaustion requires administrative quota increase",
            footprint=0,
        ),
    )


def traffic_cycle_staging_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for traffic-cycle aware staging and bulk-copy scheduling."""
    return (
        decide.Rung(
            "stage_during_offpeak_dip",
            "schedule bulk staging during observed off-peak diurnal traffic dip",
            needs=(
                decide.Need(
                    "offpeak dip available",
                    lambda f: bool(f.get("is_offpeak_available", False)),
                    "{rung} refused because off-peak dip is not available",
                ),
                decide.Need(
                    "staging bandwidth permitted",
                    lambda f: bool(f.get("staging_bandwidth_permitted", False)),
                    "{rung} refused because staging bandwidth exceeds offpeak limit",
                ),
            ),
            because="off-peak traffic dip provides maximum network and storage head-room",
            footprint=0,
        ),
        decide.Rung(
            "throttle_staging_during_peak",
            "throttle staging bandwidth during peak traffic cycle to protect production",
            needs=(
                decide.Need(
                    "peak traffic active",
                    lambda f: bool(f.get("is_peak_active", False)),
                    "{rung} refused because peak traffic is not currently active",
                ),
                decide.Need(
                    "throttling supported",
                    lambda f: bool(f.get("throttling_supported", False)),
                    "{rung} refused because staging throttling is unsupported",
                ),
            ),
            because="throttling staging bandwidth prevents production user query degradation",
            footprint=1,
        ),
        decide.Rung(
            "defer_staging_to_offpeak",
            "defer heavy staging to upcoming off-peak cycle due to peak unthrottled impact",
            needs=(
                decide.Need(
                    "peak traffic active",
                    lambda f: bool(f.get("is_peak_active", False)),
                    "{rung} refused because peak traffic is not currently active",
                ),
                decide.Need(
                    "must defer staging",
                    lambda f: bool(f.get("must_defer_staging", False)),
                    "{rung} refused because staging does not need to be deferred",
                ),
            ),
            because="deferring staging to off-peak protects production when throttling cannot safe-guard",
            footprint=1,
        ),
    )


def cutover_slack_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for cutover slack, drain margin, and consent verification."""
    return (
        decide.Rung(
            "admit_cutover_slack_satisfied",
            "admit cutover execution with fully satisfied drain, sync, and abort slack margins",
            needs=(
                decide.Need(
                    "cutover slack satisfied",
                    lambda f: bool(f.get("cutover_slack_satisfied", False)),
                    "{rung} refused because remaining window time is less than required cutover slack",
                ),
                decide.Need(
                    "switch consent or window permitted",
                    lambda f: bool(f.get("switch_consent_or_permitted", False)),
                    "{rung} refused because switch is not permitted and explicit consent is absent",
                ),
            ),
            because="all cutover slack components and consent requirements are verified",
            footprint=0,
        ),
        decide.Rung(
            "pause_without_guessing_consent",
            "pause cutover execution without guessing switch consent outside maintenance window",
            needs=(
                decide.Need(
                    "cutover slack breached",
                    lambda f: not bool(f.get("cutover_slack_satisfied", True)),
                    "{rung} refused because cutover slack is satisfied",
                ),
                decide.Need(
                    "no switch consent",
                    lambda f: not bool(f.get("switch_consent_or_permitted", False)),
                    "{rung} refused because switch consent was explicitly granted",
                ),
            ),
            because="refuses to speculate or assume switch consent when window close approaches",
            footprint=0,
        ),
        decide.Rung(
            "reject_emergency_abort_unsupported",
            "reject cutover attempt because remaining window fails emergency abort leeway",
            needs=(
                decide.Need(
                    "emergency abort leeway breached",
                    lambda f: bool(f.get("abort_leeway_breached", False)),
                    "{rung} refused because emergency abort leeway holds",
                ),
            ),
            because="guarantees sufficient time to execute clean rollback if cutover encounters fault",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Checkpointing & Durable Lease Management
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class TimeAwareCheckpointUnit:
    """Universal typed durable checkpoint unit for time-aware plan progress."""

    plan_id: str
    phase_name: str
    min_cursor: Any
    max_cursor: Any
    merkle_digest: str
    completed_at: datetime.datetime
    status: str = "COMPLETED"


class TimeAwareCheckpointStore:
    """Universal typed durable checkpoint store with O(1) phase completion lookup."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[Tuple[str, str], TimeAwareCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        for entry in raw.get("units", ()):
            unit = TimeAwareCheckpointUnit(
                plan_id=str(entry["plan_id"]),
                phase_name=str(entry["phase_name"]),
                min_cursor=checkpoint.deserialize_cursor(entry["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(entry["max_cursor"]),
                merkle_digest=str(entry["merkle_digest"]),
                completed_at=checkpoint.deserialize_cursor(entry["completed_at"]),
                status=str(entry.get("status", "COMPLETED")),
            )
            self._units[(unit.plan_id, unit.phase_name)] = unit

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": [
                {
                    "plan_id": u.plan_id,
                    "phase_name": u.phase_name,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "merkle_digest": u.merkle_digest,
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                    "status": u.status,
                }
                for _, u in sorted(self._units.items(), key=lambda kv: kv[0])
            ]
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def record_unit(self, unit: TimeAwareCheckpointUnit) -> None:
        self._units[(unit.plan_id, unit.phase_name)] = unit
        self._save()

    def is_completed_o1(self, plan_id: str, phase_name: str) -> bool:
        return (plan_id, phase_name) in self._units

    def get_unit(
        self, plan_id: str, phase_name: str
    ) -> Optional[TimeAwareCheckpointUnit]:
        return self._units.get((plan_id, phase_name))


def acquire_time_aware_control_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "time-aware control window planning",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire an active exclusive .run.lock lease with heartbeat."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()


# -----------------------------------------------------------------------------
# Adaptive Execution: AIMD Window Pacing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------


def adjust_aimd_window_pacing(
    *,
    current_throughput_bytes_sec: float,
    window_remaining_sec: float,
    phase_tail_duration_sec: float,
    credit_ratio: float,
    min_throughput_bytes_sec: float = 1_000_000.0,
    max_throughput_bytes_sec: float = 100_000_000.0,
    additive_increase_bytes_sec: float = 2_000_000.0,
    multiplicative_decrease: float = 0.50,
) -> float:
    """Dynamically adjust execution throughput pacing via AIMD based on window slack & credits."""
    if min_throughput_bytes_sec <= 0.0 or max_throughput_bytes_sec < min_throughput_bytes_sec:
        raise InvalidTimeAwareFactError("Invalid throughput bounds")
    if not (0.0 < multiplicative_decrease < 1.0):
        raise InvalidTimeAwareFactError("multiplicative_decrease must be in (0, 1)")

    # If credits are depleted or time is running tight (slack < 1.25x tail duration), decrease
    if credit_ratio < 0.15 or (window_remaining_sec < 1.25 * phase_tail_duration_sec):
        reduced = current_throughput_bytes_sec * multiplicative_decrease
        return max(min_throughput_bytes_sec, min(max_throughput_bytes_sec, reduced))

    increased = current_throughput_bytes_sec + additive_increase_bytes_sec
    return max(min_throughput_bytes_sec, min(max_throughput_bytes_sec, increased))


def estimate_time_aware_byte_weighted_eta(
    *,
    total_bytes: int,
    bytes_completed: int,
    elapsed_seconds: float,
    current_bandwidth_bytes_sec: float,
    traffic_load_factor: float = 1.0,
    uncertainty_sd_multiplier: float = 1.64,
) -> float:
    """Compute instant cold-start byte-weighted ETA with traffic cycle scaling and uncertainty."""
    if total_bytes < 0 or bytes_completed < 0:
        raise InvalidTimeAwareFactError("Byte counts cannot be negative")
    if bytes_completed > total_bytes:
        return 0.0
    if current_bandwidth_bytes_sec <= 0.0:
        raise InvalidTimeAwareFactError("current_bandwidth_bytes_sec must be > 0")
    if traffic_load_factor <= 0.0:
        raise InvalidTimeAwareFactError("traffic_load_factor must be > 0")

    bytes_remaining = total_bytes - bytes_completed
    if bytes_remaining == 0:
        return 0.0

    # Adjust bandwidth for traffic load
    effective_bw = current_bandwidth_bytes_sec / traffic_load_factor

    if elapsed_seconds > 0.0 and bytes_completed > 0:
        observed_rate = bytes_completed / elapsed_seconds
        # Blend observed and nominal bandwidth
        effective_bw = 0.70 * observed_rate + 0.30 * effective_bw

    nominal_eta = bytes_remaining / effective_bw
    # Cold-start uncertainty inflation
    uncertainty_ratio = 0.25 if bytes_completed == 0 else max(0.05, 0.20 * (bytes_remaining / total_bytes))
    return nominal_eta * (1.0 + uncertainty_sd_multiplier * uncertainty_ratio)


# -----------------------------------------------------------------------------
# Plan Synthesis & Decision Logic
# -----------------------------------------------------------------------------


def plan_time_aware_migration(
    *,
    phases: Sequence[PhaseForecast],
    windows: Sequence[TimeWindow],
    leases: Sequence[ResourceLease] = (),
    burst_bucket: Optional[BurstCreditBucket] = None,
    quotas: Sequence[QuotaResetWindow] = (),
    log_retention: Optional[LogRetentionWindow] = None,
    snapshot: Optional[SnapshotSpec] = None,
    traffic_cycle: Optional[TrafficCycle] = None,
    cutover_slack: CutoverSlackSpec = CutoverSlackSpec(),
    switch_consent_granted: bool = False,
    now: datetime.datetime,
    endpoint_host: str = "127.0.0.1",
) -> TimeAwarePlan:
    """Synthesize a complete time-aware migration plan using decision engine ladders."""
    validate_safe_endpoint(endpoint_host)

    if not phases:
        raise InvalidTimeAwareFactError("Phases cannot be empty")
    if not windows:
        raise InvalidTimeAwareFactError("Windows cannot be empty")

    plan_id = f"plan-{uuid.uuid4().hex[:8]}"
    chosen_rungs: Dict[str, str] = {}
    reasons: List[str] = []
    prerequisites: List[PrerequisiteAction] = []

    # 1. Evaluate Traffic Cycle & Staging
    tc = traffic_cycle or TrafficCycle()
    is_peak = tc.is_peak(now)
    is_offpeak = tc.is_offpeak(now)

    tc_facts = decide.Facts({
        "is_offpeak_available": is_offpeak or True,
        "staging_bandwidth_permitted": is_offpeak,
        "is_peak_active": is_peak,
        "throttling_supported": True,
        "must_defer_staging": False,
    })
    tc_choice = decide.climb("traffic cycle staging", traffic_cycle_staging_rungs(), tc_facts)
    chosen_rungs["traffic_cycle_staging"] = tc_choice.rung.name if tc_choice.rung else "none"
    if tc_choice.reason:
        reasons.append(tc_choice.reason)

    # 2. Evaluate Burst Credits & Quotas
    burst_ok = True
    if burst_bucket is not None:
        burst_ok = not burst_bucket.is_depleted

    quota_ok = True
    total_quota_needed = sum(p.required_quota_units for p in phases)
    for q in quotas:
        if not q.can_accommodate(total_quota_needed):
            quota_ok = False
            break

    bq_facts = decide.Facts({
        "burst_credits_sufficient": burst_ok,
        "quota_sufficient": quota_ok,
        "baseline_rate_acceptable": True,
        "quota_reset_admissible": True,
    })
    bq_choice = decide.climb("burst and quota", burst_and_quota_rungs(), bq_facts)
    chosen_rungs["burst_and_quota"] = bq_choice.rung.name if bq_choice.rung else "none"
    if bq_choice.reason:
        reasons.append(bq_choice.reason)

    # If quota exhausted, schedule a wait prerequisite
    if not quota_ok:
        max_reset_sec = max((q.reset_horizon_seconds for q in quotas), default=0.0)
        prerequisites.append(
            PrerequisiteAction(
                action_type="quota_wait",
                resource_id="cloud_quota",
                scheduled_at=now,
                lead_time_seconds=0.0,
                estimated_duration_seconds=max_reset_sec,
                details={"reason": "awaiting cloud quota reset horizon"},
            )
        )

    # 3. Calculate Phase Durations and Tails
    total_nominal_sec = sum(p.nominal_duration_seconds for p in phases)
    total_tail_p95_sec = sum(p.effective_duration_seconds(0.95) for p in phases)
    cutover_phase = next((p for p in phases if p.is_cutover_phase), phases[-1])
    cutover_tail_sec = cutover_phase.effective_duration_seconds(0.95)

    # 4. Evaluate Resource Leases, WAL Log Retention, and Snapshots
    lease_horizon_ok = True
    expiring_leases: List[ResourceLease] = []
    has_nonrenewable_breach = False
    for lease_item in leases:
        rem_sec = lease_item.remaining_lifetime_seconds(now)
        if rem_sec < total_tail_p95_sec:
            expiring_leases.append(lease_item)
            lease_horizon_ok = False
            if not lease_item.renewable:
                has_nonrenewable_breach = True

    log_retention_ok = True
    if log_retention is not None:
        if log_retention.is_at_risk_of_truncation(total_tail_p95_sec, apply_rate_bytes_sec=50_000_000.0):
            log_retention_ok = False

    snapshot_ok = True
    if snapshot is not None:
        if snapshot.remaining_lifetime_seconds(now) < total_tail_p95_sec:
            snapshot_ok = False

    all_expiring_renewable = (len(expiring_leases) > 0) and all(l.renewable for l in expiring_leases)
    lease_facts = decide.Facts({
        "lease_horizon_sufficient": lease_horizon_ok,
        "log_retention_sufficient": log_retention_ok,
        "snapshot_validity_sufficient": snapshot_ok,
        "lease_renewable": all_expiring_renewable,
        "lease_expires_during_phase": len(expiring_leases) > 0,
    })
    lease_choice = decide.climb("resource lease", resource_lease_rungs(), lease_facts)
    chosen_rungs["resource_lease"] = lease_choice.rung.name if lease_choice.rung else "none"
    if lease_choice.reason:
        reasons.append(lease_choice.reason)

    # Schedule prerequisite renewals for renewable leases
    for exp_lease in expiring_leases:
        if exp_lease.can_renew():
            # Lead time buffer includes 20% uncertainty margin
            buffer_sec = exp_lease.renewal_lead_time_seconds * 0.20
            prereq_time = max(now, exp_lease.valid_until - datetime.timedelta(seconds=exp_lease.renewal_lead_time_seconds + buffer_sec))
            prerequisites.append(
                PrerequisiteAction(
                    action_type=f"{exp_lease.resource_type.value}_renewal",
                    resource_id=exp_lease.resource_id,
                    scheduled_at=prereq_time,
                    lead_time_seconds=exp_lease.renewal_lead_time_seconds,
                    estimated_duration_seconds=exp_lease.renewal_cost_seconds,
                    details={"action": "proactive_renewal", "valid_until": exp_lease.valid_until.isoformat()},
                )
            )

    # 5. Evaluate Window Admissibility & Cutover Slack
    active_window = next((w for w in windows if w.is_active(now) and not w.is_blackout), None)
    upcoming_windows = [w for w in windows if w.start_time > now and not w.is_blackout]

    fits_in_active = False
    window_missed = False
    target_window: TimeWindow
    scheduled_start: datetime.datetime

    if active_window is not None:
        rem_window_sec = active_window.remaining_seconds(now)
        required_slack = cutover_slack.total_slack_required_seconds
        total_needed = total_tail_p95_sec + required_slack

        if rem_window_sec >= total_needed:
            fits_in_active = True
            target_window = active_window
            scheduled_start = now
        else:
            window_missed = True

    if not fits_in_active:
        if upcoming_windows:
            target_window = upcoming_windows[0]
            scheduled_start = target_window.start_time
        else:
            # Fall back to first window with missed flag
            target_window = windows[0]
            scheduled_start = target_window.start_time
            window_missed = True

    # Check cutover slack in target window
    target_rem_sec = target_window.duration_seconds if scheduled_start == target_window.start_time else target_window.remaining_seconds(scheduled_start)
    slack_satisfied = target_rem_sec >= (total_tail_p95_sec + cutover_slack.total_slack_required_seconds)
    abort_breached = target_rem_sec < (cutover_tail_sec + cutover_slack.abort_leeway_seconds)

    slack_facts = decide.Facts({
        "cutover_slack_satisfied": slack_satisfied,
        "switch_consent_or_permitted": switch_consent_granted or (target_window.cutover_permitted and slack_satisfied),
        "abort_leeway_breached": abort_breached,
    })
    slack_choice = decide.climb("cutover slack", cutover_slack_rungs(), slack_facts)
    chosen_rungs["cutover_slack"] = slack_choice.rung.name if slack_choice.rung else "none"
    if slack_choice.reason:
        reasons.append(slack_choice.reason)

    window_facts = decide.Facts({
        "is_window_active": active_window is not None and not active_window.is_blackout,
        "is_blackout": any(w.is_active(now) and w.is_blackout for w in windows),
        "fits_in_active_window": fits_in_active,
        "has_upcoming_window": len(upcoming_windows) > 0,
        "active_window_viable": fits_in_active,
        "window_missed": window_missed,
        "switch_consent_outside_window": switch_consent_granted,
    })
    win_choice = decide.climb("window admissibility", window_admissibility_rungs(), window_facts)
    chosen_rungs["window_admissibility"] = win_choice.rung.name if win_choice.rung else "none"
    if win_choice.reason:
        reasons.append(win_choice.reason)

    # 6. Determine Admissibility & Care Badge
    admissible = True
    care_badge = CARE_PASS

    # Rule 1: No phase starts optimistically beyond a nonrenewable lease
    if has_nonrenewable_breach:
        admissible = False
        care_badge = CARE_ACTION
        reasons.append("nonrenewable lease expires before phase tail completion")

    # Rule 2: No phase starts beyond log-retention window
    if not log_retention_ok:
        admissible = False
        care_badge = CARE_ACTION
        reasons.append("log retention window breached; risk of WAL/binlog truncation")

    # Rule 3: Snapshot expiry
    if not snapshot_ok:
        admissible = False
        care_badge = CARE_ACTION
        reasons.append("snapshot TTL expires before phase completion")

    # Rule 4: Missed windows trigger a fresh admissible plan without guessing switch consent
    if window_missed and not switch_consent_granted:
        if upcoming_windows:
            care_badge = CARE_REVIEW
            reasons.append("active window missed; deferred to next admissible upcoming window without guessing switch consent")
        else:
            admissible = False
            care_badge = CARE_ACTION
            reasons.append("maintenance window closed and no upcoming window available without switch consent")

    if not quota_ok and not bq_choice.rung:
        admissible = False
        care_badge = CARE_ACTION
        reasons.append("quota exhaustion unresolvable")

    if care_badge == CARE_PASS and len(prerequisites) > 0:
        care_badge = CARE_BENIGN

    projected_comp = scheduled_start + datetime.timedelta(seconds=total_tail_p95_sec)
    cutover_start = scheduled_start + datetime.timedelta(seconds=max(0.0, total_tail_p95_sec - cutover_tail_sec))

    return TimeAwarePlan(
        plan_id=plan_id,
        target_window_id=target_window.window_id,
        scheduled_start=scheduled_start,
        projected_cutover_start=cutover_start,
        projected_completion=projected_comp,
        prerequisites=tuple(prerequisites),
        switch_consent_granted=switch_consent_granted,
        care_badge=care_badge,
        admissible=admissible,
        chosen_rungs=chosen_rungs,
        reasons=tuple(reasons),
    )
