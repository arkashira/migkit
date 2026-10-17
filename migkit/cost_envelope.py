from dataclasses import dataclass, field
import enum
import time
from typing import Any, Dict, List, Optional, Tuple, Union


class CostEnvelopeError(RuntimeError):
    pass


class BudgetExceededAction(str, enum.Enum):
    ABORT = "abort"
    THROTTLE = "throttle"
    WARN = "warn"


class RateSource(str, enum.Enum):
    PROVIDER = "provider"
    USER = "user"
    COMMUNITY = "community"


class CostDimension(str, enum.Enum):
    SOURCE_COMPUTE = "source_compute_hours"
    TARGET_COMPUTE = "target_compute_hours"
    EGRESS_BYTES = "egress_bytes"
    STAGING_STORAGE = "staging_storage_gb_hours"
    READ_IOPS = "read_iops"
    WRITE_IOPS = "write_iops"
    CDC_CHANGE_VOLUME = "cdc_change_volume"
    VERIFICATION_PASSES = "verification_passes"
    IDLE_HEADROOM = "idle_headroom_hours"


class PlanType(str, enum.Enum):
    TARGETED_REFRESH = "targeted_refresh"
    FULL_RELOAD = "full_reload"


@dataclass(frozen=True)
class CostUnknown:
    dimension: str
    reason: str
    quantity: Optional[float] = None
    unit: Optional[str] = None
    source: Optional[str] = None
    timestamp: Optional[float] = None


class BudgetExceededError(CostEnvelopeError):
    def __init__(
        self,
        message: str,
        budget_limit: float,
        actual_spend: float,
        excess: float,
        action: BudgetExceededAction,
    ):
        super().__init__(message)
        self.budget_limit = budget_limit
        self.actual_spend = actual_spend
        self.excess = excess
        self.action = action


class PriceUnknownError(CostEnvelopeError):
    def __init__(self, message: str, dimension: str, unknown: CostUnknown):
        super().__init__(message)
        self.dimension = dimension
        self.unknown = unknown


class ProofContractMismatchError(CostEnvelopeError):
    pass


class StaleRateError(CostEnvelopeError):
    pass


@dataclass
class Rate:
    dimension: Union[CostDimension, str]
    nominal_rate: float
    min_rate: Optional[float] = None
    max_rate: Optional[float] = None
    timestamp: float = field(default_factory=time.time)
    source: Union[RateSource, str] = RateSource.USER
    unit: str = ""
    max_age_seconds: Optional[float] = None
    currency: str = "USD"

    def __post_init__(self):
        if isinstance(self.dimension, CostDimension):
            self.dimension = self.dimension.value
        if isinstance(self.source, RateSource):
            self.source = self.source.value
        if self.min_rate is None:
            self.min_rate = self.nominal_rate * 0.90
        if self.max_rate is None:
            self.max_rate = self.nominal_rate * 1.10
        if self.min_rate > self.nominal_rate:
            self.min_rate = self.nominal_rate
        if self.max_rate < self.nominal_rate:
            self.max_rate = self.nominal_rate

    def uncertainty_bounds(self) -> Tuple[float, float, float]:
        return (self.min_rate, self.nominal_rate, self.max_rate)

    def is_stale(self, now: Optional[float] = None, max_age_seconds: Optional[float] = None) -> bool:
        t_now = now if now is not None else time.time()
        max_age = max_age_seconds if max_age_seconds is not None else self.max_age_seconds
        if max_age is None:
            max_age = 86400.0 * 14.0 if self.source == RateSource.COMMUNITY.value else 86400.0 * 90.0
        return (t_now - self.timestamp) > max_age


@dataclass
class CostInterval:
    dimension: str
    quantity: float
    unit: str
    min_cost: float
    expected_cost: float
    max_cost: float
    rate: Optional[Rate] = None
    currency: str = "USD"

    def spread(self) -> float:
        return self.max_cost - self.min_cost

    def contains(self, amount: float) -> bool:
        return self.min_cost <= amount <= self.max_cost


@dataclass(frozen=True)
class ProofContract:
    name: str
    verification_passes: int = 1
    verification_mode: str = "merkle_fused"
    data_guarantee: str = "exact_hash"
    tolerance_rows: int = 0
    require_atomic_cutover: bool = True

    def assert_matches(self, other: "ProofContract"):
        if not isinstance(other, ProofContract):
            raise ProofContractMismatchError(f"Expected ProofContract, got {type(other).__name__}")
        if (
            self.name != other.name
            or self.verification_passes != other.verification_passes
            or self.verification_mode != other.verification_mode
            or self.data_guarantee != other.data_guarantee
            or self.tolerance_rows != other.tolerance_rows
            or self.require_atomic_cutover != other.require_atomic_cutover
        ):
            raise ProofContractMismatchError(
                f"Proof contract mismatch: expected ({self.name}, passes={self.verification_passes}, "
                f"mode={self.verification_mode}, guarantee={self.data_guarantee}, tol={self.tolerance_rows}), "
                f"got ({other.name}, passes={other.verification_passes}, mode={other.verification_mode}, "
                f"guarantee={other.data_guarantee}, tol={other.tolerance_rows})"
            )


@dataclass
class MigrationWorkloadProfile:
    plan_type: Union[PlanType, str] = PlanType.FULL_RELOAD
    source_compute_hours: float = 0.0
    target_compute_hours: float = 0.0
    egress_bytes: int = 0
    staging_storage_gb_hours: float = 0.0
    read_iops: int = 0
    write_iops: int = 0
    cdc_change_volume: int = 0
    verification_passes: int = 1
    verification_bytes_per_pass: int = 0
    idle_headroom_hours: float = 0.0

    def __post_init__(self):
        if isinstance(self.plan_type, PlanType):
            self.plan_type = self.plan_type.value

    def to_quantities(self) -> Dict[str, float]:
        return {
            CostDimension.SOURCE_COMPUTE.value: float(self.source_compute_hours),
            CostDimension.TARGET_COMPUTE.value: float(self.target_compute_hours),
            CostDimension.EGRESS_BYTES.value: float(self.egress_bytes),
            CostDimension.STAGING_STORAGE.value: float(self.staging_storage_gb_hours),
            CostDimension.READ_IOPS.value: float(self.read_iops),
            CostDimension.WRITE_IOPS.value: float(self.write_iops),
            CostDimension.CDC_CHANGE_VOLUME.value: float(self.cdc_change_volume),
            CostDimension.VERIFICATION_PASSES.value: float(self.verification_passes),
            CostDimension.IDLE_HEADROOM.value: float(self.idle_headroom_hours),
        }


class RateRegistry:
    def __init__(self):
        self._rates: Dict[str, Rate] = {}

    def register(self, rate: Rate):
        dim = rate.dimension.value if isinstance(rate.dimension, CostDimension) else rate.dimension
        self._rates[dim] = rate

    def get(
        self,
        dimension: Union[CostDimension, str],
        now: Optional[float] = None,
        allow_stale_community: bool = False,
    ) -> Union[Rate, CostUnknown]:
        dim = dimension.value if isinstance(dimension, CostDimension) else dimension
        rate = self._rates.get(dim)
        if rate is None:
            return CostUnknown(dimension=dim, reason=f"No rate registered for {dim}")

        if rate.source == RateSource.COMMUNITY.value:
            if not allow_stale_community and rate.is_stale(now=now):
                return CostUnknown(
                    dimension=dim,
                    reason=f"Stale community rate from {rate.timestamp} rejected as current pricing",
                    source=rate.source,
                    timestamp=rate.timestamp,
                )
        return rate


@dataclass
class CostEnvelopeForecast:
    plan_name: str
    proof_contract: Optional[ProofContract]
    items: Dict[str, Union[CostInterval, CostUnknown]]
    created_at: float = field(default_factory=time.time)

    @property
    def unknowns(self) -> Dict[str, CostUnknown]:
        return {k: v for k, v in self.items.items() if isinstance(v, CostUnknown)}

    @property
    def known_items(self) -> Dict[str, CostInterval]:
        return {k: v for k, v in self.items.items() if isinstance(v, CostInterval)}

    @property
    def is_complete(self) -> bool:
        return len(self.unknowns) == 0

    @property
    def has_unknowns(self) -> bool:
        return len(self.unknowns) > 0

    @property
    def total_expected_cost(self) -> float:
        return sum(item.expected_cost for item in self.known_items.values())

    @property
    def total_min_cost(self) -> float:
        return sum(item.min_cost for item in self.known_items.values())

    @property
    def total_max_cost(self) -> float:
        return sum(item.max_cost for item in self.known_items.values())

    def get_cost(self, dimension: Union[CostDimension, str]) -> Union[CostInterval, CostUnknown]:
        dim = dimension.value if isinstance(dimension, CostDimension) else dimension
        if dim not in self.items:
            return CostUnknown(dimension=dim, reason=f"Dimension {dim} not present in forecast")
        return self.items[dim]

    def terse_summary(self) -> str:
        lines = [
            f"plan={self.plan_name} complete={self.is_complete} "
            f"min={self.total_min_cost:.2f} exp={self.total_expected_cost:.2f} max={self.total_max_cost:.2f}"
        ]
        for k, v in sorted(self.items.items()):
            if isinstance(v, CostInterval):
                lines.append(
                    f"  {k:25s} {v.quantity:12.1f} {v.unit:10s} "
                    f"exp=${v.expected_cost:8.2f} [{v.min_cost:.2f}..{v.max_cost:.2f}]"
                )
            else:
                lines.append(f"  {k:25s} {v.quantity or 0.0:12.1f} UNKNOWN: {v.reason}")
        return "\n".join(lines)


def forecast_cost_envelope(
    profile: MigrationWorkloadProfile,
    registry: RateRegistry,
    proof_contract: Optional[ProofContract] = None,
    plan_name: Optional[str] = None,
    now: Optional[float] = None,
) -> CostEnvelopeForecast:
    name = plan_name or profile.plan_type
    quantities = profile.to_quantities()
    items: Dict[str, Union[CostInterval, CostUnknown]] = {}

    for dim, qty in quantities.items():
        rate_or_unknown = registry.get(dim, now=now)
        if isinstance(rate_or_unknown, CostUnknown):
            items[dim] = CostUnknown(
                dimension=dim,
                reason=rate_or_unknown.reason,
                quantity=qty,
                source=rate_or_unknown.source,
                timestamp=rate_or_unknown.timestamp,
            )
        else:
            rate = rate_or_unknown
            b_min, b_nom, b_max = rate.uncertainty_bounds()
            min_cost = qty * b_min
            exp_cost = qty * b_nom
            max_cost = qty * b_max
            items[dim] = CostInterval(
                dimension=dim,
                quantity=qty,
                unit=rate.unit,
                min_cost=min_cost,
                expected_cost=exp_cost,
                max_cost=max_cost,
                rate=rate,
                currency=rate.currency,
            )

    return CostEnvelopeForecast(
        plan_name=name,
        proof_contract=proof_contract,
        items=items,
        created_at=now if now is not None else time.time(),
    )


@dataclass
class PlanCostComparison:
    proof_contract: ProofContract
    targeted_forecast: CostEnvelopeForecast
    full_reload_forecast: CostEnvelopeForecast
    savings_expected: float = field(init=False)
    savings_min: float = field(init=False)
    savings_max: float = field(init=False)
    cost_ratio: float = field(init=False)
    recommended_strategy: str = field(init=False)

    def __post_init__(self):
        if self.targeted_forecast.proof_contract != self.proof_contract:
            raise ProofContractMismatchError(
                f"Targeted forecast contract ({self.targeted_forecast.proof_contract}) "
                f"does not match comparison contract ({self.proof_contract})"
            )
        if self.full_reload_forecast.proof_contract != self.proof_contract:
            raise ProofContractMismatchError(
                f"Full reload forecast contract ({self.full_reload_forecast.proof_contract}) "
                f"does not match comparison contract ({self.proof_contract})"
            )

        self.savings_expected = (
            self.full_reload_forecast.total_expected_cost
            - self.targeted_forecast.total_expected_cost
        )
        self.savings_min = (
            self.full_reload_forecast.total_min_cost - self.targeted_forecast.total_min_cost
        )
        self.savings_max = (
            self.full_reload_forecast.total_max_cost - self.targeted_forecast.total_max_cost
        )

        full_exp = self.full_reload_forecast.total_expected_cost
        if full_exp > 0.0:
            self.cost_ratio = self.targeted_forecast.total_expected_cost / full_exp
        else:
            self.cost_ratio = 1.0

        if self.savings_expected > 0:
            self.recommended_strategy = PlanType.TARGETED_REFRESH.value
        else:
            self.recommended_strategy = PlanType.FULL_RELOAD.value

    def terse_summary(self) -> str:
        return (
            f"contract={self.proof_contract.name} rec={self.recommended_strategy} "
            f"targeted=${self.targeted_forecast.total_expected_cost:.2f} "
            f"full=${self.full_reload_forecast.total_expected_cost:.2f} "
            f"savings=${self.savings_expected:.2f} (ratio={self.cost_ratio:.2f})"
        )


def compare_targeted_refresh_to_full_reload(
    targeted_profile: MigrationWorkloadProfile,
    full_reload_profile: MigrationWorkloadProfile,
    registry: RateRegistry,
    proof_contract: ProofContract,
    now: Optional[float] = None,
) -> PlanCostComparison:
    targeted_forecast = forecast_cost_envelope(
        targeted_profile,
        registry,
        proof_contract=proof_contract,
        plan_name=PlanType.TARGETED_REFRESH.value,
        now=now,
    )
    full_reload_forecast = forecast_cost_envelope(
        full_reload_profile,
        registry,
        proof_contract=proof_contract,
        plan_name=PlanType.FULL_RELOAD.value,
        now=now,
    )
    return PlanCostComparison(
        proof_contract=proof_contract,
        targeted_forecast=targeted_forecast,
        full_reload_forecast=full_reload_forecast,
    )


class CostEnvelopeGovernor:
    def __init__(
        self,
        budget_limit: Optional[float] = None,
        action: BudgetExceededAction = BudgetExceededAction.ABORT,
        forecast: Optional[CostEnvelopeForecast] = None,
        registry: Optional[RateRegistry] = None,
        throttle_worker_ceiling: int = 1,
        throttle_factor: float = 0.25,
        throttle_delay_seconds: float = 0.5,
        byte_limit: Optional[float] = None,
    ):
        self.budget_limit = float(budget_limit) if budget_limit is not None else None
        self.byte_limit = float(byte_limit) if byte_limit is not None else None
        self.action = action if isinstance(action, BudgetExceededAction) else BudgetExceededAction(action)
        self.forecast = forecast
        self.registry = registry or RateRegistry()
        self.throttle_worker_ceiling = max(1, int(throttle_worker_ceiling))
        self.throttle_factor = float(throttle_factor)
        self.throttle_delay_seconds = float(throttle_delay_seconds)

        self._actual_spend: float = 0.0
        self._actual_bytes: float = 0.0
        self._spend_by_dimension: Dict[str, float] = {}
        self._usage_by_dimension: Dict[str, float] = {}
        self._is_throttled: bool = False
        self._events: List[Dict[str, Any]] = []

    def actual_spend(self) -> float:
        return self._actual_spend

    def actual_bytes(self) -> float:
        return self._actual_bytes

    def remaining_budget(self) -> Optional[float]:
        if self.budget_limit is None:
            return None
        return max(0.0, self.budget_limit - self._actual_spend)

    def remaining_bytes(self) -> Optional[float]:
        if self.byte_limit is None:
            return None
        return max(0.0, self.byte_limit - self._actual_bytes)

    def is_budget_exceeded(self) -> bool:
        if self.budget_limit is not None and self._actual_spend > self.budget_limit:
            return True
        if self.byte_limit is not None and self._actual_bytes > self.byte_limit:
            return True
        return False

    def is_throttled(self) -> bool:
        return self._is_throttled

    def record_spend(
        self,
        dimension: Union[CostDimension, str],
        amount: float,
        quantity: float = 0.0,
        timestamp: Optional[float] = None,
    ):
        dim = dimension.value if isinstance(dimension, CostDimension) else dimension
        amt = float(amount)
        qty = float(quantity)
        t = timestamp if timestamp is not None else time.time()

        self._actual_spend += amt
        self._spend_by_dimension[dim] = self._spend_by_dimension.get(dim, 0.0) + amt
        if qty > 0.0:
            self._usage_by_dimension[dim] = self._usage_by_dimension.get(dim, 0.0) + qty

        self._events.append({
            "type": "spend",
            "dimension": dim,
            "amount": amt,
            "quantity": qty,
            "timestamp": t,
            "cumulative_spend": self._actual_spend,
        })

        if self.budget_limit is not None and self._actual_spend > self.budget_limit:
            excess = self._actual_spend - self.budget_limit
            if self.action == BudgetExceededAction.ABORT:
                raise BudgetExceededError(
                    f"Budget limit {self.budget_limit:.2f} exceeded by {excess:.2f}: "
                    f"actual spend {self._actual_spend:.2f}",
                    budget_limit=self.budget_limit,
                    actual_spend=self._actual_spend,
                    excess=excess,
                    action=self.action,
                )
            elif self.action == BudgetExceededAction.THROTTLE:
                self._is_throttled = True

    def record_bytes(
        self,
        bytes_count: float,
        dimension: Union[CostDimension, str] = CostDimension.EGRESS_BYTES,
        timestamp: Optional[float] = None,
    ):
        dim = dimension.value if isinstance(dimension, CostDimension) else dimension
        qty = float(bytes_count)
        self._actual_bytes += qty
        self.record_usage(dim, qty, timestamp=timestamp)
        if self.byte_limit is not None and self._actual_bytes > self.byte_limit:
            excess = self._actual_bytes - self.byte_limit
            if self.action == BudgetExceededAction.ABORT:
                raise BudgetExceededError(
                    f"Byte limit {self.byte_limit:g} exceeded by {excess:g}: actual bytes {self._actual_bytes:g}",
                    budget_limit=self.byte_limit,
                    actual_spend=self._actual_bytes,
                    excess=excess,
                    action=self.action,
                )
            elif self.action == BudgetExceededAction.THROTTLE:
                self._is_throttled = True

    def record_usage(
        self,
        dimension: Union[CostDimension, str],
        quantity: float,
        rate: Optional[float] = None,
        timestamp: Optional[float] = None,
        strict: bool = False,
    ):
        dim = dimension.value if isinstance(dimension, CostDimension) else dimension
        qty = float(quantity)
        t = timestamp if timestamp is not None else time.time()

        if rate is not None:
            unit_rate = float(rate)
            spend = qty * unit_rate
        else:
            resolved = self.registry.get(dim, now=t)
            if isinstance(resolved, CostUnknown):
                if strict:
                    raise PriceUnknownError(
                        f"Strict budget enforcement refused unpriced dimension {dim}: {resolved.reason}",
                        dimension=dim,
                        unknown=resolved,
                    )
                spend = 0.0
            else:
                spend = qty * resolved.nominal_rate

        self.record_spend(dim, spend, quantity=qty, timestamp=t)

    def forecast_error(self) -> Dict[str, Any]:
        forecast_total = self.forecast.total_expected_cost if self.forecast else 0.0
        total_error = self._actual_spend - forecast_total
        total_error_pct = (total_error / forecast_total * 100.0) if forecast_total > 0.0 else 0.0

        dim_errors: Dict[str, Dict[str, float]] = {}
        all_dims = set(self._spend_by_dimension.keys())
        if self.forecast:
            all_dims.update(self.forecast.known_items.keys())

        for dim in sorted(all_dims):
            actual_dim_spend = self._spend_by_dimension.get(dim, 0.0)
            forecast_item = self.forecast.get_cost(dim) if self.forecast else None
            forecast_dim_spend = (
                forecast_item.expected_cost if isinstance(forecast_item, CostInterval) else 0.0
            )
            err = actual_dim_spend - forecast_dim_spend
            err_pct = (err / forecast_dim_spend * 100.0) if forecast_dim_spend > 0.0 else 0.0

            dim_errors[dim] = {
                "actual_spend": actual_dim_spend,
                "forecast_spend": forecast_dim_spend,
                "error": err,
                "error_pct": err_pct,
            }

        return {
            "total_actual": self._actual_spend,
            "total_forecasted": forecast_total,
            "total_error": total_error,
            "total_error_pct": total_error_pct,
            "dimensions": dim_errors,
        }

    def apply_throttle_workers(self, requested_workers: int) -> int:
        if not self._is_throttled:
            return requested_workers
        throttled = int(requested_workers * self.throttle_factor)
        return max(self.throttle_worker_ceiling, throttled)

    def apply_throttle_delay(self) -> float:
        if not self._is_throttled:
            return 0.0
        return self.throttle_delay_seconds

    def reset(self):
        self._actual_spend = 0.0
        self._spend_by_dimension.clear()
        self._usage_by_dimension.clear()
        self._is_throttled = False
        self._events.clear()
