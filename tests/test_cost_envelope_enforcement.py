import time
import pytest

from migkit.cost_envelope import (
    BudgetExceededAction,
    BudgetExceededError,
    CostDimension,
    CostEnvelopeError,
    CostEnvelopeForecast,
    CostEnvelopeGovernor,
    CostInterval,
    CostUnknown,
    MigrationWorkloadProfile,
    PlanCostComparison,
    PlanType,
    PriceUnknownError,
    ProofContract,
    ProofContractMismatchError,
    Rate,
    RateRegistry,
    RateSource,
    StaleRateError,
    compare_targeted_refresh_to_full_reload,
    forecast_cost_envelope,
)


def _sample_rate_registry(now: float = 1700000000.0) -> RateRegistry:
    registry = RateRegistry()
    registry.register(
        Rate(
            dimension=CostDimension.SOURCE_COMPUTE,
            nominal_rate=0.50,
            min_rate=0.45,
            max_rate=0.55,
            timestamp=now - 3600,
            source=RateSource.PROVIDER,
            unit="$/hour",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.TARGET_COMPUTE,
            nominal_rate=0.80,
            min_rate=0.72,
            max_rate=0.88,
            timestamp=now - 3600,
            source=RateSource.PROVIDER,
            unit="$/hour",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.EGRESS_BYTES,
            nominal_rate=0.09 / (1024**3),
            min_rate=0.08 / (1024**3),
            max_rate=0.10 / (1024**3),
            timestamp=now - 7200,
            source=RateSource.PROVIDER,
            unit="$/byte",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.STAGING_STORAGE,
            nominal_rate=0.023 / 730.0,
            min_rate=0.020 / 730.0,
            max_rate=0.026 / 730.0,
            timestamp=now - 1800,
            source=RateSource.USER,
            unit="$/GB-hour",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.READ_IOPS,
            nominal_rate=0.000005,
            min_rate=0.0000045,
            max_rate=0.0000055,
            timestamp=now - 600,
            source=RateSource.PROVIDER,
            unit="$/IO",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.WRITE_IOPS,
            nominal_rate=0.000008,
            min_rate=0.000007,
            max_rate=0.000009,
            timestamp=now - 600,
            source=RateSource.PROVIDER,
            unit="$/IO",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.CDC_CHANGE_VOLUME,
            nominal_rate=0.000002,
            min_rate=0.0000018,
            max_rate=0.0000022,
            timestamp=now - 1200,
            source=RateSource.PROVIDER,
            unit="$/change",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.VERIFICATION_PASSES,
            nominal_rate=12.50,
            min_rate=10.00,
            max_rate=15.00,
            timestamp=now - 300,
            source=RateSource.USER,
            unit="$/pass",
        )
    )
    registry.register(
        Rate(
            dimension=CostDimension.IDLE_HEADROOM,
            nominal_rate=0.40,
            min_rate=0.35,
            max_rate=0.45,
            timestamp=now - 300,
            source=RateSource.USER,
            unit="$/hour",
        )
    )
    return registry


def test_forecast_all_nine_operating_cost_dimensions():
    registry = _sample_rate_registry()
    profile = MigrationWorkloadProfile(
        plan_type=PlanType.FULL_RELOAD,
        source_compute_hours=10.0,
        target_compute_hours=12.0,
        egress_bytes=100 * (1024**3),
        staging_storage_gb_hours=500.0,
        read_iops=2_000_000,
        write_iops=3_000_000,
        cdc_change_volume=1_500_000,
        verification_passes=2,
        idle_headroom_hours=48.0,
    )

    contract = ProofContract(
        name="strict-merkle-tier1",
        verification_passes=2,
        verification_mode="merkle_fused",
        data_guarantee="exact_hash",
        tolerance_rows=0,
    )

    forecast = forecast_cost_envelope(profile, registry, proof_contract=contract)

    assert isinstance(forecast, CostEnvelopeForecast)
    assert forecast.is_complete is True
    assert forecast.has_unknowns is False
    assert len(forecast.unknowns) == 0

    all_dims = {
        CostDimension.SOURCE_COMPUTE,
        CostDimension.TARGET_COMPUTE,
        CostDimension.EGRESS_BYTES,
        CostDimension.STAGING_STORAGE,
        CostDimension.READ_IOPS,
        CostDimension.WRITE_IOPS,
        CostDimension.CDC_CHANGE_VOLUME,
        CostDimension.VERIFICATION_PASSES,
        CostDimension.IDLE_HEADROOM,
    }
    for dim in all_dims:
        item = forecast.get_cost(dim)
        assert isinstance(item, CostInterval)
        assert item.min_cost <= item.expected_cost <= item.max_cost
        assert item.expected_cost > 0.0

    assert forecast.total_min_cost <= forecast.total_expected_cost <= forecast.total_max_cost
    assert forecast.total_expected_cost > 0.0
    assert forecast.proof_contract == contract


def test_rates_with_uncertainty_and_timestamped_sources():
    t0 = 1700000000.0
    provider_rate = Rate(
        dimension=CostDimension.SOURCE_COMPUTE,
        nominal_rate=1.20,
        min_rate=1.08,
        max_rate=1.32,
        timestamp=t0,
        source=RateSource.PROVIDER,
        unit="$/hour",
    )
    assert provider_rate.source == RateSource.PROVIDER
    assert provider_rate.uncertainty_bounds() == (1.08, 1.20, 1.32)
    assert provider_rate.is_stale(now=t0 + 3600, max_age_seconds=86400) is False
    assert provider_rate.is_stale(now=t0 + 100000, max_age_seconds=86400) is True

    user_rate = Rate(
        dimension=CostDimension.IDLE_HEADROOM,
        nominal_rate=0.50,
        timestamp=t0,
        source=RateSource.USER,
        unit="$/hour",
    )
    assert user_rate.source == RateSource.USER
    b_min, b_nom, b_max = user_rate.uncertainty_bounds()
    assert b_min <= b_nom <= b_max


def test_unknown_costs_declared_explicitly_never_zero():
    registry = RateRegistry()
    registry.register(
        Rate(
            dimension=CostDimension.SOURCE_COMPUTE,
            nominal_rate=1.00,
            timestamp=1700000000.0,
            source=RateSource.USER,
        )
    )

    profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=5.0,
        target_compute_hours=5.0,
        egress_bytes=10 * (1024**3),
        staging_storage_gb_hours=50.0,
        read_iops=100_000,
        write_iops=100_000,
        cdc_change_volume=50_000,
        verification_passes=1,
        idle_headroom_hours=12.0,
    )

    forecast = forecast_cost_envelope(profile, registry)
    assert forecast.is_complete is False
    assert forecast.has_unknowns is True
    assert len(forecast.unknowns) == 8

    egress_cost = forecast.get_cost(CostDimension.EGRESS_BYTES)
    assert isinstance(egress_cost, CostUnknown)
    assert egress_cost.dimension == CostDimension.EGRESS_BYTES
    assert egress_cost.quantity == 10 * (1024**3)
    assert "No rate registered" in egress_cost.reason

    source_cost = forecast.get_cost(CostDimension.SOURCE_COMPUTE)
    assert isinstance(source_cost, CostInterval)
    assert source_cost.expected_cost == 5.00


def test_stale_community_rate_rejected_as_cost_unknown():
    registry = RateRegistry()
    stale_timestamp = 1600000000.0
    now = 1700000000.0

    registry.register(
        Rate(
            dimension=CostDimension.EGRESS_BYTES,
            nominal_rate=0.09 / (1024**3),
            timestamp=stale_timestamp,
            source=RateSource.COMMUNITY,
            max_age_seconds=86400 * 30,
        )
    )

    resolved = registry.get(CostDimension.EGRESS_BYTES, now=now, allow_stale_community=False)
    assert isinstance(resolved, CostUnknown)
    assert "Stale community rate" in resolved.reason
    assert "rejected as current pricing" in resolved.reason


def test_plan_comparison_under_identical_proof_contract():
    now = 1700000000.0
    registry = _sample_rate_registry(now)

    contract = ProofContract(
        name="contract-fused-audit",
        verification_passes=2,
        verification_mode="merkle_fused",
        data_guarantee="exact_hash",
        tolerance_rows=0,
    )

    targeted_profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=3.0,
        target_compute_hours=4.0,
        egress_bytes=5 * (1024**3),
        staging_storage_gb_hours=50.0,
        read_iops=200_000,
        write_iops=300_000,
        cdc_change_volume=200_000,
        verification_passes=2,
        idle_headroom_hours=24.0,
    )

    full_reload_profile = MigrationWorkloadProfile(
        plan_type=PlanType.FULL_RELOAD,
        source_compute_hours=20.0,
        target_compute_hours=25.0,
        egress_bytes=500 * (1024**3),
        staging_storage_gb_hours=2500.0,
        read_iops=10_000_000,
        write_iops=15_000_000,
        cdc_change_volume=5_000_000,
        verification_passes=2,
        idle_headroom_hours=48.0,
    )

    comparison = compare_targeted_refresh_to_full_reload(
        targeted_profile, full_reload_profile, registry, contract
    )

    assert isinstance(comparison, PlanCostComparison)
    assert comparison.proof_contract == contract
    assert comparison.savings_expected > 0.0
    assert comparison.targeted_forecast.total_expected_cost < comparison.full_reload_forecast.total_expected_cost
    assert comparison.recommended_strategy == PlanType.TARGETED_REFRESH
    assert comparison.cost_ratio < 0.5


def test_plan_comparison_mismatched_proof_contract_refused():
    now = 1700000000.0
    registry = _sample_rate_registry(now)

    targeted_profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=2.0,
        target_compute_hours=2.0,
        verification_passes=1,
    )
    full_reload_profile = MigrationWorkloadProfile(
        plan_type=PlanType.FULL_RELOAD,
        source_compute_hours=10.0,
        target_compute_hours=10.0,
        verification_passes=3,
    )

    contract1 = ProofContract(name="contract-1", verification_passes=1)
    contract2 = ProofContract(name="contract-2", verification_passes=3)

    targeted_forecast = forecast_cost_envelope(targeted_profile, registry, proof_contract=contract1)
    full_forecast = forecast_cost_envelope(full_reload_profile, registry, proof_contract=contract2)

    with pytest.raises(ProofContractMismatchError):
        PlanCostComparison(
            proof_contract=contract1,
            targeted_forecast=targeted_forecast,
            full_reload_forecast=full_forecast,
        )


def test_governor_hard_spending_limit_abort():
    registry = _sample_rate_registry()
    profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=4.0,
        target_compute_hours=4.0,
        egress_bytes=10 * (1024**3),
    )
    forecast = forecast_cost_envelope(profile, registry)

    governor = CostEnvelopeGovernor(
        budget_limit=25.00,
        action=BudgetExceededAction.ABORT,
        forecast=forecast,
        registry=registry,
    )

    governor.record_spend(CostDimension.SOURCE_COMPUTE, 10.00, quantity=20.0)
    assert governor.actual_spend() == 10.00
    assert governor.remaining_budget() == 15.00
    assert governor.is_budget_exceeded() is False

    governor.record_spend(CostDimension.TARGET_COMPUTE, 12.00, quantity=15.0)
    assert governor.actual_spend() == 22.00
    assert governor.remaining_budget() == 3.00

    with pytest.raises(BudgetExceededError) as exc_info:
        governor.record_spend(CostDimension.EGRESS_BYTES, 5.00, quantity=50 * (1024**3))

    err = exc_info.value
    assert err.budget_limit == 25.00
    assert err.actual_spend == 27.00
    assert err.excess == 2.00
    assert err.action == BudgetExceededAction.ABORT


def test_governor_hard_spending_limit_throttle():
    registry = _sample_rate_registry()
    profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=5.0,
        target_compute_hours=5.0,
    )
    forecast = forecast_cost_envelope(profile, registry)

    governor = CostEnvelopeGovernor(
        budget_limit=20.00,
        action=BudgetExceededAction.THROTTLE,
        forecast=forecast,
        registry=registry,
        throttle_worker_ceiling=2,
    )

    assert governor.is_throttled() is False
    assert governor.apply_throttle_workers(16) == 16
    assert governor.apply_throttle_delay() == 0.0

    governor.record_spend(CostDimension.SOURCE_COMPUTE, 15.00, quantity=30.0)
    assert governor.is_throttled() is False

    governor.record_spend(CostDimension.TARGET_COMPUTE, 10.00, quantity=12.5)
    assert governor.actual_spend() == 25.00
    assert governor.is_budget_exceeded() is True
    assert governor.is_throttled() is True

    throttled_workers = governor.apply_throttle_workers(16)
    assert throttled_workers <= 4
    assert throttled_workers >= 2

    delay = governor.apply_throttle_delay()
    assert delay > 0.0


def test_governor_records_actual_spend_and_forecast_error():
    registry = _sample_rate_registry()
    profile = MigrationWorkloadProfile(
        plan_type=PlanType.TARGETED_REFRESH,
        source_compute_hours=10.0,
        target_compute_hours=10.0,
        egress_bytes=10 * (1024**3),
        staging_storage_gb_hours=100.0,
        read_iops=100_000,
        write_iops=100_000,
        cdc_change_volume=100_000,
        verification_passes=1,
        idle_headroom_hours=10.0,
    )
    forecast = forecast_cost_envelope(profile, registry)

    governor = CostEnvelopeGovernor(
        budget_limit=1000.00,
        action=BudgetExceededAction.ABORT,
        forecast=forecast,
        registry=registry,
    )

    governor.record_usage(CostDimension.SOURCE_COMPUTE, 12.0)
    governor.record_usage(CostDimension.TARGET_COMPUTE, 8.0)
    governor.record_usage(CostDimension.VERIFICATION_PASSES, 1.0)

    err_report = governor.forecast_error()
    assert "total_actual" in err_report
    assert "total_forecasted" in err_report
    assert "total_error" in err_report
    assert "dimensions" in err_report

    src_err = err_report["dimensions"][CostDimension.SOURCE_COMPUTE]
    assert src_err["actual_spend"] == pytest.approx(12.0 * 0.50, rel=1e-3)
    assert src_err["forecast_spend"] == pytest.approx(10.0 * 0.50, rel=1e-3)
    assert src_err["error"] > 0.0


def test_governor_price_unknown_strict_enforcement():
    empty_registry = RateRegistry()
    governor = CostEnvelopeGovernor(
        budget_limit=50.00,
        action=BudgetExceededAction.ABORT,
        registry=empty_registry,
    )

    with pytest.raises(PriceUnknownError) as exc_info:
        governor.record_usage(CostDimension.EGRESS_BYTES, 1024**3, strict=True)

    assert exc_info.value.dimension == CostDimension.EGRESS_BYTES
    assert isinstance(exc_info.value.unknown, CostUnknown)


def test_governor_byte_limit_independent_of_price():
    empty_registry = RateRegistry()
    governor = CostEnvelopeGovernor(
        byte_limit=100 * 1024**2,  # 100 MB byte budget with no price / money limit
        action=BudgetExceededAction.ABORT,
        registry=empty_registry,
    )
    assert governor.remaining_bytes() == 100 * 1024**2
    assert governor.remaining_budget() is None

    # Record 60 MB: within budget
    governor.record_bytes(60 * 1024**2)
    assert governor.actual_bytes() == 60 * 1024**2
    assert governor.remaining_bytes() == 40 * 1024**2
    assert not governor.is_budget_exceeded()

    # Record 50 MB: exceeds 100 MB ceiling (total 110 MB)
    with pytest.raises(BudgetExceededError) as exc_info:
        governor.record_bytes(50 * 1024**2)

    assert "Byte limit" in str(exc_info.value)
    assert exc_info.value.excess == 10 * 1024**2
    assert governor.is_budget_exceeded()


def test_cli_copy_tables_in_enforces_byte_limit_ceiling(tmp_path):
    from migkit import cli

    class FakeHop:
        name = "test-hop"
        options = {
            "limits": {
                "bytes_billed": 100 * 1024**2,
            }
        }
        def report_dir(self, db=None):
            return tmp_path

    class FakeEngine:
        def __init__(self, hop):
            self.hop = hop
            self.src_engine = self
        def table_facts(self, side, db):
            return {
                "public.t1": {"bytes": 60 * 1024**2, "rows": 1000},
                "public.t2": {"bytes": 50 * 1024**2, "rows": 1000},
            }
        def move_table(self, d, sch, t, chunk, ck, log):
            pass
        def through_pair(self, d, name):
            return False
        def move_key(self, d, sch, t):
            return f"{d}.{sch}.{t}"
        def session_health(self):
            return ()

    hop = FakeHop()
    eng = FakeEngine(hop)
    tables = [("public", "t1"), ("public", "t2")]

    # 1 worker test
    with pytest.raises(SystemExit) as exc_info:
        cli._copy_tables_in(hop, eng, "testdb", tables, chunk=1000, ck={}, workers=1)
    assert "ACTION: move halted at hard limit ceiling" in str(exc_info.value)
    assert "Byte limit" in str(exc_info.value)

    # 2 workers test
    with pytest.raises(SystemExit) as exc_info2:
        cli._copy_tables_in(hop, eng, "testdb", tables, chunk=1000, ck={}, workers=2)
    assert "ACTION: move halted at hard limit ceiling" in str(exc_info2.value)
    assert "Byte limit" in str(exc_info2.value)


def test_cli_copy_tables_in_enforces_money_limit_ceiling(tmp_path):
    from migkit import cli

    class FakeHop:
        name = "test-hop"
        options = {
            "limits": {
                "money": 5.0,  # $5 max budget
            },
            "transfer_price_per_gb": 0.10,  # $0.10 per GB
        }
        def report_dir(self, db=None):
            return tmp_path

    class FakeEngine:
        def __init__(self, hop):
            self.hop = hop
            self.src_engine = self
        def table_facts(self, side, db):
            return {
                "public.t1": {"bytes": 40 * 1024**3, "rows": 1000},  # $4.00
                "public.t2": {"bytes": 20 * 1024**3, "rows": 1000},  # $2.00 -> total $6.00 > $5.00
            }
        def move_table(self, d, sch, t, chunk, ck, log):
            pass
        def through_pair(self, d, name):
            return False
        def move_key(self, d, sch, t):
            return f"{d}.{sch}.{t}"
        def session_health(self):
            return ()

    hop = FakeHop()
    eng = FakeEngine(hop)
    tables = [("public", "t1"), ("public", "t2")]

    with pytest.raises(SystemExit) as exc_info:
        cli._copy_tables_in(hop, eng, "testdb", tables, chunk=1000, ck={}, workers=1)
    assert "ACTION: move halted at hard limit ceiling" in str(exc_info.value)
    assert "Budget limit" in str(exc_info.value)


