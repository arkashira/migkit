import json
import pytest

from migkit.demand_benchmark import (
    BenchmarkFault,
    BenchmarkScorecard,
    BlockerRecord,
    BlockerSeverity,
    CompoundFault,
    DemandBenchmarkHarness,
    EndpointConfig,
    FaultType,
    InterventionRecord,
    LockContentionFault,
    LockType,
    MigrationPhase,
    NetworkFlapFault,
    PartialAccessFault,
    SchemaShiftFault,
    ShiftType,
    TableSpec,
    ThrottledIopsFault,
    UnsafeEndpointError,
    WorkloadSpec,
)


def make_safe_endpoints():
    src = EndpointConfig(
        engine="postgres",
        host="127.0.0.1",
        port=5432,
        database="source_db",
        username="bench_src",
    )
    tgt = EndpointConfig(
        engine="mysql",
        host="10.0.0.1",
        port=3306,
        database="target_db",
        username="bench_tgt",
    )
    return src, tgt


def make_standard_workload():
    users = TableSpec(
        name="users",
        primary_key="id",
        columns={"id": "BIGINT", "name": "VARCHAR(255)", "email": "VARCHAR(255)"},
        row_count=500,
    )
    orders = TableSpec(
        name="orders",
        primary_key="id",
        columns={"id": "BIGINT", "user_id": "BIGINT", "amount": "DECIMAL(10,2)"},
        row_count=1000,
        foreign_keys={"user_id": "users.id"},
    )
    audit = TableSpec(
        name="audit_logs",
        primary_key="log_id",
        columns={"log_id": "BIGINT", "event": "VARCHAR(100)", "ts": "BIGINT"},
        row_count=500,
    )
    return WorkloadSpec(tables=[users, orders, audit], batch_size=100)


def test_endpoint_ip_safety_validation():
    EndpointConfig(
        engine="postgres",
        host="127.0.0.1",
        port=5432,
        database="db",
        username="usr",
    )
    EndpointConfig(
        engine="postgres",
        host="10.0.0.25",
        port=5432,
        database="db",
        username="usr",
    )
    EndpointConfig(
        engine="postgres",
        host="localhost",
        port=5432,
        database="db",
        username="usr",
    )

    with pytest.raises(UnsafeEndpointError):
        EndpointConfig(
            engine="postgres",
            host="192.168.1.1",
            port=5432,
            database="db",
            username="usr",
        )

    with pytest.raises(UnsafeEndpointError):
        EndpointConfig(
            engine="postgres",
            host="8.8.8.8",
            port=5432,
            database="db",
            username="usr",
        )


def test_baseline_clean_run_without_faults():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        seed=123,
    )

    scorecard = harness.execute()

    assert scorecard.engine_pair == ("postgres", "mysql")
    assert scorecard.verified_completion_rate == 1.0
    assert scorecard.total_interventions == 0
    assert len(scorecard.honest_blockers) == 0
    assert scorecard.waste.wasted_rows == 0
    assert scorecard.waste.wasted_bytes == 0
    assert scorecard.schema_metrics.statements_succeeded == 3
    assert scorecard.schema_metrics.statements_failed == 0
    assert scorecard.relocation_metrics.rows_verified == 2000
    assert scorecard.success is True


def test_network_flap_simulation():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    flap_fault = NetworkFlapFault(
        disconnect_count=4,
        reconnect_delay_ms=150.0,
        packet_loss_ratio=0.1,
        drop_inflight_batches=True,
    )
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[flap_fault],
        seed=42,
    )

    scorecard = harness.execute()

    assert scorecard.relocation_metrics.reconnect_count >= 4
    assert scorecard.relocation_metrics.retry_count >= 4
    assert scorecard.waste.wasted_rows > 0
    assert scorecard.waste.wasted_bytes > 0
    assert scorecard.waste.waste_ratio > 0.0
    assert scorecard.source_impact.latency_inflation_pct > 0.0
    assert scorecard.verified_completion_rate == 1.0


def test_transient_lock_contention_simulation():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    lock_fault = LockContentionFault(
        lock_type=LockType.EXCLUSIVE_TABLE,
        wait_duration_ms=400.0,
        deadlock_frequency=0.2,
        lock_wait_timeout_ms=1000.0,
        affected_tables=["orders"],
    )
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[lock_fault],
        seed=77,
    )

    scorecard = harness.execute()

    assert scorecard.schema_metrics.lock_contention_events > 0 or scorecard.source_impact.max_lock_hold_duration_ms > 0
    assert scorecard.source_impact.max_lock_hold_duration_ms >= 400.0
    assert scorecard.source_impact.latency_inflation_pct > 0.0
    assert scorecard.verified_completion_rate == 1.0


def test_schema_shift_unresolved_demands_manual_intervention():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    shift_fault = SchemaShiftFault(
        shift_type=ShiftType.ADD_COLUMN_NO_DEFAULT,
        target_table="orders",
        column_name="shipping_tier",
        new_type="VARCHAR(64)",
        breaking=True,
    )

    harness_auto = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[shift_fault],
        autonomous_mode=True,
        seed=101,
    )
    scorecard_auto = harness_auto.execute()

    assert scorecard_auto.total_interventions >= 1
    assert any(i.cause == "unresolved_schema_shift" for i in scorecard_auto.interventions)
    assert any(b.code == "UNRESOLVED_BREAKING_SCHEMA_SHIFT" for b in scorecard_auto.honest_blockers)
    assert scorecard_auto.verified_completion_rate < 1.0
    assert scorecard_auto.success is False

    harness_assisted = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[shift_fault],
        autonomous_mode=False,
        seed=101,
    )
    scorecard_assisted = harness_assisted.execute()

    assert scorecard_assisted.total_interventions >= 1
    assert all(i.resolved for i in scorecard_assisted.interventions)
    assert scorecard_assisted.verified_completion_rate == 1.0
    assert scorecard_assisted.success is True


def test_throttled_iops_resource_exhaustion():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    iops_fault = ThrottledIopsFault(
        burst_limit=200,
        sustained_limit=50,
        exhaustion_threshold_ops=300,
        throttle_latency_multiplier=3.5,
    )
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[iops_fault],
        seed=999,
    )

    scorecard = harness.execute()

    assert scorecard.relocation_metrics.throttled_seconds > 0.0
    assert scorecard.source_impact.latency_inflation_pct >= 200.0
    assert scorecard.source_impact.iops_consumed >= 300
    assert scorecard.relocation_metrics.relocation_cost_usd > 0.0


def test_partial_access_honest_blockers_and_coverage():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    access_fault = PartialAccessFault(
        restricted_tables=["audit_logs"],
        missing_privileges=["SELECT_AUDIT_LOGS"],
    )
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[access_fault],
        autonomous_mode=True,
        seed=55,
    )

    scorecard = harness.execute()

    assert any(b.code == "RESTRICTED_ENDPOINT_ACCESS" for b in scorecard.honest_blockers)
    assert any("audit_logs" in b.affected_entity for b in scorecard.honest_blockers)
    assert scorecard.total_interventions >= 1
    assert scorecard.verified_completion_rate == pytest.approx(1500 / 2000, 0.01)
    assert scorecard.relocation_metrics.rows_verified == 1500
    assert scorecard.success is False


def test_compound_fault_scenario():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    compound = CompoundFault(
        faults=[
            NetworkFlapFault(disconnect_count=2, reconnect_delay_ms=100.0),
            LockContentionFault(wait_duration_ms=250.0, affected_tables=["orders"]),
            ThrottledIopsFault(exhaustion_threshold_ops=200),
        ]
    )
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=[compound],
        seed=888,
    )

    scorecard = harness.execute()

    assert scorecard.relocation_metrics.reconnect_count >= 2
    assert scorecard.waste.wasted_rows > 0
    assert scorecard.source_impact.max_lock_hold_duration_ms >= 250.0
    assert scorecard.relocation_metrics.throttled_seconds > 0.0
    assert scorecard.verified_completion_rate == 1.0
    assert scorecard.total_cost_usd > 0.0


def test_compound_fault_all_five_scenarios_separates_phases():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    all_faults = [
        NetworkFlapFault(disconnect_count=3, phase=MigrationPhase.DATABASE_RELOCATION),
        LockContentionFault(
            lock_type=LockType.DDL_METADATA_LOCK,
            wait_duration_ms=350.0,
            phase=MigrationPhase.SCHEMA_DEPLOYMENT,
        ),
        SchemaShiftFault(
            shift_type=ShiftType.ADD_COLUMN_NO_DEFAULT,
            target_table="users",
            breaking=True,
            phase=MigrationPhase.SCHEMA_DEPLOYMENT,
        ),
        ThrottledIopsFault(exhaustion_threshold_ops=150, phase=MigrationPhase.DATABASE_RELOCATION),
        PartialAccessFault(restricted_tables=["audit_logs"], phase=MigrationPhase.DATABASE_RELOCATION),
    ]

    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=all_faults,
        autonomous_mode=True,
        seed=4321,
    )

    scorecard = harness.execute()

    assert scorecard.schema_metrics.lock_contention_events > 0
    assert scorecard.schema_metrics.schema_shifts_detected >= 1
    assert any(b.phase == MigrationPhase.SCHEMA_DEPLOYMENT for b in scorecard.honest_blockers)

    assert scorecard.relocation_metrics.reconnect_count >= 3
    assert scorecard.relocation_metrics.throttled_seconds > 0.0
    assert any(b.phase == MigrationPhase.DATABASE_RELOCATION for b in scorecard.honest_blockers)

    assert scorecard.total_interventions >= 2
    assert scorecard.verified_completion_rate < 1.0
    assert scorecard.success is False


def test_scorecard_reproducibility_and_serialization():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    faults = [NetworkFlapFault(disconnect_count=2)]

    h1 = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=faults,
        seed=777,
    )
    s1 = h1.execute()

    h2 = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=faults,
        seed=777,
    )
    s2 = h2.execute()

    assert s1.fault_manifest_hash == s2.fault_manifest_hash
    assert s1.verified_completion_rate == s2.verified_completion_rate
    assert s1.waste.wasted_rows == s2.waste.wasted_rows
    assert s1.total_cost_usd == s2.total_cost_usd

    d = s1.to_dict()
    assert d["engine_pair"] == ["postgres", "mysql"]
    assert d["fault_manifest_hash"] == s1.fault_manifest_hash
    raw_json = s1.to_json()
    assert json.loads(raw_json)["run_id"] == s1.run_id

    summary = s1.render_summary()
    assert "POSTGRES -> MYSQL" in summary.upper()
    assert "COMPLETION" in summary.upper()
    assert "INTERVENTIONS" in summary.upper()


def test_anti_fraud_audit_flags_dishonest_claims():
    src, tgt = make_safe_endpoints()
    workload = make_standard_workload()
    faults = [
        SchemaShiftFault(breaking=True, target_table="orders"),
        PartialAccessFault(restricted_tables=["audit_logs"]),
        NetworkFlapFault(disconnect_count=3),
    ]
    harness = DemandBenchmarkHarness(
        engine_pair=("postgres", "mysql"),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=faults,
        autonomous_mode=True,
        seed=333,
    )
    scorecard = harness.execute()

    fraudulent_claims_1 = {
        "claimed_interventions": 0,
        "claimed_completion_rate": 1.0,
        "claimed_waste_rows": 0,
    }
    is_honest_1, discrepancies_1 = scorecard.validate_claims(fraudulent_claims_1)
    assert is_honest_1 is False
    assert any("zero-intervention" in disc for disc in discrepancies_1)
    assert any("completion" in disc for disc in discrepancies_1)
    assert any("waste" in disc for disc in discrepancies_1)

    honest_claims = {
        "claimed_interventions": scorecard.total_interventions,
        "claimed_completion_rate": scorecard.verified_completion_rate,
        "claimed_waste_rows": scorecard.waste.wasted_rows,
    }
    is_honest_2, discrepancies_2 = scorecard.validate_claims(honest_claims)
    assert is_honest_2 is True
    assert len(discrepancies_2) == 0


def test_multiple_engine_pairs():
    pairs = [
        ("postgres", "mysql"),
        ("mysql", "postgres"),
        ("oracle", "postgres"),
        ("sqlserver", "postgres"),
        ("postgres", "yugabyte"),
    ]
    workload = make_standard_workload()
    for src_eng, tgt_eng in pairs:
        src = EndpointConfig(engine=src_eng, host="127.0.0.1", port=5432, database="sdb", username="u")
        tgt = EndpointConfig(engine=tgt_eng, host="10.0.0.2", port=3306, database="tdb", username="u")
        harness = DemandBenchmarkHarness(
            engine_pair=(src_eng, tgt_eng),
            source_endpoint=src,
            target_endpoint=tgt,
            workload=workload,
            faults=[NetworkFlapFault(disconnect_count=1)],
            seed=42,
        )
        scorecard = harness.execute()
        assert scorecard.engine_pair == (src_eng, tgt_eng)
        assert scorecard.verified_completion_rate == 1.0
