import dataclasses
import datetime
import decimal
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from click.testing import CliRunner
import pytest

from migkit import autopilot as ap
from migkit import cli, decide
from migkit.progress_liveness import LeaseConflictError
from tests import rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("autopilot", "autopilot_admission_rungs", "admit_phase_preconditions_met", "holds"),
    ("autopilot", "autopilot_admission_rungs", "admit_phase_preconditions_met", "refuses"),
    ("autopilot", "autopilot_admission_rungs", "skip_optional_phase_requested", "holds"),
    ("autopilot", "autopilot_admission_rungs", "skip_optional_phase_requested", "refuses"),
    ("autopilot", "autopilot_admission_rungs", "refuse_phase_unmet_preconditions", "holds"),
    ("autopilot", "autopilot_admission_rungs", "refuse_phase_unmet_preconditions", "refuses"),
    ("autopilot", "autopilot_cutover_gate_rungs", "abort_cutover_data_divergence", "holds"),
    ("autopilot", "autopilot_cutover_gate_rungs", "abort_cutover_data_divergence", "refuses"),
    ("autopilot", "autopilot_cutover_gate_rungs", "certify_zero_drift_cutover_admit", "holds"),
    ("autopilot", "autopilot_cutover_gate_rungs", "certify_zero_drift_cutover_admit", "refuses"),
    ("autopilot", "autopilot_cutover_gate_rungs", "pause_for_manual_cutover_consent", "holds"),
    ("autopilot", "autopilot_cutover_gate_rungs", "pause_for_manual_cutover_consent", "refuses"),
    ("autopilot", "autopilot_reverse_rungs", "prewarm_reverse_cdc_stream", "holds"),
    ("autopilot", "autopilot_reverse_rungs", "prewarm_reverse_cdc_stream", "refuses"),
    ("autopilot", "autopilot_reverse_rungs", "refuse_reverse_unsupported", "holds"),
    ("autopilot", "autopilot_reverse_rungs", "refuse_reverse_unsupported", "refuses"),
    ("autopilot", "autopilot_strategy_rungs", "execute_dry_run_rehearsal", "holds"),
    ("autopilot", "autopilot_strategy_rungs", "execute_dry_run_rehearsal", "refuses"),
    ("autopilot", "autopilot_strategy_rungs", "execute_recovery_or_abort", "holds"),
    ("autopilot", "autopilot_strategy_rungs", "execute_recovery_or_abort", "refuses"),
    ("autopilot", "autopilot_strategy_rungs", "execute_turnkey_production_pipeline", "holds"),
    ("autopilot", "autopilot_strategy_rungs", "execute_turnkey_production_pipeline", "refuses"),
]


@dataclasses.dataclass
class DummyHop:
    name: str = "prod_migration"
    target_host: str = "127.0.0.1"
    target_port: int = 5432
    target_db: str = "mydb"
    source_host: str = "127.0.0.1"
    source_port: int = 5433
    source_db: str = "sourcedb"
    bastion: Optional[str] = None
    reverse_retention_hours: float = 48.0
    timeout: float = 3600.0
    options: Dict[str, Any] = dataclasses.field(default_factory=dict)


class DeterministicTunnelManager:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.closed = False
        self.bound = False

    def bind_hop(self, hop: Any) -> Any:
        if self.should_fail:
            raise RuntimeError("tunnel connection failed")
        self.bound = True
        return type("Tunnel", (), {"local_port": 15432})()

    def close(self) -> None:
        self.closed = True


class DeterministicMover:
    def __init__(self, tables: int = 5, should_fail: bool = False):
        self.tables = tables
        self.should_fail = should_fail

    def copy_tables(self) -> int:
        if self.should_fail:
            raise RuntimeError("bulk copy failed")
        return self.tables


class DeterministicCDCEngine:
    def __init__(self, drained: int = 250, should_fail: bool = False):
        self.drained = drained
        self.should_fail = should_fail

    def catchup(self) -> int:
        if self.should_fail:
            raise RuntimeError("cdc catchup failed")
        return self.drained


class DeterministicMerkleFolder:
    def __init__(self, in_sync: bool = True):
        self.in_sync = in_sync

    def verify_parity(self) -> Dict[str, Any]:
        return {"in_sync": self.in_sync, "drifted_tables": 0 if self.in_sync else 2}


class DeterministicFlightSim:
    def __init__(self, sla_verdict: str = "PASS", should_fail: bool = False):
        self.sla_verdict = sla_verdict
        self.should_fail = should_fail

    def run_rehearsal(self) -> Any:
        if self.should_fail:
            raise RuntimeError("flight sim rehearsal failed")
        return type(
            "Certificate",
            (),
            {
                "sla_verdict": self.sla_verdict,
                "to_dict": lambda s: {"readiness": "ready" if self.sla_verdict == "PASS" else "unready"},
            },
        )()

    def run(self) -> Any:
        return self.run_rehearsal()

    def auto_rollback_to_savepoint(self) -> float:
        return 0.05


class DeterministicPoolerCoordinator:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.switched = False

    def shift_traffic(self, host: str, port: int) -> bool:
        if self.should_fail:
            raise RuntimeError("cutover traffic shift failed")
        self.switched = True
        return True


class DeterministicReverseLoop:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.prewarmed = False

    def pre_warm(self, *args: Any, **kwargs: Any) -> None:
        if self.should_fail:
            raise RuntimeError("reverse prewarm failed")
        self.prewarmed = True

    def start(self) -> None:
        pass


class DeterministicTuner:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.bulk_applied = False
        self.runtime_applied = False

    def apply_bulk(self, target_eng: Any, db: Any = None) -> Any:
        if self.should_fail:
            raise RuntimeError("bulk tune failed")
        self.bulk_applied = True
        return type("TuningResult", (), {"applied": ["work_mem=64MB", "synchronous_commit=off"]})()

    def apply_runtime(self, target_eng: Any, db: Any = None) -> Any:
        if self.should_fail:
            raise RuntimeError("runtime tune restore failed")
        self.runtime_applied = True
        return type("TuningResult", (), {"applied": ["work_mem=16MB", "synchronous_commit=on"]})()


class DeterministicSchemaLifecycle:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.dropped = False
        self.rebuilt = False

    def drop_non_unique_indexes(self) -> List[str]:
        if self.should_fail:
            raise RuntimeError("index drop failed")
        self.dropped = True
        return ["idx_orders_customer_id", "idx_items_product_id"]

    def rebuild_indexes(self) -> List[str]:
        if self.should_fail:
            raise RuntimeError("index rebuild failed")
        self.rebuilt = True
        return ["idx_orders_customer_id", "idx_items_product_id"]

    def restore_foreign_keys(self) -> List[str]:
        return ["fk_orders_customer"]


def test_autopilot_decide_rungs_exercised_on_both_branches():
    with watched() as seen:
        # 1. Admission ladder branches:
        # 1a. Admit holds; skip refuses; refuse refuses
        c1 = decide.climb("test:work", ap.autopilot_admission_rungs(), {"preconditions_met": True, "skip_requested": False})
        assert c1.rung.name == "admit_phase_preconditions_met"

        # 1b. Skip holds; admit refuses; refuse refuses
        c2 = decide.climb("test:work", ap.autopilot_admission_rungs(), {"preconditions_met": True, "skip_requested": True, "phase_optional": True})
        assert c2.rung.name == "skip_optional_phase_requested"

        # 1c. Refuse holds; admit refuses; skip refuses
        c3 = decide.climb("test:work", ap.autopilot_admission_rungs(), {"preconditions_met": False, "skip_requested": False, "unmet_preconditions": True})
        assert c3.rung.name == "refuse_phase_unmet_preconditions"

        # 2. Strategy ladder branches:
        # 2a. Turnkey holds; dry_run refuses; recovery refuses
        s1 = decide.climb("test:strat", ap.autopilot_strategy_rungs(), {"dry_run": False, "auto_approve": True})
        assert s1.rung.name == "execute_turnkey_production_pipeline"

        # 2b. Dry run holds; turnkey refuses; recovery refuses
        s2 = decide.climb("test:strat", ap.autopilot_strategy_rungs(), {"dry_run": True})
        assert s2.rung.name == "execute_dry_run_rehearsal"

        # 2c. Recovery holds; turnkey refuses; dry_run refuses
        s3 = decide.climb("test:strat", ap.autopilot_strategy_rungs(), {"dry_run": False, "auto_approve": False, "recovery_requested": True})
        assert s3.rung.name == "execute_recovery_or_abort"

        # 3. Cutover gate ladder branches:
        # 3a. Certify holds; pause refuses; abort refuses
        g1 = decide.climb("test:gate", ap.autopilot_cutover_gate_rungs(), {"zero_drift": True, "auto_approve": True})
        assert g1.rung.name == "certify_zero_drift_cutover_admit"

        # 3b. Pause holds; certify refuses; abort refuses
        g2 = decide.climb("test:gate", ap.autopilot_cutover_gate_rungs(), {"zero_drift": True, "auto_approve": False, "operator_consented": False})
        assert g2.rung.name == "pause_for_manual_cutover_consent"

        # 3c. Abort holds; certify refuses; pause refuses
        g3 = decide.climb("test:gate", ap.autopilot_cutover_gate_rungs(), {"zero_drift": False, "data_divergence": True})
        assert g3.rung.name == "abort_cutover_data_divergence"

        # 4. Reverse ladder branches:
        # 4a. Prewarm holds; refuse refuses
        r1 = decide.climb("test:rev", ap.autopilot_reverse_rungs(), {"reverse_supported": True, "dry_run": False})
        assert r1.rung.name == "prewarm_reverse_cdc_stream"

        # 4b. Refuse holds; prewarm refuses
        r2 = decide.climb("test:rev", ap.autopilot_reverse_rungs(), {"reverse_supported": False, "dry_run": False})
        assert r2.rung.name == "refuse_reverse_unsupported"

    # Verify that all 11 rungs reached both HOLDS and REFUSES
    for ladder_name, rung_name, branch in [
        ("autopilot_admission_rungs", "admit_phase_preconditions_met", "holds"),
        ("autopilot_admission_rungs", "admit_phase_preconditions_met", "refuses"),
        ("autopilot_admission_rungs", "skip_optional_phase_requested", "holds"),
        ("autopilot_admission_rungs", "skip_optional_phase_requested", "refuses"),
        ("autopilot_admission_rungs", "refuse_phase_unmet_preconditions", "holds"),
        ("autopilot_admission_rungs", "refuse_phase_unmet_preconditions", "refuses"),
        ("autopilot_strategy_rungs", "execute_turnkey_production_pipeline", "holds"),
        ("autopilot_strategy_rungs", "execute_turnkey_production_pipeline", "refuses"),
        ("autopilot_strategy_rungs", "execute_dry_run_rehearsal", "holds"),
        ("autopilot_strategy_rungs", "execute_dry_run_rehearsal", "refuses"),
        ("autopilot_strategy_rungs", "execute_recovery_or_abort", "holds"),
        ("autopilot_strategy_rungs", "execute_recovery_or_abort", "refuses"),
        ("autopilot_cutover_gate_rungs", "certify_zero_drift_cutover_admit", "holds"),
        ("autopilot_cutover_gate_rungs", "certify_zero_drift_cutover_admit", "refuses"),
        ("autopilot_cutover_gate_rungs", "pause_for_manual_cutover_consent", "holds"),
        ("autopilot_cutover_gate_rungs", "pause_for_manual_cutover_consent", "refuses"),
        ("autopilot_cutover_gate_rungs", "abort_cutover_data_divergence", "holds"),
        ("autopilot_cutover_gate_rungs", "abort_cutover_data_divergence", "refuses"),
        ("autopilot_reverse_rungs", "prewarm_reverse_cdc_stream", "holds"),
        ("autopilot_reverse_rungs", "prewarm_reverse_cdc_stream", "refuses"),
        ("autopilot_reverse_rungs", "refuse_reverse_unsupported", "holds"),
        ("autopilot_reverse_rungs", "refuse_reverse_unsupported", "refuses"),
    ]:
        branches = rung_branches.branch(seen, rung_name)
        assert branch in branches, f"Expected {rung_name} to reach {branch}, saw {branches}"


def test_autopilot_end_to_end_success(tmp_path):
    hop = DummyHop()
    tunnel = DeterministicTunnelManager()
    mover = DeterministicMover(tables=5)
    cdc = DeterministicCDCEngine(drained=250)
    merkle = DeterministicMerkleFolder(in_sync=True)
    flight = DeterministicFlightSim(sla_verdict="PASS")
    pooler = DeterministicPoolerCoordinator()
    reverse = DeterministicReverseLoop()
    tuner = DeterministicTuner()
    schema_life = DeterministicSchemaLifecycle()

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=False,
        skip_tuning=False,
        skip_flight_sim=False,
        auto_approve=True,
        state_dir=tmp_path / "state",
        tunnel_mgr=tunnel,
        mover=mover,
        cdc_engine=cdc,
        merkle_folder=merkle,
        flight_sim=flight,
        pooler_coord=pooler,
        reverse_loop=reverse,
        tuner=tuner,
        schema_lifecycle=schema_life,
    )

    res = orch.execute()
    assert res.ok is True
    assert res.status == ap.AutopilotStatus.COMPLETED
    assert res.failback_available is True
    assert res.total_duration_seconds > 0.0
    assert res.certificate is not None

    pmap = res.phase_map()
    assert len(pmap) == 12
    for phase_name in [
        "preflight",
        "bastion_tunnel",
        "target_tune_bulk",
        "deferred_index_drop",
        "parallel_bulk_load",
        "deferred_index_restore",
        "target_tune_restore",
        "cdc_replication",
        "merkle_verify",
        "flight_simulator",
        "seamless_cutover",
        "reverse_standby",
    ]:
        assert pmap[phase_name].status == ap.AutopilotStatus.COMPLETED
        assert pmap[phase_name].care_badge == ap.CareBadge.PASS

    assert pooler.switched is True
    assert reverse.prewarmed is True
    assert tuner.bulk_applied is True
    assert tuner.runtime_applied is True
    assert schema_life.dropped is True
    assert schema_life.rebuilt is True


def test_autopilot_dry_run_mode(tmp_path):
    hop = DummyHop()
    mover = DeterministicMover()
    merkle = DeterministicMerkleFolder(in_sync=True)
    flight = DeterministicFlightSim(sla_verdict="PASS")

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=True,
        state_dir=tmp_path / "state",
        mover=mover,
        merkle_folder=merkle,
        flight_sim=flight,
    )

    res = orch.execute()
    assert res.ok is True
    assert res.status == ap.AutopilotStatus.COMPLETED
    assert res.failback_available is False

    pmap = res.phase_map()
    assert pmap["flight_simulator"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["seamless_cutover"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["reverse_standby"].status == ap.AutopilotStatus.SKIPPED


def test_autopilot_skip_tuning_and_flight_sim(tmp_path):
    hop = DummyHop()
    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=True,
        skip_tuning=True,
        skip_flight_sim=True,
        state_dir=tmp_path / "state",
    )
    res = orch.execute()
    assert res.ok is True
    pmap = res.phase_map()
    assert pmap["target_tune_bulk"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["target_tune_restore"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["flight_simulator"].status == ap.AutopilotStatus.SKIPPED


def test_autopilot_failure_and_rollback(tmp_path):
    hop = DummyHop()
    merkle = DeterministicMerkleFolder(in_sync=False)
    tuner = DeterministicTuner()
    schema_life = DeterministicSchemaLifecycle()

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        state_dir=tmp_path / "state",
        merkle_folder=merkle,
        tuner=tuner,
        schema_lifecycle=schema_life,
    )
    res = orch.execute()
    assert res.ok is False
    assert res.status == ap.AutopilotStatus.FAILED
    assert "Merkle tree verification detected data drift" in str(res.error)

    pmap = res.phase_map()
    assert pmap["merkle_verify"].status == ap.AutopilotStatus.FAILED
    assert pmap["merkle_verify"].care_badge == ap.CareBadge.ACTION
    assert tuner.runtime_applied is True


def test_autopilot_cutover_gate_consent_and_divergence(tmp_path):
    hop = DummyHop()
    # 1. Manual consent pause
    orch_manual = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=False,
        auto_approve=False,
        operator_consented=False,
        state_dir=tmp_path / "state_manual",
    )
    res_manual = orch_manual.execute()
    assert res_manual.ok is False
    assert "manual consent" in str(res_manual.error)

    # 2. Data divergence abort
    merkle_drift = DeterministicMerkleFolder(in_sync=False)
    orch_drift = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=False,
        auto_approve=True,
        state_dir=tmp_path / "state_drift",
        merkle_folder=merkle_drift,
    )
    res_drift = orch_drift.execute()
    assert res_drift.ok is False
    assert res_drift.status == ap.AutopilotStatus.FAILED


def test_autopilot_checkpoint_store_typed_o1_resume(tmp_path):
    store_file = tmp_path / "checkpoint.json"
    store = ap.AutopilotCheckpointStore(store_file)

    now = datetime.datetime.now(datetime.timezone.utc)
    u_id = uuid.uuid4()
    max_num = decimal.Decimal("1234567.89")

    unit = ap.AutopilotCheckpointUnit(
        phase="parallel_bulk_load",
        status=ap.AutopilotStatus.COMPLETED.value,
        min_cursor=u_id,
        max_cursor=max_num,
        completed_at=now,
        duration_seconds=12.5,
        details={"batches": 42, "ratio": decimal.Decimal("0.999")},
    )
    store.record_phase(unit)

    # Reload store from disk
    store_loaded = ap.AutopilotCheckpointStore(store_file)
    assert store_loaded.is_completed_o1("parallel_bulk_load") is True
    assert store_loaded.is_completed_o1("seamless_cutover") is False

    retrieved = store_loaded.get_unit("parallel_bulk_load")
    assert retrieved is not None
    assert retrieved.phase == "parallel_bulk_load"
    assert retrieved.min_cursor == u_id
    assert isinstance(retrieved.min_cursor, uuid.UUID)
    assert retrieved.max_cursor == max_num
    assert isinstance(retrieved.max_cursor, decimal.Decimal)
    assert retrieved.completed_at.isoformat() == now.isoformat()
    assert retrieved.duration_seconds == 12.5
    assert retrieved.details["ratio"] == decimal.Decimal("0.999")

    # Verify resume in orchestrator skips completed phase in O(1)
    hop = DummyHop()
    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=True,
        state_dir=tmp_path,
    )
    # The checkpoint store in orch already has parallel_bulk_load completed!
    res = orch.execute()
    assert res.ok is True
    pmap = res.phase_map()
    assert pmap["parallel_bulk_load"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["parallel_bulk_load"].duration_seconds == 12.5


def test_autopilot_lease_lock_mutual_exclusion(tmp_path):
    hop = DummyHop()
    lock_dir = tmp_path / "state_lock"
    orch1 = ap.AutopilotOrchestrator(hop=hop, state_dir=lock_dir)

    from migkit.progress_liveness import ActiveVerificationLeaseLock
    lock = ActiveVerificationLeaseLock(lock_dir / ".run.lock", lease_seconds=30.0)
    lock.acquire()
    try:
        with pytest.raises(LeaseConflictError):
            orch1.execute()
    finally:
        lock.release()


def test_autopilot_cli_command(tmp_path, monkeypatch):
    runner = CliRunner()
    hop = DummyHop()
    hop.report_dir = lambda: tmp_path / "cli_state"
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)

    result = runner.invoke(cli.main, ["autopilot", "prod_migration", "--dry-run"])
    assert result.exit_code == 0
    assert "autopilot finished successfully" in result.output
