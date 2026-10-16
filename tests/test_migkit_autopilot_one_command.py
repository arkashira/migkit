import dataclasses
from unittest.mock import MagicMock
from click.testing import CliRunner

from migkit import autopilot as ap
from migkit import cli


@dataclasses.dataclass
class DummyHop:
    name: str = "prod_migration"
    target_host: str = "127.0.0.1"
    target_port: int = 5432
    target_db: str = "mydb"
    bastion: str = "10.0.0.1:22"


def test_autopilot_end_to_end_success():
    hop = DummyHop()
    mock_tunnel = MagicMock()
    mock_mover = MagicMock()
    mock_mover.copy_tables.return_value = 5
    mock_cdc = MagicMock()
    mock_cdc.catchup.return_value = 250
    mock_merkle = MagicMock()
    mock_merkle.verify_parity.return_value = {"in_sync": True}
    mock_flight = MagicMock()
    mock_flight.run_rehearsal.return_value = MagicMock(to_dict=lambda: {"readiness": "ready"})
    mock_pooler = MagicMock()
    mock_pooler.shift_traffic.return_value = True
    mock_reverse = MagicMock()

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=False,
        skip_tuning=False,
        skip_flight_sim=False,
        tunnel_mgr=mock_tunnel,
        mover=mock_mover,
        cdc_engine=mock_cdc,
        merkle_folder=mock_merkle,
        flight_sim=mock_flight,
        pooler_coord=mock_pooler,
        reverse_loop=mock_reverse,
    )

    res = orch.execute()
    assert res.ok is True
    assert res.status == ap.AutopilotStatus.COMPLETED
    assert res.failback_available is True
    assert res.total_duration_seconds > 0.0

    pmap = res.phase_map()
    assert pmap["preflight"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["bastion_tunnel"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["target_tune_bulk"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["deferred_index_drop"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["parallel_bulk_load"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["deferred_index_restore"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["target_tune_restore"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["cdc_replication"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["merkle_verify"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["flight_simulator"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["seamless_cutover"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["reverse_standby"].status == ap.AutopilotStatus.COMPLETED


def test_autopilot_dry_run_mode():
    hop = DummyHop()
    mock_mover = MagicMock()
    mock_merkle = MagicMock()
    mock_merkle.verify_parity.return_value = {"in_sync": True}
    mock_flight = MagicMock()
    mock_flight.run_rehearsal.return_value = MagicMock(to_dict=lambda: {"readiness": "ready"})

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        dry_run=True,
        mover=mock_mover,
        merkle_folder=mock_merkle,
        flight_sim=mock_flight,
    )

    res = orch.execute()
    assert res.ok is True
    assert res.status == ap.AutopilotStatus.COMPLETED
    assert res.failback_available is False

    pmap = res.phase_map()
    assert pmap["flight_simulator"].status == ap.AutopilotStatus.COMPLETED
    assert pmap["seamless_cutover"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["reverse_standby"].status == ap.AutopilotStatus.SKIPPED


def test_autopilot_skip_tuning_and_flight_sim():
    hop = DummyHop()
    orch = ap.AutopilotOrchestrator(
        hop=hop,
        skip_tuning=True,
        skip_flight_sim=True,
        dry_run=True,
    )
    res = orch.execute()
    assert res.ok is True
    pmap = res.phase_map()
    assert pmap["target_tune_bulk"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["target_tune_restore"].status == ap.AutopilotStatus.SKIPPED
    assert pmap["flight_simulator"].status == ap.AutopilotStatus.SKIPPED


def test_autopilot_failure_and_rollback():
    hop = DummyHop()
    mock_merkle = MagicMock()
    mock_merkle.verify_parity.return_value = {"in_sync": False}

    orch = ap.AutopilotOrchestrator(
        hop=hop,
        merkle_folder=mock_merkle,
    )
    res = orch.execute()
    assert res.ok is False
    assert res.status == ap.AutopilotStatus.FAILED
    assert "Merkle tree verification detected data drift" in str(res.error)

    pmap = res.phase_map()
    assert pmap["merkle_verify"].status == ap.AutopilotStatus.FAILED


def test_autopilot_cli_command(monkeypatch):
    runner = CliRunner()
    hop = DummyHop()
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)

    result = runner.invoke(cli.main, ["autopilot", "prod_migration", "--dry-run"])
    assert result.exit_code == 0
    assert "autopilot finished successfully" in result.output
