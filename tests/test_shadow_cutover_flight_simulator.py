import json
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from migkit import cli, cutover
from migkit.flight_simulator import (
    CutoverReadinessSLACertificate,
    PhaseResult,
    SavepointSnapshot,
    ShadowCutoverFlightSimulator,
)


class MockHop:
    name = "hop_sim_test"
    engine = "postgres"
    databases = ["appdb"]
    options = {
        "cutover": {
            "budget": 25,
            "freeze": "auto",
            "keys": "apart:5000",
            "pooler": {"kind": "pgbouncer", "host": "127.0.0.1", "port": 6432},
        }
    }

    def __init__(self, root: Path):
        self.root = root

    def report_dir(self, db: str = "") -> Path:
        p = self.root / (db or "")
        p.mkdir(parents=True, exist_ok=True)
        return p


class MockEngine:
    def __init__(self):
        self.savepoints = []
        self.rolled_back = []

    def create_savepoint(self, db, sp_id):
        self.savepoints.append((db, sp_id))

    def rollback_savepoint(self, db, sp_id):
        self.rolled_back.append((db, sp_id))


def test_flight_simulator_all_11_phases_rehearsed(tmp_path):
    hop = MockHop(tmp_path)
    eng = MockEngine()
    sim = ShadowCutoverFlightSimulator(hop, eng=eng, db="appdb")
    cert = sim.run()

    assert len(cert.phases) == 11
    expected_phases = [
        "prepare", "jobs_off", "pause", "freeze", "drain", "verify",
        "counters", "stream", "reverse", "jobs_on", "flip"
    ]
    for p in expected_phases:
        assert p in cert.phases
        res = cert.phases[p]
        assert res["status"] == "PASSED"
        assert res["duration"] >= 0.0

    assert cert.sla_checks["all_phases_passed"] is True
    assert cert.sla_verdict == "PASS"
    assert cert.status == "READY"


def test_target_storage_savepoint_and_auto_rollback_under_2_seconds(tmp_path):
    hop = MockHop(tmp_path)
    eng = MockEngine()
    sim = ShadowCutoverFlightSimulator(hop, eng=eng, db="appdb")

    sp = sim.create_savepoint()
    assert sp.active is True
    assert sp.savepoint_id.startswith("flight_sim_")
    assert eng.savepoints

    sim.mock_state["sequences"]["orders_id_seq"] = 99999
    sim.mock_state["roles"]["app_user"] = []
    sim.mock_state["pooler_paused"] = True

    rollback_dur = sim.auto_rollback_to_savepoint()
    assert rollback_dur < 2.0
    assert sp.active is False
    assert eng.rolled_back

    assert sim.mock_state["sequences"]["orders_id_seq"] == 1000
    assert "INSERT" in sim.mock_state["roles"]["app_user"]
    assert sim.mock_state["pooler_paused"] is False


def test_simulate_pooler_pause(tmp_path):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")
    res = sim.rehearse_pause()

    assert res.status == "PASSED"
    assert sim.pooler_sim["pause_simulated"] is True
    assert sim.pooler_sim["pooler_kind"] == "pgbouncer"
    assert sim.pooler_sim["connection_queuing_verified"] is True
    assert sim.mock_state["pooler_paused"] is True


def test_measure_exact_cdc_drain_duration(tmp_path):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")
    res = sim.rehearse_drain()

    assert res.status == "PASSED"
    meas = sim.drain_meas
    assert "exact_drain_duration_seconds" in meas
    assert meas["exact_drain_duration_seconds"] > 0.0
    assert meas["start_position"] == "0/16B2F88"
    assert meas["end_position"] == "0/16B3100"
    assert meas["within_budget"] is True
    assert meas["budget_seconds"] == 25.0


def test_verify_sequence_headroom_margin_distance(tmp_path):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")
    res = sim.rehearse_counters()

    assert res.status == "PASSED"
    hd = sim.headroom_verif
    assert hd["margin_distance"] == 5000
    assert hd["target_next_val"] == 6000
    assert hd["source_max_id"] == 1000
    assert hd["collision_risk"] == "ZERO"
    assert hd["headroom_verified"] is True


def test_role_revocation_and_regrant(tmp_path):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")

    freeze_res = sim.rehearse_freeze()
    assert freeze_res.status == "PASSED"
    assert sim.role_gov["revocation_verified"] is True
    assert sim.mock_state["roles"]["app_user"] == []
    assert sim.role_gov["regrant_verified"] is False

    flip_res = sim.rehearse_flip()
    assert flip_res.status == "PASSED"
    assert sim.role_gov["regrant_verified"] is True
    assert "INSERT" in sim.mock_state["roles"]["app_user"]
    assert "DELETE" in sim.mock_state["roles"]["app_user"]


def test_structured_cutover_readiness_sla_certificate(tmp_path):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")
    cert = sim.run()

    assert isinstance(cert, CutoverReadinessSLACertificate)
    assert cert.certificate_id.startswith("SLA-FLIGHT-")
    assert cert.hop_name == "hop_sim_test"
    assert cert.status == "READY"
    assert cert.sla_verdict == "PASS"

    d = cert.to_dict()
    assert isinstance(d, dict)
    assert d["target_savepoint"]["rollback_sla_met"] is True
    assert d["target_savepoint"]["rollback_duration_seconds"] < 2.0
    assert d["sequence_headroom"]["collision_risk"] == "ZERO"
    assert d["cdc_drain_measurement"]["within_budget"] is True

    raw_json = cert.to_json()
    parsed = json.loads(raw_json)
    assert parsed["certificate_id"] == cert.certificate_id
    assert parsed["sla_verdict"] == "PASS"

    rendered = cert.render()
    assert "CUTOVER READINESS SLA CERTIFICATE" in rendered
    assert "Target Savepoint Rollback" in rendered
    assert "Pooler Pause Simulation" in rendered
    assert "CDC Drain to Log Tail" in rendered
    assert "Sequence Headroom Margin" in rendered

    cert_file = tmp_path / "cutover-readiness-sla.json"
    assert cert_file.exists()
    saved = json.loads(cert_file.read_text())
    assert saved["certificate_id"] == cert.certificate_id


def test_flight_simulator_rollback_on_phase_failure(tmp_path, monkeypatch):
    hop = MockHop(tmp_path)
    sim = ShadowCutoverFlightSimulator(hop, db="appdb")

    def fail_drain():
        raise RuntimeError("simulated CDC pipeline network stall")

    monkeypatch.setattr(sim, "rehearse_drain", fail_drain)
    cert = sim.run()

    assert cert.sla_verdict == "FAIL"
    assert cert.status == "NOT_READY"
    assert cert.sla_checks["all_phases_passed"] is False
    assert cert.rehearsal_metrics["rehearsal_error"] == "simulated CDC pipeline network stall"
    assert cert.target_savepoint["rollback_sla_met"] is True
    assert cert.target_savepoint["rollback_duration_seconds"] < 2.0


def test_cutover_helper_in_cutover_module(tmp_path):
    hop = MockHop(tmp_path)
    sim = cutover.flight_simulator(hop, db="appdb")
    assert isinstance(sim, ShadowCutoverFlightSimulator)
    cert = sim.run()
    assert cert.sla_verdict == "PASS"


def test_cli_cutover_dry_run_command(tmp_path, monkeypatch):
    import migkit.config as cfg
    conf_file = tmp_path / "hops.yaml"
    conf_file.write_text(
        "hops:\n"
        "  test_hop:\n"
        "    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 's.db'}, user: u, password: p}}\n"
        f"    target: {{host: {tmp_path / 't.db'}, user: u, password: p}}\n"
        "    databases: [appdb]\n"
        "    options:\n"
        "      cutover:\n"
        "        budget: 30\n"
    )
    monkeypatch.setattr(cfg, "CONF", str(conf_file))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    runner = CliRunner()
    result = runner.invoke(cli.main, ["cutover", "test_hop", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "CUTOVER READINESS SLA CERTIFICATE" in result.output
    assert "SLA Verdict: PASS" in result.output
    assert "Rehearsed Cutover Phases (11 of 11)" in result.output
    assert "Target Savepoint Rollback" in result.output
