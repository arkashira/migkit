"""The measurement counts effort and refuses an unverified finish."""
import json
import subprocess

from tools import ease_measure as ease


def test_fake_transcript_counts_effort_not_generated_configuration():
    transcript = (
        "Connected to source\n"
        "Password for source: \n"
        "Continue? [y/n]\n"
        "\x1b[32m  migkit move ease --go\x1b[0m\n"
        "Please set the source password\n"
        "You need to enable access\n"
        "wrote hop ease (12 lines)\n"
        "verdict: same\n"
    )
    assert ease.measure_transcript(transcript, 2.125, "# hop\nsource: address\n\ntarget: address\n") == {
        "elapsed_seconds": 2.125, "operator_config_lines": 2,
        "questions": 2, "action_messages": 3,
    }
    assert ease.measure_transcript("wrote hop ease (12 lines)", 0)["operator_config_lines"] == 0


def test_successful_exit_is_not_a_verified_move():
    steps = [{"exit": 0}] * 3
    assert ease.verified(steps, {"status": "same"}, ease.SAMPLE)
    for verdict in ({}, {"status": "different"}, {"status": "incomplete"}, {"status": "error"}):
        assert not ease.verified(steps, verdict, ease.SAMPLE)
    assert not ease.verified(steps, {"status": "same"}, [])
    assert not ease.verified(steps[:2], {"status": "same"}, ease.SAMPLE)
    assert not ease.verified([{"exit": 1}] * 3, {"status": "same"}, ease.SAMPLE)


def test_core_pairs_come_from_sandbox_profiles():
    pairs = ease.pairs()
    assert {("pg", "pg"), ("mysql", "mysql"), ("mysql", "pg"), ("mongo", "mongo")} <= set(pairs)


def test_workflow_starts_empty_and_does_not_print_credentials(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setenv("MIGKIT_CONF", "/unrelated/private/config")
    monkeypatch.setenv("MIGKIT_PREFLIGHT", "warn")
    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["input"] == ""
        assert kwargs["env"]["MIGKIT_CONF"] == str(tmp_path / "conf/hops.yaml")
        assert "MIGKIT_PREFLIGHT" not in kwargs["env"]
        if "init" in argv:
            assert not list(tmp_path.iterdir())
        if "check" in argv:
            report = tmp_path / "reports/ease/verdict.json"
            report.parent.mkdir(parents=True)
            report.write_text('{"status": "same"}')
        return subprocess.CompletedProcess(argv, 0, "verdict: same\n", "")
    monkeypatch.setattr(ease.subprocess, "run", run)
    steps, verdict, metrics = ease.run_workflow("source", "target", ("secret", "secret"), tmp_path, 1)
    assert [s["command"] for s in steps] == ["init", "move", "check"]
    assert verdict == {"status": "same"}
    assert metrics["operator_config_lines"] == 0
    assert "secret" not in str(calls)


def test_stopped_sandbox_never_runs_a_measurement(tmp_path, monkeypatch):
    monkeypatch.setattr(ease, "colima_running", lambda: False)
    monkeypatch.setattr(ease.sys, "argv", ["ease_measure", "--output", str(tmp_path)])
    def forbidden(*args):
        raise AssertionError("must not start the sandbox")
    monkeypatch.setattr(ease, "measure_pair", forbidden)
    assert ease.main() == 1
    result = json.loads(next(tmp_path.glob("*.json")).read_text())
    assert len(result["results"]) >= 4
    assert all("elapsed_seconds" not in r for r in result["results"])
    assert "not measured" in next(tmp_path.glob("*.md")).read_text()


def test_every_independent_pair_profile_is_measured_without_program_names():
    from tests.servers import PROFILES
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    pairs = ease.pairs()
    assert {key for key, p in PROFILES.items() if p.roles == ("src", "dst") and not p.tls} <= {a for a, b in pairs if a == b}
    text = "\n".join(ease.pair_label(a, b) for a, b in pairs).lower()
    assert not [name for name in TOOLS if name in text]
