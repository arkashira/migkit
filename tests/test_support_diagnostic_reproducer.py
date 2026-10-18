import json
import os
import zipfile
from pathlib import Path
import pytest
from click.testing import CliRunner

from tests.rung_branches import watched
from migkit.cli import main
from migkit.support_reproducer import (
    DiagnosticBundle,
    SupportReproducer,
    SyntheticFixture,
    SyntheticFixtureGenerator,
    OfflineReproHarness,
    PrivacyScanner,
    PrivacyScanReport,
    ReproResult,
    SCHEMA_FAILURE_CLASSES,
    DATA_FAILURE_CLASSES,
    PROCESS_FAILURE_CLASSES,
    get_safe_test_ips,
    get_max_log_lines,
    get_max_log_bytes,
    support_reproducer_rungs,
    decide_support_reproducer,
    diagnostic_scrubbing_rungs,
    decide_diagnostic_scrubbing,
)

PROVES_RUNGS = [
    ("support_reproducer", "support_reproducer_rungs", "schema_fault_reproduction", "holds"),
    ("support_reproducer", "support_reproducer_rungs", "schema_fault_reproduction", "refuses"),
    ("support_reproducer", "support_reproducer_rungs", "data_integrity_reproduction", "holds"),
    ("support_reproducer", "support_reproducer_rungs", "data_integrity_reproduction", "refuses"),
    ("support_reproducer", "support_reproducer_rungs", "process_lifecycle_reproduction", "holds"),
    ("support_reproducer", "support_reproducer_rungs", "process_lifecycle_reproduction", "refuses"),
    ("support_reproducer", "support_reproducer_rungs", "offline_sandbox_reproduction", "holds"),
    ("support_reproducer", "support_reproducer_rungs", "offline_sandbox_reproduction", "refuses"),
    ("support_reproducer", "support_reproducer_rungs", "refuse_unbounded_unredacted_bundle", "holds"),
    ("support_reproducer", "support_reproducer_rungs", "refuse_unbounded_unredacted_bundle", "refuses"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "secret_credential_scrubbing", "holds"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "secret_credential_scrubbing", "refuses"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "ip_address_scrubbing", "holds"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "ip_address_scrubbing", "refuses"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "pii_ssn_email_scrubbing", "holds"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "pii_ssn_email_scrubbing", "refuses"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "log_size_truncation", "holds"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "log_size_truncation", "refuses"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "refuse_unscrubbed_exposure", "holds"),
    ("support_reproducer", "diagnostic_scrubbing_rungs", "refuse_unscrubbed_exposure", "refuses"),
]



def test_diagnostic_bundle_creation_and_fields():
    bundle = DiagnosticBundle(
        tool_versions={"migkit": "0.9.0", "python": "3.12.0", "platform": "darwin"},
        engine_versions={"source": "postgres 16.2", "target": "mysql 8.0.35"},
        epochs={"contract": 4, "model": 2, "fact": 12},
        phase_timings={"discovery": 0.12, "schema": 0.45, "data": 1.20},
        diagnostic_logs=["phase=discovery status=ok", "phase=data status=failed"],
    )
    assert bundle.tool_versions["migkit"] == "0.9.0"
    assert bundle.engine_versions["source"] == "postgres 16.2"
    assert bundle.epochs["contract"] == 4
    assert bundle.phase_timings["data"] == 1.20
    assert len(bundle.diagnostic_logs) == 2
    d = bundle.to_dict()
    assert d["tool_versions"]["python"] == "3.12.0"
    assert d["epochs"]["model"] == 2


def test_diagnostic_bundle_bounded_logs():
    reproducer = SupportReproducer(max_log_lines=5, max_log_bytes=200)
    raw_logs = [f"log entry number {i} with payload data" for i in range(50)]
    bundle = reproducer.capture_bundle(
        diagnostic_logs=raw_logs,
        tool_versions={"migkit": "0.9.0"},
    )
    assert len(bundle.diagnostic_logs) <= 5
    total_bytes = sum(len(line.encode("utf-8")) for line in bundle.diagnostic_logs)
    assert total_bytes <= 200


def test_diagnostic_bundle_redacts_secrets_and_ips():
    reproducer = SupportReproducer(
        secrets=["top_secret_pass_123"],
        safe_ips={"127.0.0.1", "10.0.0.1"},
    )
    raw_logs = [
        "connecting to postgres://admin:top_secret_pass_123@192.168.1.100:5432/db",
        "api_token = 'tok-xyz-98765'",
        "peer IP 172.16.0.40 failed handshake",
        "safe local 127.0.0.1 connection established",
    ]
    bundle = reproducer.capture_bundle(
        diagnostic_logs=raw_logs,
        config_snippet={"password": "top_secret_pass_123", "target_ip": "192.168.1.100"},
    )
    joined_logs = "\n".join(bundle.diagnostic_logs)
    assert "top_secret_pass_123" not in joined_logs
    assert "tok-xyz-98765" not in joined_logs
    assert "192.168.1.100" not in joined_logs
    assert "172.16.0.40" not in joined_logs
    assert "127.0.0.1" in joined_logs
    assert bundle.config_snippet["password"] != "top_secret_pass_123"


def test_diagnostic_bundle_opaque_provider_internals():
    reproducer = SupportReproducer()
    bundle = reproducer.capture_bundle(
        failure_class="waitid_no_child",
        opaque_error="waitid: no child processes in provider worker pool",
    )
    assert bundle.unknown_internals is not None
    assert bundle.unknown_internals["status"] == "unknown"
    assert "provider internals unobservable" in bundle.unknown_internals["reason"]


def test_synthetic_fixture_schema_failure_classes():
    gen = SyntheticFixtureGenerator()
    for fclass in SCHEMA_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        assert isinstance(fix, SyntheticFixture)
        assert fix.failure_class == fclass
        assert fix.category == "schema"
        assert len(fix.schema_definition) > 0
        assert fix.source_config["host"] in ("127.0.0.1", "10.0.0.1")
        assert "password" not in fix.source_config or fix.source_config["password"] in ("", "<redacted>")
        scanner = PrivacyScanner()
        report = scanner.scan(fix.to_dict())
        assert report.is_clean, f"Privacy violation in schema fixture {fclass}: {report.violations}"


def test_synthetic_fixture_data_failure_classes():
    gen = SyntheticFixtureGenerator()
    for fclass in DATA_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        assert isinstance(fix, SyntheticFixture)
        assert fix.failure_class == fclass
        assert fix.category == "data"
        assert len(fix.records) <= 3
        for rec in fix.records:
            assert "sequence_id" in rec or "id" in rec
        scanner = PrivacyScanner()
        report = scanner.scan(fix.to_dict())
        assert report.is_clean, f"Privacy violation in data fixture {fclass}: {report.violations}"


def test_synthetic_fixture_process_failure_classes():
    gen = SyntheticFixtureGenerator()
    for fclass in PROCESS_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        assert isinstance(fix, SyntheticFixture)
        assert fix.failure_class == fclass
        assert fix.category == "process"
        assert fix.unknown_internals is not None
        assert fix.unknown_internals["status"] == "unknown"


def test_offline_repro_harness_replays_schema_failures():
    harness = OfflineReproHarness()
    gen = SyntheticFixtureGenerator()
    for fclass in SCHEMA_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        res1 = harness.replay(fix)
        res2 = harness.replay(fix)
        assert isinstance(res1, ReproResult)
        assert res1.reproduced is True
        assert res1.failure_class == fclass
        assert res1.deterministic is True
        assert res1.exit_code != 0
        assert res1.error_detail == res2.error_detail


def test_offline_repro_harness_replays_data_failures():
    harness = OfflineReproHarness()
    gen = SyntheticFixtureGenerator()
    for fclass in DATA_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        res1 = harness.replay(fix)
        res2 = harness.replay(fix)
        assert isinstance(res1, ReproResult)
        assert res1.reproduced is True
        assert res1.failure_class == fclass
        assert res1.deterministic is True
        assert res1.exit_code != 0
        assert res1.error_detail == res2.error_detail


def test_offline_repro_harness_replays_process_failures():
    harness = OfflineReproHarness()
    gen = SyntheticFixtureGenerator()
    for fclass in PROCESS_FAILURE_CLASSES:
        fix = gen.generate(fclass)
        res = harness.replay(fix)
        assert res.reproduced is True
        assert res.failure_class == fclass
        assert res.unknown_internals is not None
        assert res.unknown_internals["status"] == "unknown"


def test_privacy_scanner_detects_violations():
    scanner = PrivacyScanner()
    clean_data = {
        "host": "127.0.0.1",
        "port": 5432,
        "table": "synth_t1",
        "rows": [{"id": 1, "val": "abc"}],
    }
    rep = scanner.scan(clean_data)
    assert rep.is_clean
    assert len(rep.violations) == 0

    dirty_data = {
        "host": "192.168.1.55",
        "secret": "my-secret-token",
        "email": "customer@company.com",
        "ssn": "123-45-6789",
        "card": "4111-2222-3333-4444",
    }
    rep_dirty = scanner.scan(dirty_data)
    assert not rep_dirty.is_clean
    assert len(rep_dirty.violations) >= 4


def test_bundle_serialization_roundtrip_zip(tmp_path):
    reproducer = SupportReproducer()
    gen = SyntheticFixtureGenerator()
    fix = gen.generate("duplicate_pk")
    bundle = reproducer.capture_bundle(
        tool_versions={"migkit": "0.9.0", "python": "3.12.0"},
        engine_versions={"source": "pg 16", "target": "mysql 8"},
        epochs={"contract": 1, "model": 2, "fact": 3},
        phase_timings={"phase1": 0.5},
        diagnostic_logs=["log line 1", "log line 2"],
        synthetic_fixtures=[fix],
        failure_class="duplicate_pk",
    )
    zip_file = tmp_path / "repro_bundle.zip"
    bundle.write_zip(zip_file)
    assert zip_file.exists()

    loaded = DiagnosticBundle.read_zip(zip_file)
    assert loaded.tool_versions == bundle.tool_versions
    assert loaded.engine_versions == bundle.engine_versions
    assert loaded.epochs == bundle.epochs
    assert loaded.phase_timings == bundle.phase_timings
    assert loaded.diagnostic_logs == bundle.diagnostic_logs
    assert len(loaded.synthetic_fixtures) == 1
    assert loaded.synthetic_fixtures[0].failure_class == "duplicate_pk"


def test_support_reproducer_end_to_end_replay(tmp_path):
    reproducer = SupportReproducer()
    gen = SyntheticFixtureGenerator()
    fix = gen.generate("unsupported_type")

    bundle = reproducer.capture_bundle(
        tool_versions={"migkit": "0.9.0"},
        epochs={"contract": 2, "model": 1, "fact": 5},
        phase_timings={"discovery": 0.1, "schema_inspect": 0.2},
        diagnostic_logs=["inspecting source schema", "unknown datatype geo_polygon encountered"],
        synthetic_fixtures=[fix],
        failure_class="unsupported_type",
    )

    scanner = PrivacyScanner()
    report = scanner.scan(bundle.to_dict())
    assert report.is_clean, f"Bundle privacy scan failed: {report.violations}"

    harness = OfflineReproHarness()
    repro_res = harness.replay(bundle)
    assert repro_res.reproduced is True
    assert repro_res.failure_class == "unsupported_type"
    assert repro_res.deterministic is True


def test_bundle_replay_from_failure_class_without_fixtures():
    bundle = DiagnosticBundle(failure_class="duplicate_pk")
    harness = OfflineReproHarness()
    res = harness.replay(bundle)
    assert res.reproduced is True
    assert res.failure_class == "duplicate_pk"
    assert res.deterministic is True


def test_bundle_replay_missing_fixtures_and_failure_class_raises():
    bundle = DiagnosticBundle()
    harness = OfflineReproHarness()
    with pytest.raises(ValueError, match="DiagnosticBundle contains no fixtures"):
        harness.replay(bundle)


def test_synthetic_fixture_generator_unknown_class_raises():
    gen = SyntheticFixtureGenerator()
    with pytest.raises(ValueError, match="unknown failure class"):
        gen.generate("nonexistent_class_xyz")


def test_synthetic_fixture_generator_available_classes():
    gen = SyntheticFixtureGenerator()
    classes = gen.available_classes()
    for item in SCHEMA_FAILURE_CLASSES + DATA_FAILURE_CLASSES + PROCESS_FAILURE_CLASSES:
        assert item in classes


def test_offline_repro_harness_unsupported_class_raises():
    harness = OfflineReproHarness()
    bad_fixture = SyntheticFixture(
        failure_class="invalid_class",
        category="custom",
        schema_definition={},
        source_config={},
        target_config={},
    )
    with pytest.raises(ValueError, match="unsupported replay failure class"):
        harness.replay(bad_fixture)


def test_privacy_scanner_nested_structure():
    scanner = PrivacyScanner()
    nested = {
        "level1": [
            {"level2": {"safe": "10.0.0.5", "user_email": "test@domain.com"}},
            {"level2_b": {"clean": "127.0.0.1"}},
        ]
    }
    report = scanner.scan(nested)
    assert not report.is_clean
    assert any("detected email" in v for v in report.violations)


def test_diagnostic_bundle_json_and_dict_roundtrip():
    bundle = DiagnosticBundle(
        bundle_id="b123",
        tool_versions={"migkit": "0.9.0"},
        engine_versions={"source": "pg"},
        epochs={"contract": 1},
        phase_timings={"phase": 0.1},
        diagnostic_logs=["log line"],
        failure_class="schema_drift",
    )
    raw_json = bundle.to_json()
    parsed = json.loads(raw_json)
    assert parsed["bundle_id"] == "b123"
    assert parsed["failure_class"] == "schema_drift"

    reconstructed = DiagnosticBundle.from_dict(parsed)
    assert reconstructed.bundle_id == "b123"
    assert reconstructed.failure_class == "schema_drift"
    assert reconstructed.tool_versions == {"migkit": "0.9.0"}


def test_support_reproducer_rungs_under_watched():
    with watched():
        # support_reproducer_rungs:
        # 1. schema_fault_reproduction
        c1 = decide_support_reproducer({"category": "schema"})
        assert c1.rung.name == "schema_fault_reproduction"

        # 2. data_integrity_reproduction
        c2 = decide_support_reproducer({"category": "data"})
        assert c2.rung.name == "data_integrity_reproduction"

        # 3. process_lifecycle_reproduction
        c3 = decide_support_reproducer({"category": "process"})
        assert c3.rung.name == "process_lifecycle_reproduction"

        # 4. offline_sandbox_reproduction
        c4 = decide_support_reproducer({"category": "other", "offline_sandbox": True})
        assert c4.rung.name == "offline_sandbox_reproduction"

        # 5. refuse_unbounded_unredacted_bundle
        c5 = decide_support_reproducer({"category": "other", "offline_sandbox": False})
        assert c5.rung.name == "refuse_unbounded_unredacted_bundle"

        # 6. refuse fallback
        c6 = decide_support_reproducer({"category": "other", "offline_sandbox": False, "allow_unredacted_or_unbounded": True})
        assert c6.rung is None

        # diagnostic_scrubbing_rungs:
        # 1. secret_credential_scrubbing
        s1 = decide_diagnostic_scrubbing({"scrub_secrets": True})
        assert s1.rung.name == "secret_credential_scrubbing"

        # 2. ip_address_scrubbing
        s2 = decide_diagnostic_scrubbing({"scrub_secrets": False, "scrub_ips": True})
        assert s2.rung.name == "ip_address_scrubbing"

        # 3. pii_ssn_email_scrubbing
        s3 = decide_diagnostic_scrubbing({"scrub_secrets": False, "scrub_ips": False, "scrub_pii": True})
        assert s3.rung.name == "pii_ssn_email_scrubbing"

        # 4. log_size_truncation
        s4 = decide_diagnostic_scrubbing({"scrub_secrets": False, "scrub_ips": False, "scrub_pii": False, "truncate_logs": True})
        assert s4.rung.name == "log_size_truncation"

        # 5. refuse_unscrubbed_exposure
        s5 = decide_diagnostic_scrubbing({"scrub_secrets": False, "scrub_ips": False, "scrub_pii": False, "truncate_logs": False})
        assert s5.rung.name == "refuse_unscrubbed_exposure"

        # 6. refuse fallback
        s6 = decide_diagnostic_scrubbing({"scrub_secrets": False, "scrub_ips": False, "scrub_pii": False, "truncate_logs": False, "allow_unscrubbed_exposure": True})
        assert s6.rung is None


def test_dynamic_bounds_and_cli_bundle(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_SAFE_TEST_IPS", "127.0.0.1, 192.168.10.1")
    monkeypatch.setenv("MIGKIT_MAX_LOG_LINES", "120")
    monkeypatch.setenv("MIGKIT_MAX_LOG_BYTES", "4096")

    assert "192.168.10.1" in get_safe_test_ips()
    assert get_max_log_lines() == 120
    assert get_max_log_bytes() == 4096

    # Test CLI command diagnostics-bundle
    runner = CliRunner()
    zip_dest = str(tmp_path / "test_bundle.zip")
    res = runner.invoke(main, ["diagnostics-bundle", "--failure-class", "schema_drift", "--out", zip_dest])
    assert res.exit_code == 0
    assert "created support diagnostic bundle" in res.output
    assert Path(zip_dest).exists()
    with zipfile.ZipFile(zip_dest, "r") as z:
        names = z.namelist()
        assert "bundle.json" in names


