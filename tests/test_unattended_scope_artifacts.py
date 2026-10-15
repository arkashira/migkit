"""Tests for P0-VALIDATE.unattended.scope (#500) and P0-RESULT-SCOPE.scope (#467):
Separation of application defects, replication artifacts and safe repair boundaries.
"""
from types import SimpleNamespace
import pytest
from migkit import leftovers, verdict, repairways


def test_leftovers_artifact_origin_detection():
    assert leftovers.is_replication_artifact("awsdms_apply_exceptions") is True
    assert leftovers.artifact_origin("awsdms_apply_exceptions") == "AWS DMS"

    assert leftovers.is_replication_artifact("debezium_offset_storage") is True
    assert leftovers.artifact_origin("debezium_offset_storage") == "Debezium"

    assert leftovers.is_replication_artifact("dts_change_events") is True
    assert leftovers.artifact_origin("dts_change_events") == "Tencent DTS"

    assert leftovers.is_replication_artifact("orders") is False
    assert leftovers.artifact_origin("orders") == ""


def test_verdict_finding_distinguishes_replication_artifact():
    hop = SimpleNamespace(
        name="test-hop",
        engine="postgres",
        options={},
        report_dir=lambda *a: None,
        scope_fingerprint=lambda: "dummy-scope",
        scope_changed=lambda prev: False,
    )
    records = [
        {
            "category": "data",
            "check": "data",
            "scope": "public.orders",
            "status": "diff",
            "detail": "1 row missing",
        },
        {
            "category": "data",
            "check": "data",
            "scope": "public.awsdms_apply_exceptions",
            "status": "diff",
            "detail": "replication errors logged",
        },
    ]

    summary = verdict.summarize(hop, records)
    findings = summary["findings"]
    assert len(findings) == 2

    app_finding = next(f for f in findings if f["scope"] == "public.orders")
    assert app_finding["defect_class"] == "application_defect"
    assert app_finding["is_replication_artifact"] is False
    assert "artifact_origin" not in app_finding

    dms_finding = next(f for f in findings if f["scope"] == "public.awsdms_apply_exceptions")
    assert dms_finding["defect_class"] == "replication_artifact"
    assert dms_finding["is_replication_artifact"] is True
    assert dms_finding["artifact_origin"] == "AWS DMS"


def test_repairways_refuses_repair_on_replication_artifacts(tmp_path):
    eng = SimpleNamespace(
        columns_pair=lambda: None,
        ROWS_PUT_BACK=True,
        repair_found=lambda db, t: {"missing": [(1,)]},
        repair_sides=lambda db, t: None,
        repair_leaves=lambda db, t: None,
        hop=SimpleNamespace(report_dir=lambda *a: tmp_path),
        apply=lambda db, act: pytest.fail("apply must not be called on replication artifacts"),
    )
    action = SimpleNamespace(by="engine", table="awsdms_apply_exceptions")
    ladder = repairways.Ladder(eng, "shop", action)

    assert ladder.ready is False
    assert "replication/mover bookkeeping artifact (AWS DMS)" in ladder.why_not

    foreseen = ladder.foreseen()
    assert "not repairable" in foreseen
    assert "AWS DMS" in foreseen

    outcome = ladder.run()
    assert outcome.way == ""
    assert "not an application repair target" in outcome.reason
