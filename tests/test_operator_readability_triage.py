import pytest
from migkit import cli, report, verdict
from migkit.config import Endpoint, Hop
from migkit.result_scope import build_grouped_summary, classify_care, format_console, triage_summary


def _fake_hop(tmp_path=None):
    h = Hop(name="test-hop", engine="postgres",
            source=Endpoint(host="10.0.0.1", port=5432, user="u", password="p"),
            target=Endpoint(host="10.0.0.2", port=5432, user="u", password="p"),
            databases=["shop"])
    if tmp_path:
        h.report_dir = lambda db=None: tmp_path
    return h


def test_classify_care_distinguishes_must_care_from_dont_care():
    data_diff = {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "14 rows differ"}
    c1 = classify_care(data_diff)
    assert c1["tier"] == "action"
    assert c1["care_level"] == "must_care"
    assert c1["action_required"] is True

    artifact_finding = {"check": "schema", "scope": "shop.awsdms_apply_exceptions", "status": "diff", "detail": "extra table on target"}
    c2 = classify_care(artifact_finding)
    assert c2["tier"] == "benign"
    assert c2["care_level"] == "dont_care"
    assert c2["action_required"] is False
    assert "artifact" in c2["care_reason"].lower()

    kept_finding = {"check": "data", "scope": "shop.audit_log", "status": "diff", "detail": "10 rows kept by the hop: deletes not carried"}
    c3 = classify_care(kept_finding)
    assert c3["tier"] == "benign"
    assert c3["care_level"] == "dont_care"
    assert c3["action_required"] is False

    param_finding = {"check": "params", "scope": "shop", "status": "warn", "detail": "statement_timeout 30s vs 60s"}
    c4 = classify_care(param_finding)
    assert c4["tier"] == "benign"
    assert c4["care_level"] == "dont_care"
    assert c4["action_required"] is False

    schema_drift = {"check": "schema", "scope": "shop.users", "status": "warn", "detail": "missing index idx_users_email"}
    c5 = classify_care(schema_drift)
    assert c5["tier"] == "review"
    assert c5["care_level"] == "review"
    assert c5["action_required"] is False

    ok_finding = {"check": "counts", "scope": "shop.orders", "status": "ok", "detail": "1000==1000"}
    c6 = classify_care(ok_finding)
    assert c6["tier"] == "pass"
    assert c6["care_level"] == "no_action"
    assert c6["action_required"] is False


def test_triage_summary_counts_and_care_status():
    records = [
        {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "10 rows missing"},
        {"check": "schema", "scope": "shop.users", "status": "warn", "detail": "column varchar length differs"},
        {"check": "schema", "scope": "shop.awsdms_apply_exceptions", "status": "diff", "detail": "artifact table"},
        {"check": "counts", "scope": "shop.items", "status": "ok", "detail": "matches"},
    ]
    triage = triage_summary(records, _fake_hop())
    assert triage["has_action_required"] is True
    assert triage["has_attention"] is True
    assert triage["has_benign"] is True
    assert triage["care_status"] == "action"
    assert triage["counts"]["action_required"] == 1
    assert triage["counts"]["attention"] == 1
    assert triage["counts"]["benign"] == 1
    assert triage["counts"]["passed"] == 1
    assert "1 action" in triage["headline"]
    assert "1 benign" in triage["headline"]


def test_format_console_executive_triage_dashboard():
    records = [
        {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "10 rows missing on target"},
        {"check": "schema", "scope": "shop.users", "status": "warn", "detail": "missing index idx_u_email"},
        {"check": "schema", "scope": "shop.awsdms_apply_exceptions", "status": "diff", "detail": "internal artifact table"},
        {"check": "counts", "scope": "shop.items", "status": "ok", "detail": "matches"},
    ]
    envelope = build_grouped_summary(records, hop=_fake_hop())
    console_out = format_console(envelope)

    assert "MIGRATION RESULT SUMMARY: DIFFERENT" in console_out
    assert "OVERALL: DIFFERENT (action: 1, review: 1, benign: 1, ok: 1)" in console_out
    assert "ACTIONABLE ISSUES & IMPACT:" in console_out
    assert "BENIGN (safe to ignore):" in console_out


def test_cli_said_row_visual_badges(capsys):
    cli._said_row({"check": "data", "scope": "shop.orders", "status": "diff", "detail": "10 rows differ"})
    out1 = capsys.readouterr().out
    assert "ACTION" in out1
    assert "DIFF" in out1
    assert "shop.orders" in out1

    cli._said_row({"check": "schema", "scope": "shop.awsdms_apply_exceptions", "status": "diff", "detail": "internal tracker"})
    out2 = capsys.readouterr().out
    assert "BENIGN" in out2
    assert "internal tracker" in out2

    cli._said_row({"check": "counts", "scope": "shop.items", "status": "ok", "detail": "100 rows"})
    out3 = capsys.readouterr().out
    assert "OK" in out3


def test_verdict_summarize_carries_operator_summary(tmp_path):
    hop = _fake_hop(tmp_path)
    records = [
        {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "1 row missing"},
        {"check": "counts", "scope": "shop.items", "status": "ok", "detail": "matches"},
    ]
    env = verdict.summarize(hop, records)
    assert env["action_required"] is True
    assert "operator_summary" in env
    assert env["operator_summary"]["counts"]["action_required"] == 1
    assert env["findings"][0]["tier"] == "action"
    assert env["findings"][0]["care_level"] == "must_care"


def test_report_html_renders_safe_to_ignore_pill_and_reason(tmp_path):
    hop = _fake_hop(tmp_path)
    records = [
        {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "1 row missing"},
        {"check": "schema", "scope": "shop.awsdms_apply_exceptions", "status": "diff", "detail": "tracker table"},
    ]
    decided = verdict.decide(records, hop)
    html_out = report.render(hop, records, decided=decided)
    assert "ACTION" in html_out
    assert "BENIGN" in html_out
