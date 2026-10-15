import json
from types import SimpleNamespace
import pytest
from migkit import report, result_scope


def _fake_hop(name="test-hop", engine="postgres"):
    return SimpleNamespace(
        name=name,
        engine=engine,
        service=None,
        options={},
        source=SimpleNamespace(user="u_src", host="h_src", port=5432),
        target=SimpleNamespace(
            user="u_dst",
            host="h_dst",
            port=5432,
            configured=lambda: True,
            options={},
        ),
        report_dir=lambda *a: None,
        scope_fingerprint=lambda: "dummy-fp",
        scope_changed=lambda prev: False,
    )


def test_grouped_summary_clean_run():
    records = [
        {"check": "schema", "scope": "shop.orders", "status": "ok", "detail": "schema matches"},
        {"check": "counts", "scope": "shop.orders", "status": "ok", "detail": "rows 100==100"},
        {"check": "data", "scope": "shop.orders", "status": "ok", "detail": "all bytes identical"},
        {"check": "schema", "scope": "users.accounts", "status": "ok", "detail": "schema matches"},
        {"check": "counts", "scope": "users.accounts", "status": "ok", "detail": "rows 50==50"},
        {"check": "data", "scope": "users.accounts", "status": "ok", "detail": "all bytes identical"},
    ]
    envelope = result_scope.build_grouped_summary(
        records,
        hop=_fake_hop(),
        coverage={"checks": ["schema", "counts", "data"]},
    )

    assert envelope.status == "same"
    assert envelope.has_differences is False
    assert set(envelope.databases.keys()) == {"shop", "users"}
    assert envelope.databases_clean == ["shop", "users"]
    assert envelope.databases_with_issues == []
    assert envelope.databases_incomplete == []
    assert envelope.total_checks == 6
    assert envelope.total_passed == 6
    assert envelope.total_issues == 0

    shop_summary = envelope.databases["shop"]
    assert shop_summary.status == "same"
    assert shop_summary.checks_completed == 3
    assert shop_summary.checks_planned == 3
    assert shop_summary.issues_count == 0
    assert shop_summary.practical_problems == []
    assert shop_summary.remaining_work == []


def test_grouped_summary_mixed_issues_and_artifacts():
    records = [
        {
            "category": "data",
            "check": "data",
            "scope": "shop.orders",
            "status": "diff",
            "detail": "missing 2 rows on target",
            "source_state": "1000 rows",
            "target_state": "998 rows",
            "fix_hint": "run migkit repair --table shop.orders",
        },
        {
            "category": "schema",
            "check": "schema",
            "scope": "shop.awsdms_apply_exceptions",
            "status": "diff",
            "detail": "extra table on target",
        },
        {
            "category": "autoinc",
            "check": "autoinc",
            "scope": "users.accounts",
            "status": "diff",
            "detail": "target autoinc value 10 behind source",
            "source_state": "max=110",
            "target_state": "next=100",
        },
        {
            "category": "schema",
            "check": "schema",
            "scope": "inventory.items",
            "status": "ok",
            "detail": "matches",
        },
    ]

    coverage = {"checks": ["schema", "counts", "autoinc", "data", "deep", "delta"]}
    envelope = result_scope.build_grouped_summary(records, hop=_fake_hop(), coverage=coverage)

    assert envelope.status == "different"
    assert envelope.has_differences is True
    assert "shop" in envelope.databases_with_issues
    assert "users" in envelope.databases_with_issues
    assert "inventory" in envelope.databases_incomplete

    shop_db = envelope.databases["shop"]
    assert shop_db.status == "different"
    assert shop_db.issues_count == 2

    app_prob = next(p for p in shop_db.practical_problems if p.object_name == "shop.orders")
    assert app_prob.defect_class == "application_defect"
    assert app_prob.is_replication_artifact is False
    assert "Row data differs" in app_prob.impact
    assert "migkit repair" in app_prob.next_action

    artifact_prob = next(p for p in shop_db.practical_problems if "awsdms" in p.object_name)
    assert artifact_prob.defect_class == "replication_artifact"
    assert artifact_prob.is_replication_artifact is True
    assert artifact_prob.artifact_origin == "AWS DMS"
    assert "Replication engine leftover" in artifact_prob.impact

    users_db = envelope.databases["users"]
    assert users_db.status == "different"
    autoinc_prob = users_db.practical_problems[0]
    assert "sequence/autoinc is behind source" in autoinc_prob.impact

    assert "counts" in inventory_db_work if (inventory_db_work := envelope.databases["inventory"].remaining_work) else True


def test_console_and_html_rendering():
    records = [
        {
            "category": "data",
            "check": "data",
            "scope": "shop.orders",
            "status": "diff",
            "detail": "missing 1 row on target",
        },
        {
            "category": "data",
            "check": "data",
            "scope": "billing.invoices",
            "status": "ok",
            "detail": "rows 200==200",
        },
    ]
    envelope = result_scope.build_grouped_summary(records, hop=_fake_hop())

    console_out = result_scope.format_console(envelope)
    assert "MIGRATION RESULT SUMMARY: DIFFERENT" in console_out
    assert "shop" in console_out
    assert "billing" in console_out
    assert "ACTIONABLE ISSUES & IMPACT:" in console_out
    assert "missing 1 row on target" in console_out

    html_out = result_scope.format_html(envelope)
    assert "database summary (database-first grouping)" in html_out
    assert "shop" in html_out
    assert "billing" in html_out
    assert "All checks verified clean" in html_out


def test_structured_export_dictionary():
    records = [
        {"check": "counts", "scope": "db1.t1", "status": "ok", "detail": "5==5"},
        {"check": "counts", "scope": "db2.t2", "status": "diff", "detail": "5!=6"},
    ]
    envelope = result_scope.build_grouped_summary(records, hop=_fake_hop())
    exported = envelope.to_dict()

    assert exported["format_version"] == "2.0-result-scope"
    assert exported["status"] == "different"
    assert exported["has_differences"] is True
    assert "db1" in exported["databases_clean"]
    assert "db2" in exported["databases_with_issues"]
    assert exported["total_checks"] == 2
    assert exported["total_passed"] == 1
    assert exported["total_issues"] == 1

    json_str = json.dumps(exported)
    loaded = json.loads(json_str)
    assert loaded["format_version"] == "2.0-result-scope"


def test_report_render_integration():
    hop = _fake_hop()
    results = [
        {"check": "counts", "scope": "shop.orders", "status": "ok", "detail": "rows 10==10"},
        {"check": "data", "scope": "shop.orders", "status": "diff", "detail": "checksum mismatch"},
    ]
    rendered = report.render(hop, results)

    assert "DIFFERENT" in rendered
    assert "database summary (database-first grouping)" in rendered
    assert "shop" in rendered
    # the report compares the two sides, never migkit with a product
    low = rendered.lower()
    assert "commercial" not in low
    for name in ("veridata", "debezium", "awsdms", "qlik", "redgate",
                 "datafold", "database migration service"):
        assert name not in low, name


def test_unmatched_dimensions_catalog_and_formatting():
    unmatched = result_scope.UNMATCHED_DIMENSIONS
    expected_keys = [
        "bidirectional_drift_proof",
        "schema_invariant_barriers",
        "replication_artifact_partitioning",
        "sequence_autoinc_headroom",
        "routine_event_semantic_parity",
        "runtime_parameter_invariants",
        "proof_invalidation_ddl_boundary",
    ]
    for k in expected_keys:
        assert k in unmatched
        assert "name" in unmatched[k]
        assert "barrier_type" in unmatched[k]
        assert "commercial_status" not in unmatched[k]
        for name in ("awsdms", "debezium", "dts_"):
            assert name not in unmatched[k]["description"], (k, name)

    sample_finding = {
        "dimension": "bidirectional_drift_proof",
        "database": "fintech_db",
        "affected_object": "fintech_db.ledger",
        "short_problem": "multiset hash discrepancy during live CDC",
        "status": "diff",
        "source_state": "hash=0xbeef",
        "target_state": "hash=0xdead",
        "detail": "bidirectional multiset proof failed",
    }
    formatted = result_scope.format_unmatched_dimension(sample_finding)
    assert formatted["name"] == "Bidirectional Drift Proof"
    assert formatted["barrier_type"] == "mathematical_proof"
    assert formatted["database"] == "fintech_db"
    assert formatted["affected_object"] == "fintech_db.ledger"
    assert formatted["barrier_status"] == "violated"
    assert "Live writes diverted or lost" in formatted["impact"]


def test_unmatched_dimensions_in_grouped_summary_and_reports():
    records = [
        {
            "category": "verify",
            "check": "deep",
            "dimension": "schema_invariant_barriers",
            "scope": "crm.contracts",
            "status": "diff",
            "detail": "foreign key dependency cycle detected between contracts and customers",
        },
        {
            "category": "verify",
            "check": "delta",
            "dimension": "bidirectional_drift_proof",
            "scope": "crm.accounts",
            "status": "ok",
            "detail": "multiset hash converged to identical proof",
        },
    ]

    envelope = result_scope.build_grouped_summary(records, hop=_fake_hop())
    assert len(envelope.unmatched_dimensions) == 2

    barrier_finding = next(u for u in envelope.unmatched_dimensions if u["dimension"] == "schema_invariant_barriers")
    assert barrier_finding["barrier_status"] == "violated"
    assert "Foreign key dependency ordering violated" in barrier_finding["impact"]

    drift_finding = next(u for u in envelope.unmatched_dimensions if u["dimension"] == "bidirectional_drift_proof")
    assert drift_finding["barrier_status"] == "passed"

    console_out = result_scope.format_console(envelope)
    assert "PROOFS BEYOND ROW COMPARISON:" in console_out
    assert "Schema Invariant Barriers [VIOLATED]" in console_out
    assert "Bidirectional Drift Proof [PASSED]" in console_out

    html_out = result_scope.format_html(envelope)
    assert "proofs beyond row comparison" in html_out
    assert "commercial" not in html_out.lower()
    assert "Schema Invariant Barriers" in html_out
    assert "Bidirectional Drift Proof" in html_out
