"""Tests for RCH18.conversion-ledger: scope accounting for code/ETL/app artefacts.

Verifies:
- Distinct counts for submitted, extracted, parsed, converted, deployed, executed, covered.
- Explicit debt tracking with care badges for unsupported encodings, encrypted code, and dynamic SQL.
- Reporting of every denominator (no conversion percentage hides excluded/unparsed objects).
- One unparsed critical routine blocks complete readiness even when 100% of parsed code converted.
- Scope survives rename and restart across persistence cycles.
- Active lease lock (.run.lock) mutual exclusion.
- O(1) typed durable resume skipping completed units.
"""
import json
from pathlib import Path
import pytest
import time

from migkit import decide, durable
from migkit.conversion_ledger import (
    ActiveLeaseLock,
    ArtefactCriticality,
    ArtefactIdentity,
    ArtefactKind,
    ArtefactRecord,
    CareBadge,
    ConversionLedger,
    CorruptLockError,
    LockAcquisitionError,
    DebtRecord,
    DebtType,
    DenominatorRatio,
    LifecycleStage,
    ReadinessVerdict,
    ScopeMetrics,
    conversion_scope_rungs,
    decide_conversion_scope,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("conversion_ledger", "conversion_scope_rungs", "certified_readiness_pass", "holds"),
    ("conversion_ledger", "conversion_scope_rungs", "certified_readiness_pass", "refuses"),
    ("conversion_ledger", "conversion_scope_rungs", "provisional_readiness_review", "holds"),
    ("conversion_ledger", "conversion_scope_rungs", "provisional_readiness_review", "refuses"),
    ("conversion_ledger", "conversion_scope_rungs", "critical_routine_blocked_action", "holds"),
    ("conversion_ledger", "conversion_scope_rungs", "critical_routine_blocked_action", "refuses"),
    ("conversion_ledger", "conversion_scope_rungs", "action_debt_blocked_action", "holds"),
]


def test_scope_accounting_keeps_distinct_stages(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="test_distinct_stages", base_dir=tmp_path)

    # 1. Submit 5 artefacts
    p1 = ledger.submit_artefact(
        name="calc_interest",
        kind=ArtefactKind.PROCEDURE,
        schema="finance",
        content="CREATE PROCEDURE calc_interest() ...",
        criticality=ArtefactCriticality.CRITICAL,
    )
    p2 = ledger.submit_artefact(
        name="generate_invoice",
        kind=ArtefactKind.PROCEDURE,
        schema="billing",
        content="CREATE PROCEDURE generate_invoice() ...",
        criticality=ArtefactCriticality.HIGH,
    )
    v1 = ledger.submit_artefact(
        name="v_monthly_summary",
        kind=ArtefactKind.VIEW,
        schema="reporting",
        content="CREATE VIEW v_monthly_summary AS SELECT ...",
        criticality=ArtefactCriticality.MEDIUM,
    )
    etl1 = ledger.submit_artefact(
        name="daily_extract_job",
        kind=ArtefactKind.ETL_JOB,
        schema="etl",
        content="COPY orders TO s3://...",
        criticality=ArtefactCriticality.MEDIUM,
    )
    app1 = ledger.submit_artefact(
        name="lookup_customer_by_uuid",
        kind=ArtefactKind.APP_QUERY,
        schema="app",
        content="SELECT * FROM customers WHERE id = ?",
        criticality=ArtefactCriticality.LOW,
    )

    metrics = ledger.compute_metrics()
    assert metrics.submitted == 5
    assert metrics.extracted == 0
    assert metrics.parsed == 0
    assert metrics.converted == 0
    assert metrics.deployed == 0
    assert metrics.executed == 0
    assert metrics.covered == 0
    assert metrics.unextracted == 5

    # 2. Extract 4 of them (etl1 fails extraction)
    for art in [p1, p2, v1, app1]:
        ledger.mark_extracted(art.identity.canonical_id)
    ledger.record_extraction_debt(
        canonical_id=etl1.identity.canonical_id,
        debt_type=DebtType.UNRESOLVED_DEPENDENCY,
        reason="Source ETL definition file not accessible on remote host 10.0.0.12",
        care_badge=CareBadge.ACTION,
    )

    metrics = ledger.compute_metrics()
    assert metrics.submitted == 5
    assert metrics.extracted == 4
    assert metrics.unextracted == 1

    # 3. Parse 3 of the extracted (app1 has syntax error / debt)
    for art in [p1, p2, v1]:
        ledger.mark_parsed(art.identity.canonical_id)
    ledger.record_parse_debt(
        canonical_id=app1.identity.canonical_id,
        debt_type=DebtType.SYNTAX_ERROR,
        reason="Dialect token error near '?'",
        care_badge=CareBadge.ACTION,
    )

    metrics = ledger.compute_metrics()
    assert metrics.extracted == 4
    assert metrics.parsed == 3
    assert metrics.unparsed == 1

    # 4. Convert 2 of the parsed (v1 fails translation)
    ledger.mark_converted(
        p1.identity.canonical_id,
        target_code="CREATE OR REPLACE PROCEDURE calc_interest() LANGUAGE plpgsql ...",
        rung="migkit_rules",
    )
    ledger.mark_converted(
        p2.identity.canonical_id,
        target_code="CREATE OR REPLACE PROCEDURE generate_invoice() LANGUAGE plpgsql ...",
        rung="snowconvert",
    )
    ledger.record_conversion_debt(
        canonical_id=v1.identity.canonical_id,
        debt_type=DebtType.CONVERSION_REFUSED,
        reason="Target dialect lacks recursive window clause",
        care_badge=CareBadge.ACTION,
    )

    metrics = ledger.compute_metrics()
    assert metrics.parsed == 3
    assert metrics.converted == 2
    assert metrics.unconverted == 1

    # 5. Deploy 2 converted
    ledger.mark_deployed(p1.identity.canonical_id)
    ledger.mark_deployed(p2.identity.canonical_id)
    metrics = ledger.compute_metrics()
    assert metrics.converted == 2
    assert metrics.deployed == 2
    assert metrics.undeployed == 0

    # 6. Execute 1 deployed (p2 execution pending)
    ledger.mark_executed(p1.identity.canonical_id, elapsed_sec=0.125)
    metrics = ledger.compute_metrics()
    assert metrics.deployed == 2
    assert metrics.executed == 1
    assert metrics.unexecuted == 1

    # 7. Cover 1 executed
    ledger.mark_covered(p1.identity.canonical_id, coverage_ratio=1.0)
    metrics = ledger.compute_metrics()
    assert metrics.executed == 1
    assert metrics.covered == 1
    assert metrics.uncovered == 0

    # Every stage count is distinct!
    assert metrics.submitted == 5
    assert metrics.extracted == 4
    assert metrics.parsed == 3
    assert metrics.converted == 2
    assert metrics.deployed == 2
    assert metrics.executed == 1
    assert metrics.covered == 1


def test_explicit_debt_tracking_for_encodings_encryption_and_dynamic_sql(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="test_debt", base_dir=tmp_path)

    # 1. Unsupported encoding
    enc_art = ledger.submit_artefact(
        name="legacy_shift_jis_pkg",
        kind=ArtefactKind.PACKAGE,
        schema="legacy",
        content="-- legacy package header",
        criticality=ArtefactCriticality.HIGH,
    )
    ledger.mark_extracted(enc_art.identity.canonical_id)
    debt1 = ledger.record_parse_debt(
        canonical_id=enc_art.identity.canonical_id,
        debt_type=DebtType.UNSUPPORTED_ENCODING,
        reason="File contains unmapped byte 0x82a0 under ISO-8859-1 fallback",
        care_badge=CareBadge.ACTION,
        remediation_hint="Convert source encoding from CP932/Shift-JIS to UTF-8",
    )

    # 2. Encrypted code (Oracle WRAPPED or SQL Server WITH ENCRYPTION)
    enc_proc = ledger.submit_artefact(
        name="pr_proprietary_pricing",
        kind=ArtefactKind.PROCEDURE,
        schema="pricing",
        content="CREATE PROCEDURE pr_proprietary_pricing WRAPPED a000000 ...",
        criticality=ArtefactCriticality.CRITICAL,
    )
    ledger.mark_extracted(enc_proc.identity.canonical_id)
    debt2 = ledger.record_parse_debt(
        canonical_id=enc_proc.identity.canonical_id,
        debt_type=DebtType.ENCRYPTED_CODE,
        reason="Encrypted PL/SQL body (WRAPPED); source plain text required",
        care_badge=CareBadge.ACTION,
        remediation_hint="Obtain original unwrapped source definition before migration",
    )

    # 3. Dynamic SQL (reviewed benign vs action)
    dyn_art = ledger.submit_artefact(
        name="sp_dynamic_report_builder",
        kind=ArtefactKind.PROCEDURE,
        schema="reports",
        content="CREATE PROCEDURE sp_dynamic_report_builder() ...",
        criticality=ArtefactCriticality.MEDIUM,
    )
    ledger.mark_extracted(dyn_art.identity.canonical_id)
    ledger.mark_parsed(dyn_art.identity.canonical_id)
    debt3 = ledger.record_debt(
        canonical_id=dyn_art.identity.canonical_id,
        debt_type=DebtType.DYNAMIC_SQL,
        care_badge=CareBadge.REVIEW,
        reason="Dynamic SQL construction using sp_executesql requires runtime parameter bind check",
        offset_or_symbol="sp_executesql @stmt, @params",
        remediation_hint="Verify dynamic statement parameterization against target database 10.0.0.21",
    )

    metrics = ledger.compute_metrics()
    assert metrics.unsupported_encoding_debt == 1
    assert metrics.encrypted_code_debt == 1
    assert metrics.dynamic_sql_debt == 1
    assert metrics.action_debt_count == 2
    assert metrics.review_debt_count == 1
    assert metrics.benign_debt_count == 0
    assert metrics.total_active_debts == 3

    # Resolve debt1
    assert ledger.resolve_debt(debt1.debt_id, note="Source file re-encoded to UTF-8")
    assert debt1.resolved is True

    metrics_after = ledger.compute_metrics()
    assert metrics_after.unsupported_encoding_debt == 0
    assert metrics_after.action_debt_count == 1
    assert metrics_after.total_active_debts == 2


def test_every_denominator_reported_no_hidden_exclusions(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="test_denominators", base_dir=tmp_path)

    # 10 submitted artefacts
    # 2 fail parsing due to encrypted code
    # 8 parsed successfully
    # All 8 parsed are converted (100% of parsed!)
    for i in range(1, 9):
        rec = ledger.submit_artefact(
            name=f"proc_{i}",
            kind=ArtefactKind.PROCEDURE,
            schema="public",
            content=f"CREATE PROCEDURE proc_{i}() BEGIN NULL; END;",
        )
        ledger.mark_extracted(rec.identity.canonical_id)
        ledger.mark_parsed(rec.identity.canonical_id)
        ledger.mark_converted(
            rec.identity.canonical_id,
            target_code=f"CREATE OR REPLACE PROCEDURE proc_{i}() LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$;",
        )

    # 2 encrypted routines
    for j in [9, 10]:
        rec = ledger.submit_artefact(
            name=f"proc_enc_{j}",
            kind=ArtefactKind.PROCEDURE,
            schema="public",
            content=f"CREATE PROCEDURE proc_enc_{j} WRAPPED ...",
        )
        ledger.mark_extracted(rec.identity.canonical_id)
        ledger.record_parse_debt(
            rec.identity.canonical_id,
            debt_type=DebtType.ENCRYPTED_CODE,
            reason="Oracle WRAPPED binary header",
            care_badge=CareBadge.ACTION,
        )

    metrics = ledger.compute_metrics()

    # Parsed denominator
    assert metrics.converted_over_parsed.numerator == 8
    assert metrics.converted_over_parsed.denominator == 8
    assert metrics.converted_over_parsed.percentage == 100.0

    # Submitted denominator reveals excluded objects
    assert metrics.converted_over_submitted.numerator == 8
    assert metrics.converted_over_submitted.denominator == 10
    assert metrics.converted_over_submitted.percentage == 80.0

    # Parse rate over submitted
    assert metrics.parsed_over_submitted.numerator == 8
    assert metrics.parsed_over_submitted.denominator == 10
    assert metrics.parsed_over_submitted.percentage == 80.0

    # Check text format contains both denominators explicitly
    unix_report = metrics.format_unix()
    assert "converted_over_parsed" in unix_report
    assert "8/8     (100.00%)" in unix_report
    assert "converted_over_submitted" in unix_report
    assert "8/10    ( 80.00%)" in unix_report
    assert "ENCRYPTED CODE       : 2" in unix_report


def test_unparsed_critical_routine_blocks_complete_readiness_even_when_100_pct_parsed_converted(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="test_readiness_gate", base_dir=tmp_path)

    # 5 standard routines: all parsed and all converted
    for i in range(1, 6):
        rec = ledger.submit_artefact(
            name=f"routine_{i}",
            kind=ArtefactKind.FUNCTION,
            schema="app",
            content=f"CREATE FUNCTION routine_{i}() RETURNS INT ...",
            criticality=ArtefactCriticality.MEDIUM,
        )
        ledger.mark_extracted(rec.identity.canonical_id)
        ledger.mark_parsed(rec.identity.canonical_id)
        ledger.mark_converted(
            rec.identity.canonical_id,
            target_code=f"CREATE OR REPLACE FUNCTION routine_{i}() ...",
        )

    # 1 CRITICAL routine: fails parsing due to unsupported dialect / encoding
    crit = ledger.submit_artefact(
        name="sp_settle_core_ledger",
        kind=ArtefactKind.PROCEDURE,
        schema="core",
        content="CREATE PROCEDURE sp_settle_core_ledger() ...",
        criticality=ArtefactCriticality.CRITICAL,
    )
    ledger.mark_extracted(crit.identity.canonical_id)
    ledger.record_parse_debt(
        canonical_id=crit.identity.canonical_id,
        debt_type=DebtType.UNSUPPORTED_ENCODING,
        reason="Non-decodable byte sequence in critical transaction handler",
        care_badge=CareBadge.ACTION,
    )

    metrics = ledger.compute_metrics()
    # 5 parsed, 5 converted => 100% of parsed code converted!
    assert metrics.converted_over_parsed.percentage == 100.0
    assert metrics.converted == 5
    assert metrics.parsed == 5
    assert metrics.submitted == 6

    # Complete readiness MUST BE BLOCKED because critical routine is unparsed!
    verdict = ledger.evaluate_readiness()
    assert verdict.is_ready is False
    assert verdict.care_badge == CareBadge.ACTION
    assert any("sp_settle_core_ledger" in b and "CRITICAL routine" in b for b in verdict.blockers)

    # Format verification
    verdict_text = verdict.format_unix()
    assert "[ACTION] READINESS VERDICT: BLOCKED" in verdict_text
    assert "Converted / Parsed    : 5/5 (100.00%)" in verdict_text
    assert "Converted / Submitted : 5/6 (83.33%)" in verdict_text

    # Now simulate fixing the critical routine: parse and convert it
    ledger.resolve_debt(crit.debts[0].debt_id, note="Repaired file encoding with UTF-8 BOM")
    ledger.mark_parsed(crit.identity.canonical_id)
    ledger.mark_converted(
        crit.identity.canonical_id,
        target_code="CREATE OR REPLACE PROCEDURE sp_settle_core_ledger() LANGUAGE plpgsql ...",
    )

    # Re-evaluate readiness
    new_verdict = ledger.evaluate_readiness()
    assert new_verdict.is_ready is True
    assert new_verdict.care_badge == CareBadge.PASS
    assert len(new_verdict.blockers) == 0
    assert new_verdict.metrics.converted_over_submitted.percentage == 100.0


def test_scope_survives_rename_and_restart(tmp_path: Path):
    ledger_file = tmp_path / "conversion-ledger.json"
    ledger = ConversionLedger(ledger_id="survive_test", base_dir=tmp_path)

    # Create artefact with debt and partial lifecycle
    art = ledger.submit_artefact(
        name="sp_calculate_tax_legacy",
        kind=ArtefactKind.PROCEDURE,
        schema="tax",
        content="CREATE PROCEDURE sp_calculate_tax_legacy() ...",
        criticality=ArtefactCriticality.HIGH,
    )
    cid = art.identity.canonical_id
    ledger.mark_extracted(cid)
    ledger.mark_parsed(cid)
    debt = ledger.record_debt(
        canonical_id=cid,
        debt_type=DebtType.DYNAMIC_SQL,
        care_badge=CareBadge.REVIEW,
        reason="Dynamic tax bracket lookup table concatenation",
    )

    # Rename artefact in ledger
    renamed_art = ledger.rename_artefact(
        old_name="sp_calculate_tax_legacy",
        new_name="sp_calculate_tax_v2",
        schema="tax",
    )
    assert renamed_art.identity.canonical_id == cid
    assert renamed_art.identity.name == "sp_calculate_tax_v2"
    assert "tax.sp_calculate_tax_legacy" in renamed_art.identity.aliases

    # Lookup works by both new name and legacy alias
    assert ledger.find_canonical_id("sp_calculate_tax_v2", schema="tax") == cid
    assert ledger.find_canonical_id("sp_calculate_tax_legacy", schema="tax") == cid

    # Advance stage to converted under new name
    ledger.mark_converted(
        canonical_id=cid,
        target_code="CREATE OR REPLACE PROCEDURE sp_calculate_tax_v2() LANGUAGE plpgsql ...",
    )

    # Save to disk (checkpoint)
    ledger.save_to_file(ledger_file)
    assert ledger_file.exists()

    # Restart: instantiate fresh ledger instance and load state
    fresh_ledger = ConversionLedger(ledger_id="fresh_restart", base_dir=tmp_path)
    fresh_ledger.load_from_file(ledger_file)

    # Assert scope survives restart
    restarted_art = fresh_ledger.get_artefact(cid)
    assert restarted_art is not None
    assert restarted_art.identity.canonical_id == cid
    assert restarted_art.identity.name == "sp_calculate_tax_v2"
    assert restarted_art.identity.criticality == ArtefactCriticality.HIGH
    assert restarted_art.current_stage == LifecycleStage.CONVERTED
    assert LifecycleStage.PARSED in restarted_art.stages_reached
    assert LifecycleStage.CONVERTED in restarted_art.stages_reached
    assert len(restarted_art.debts) == 1
    assert restarted_art.debts[0].debt_id == debt.debt_id
    assert restarted_art.debts[0].debt_type == DebtType.DYNAMIC_SQL
    assert restarted_art.debts[0].care_badge == CareBadge.REVIEW

    # Lookup by alias and new name still works after restart
    assert fresh_ledger.find_canonical_id("tax.sp_calculate_tax_legacy") == cid
    assert fresh_ledger.find_canonical_id("tax.sp_calculate_tax_v2") == cid

    # Verify metrics preserved
    metrics = fresh_ledger.compute_metrics()
    assert metrics.submitted == 1
    assert metrics.extracted == 1
    assert metrics.parsed == 1
    assert metrics.converted == 1
    assert metrics.dynamic_sql_debt == 1


def test_o1_typed_durable_resume_skips_completed_units(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="resume_test", base_dir=tmp_path)

    # Create 4 artefacts
    # 2 already converted
    # 2 pending conversion (1 parsed, 1 unparsed)
    a1 = ledger.submit_artefact("art_1", ArtefactKind.PROCEDURE)
    a2 = ledger.submit_artefact("art_2", ArtefactKind.PROCEDURE)
    a3 = ledger.submit_artefact("art_3", ArtefactKind.PROCEDURE)
    a4 = ledger.submit_artefact("art_4", ArtefactKind.PROCEDURE)

    for a in [a1, a2, a3]:
        ledger.mark_extracted(a.identity.canonical_id)
        ledger.mark_parsed(a.identity.canonical_id)

    ledger.mark_converted(a1.identity.canonical_id, "target_1")
    ledger.mark_converted(a2.identity.canonical_id, "target_2")

    # Fast filter pending units for CONVERTED stage
    pending_for_conversion = ledger.filter_pending(LifecycleStage.CONVERTED)
    pending_ids = {p.identity.canonical_id for p in pending_for_conversion}

    # Completed units a1 and a2 are skipped in O(1)
    assert a1.identity.canonical_id not in pending_ids
    assert a2.identity.canonical_id not in pending_ids
    assert a3.identity.canonical_id in pending_ids
    assert a4.identity.canonical_id in pending_ids
    assert len(pending_for_conversion) == 2


def test_active_lease_lock_mutual_exclusion(tmp_path: Path):
    lock_path = tmp_path / ".run.lock"
    lease_a = ActiveLeaseLock(lock_path, term_seconds=10.0)
    lease_b = ActiveLeaseLock(lock_path, term_seconds=10.0)

    # Acquire lease A
    assert lease_a.acquire() is True
    assert lock_path.exists()

    with open(lock_path, "r", encoding="ascii") as f:
        payload = json.load(f)
    assert payload["pid"] > 0
    assert "expires_at" in payload

    # Lease B cannot acquire concurrently
    assert lease_b.acquire() is False

    # Renew lease A
    assert lease_a.renew() is True

    # Release lease A
    lease_a.release()

    # Now lease B can acquire
    assert lease_b.acquire() is True
    lease_b.release()


def test_pure_ascii_and_unix_formatting(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="test_format", base_dir=tmp_path)
    rec = ledger.submit_artefact(
        name="ascii_test_proc",
        kind=ArtefactKind.PROCEDURE,
        schema="public",
        content="CREATE PROCEDURE ascii_test_proc() ...",
    )
    ledger.mark_extracted(rec.identity.canonical_id)
    ledger.mark_parsed(rec.identity.canonical_id)
    ledger.mark_converted(rec.identity.canonical_id, "CREATE PROCEDURE target() ...")

    report = ledger.format_unix_report()

    # Must be 100% ASCII
    assert all(ord(c) < 128 for c in report)
    # Care badge present
    assert "[PASS]" in report or "[ACTION]" in report or "[REVIEW]" in report
    # Standard UNIX banner lines
    assert "SCOPE ACCOUNTING LEDGER" in report
    assert "READINESS VERDICT" in report


def test_empty_ledger_metrics_and_zero_division(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="empty_test", base_dir=tmp_path)
    metrics = ledger.compute_metrics()

    assert metrics.submitted == 0
    assert metrics.parsed == 0
    assert metrics.converted == 0
    assert metrics.converted_over_parsed.percentage == 0.0
    assert metrics.converted_over_submitted.percentage == 0.0
    assert metrics.covered_over_submitted.percentage == 0.0

    verdict = ledger.evaluate_readiness()
    assert verdict.is_ready is True
    assert verdict.care_badge == CareBadge.PASS
    assert len(verdict.blockers) == 0


def test_dynamic_sql_badge_policy_and_strict_reviews(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="policy_test", base_dir=tmp_path)

    # 1. Benign dynamic SQL
    b_art = ledger.submit_artefact("benign_proc", ArtefactKind.PROCEDURE)
    ledger.mark_extracted(b_art.identity.canonical_id)
    ledger.mark_parsed(b_art.identity.canonical_id)
    ledger.mark_converted(b_art.identity.canonical_id, "target")
    ledger.record_debt(
        b_art.identity.canonical_id,
        debt_type=DebtType.DYNAMIC_SQL,
        care_badge=CareBadge.BENIGN,
        reason="Static string literal concatenation only",
    )

    verdict_b = ledger.evaluate_readiness()
    assert verdict_b.is_ready is True
    assert verdict_b.care_badge == CareBadge.PASS

    # 2. Review dynamic SQL
    r_art = ledger.submit_artefact("review_proc", ArtefactKind.PROCEDURE)
    ledger.mark_extracted(r_art.identity.canonical_id)
    ledger.mark_parsed(r_art.identity.canonical_id)
    ledger.mark_converted(r_art.identity.canonical_id, "target")
    ledger.record_debt(
        r_art.identity.canonical_id,
        debt_type=DebtType.DYNAMIC_SQL,
        care_badge=CareBadge.REVIEW,
        reason="Dynamic WHERE clause requires inspection on 127.0.0.1 sandbox",
    )

    # Standard review allows cutover with warning badge
    verdict_r = ledger.evaluate_readiness(strict_reviews=False)
    assert verdict_r.is_ready is True
    assert verdict_r.care_badge == CareBadge.REVIEW
    assert len(verdict_r.warnings) == 1

    # Strict review blocks cutover
    verdict_r_strict = ledger.evaluate_readiness(strict_reviews=True)
    assert verdict_r_strict.is_ready is False
    assert verdict_r_strict.care_badge == CareBadge.REVIEW

    # 3. Action dynamic SQL
    a_art = ledger.submit_artefact("action_proc", ArtefactKind.PROCEDURE)
    ledger.mark_extracted(a_art.identity.canonical_id)
    ledger.mark_parsed(a_art.identity.canonical_id)
    ledger.record_debt(
        a_art.identity.canonical_id,
        debt_type=DebtType.DYNAMIC_SQL,
        care_badge=CareBadge.ACTION,
        reason="Arbitrary unverified user SQL execution string",
    )
    verdict_a = ledger.evaluate_readiness()
    assert verdict_a.is_ready is False
    assert verdict_a.care_badge == CareBadge.ACTION
    assert any("Arbitrary unverified user SQL" in b for b in verdict_a.blockers)


def test_chained_renames_preserve_identity_and_all_aliases(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="chain_test", base_dir=tmp_path)
    art = ledger.submit_artefact("proc_v1", ArtefactKind.PROCEDURE, schema="billing")
    cid = art.identity.canonical_id

    ledger.rename_artefact("proc_v1", "proc_v2", schema="billing")
    ledger.rename_artefact("proc_v2", "proc_final", schema="billing")

    assert art.identity.name == "proc_final"
    assert "billing.proc_v1" in art.identity.aliases
    assert "billing.proc_v2" in art.identity.aliases

    # All alias lookups resolve to identical canonical id
    assert ledger.find_canonical_id("billing.proc_v1") == cid
    assert ledger.find_canonical_id("billing.proc_v2") == cid
    assert ledger.find_canonical_id("billing.proc_final") == cid
    assert ledger.find_canonical_id("proc_final") == cid


def test_binary_artefact_submission_and_hash(tmp_path: Path):
    ledger = ConversionLedger(ledger_id="bin_test", base_dir=tmp_path)
    raw = b"\x00\x01\x02\x03BINARY_PACKAGE_PAYLOAD"
    art = ledger.submit_artefact(
        name="bin_blob_routine",
        kind=ArtefactKind.PACKAGE_BODY,
        raw_bytes=raw,
        schema="dw",
    )
    assert art.identity.source_hash != ""
    assert art.current_stage == LifecycleStage.SUBMITTED


def test_conversion_scope_decide_rungs_coverage():
    """Proves all 7 branches of conversion_scope_rungs under watched()."""
    with watched() as seen:
        # 1. Clean run: all submitted parsed and converted, 0 critical unparsed, 0 debts
        # -> certified_readiness_pass holds
        c1 = decide_conversion_scope({
            "submitted_count": 5,
            "parsed_count": 5,
            "converted_count": 5,
            "unparsed_critical_count": 0,
            "action_debt_count": 0,
            "review_debt_count": 0,
            "strict_reviews": False,
        })
        assert c1.path == "certified_readiness_pass"

        # 2. Tolerated review debt: review_debt_count > 0, strict_reviews=False
        # -> certified_readiness_pass refuses (review debt > 0), provisional_readiness_review holds
        c2 = decide_conversion_scope({
            "submitted_count": 5,
            "parsed_count": 5,
            "converted_count": 5,
            "unparsed_critical_count": 0,
            "action_debt_count": 0,
            "review_debt_count": 1,
            "strict_reviews": False,
        })
        assert c2.path == "provisional_readiness_review"

        # 3. Unparsed critical routine: unparsed_critical_count > 0
        # -> certified refuses, provisional refuses, critical_routine_blocked_action holds
        c3 = decide_conversion_scope({
            "submitted_count": 5,
            "parsed_count": 4,
            "converted_count": 4,
            "unparsed_critical_count": 1,
            "action_debt_count": 0,
            "review_debt_count": 0,
            "strict_reviews": False,
        })
        assert c3.path == "critical_routine_blocked_action"

        # 4. Action debt present with 0 unparsed critical routines: action_debt_count > 0, unparsed_critical=0
        # -> certified refuses, provisional refuses, critical refuses, action_debt_blocked_action holds
        c4 = decide_conversion_scope({
            "submitted_count": 5,
            "parsed_count": 5,
            "converted_count": 5,
            "unparsed_critical_count": 0,
            "action_debt_count": 1,
            "review_debt_count": 0,
            "strict_reviews": False,
        })
        assert c4.path == "action_debt_blocked_action"

    assert set(branch(seen, "certified_readiness_pass")) == {HOLDS, REFUSES}
    assert set(branch(seen, "provisional_readiness_review")) == {HOLDS, REFUSES}
    assert set(branch(seen, "critical_routine_blocked_action")) == {HOLDS, REFUSES}
    assert branch(seen, "action_debt_blocked_action") == [HOLDS]


def test_active_lease_corrupt_lock_raises_corrupt_lock_error(tmp_path: Path):
    lock_file = tmp_path / ".run.lock"
    lock_file.write_text("{NOT_VALID_JSON:;;;}")
    lease = ActiveLeaseLock(lock_file, term_seconds=10.0)
    with pytest.raises(CorruptLockError) as excinfo:
        lease.acquire()
    assert "Corrupt lease file" in str(excinfo.value)


def test_durable_torn_state_raises_torn(tmp_path: Path):
    ledger_file = tmp_path / "conversion-ledger.json"
    ledger_file.write_text("{INCOMPLETE_JSON_TORN")
    ledger = ConversionLedger(ledger_id="torn_test", base_dir=tmp_path)
    with pytest.raises(SystemExit):  # durable.Torn exits/raises SystemExit or durable.Torn
        ledger.load_from_file(ledger_file)

