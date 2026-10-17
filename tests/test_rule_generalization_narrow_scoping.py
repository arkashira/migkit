"""Tests for rule generalization with narrow scoping and proof obligations (RCH20).

Validates:
- Narrow scoping with preconditions, dialect/version constraints, type signatures.
- Deterministic baseline preservation and immediate rollback.
- Anti-overgeneralization: a decimal/NULL fix does not alter unrelated routines.
- Shadow evaluation, proof obligations, withheld object family shielding.
- Conflicting-rule resolution by specificity and ambiguous conflict safety.
- Cross-project proof earning and independent revocation.
- Pure UNIX style, care badges, and pure ASCII compliance.
"""
from __future__ import annotations

import decimal
import json
import re
from decimal import Decimal
from typing import Any, Dict

import pytest

from migkit.rule_generalization import (
    CareBadge,
    ContextPrecondition,
    CrossProjectProofReport,
    CrossProjectRegistry,
    DialectConstraint,
    DialectKind,
    LearnedRewriteRule,
    NarrowScope,
    ObjectFamily,
    ProofObligation,
    ProofObligationKind,
    RewriteResult,
    RollbackResult,
    RoutineDefinition,
    RoutineMetadata,
    RoutineTestCase,
    RuleGeneralizationEngine,
    RuleProvenance,
    RuleResolutionResult,
    RuleStatus,
    ShadowEvaluationResult,
    TypeSignatureConstraint,
    VersionConstraint,
    WithheldSuiteReport,
    decide_rule_generalization,
    format_unix_report,
    rule_generalization_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("rule_generalization", "rule_generalization_rungs", "admitted", "holds"),
    ("rule_generalization", "rule_generalization_rungs", "admitted", "refuses"),
    ("rule_generalization", "rule_generalization_rungs", "shadow_divergence_review", "holds"),
    ("rule_generalization", "rule_generalization_rungs", "shadow_divergence_review", "refuses"),
    ("rule_generalization", "rule_generalization_rungs", "rollback_untested_review", "holds"),
    ("rule_generalization", "rule_generalization_rungs", "rollback_untested_review", "refuses"),
    ("rule_generalization", "rule_generalization_rungs", "rejection_action", "holds"),
]



def _make_decimal_calc_routine(
    name: str = "calc_order_tax",
    family: ObjectFamily = ObjectFamily.STORED_PROCEDURE,
    schema: str = "sales",
    project: str = "project-alpha",
) -> RoutineDefinition:
    meta = RoutineMetadata(
        project_id=project,
        schema=schema,
        name=name,
        family=family,
        dialect=DialectKind.ORACLE,
        version="19c",
        signature={
            "params": ["DECIMAL(12,2)", "DECIMAL(5,4)"],
            "return_type": "DECIMAL(12,2)",
            "nullable": True,
            "precision": 12,
            "scale": 2,
        },
    )
    src = (
        "CREATE OR REPLACE PROCEDURE sales.calc_order_tax(p_amount IN NUMBER, p_tax_rate IN NUMBER, p_out OUT NUMBER) IS\n"
        "BEGIN\n"
        "  p_out := NVL(p_amount * p_tax_rate, 0.00);\n"
        "END;\n"
    )
    baseline = (
        "CREATE OR REPLACE PROCEDURE sales.calc_order_tax(p_amount numeric, p_tax_rate numeric, INOUT p_out numeric) LANGUAGE plpgsql AS $$\n"
        "BEGIN\n"
        "  p_out := NVL(p_amount * p_tax_rate, 0.00);\n"
        "END; $$;"
    )

    def eval_logic(inputs: Dict[str, Any]) -> Any:
        amt = inputs.get("p_amount")
        rate = inputs.get("p_tax_rate")
        if amt is None or rate is None:
            return Decimal("0.00")
        return (Decimal(str(amt)) * Decimal(str(rate))).quantize(Decimal("0.01"))

    test_cases = [
        RoutineTestCase("tc-01", {"p_amount": Decimal("100.00"), "p_tax_rate": Decimal("0.0750")}, Decimal("7.50")),
        RoutineTestCase("tc-02", {"p_amount": None, "p_tax_rate": Decimal("0.0500")}, Decimal("0.00"), is_null_case=True),
        RoutineTestCase("tc-03", {"p_amount": Decimal("250.50"), "p_tax_rate": None}, Decimal("0.00"), is_null_case=True),
    ]

    return RoutineDefinition(
        metadata=meta,
        source_code=src,
        baseline_sql=baseline,
        test_cases=test_cases,
        eval_fn=eval_logic,
    )


def _make_unrelated_routine_user_profile() -> RoutineDefinition:
    meta = RoutineMetadata(
        project_id="project-alpha",
        schema="auth",
        name="get_user_display_name",
        family=ObjectFamily.SCALAR_FUNCTION,
        dialect=DialectKind.ORACLE,
        version="19c",
        signature={
            "params": ["VARCHAR(100)", "VARCHAR(100)"],
            "return_type": "VARCHAR(200)",
            "nullable": False,
        },
    )
    src = (
        "CREATE OR REPLACE FUNCTION auth.get_user_display_name(p_first VARCHAR2, p_nick VARCHAR2) RETURN VARCHAR2 IS\n"
        "BEGIN\n"
        "  RETURN NVL(p_nick, p_first);\n"
        "END;\n"
    )
    baseline = (
        "CREATE OR REPLACE FUNCTION auth.get_user_display_name(p_first text, p_nick text) RETURNS text LANGUAGE plpgsql AS $$\n"
        "BEGIN\n"
        "  RETURN NVL(p_nick, p_first);\n"
        "END; $$;"
    )
    test_cases = [
        RoutineTestCase("tc-str-01", {"p_first": "John", "p_nick": "Johnny"}, "Johnny"),
        RoutineTestCase("tc-str-02", {"p_first": "John", "p_nick": None}, "John", is_null_case=True),
    ]

    def eval_str(inputs: Dict[str, Any]) -> Any:
        return inputs.get("p_nick") or inputs.get("p_first")

    return RoutineDefinition(
        metadata=meta,
        source_code=src,
        baseline_sql=baseline,
        test_cases=test_cases,
        eval_fn=eval_str,
    )


def _make_unrelated_routine_audit_log() -> RoutineDefinition:
    meta = RoutineMetadata(
        project_id="project-alpha",
        schema="audit",
        name="log_event_timestamp",
        family=ObjectFamily.STORED_PROCEDURE,
        dialect=DialectKind.ORACLE,
        version="19c",
        signature={
            "params": ["VARCHAR(50)", "TIMESTAMP"],
            "return_type": None,
            "nullable": True,
        },
    )
    src = (
        "CREATE OR REPLACE PROCEDURE audit.log_event_timestamp(p_event VARCHAR2, p_ts TIMESTAMP) IS\n"
        "BEGIN\n"
        "  INSERT INTO audit_log VALUES(p_event, NVL(p_ts, SYSTIMESTAMP));\n"
        "END;\n"
    )
    baseline = (
        "CREATE OR REPLACE PROCEDURE audit.log_event_timestamp(p_event text, p_ts timestamp) LANGUAGE plpgsql AS $$\n"
        "BEGIN\n"
        "  INSERT INTO audit_log VALUES(p_event, NVL(p_ts, clock_timestamp()));\n"
        "END; $$;"
    )
    return RoutineDefinition(
        metadata=meta,
        source_code=src,
        baseline_sql=baseline,
        test_cases=[],
        eval_fn=lambda _: None,
    )


def test_narrow_scope_preconditions_and_dialect_constraints():
    # Setup Dialect constraint: Oracle -> Postgres 14 to 16
    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
        target_version=VersionConstraint(min_version="14.0", max_version="16.99"),
    )

    assert dialect_constraint.matches(DialectKind.ORACLE, DialectKind.POSTGRES, "19c", "15.2")
    assert not dialect_constraint.matches(DialectKind.MYSQL, DialectKind.POSTGRES, "8.0", "15.2")
    assert not dialect_constraint.matches(DialectKind.ORACLE, DialectKind.POSTGRES, "19c", "13.8")
    assert not dialect_constraint.matches(DialectKind.ORACLE, DialectKind.POSTGRES, "19c", "17.0")

    # Scope with precision & scale, allowed family
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        allowed_schemas={"sales"},
        type_constraints=[
            TypeSignatureConstraint(
                return_type="DECIMAL(12,2)",
                allows_null=True,
                required_precision=12,
                required_scale=2,
            )
        ],
        context_preconditions=[
            ContextPrecondition(
                name="arithmetic_multiply",
                required_pattern=r"\bNVL\s*\([^,]+?\*",
            )
        ],
    )

    routine = _make_decimal_calc_routine()
    in_scope, reason = scope.is_in_scope(routine.metadata, routine.source_code, {})
    assert in_scope, reason

    # Out of scope: scalar function family
    unrelated = _make_unrelated_routine_user_profile()
    in_scope_unrel, reason_unrel = scope.is_in_scope(unrelated.metadata, unrelated.source_code, {})
    assert not in_scope_unrel
    assert "Object family SCALAR_FUNCTION not in allowed families" in reason_unrel


def test_deterministic_baseline_preservation_and_rollback():
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    # Before rules applied: strictly baseline
    res0 = engine.apply_rules(routine)
    assert res0.final_output == routine.baseline_sql
    assert res0.rules_applied == []
    assert res0.badge == CareBadge.BENIGN

    # Create narrow rule
    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    rule = engine.learn_rule_candidate(
        name="nvl_decimal_to_coalesce_numeric",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Oracle NVL with decimal expression requires COALESCE with explicit numeric literal",
        author_or_model="lead-orchestrator",
    )

    # Active status
    rule.status = RuleStatus.ACTIVE

    # Apply
    res1 = engine.apply_rules(routine)
    assert res1.badge == CareBadge.PASS
    assert rule.rule_id in res1.rules_applied
    assert "COALESCE(p_amount * p_tax_rate, 0.00::numeric)" in res1.final_output
    assert res1.final_output != routine.baseline_sql

    # Rollback rule
    rb_res = engine.rollback_rule(rule.rule_id, [routine], reason="Verification rollback check")
    assert rb_res.badge == CareBadge.PASS
    assert rb_res.verified_baseline_match
    assert rb_res.restored_routines_count == 1

    # Apply again after rollback: strictly pristine baseline
    res2 = engine.apply_rules(routine)
    assert res2.final_output == routine.baseline_sql
    assert res2.rules_applied == []


def test_anti_overgeneralization_unrelated_routines_untouched():
    """A correction for one decimal/NULL pattern cannot alter unrelated routines."""
    engine = RuleGeneralizationEngine()

    training_routine = _make_decimal_calc_routine()
    unrelated_user = _make_unrelated_routine_user_profile()
    unrelated_audit = _make_unrelated_routine_audit_log()

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    # Narrow scope with type signature and context precondition
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[
            TypeSignatureConstraint(
                return_type="DECIMAL(12,2)",
                required_precision=12,
                required_scale=2,
            )
        ],
        context_preconditions=[
            ContextPrecondition(
                name="decimal_multiplication",
                required_pattern=r"\bNVL\s*\([^,]+?\*\s*[^,]+?,\s*0\.00\)",
            )
        ],
    )

    rule = engine.learn_rule_candidate(
        name="nvl_decimal_multiply_to_coalesce",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=training_routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Fix NVL decimal multiplication to COALESCE with numeric cast",
        author_or_model="lead-orchestrator",
    )

    withheld_routines = [
        # Withheld routine in same family but different signature or no tax rate
        _make_decimal_calc_routine(name="calc_discount", schema="sales"),
    ]
    unrelated_routines = [unrelated_user, unrelated_audit]

    report = engine.verify_against_withheld_suite(
        rule=rule,
        training_routine=training_routine,
        withheld_routines=withheld_routines,
        unrelated_routines=unrelated_routines,
    )

    assert report.badge == CareBadge.PASS
    assert report.training_passed
    assert report.unrelated_routines_untouched
    assert report.unrelated_alterations_count == 0
    assert report.withheld_families_unaffected
    assert report.withheld_regressions_count == 0
    assert report.is_eligible_for_promotion

    # Verify that applying rules to unrelated routines leaves them 100% bit-for-bit unchanged
    rule.status = RuleStatus.ACTIVE
    res_user = engine.apply_rules(unrelated_user, rules=[rule])
    assert res_user.final_output == unrelated_user.baseline_sql
    assert res_user.rules_applied == []

    res_audit = engine.apply_rules(unrelated_audit, rules=[rule])
    assert res_audit.final_output == unrelated_audit.baseline_sql
    assert res_audit.rules_applied == []


def test_rejection_when_unrelated_routine_is_altered():
    """An overly broad rule that mutates unrelated routines must be rejected with ACTION."""
    engine = RuleGeneralizationEngine()

    training_routine = _make_decimal_calc_routine()
    unrelated_user = _make_unrelated_routine_user_profile()

    # Broken broad scope that matches everything
    broad_scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE, ObjectFamily.SCALAR_FUNCTION},
        type_constraints=[],
        context_preconditions=[],
    )
    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )

    # Broad rule: rewrites NVL(x, y) indiscriminately to 0.00::numeric
    content_hash = "fakehash12345678"
    provenance = RuleProvenance(
        origin_project_id="project-alpha",
        origin_hop_id="hop-01",
        origin_routine_name="calc_order_tax",
        origin_object_family=ObjectFamily.STORED_PROCEDURE,
        author_or_model="careless-model",
        created_at_epoch=1700000000.0,
        source_sha256=content_hash,
        rationale="Careless broad NVL replacement",
    )

    bad_rule = LearnedRewriteRule(
        rule_id="rule-bad-broad",
        name="bad_broad_nvl",
        description="Broad rule with no preconditions",
        provenance=provenance,
        dialect_constraint=dialect_constraint,
        scope=broad_scope,
        source_pattern=r"\bNVL\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
        replacement_template=r"COALESCE(\1, 0.00::numeric)",
        proof_obligations=[
            ProofObligation(kind=ProofObligationKind.SEMANTIC_EQUIVALENCE, description="Equivalence"),
            ProofObligation(kind=ProofObligationKind.ZERO_REGRESSION_WITHHELD, description="Zero Regressions"),
        ],
        status=RuleStatus.PROPOSED,
    )

    report = engine.verify_against_withheld_suite(
        rule=bad_rule,
        training_routine=training_routine,
        withheld_routines=[],
        unrelated_routines=[unrelated_user],
    )

    assert report.badge == CareBadge.ACTION
    assert not report.unrelated_routines_untouched
    assert report.unrelated_alterations_count == 1
    assert not report.is_eligible_for_promotion
    assert bad_rule.status == RuleStatus.REJECTED


def test_withheld_object_families_shielded():
    """Rules scoped to stored procedures must not fire on withheld object families like views or triggers."""
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    # Trigger routine (withheld family)
    trigger_routine = _make_decimal_calc_routine(
        name="trg_audit_tax",
        family=ObjectFamily.TRIGGER_ROUTINE,
    )
    # View definition (withheld family)
    view_routine = _make_decimal_calc_routine(
        name="v_monthly_tax",
        family=ObjectFamily.VIEW_DEFINITION,
    )

    in_scope_trg, reason_trg = scope.is_in_scope(trigger_routine.metadata, trigger_routine.source_code, {})
    assert not in_scope_trg
    assert "TRIGGER_ROUTINE not in allowed families" in reason_trg

    in_scope_view, reason_view = scope.is_in_scope(view_routine.metadata, view_routine.source_code, {})
    assert not in_scope_view
    assert "VIEW_DEFINITION not in allowed families" in reason_view


def test_shadow_evaluation_and_proof_obligations():
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    rule = engine.learn_rule_candidate(
        name="nvl_decimal_to_coalesce_numeric",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Numeric scale safety in COALESCE",
        author_or_model="lead-orchestrator",
    )

    shadow_res = engine.evaluate_shadow(rule, routine)

    assert shadow_res.applied
    assert shadow_res.syntax_valid
    assert shadow_res.badge == CareBadge.PASS
    assert shadow_res.test_results_pass == 3
    assert shadow_res.test_results_fail == 0
    assert ProofObligationKind.SEMANTIC_EQUIVALENCE in shadow_res.proofs_satisfied
    assert ProofObligationKind.NULL_PRESERVATION in shadow_res.proofs_satisfied
    assert ProofObligationKind.DECIMAL_PRECISION_SCALE_INVARIANT in shadow_res.proofs_satisfied
    assert ProofObligationKind.AST_WELL_FORMED in shadow_res.proofs_satisfied


def test_conflicting_rule_resolution_by_specificity():
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )

    # Rule 1: Broad rule (lower specificity)
    scope_broad = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE, ObjectFamily.SCALAR_FUNCTION},
        type_constraints=[],
        context_preconditions=[],
    )
    rule_broad = engine.learn_rule_candidate(
        name="broad_nvl_coalesce",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope_broad,
        rationale="Generic NVL replacement",
        author_or_model="general-converter",
    )

    # Rule 2: Narrow rule (higher specificity)
    scope_narrow = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[
            TypeSignatureConstraint(
                return_type="DECIMAL(12,2)",
                required_precision=12,
                required_scale=2,
            )
        ],
        context_preconditions=[
            ContextPrecondition(
                name="multiply_pattern",
                required_pattern=r"\*",
            )
        ],
    )
    rule_narrow = engine.learn_rule_candidate(
        name="narrow_decimal_nvl_coalesce",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope_narrow,
        rationale="Specific numeric cast replacement",
        author_or_model="expert-generalizer",
    )

    assert rule_narrow.specificity_score > rule_broad.specificity_score

    resolution = engine.resolve_matching_rules([rule_broad, rule_narrow], routine)
    assert not resolution.conflict_detected
    assert resolution.badge == CareBadge.PASS
    assert resolution.selected_rule.rule_id == rule_narrow.rule_id


def test_ambiguous_conflict_refuses_to_guess_and_falls_back_to_baseline():
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope_shared = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    # Two rules with identical specificity but conflicting transformations
    rule_alt1 = engine.learn_rule_candidate(
        name="alt1_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope_shared,
        rationale="Alt 1",
        author_or_model="model-1",
    )
    rule_alt2 = engine.learn_rule_candidate(
        name="alt2_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, CAST(0 AS NUMERIC(12,2)))",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope_shared,
        rationale="Alt 2",
        author_or_model="model-2",
    )

    rule_alt1.status = RuleStatus.ACTIVE
    rule_alt2.status = RuleStatus.ACTIVE

    res = engine.apply_rules(routine, rules=[rule_alt1, rule_alt2])

    assert res.badge == CareBadge.ACTION
    assert res.final_output == routine.baseline_sql
    assert res.rules_applied == []


def test_cross_project_reuse_earns_proof_before_activation():
    engine = RuleGeneralizationEngine()
    registry = CrossProjectRegistry(engine)

    proj_a_routine = _make_decimal_calc_routine(project="project-alpha")
    proj_b_routine = _make_decimal_calc_routine(project="project-beta")
    proj_b_withheld = [_make_decimal_calc_routine(name="calc_withheld_beta", project="project-beta")]
    proj_b_unrelated = [_make_unrelated_routine_user_profile()]

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    rule = engine.learn_rule_candidate(
        name="nvl_decimal_to_coalesce_numeric",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=proj_a_routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Learned from Project Alpha",
        author_or_model="lead-orchestrator",
        origin_project_id="project-alpha",
    )
    rule.status = RuleStatus.ACTIVE

    registry.register_rule_for_project("project-alpha", rule, RuleStatus.ACTIVE)
    assert registry.is_rule_active_for_project(rule.rule_id, "project-alpha")

    # Share to project-beta
    initial_st = registry.share_rule_to_project(rule.rule_id, "project-alpha", "project-beta")
    assert initial_st == RuleStatus.SHADOW
    assert not registry.is_rule_active_for_project(rule.rule_id, "project-beta")

    # Earn proof for project-beta
    proof_rep = registry.earn_proof_for_project(
        rule_id=rule.rule_id,
        to_project="project-beta",
        training_routine=proj_b_routine,
        withheld_routines=proj_b_withheld,
        unrelated_routines=proj_b_unrelated,
    )

    assert proof_rep.badge == CareBadge.PASS
    assert proof_rep.earned_proof
    assert proof_rep.status == RuleStatus.ACTIVE
    assert registry.is_rule_active_for_project(rule.rule_id, "project-beta")


def test_cross_project_reuse_rejection_isolated_from_origin():
    """If target project withheld test fails, rule is rejected in target but stays active in origin."""
    engine = RuleGeneralizationEngine()
    registry = CrossProjectRegistry(engine)

    proj_a_routine = _make_decimal_calc_routine(project="project-alpha")
    proj_gamma_routine = _make_decimal_calc_routine(project="project-gamma")

    # Gamma has a conflicting unrelated routine that would be altered
    gamma_unrelated = _make_unrelated_routine_user_profile()

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    # Permissive scope that leaks into gamma_unrelated
    scope_leaky = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE, ObjectFamily.SCALAR_FUNCTION},
        type_constraints=[],
    )

    rule = engine.learn_rule_candidate(
        name="leaky_nvl_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=proj_a_routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope_leaky,
        rationale="Leaky rule",
        author_or_model="lead-orchestrator",
        origin_project_id="project-alpha",
    )
    rule.status = RuleStatus.ACTIVE
    rule.source_pattern = r"\bNVL\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)"
    rule.replacement_template = r"COALESCE(\1, 0.00::numeric)"
    registry.register_rule_for_project("project-alpha", rule, RuleStatus.ACTIVE)

    registry.share_rule_to_project(rule.rule_id, "project-alpha", "project-gamma")

    # Fail proof in gamma by having an unrelated routine that gets erroneously touched
    proof_rep = registry.earn_proof_for_project(
        rule_id=rule.rule_id,
        to_project="project-gamma",
        training_routine=proj_gamma_routine,
        withheld_routines=[],
        unrelated_routines=[gamma_unrelated],
    )

    assert proof_rep.badge == CareBadge.ACTION
    assert not proof_rep.earned_proof
    assert proof_rep.status == RuleStatus.REJECTED
    assert not registry.is_rule_active_for_project(rule.rule_id, "project-gamma")

    # Origin project-alpha remains intact and ACTIVE
    assert registry.is_rule_active_for_project(rule.rule_id, "project-alpha")


def test_independent_revocation_across_projects():
    engine = RuleGeneralizationEngine()
    registry = CrossProjectRegistry(engine)

    routine = _make_decimal_calc_routine()
    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    rule = engine.learn_rule_candidate(
        name="rule_independent_revocation",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Test revocation",
        author_or_model="lead-orchestrator",
    )

    registry.register_rule_for_project("proj-1", rule, RuleStatus.ACTIVE)
    registry.register_rule_for_project("proj-2", rule, RuleStatus.ACTIVE)

    assert registry.is_rule_active_for_project(rule.rule_id, "proj-1")
    assert registry.is_rule_active_for_project(rule.rule_id, "proj-2")

    # Revoke in proj-2 only
    registry.revoke_rule_in_project(rule.rule_id, "proj-2", reason="Discovered edge case in proj-2")

    assert registry.is_rule_active_for_project(rule.rule_id, "proj-1")
    assert not registry.is_rule_active_for_project(rule.rule_id, "proj-2")


def test_rule_provenance_and_auditability():
    routine = _make_decimal_calc_routine()
    engine = RuleGeneralizationEngine()
    rule = engine.learn_rule_candidate(
        name="provenance_test_rule",
        before_sql="NVL(a, 0)",
        fixed_sql="COALESCE(a, 0)",
        metadata=routine.metadata,
        dialect_constraint=DialectConstraint(
            source_dialect=DialectKind.ORACLE,
            target_dialect=DialectKind.POSTGRES,
        ),
        scope=NarrowScope(allowed_families={ObjectFamily.STORED_PROCEDURE}),
        rationale="NVL zero coalesce",
        author_or_model="claude-3-opus",
        origin_project_id="proj-finance",
        origin_hop_id="hop-88",
    )

    prov = rule.provenance
    assert prov.origin_project_id == "proj-finance"
    assert prov.origin_hop_id == "hop-88"
    assert prov.origin_routine_name == "calc_order_tax"
    assert prov.origin_object_family == ObjectFamily.STORED_PROCEDURE
    assert prov.author_or_model == "claude-3-opus"
    assert len(prov.source_sha256) == 64

    d = rule.to_dict()
    assert d["rule_id"] == rule.rule_id
    assert d["provenance"]["origin_project_id"] == "proj-finance"


def test_pure_unix_care_badges_and_ascii_compliance():
    rep_dict = {
        "rule_id": "rule-7a8b9c",
        "badge": CareBadge.PASS.value,
        "training_passed": True,
        "withheld_regressions_count": 0,
        "unrelated_alterations_count": 0,
        "safe_host": "127.0.0.1",
        "details": ["Zero regressions detected", "All proof obligations satisfied"],
    }

    report = format_unix_report(rep_dict)

    # Check 100% pure ASCII
    assert all(ord(c) < 128 for c in report)

    # Check Care Badge in report
    assert "[PASS]" in report
    assert "MIGKIT RULE GENERALIZATION REPORT" in report

    # Check no emojis or decorative glyphs
    for c in report:
        assert c.isascii()


def test_withheld_suite_regression_rejection():
    """If a withheld routine regresses on test cases, rule is rejected with ACTION."""
    engine = RuleGeneralizationEngine()
    training_routine = _make_decimal_calc_routine()

    # Withheld routine where test case expects 99.99 but rule causes failure
    withheld_broken = _make_decimal_calc_routine(name="calc_withheld_broken")
    # Custom eval function that fails
    withheld_broken.eval_fn = lambda _: Decimal("0.00")
    withheld_broken.test_cases = [
        RoutineTestCase("tc-fail", {"p_amount": Decimal("100.00"), "p_tax_rate": Decimal("0.1000")}, Decimal("10.00"))
    ]

    dialect_constraint = DialectConstraint(
        source_dialect=DialectKind.ORACLE,
        target_dialect=DialectKind.POSTGRES,
    )
    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        type_constraints=[TypeSignatureConstraint(required_scale=2)],
    )

    rule = engine.learn_rule_candidate(
        name="test_withheld_regression_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=training_routine.metadata,
        dialect_constraint=dialect_constraint,
        scope=scope,
        rationale="Test withheld regression",
        author_or_model="lead-orchestrator",
    )

    report = engine.verify_against_withheld_suite(
        rule=rule,
        training_routine=training_routine,
        withheld_routines=[withheld_broken],
        unrelated_routines=[],
    )

    assert report.badge == CareBadge.ACTION
    assert not report.withheld_families_unaffected
    assert report.withheld_regressions_count == 1
    assert not report.is_eligible_for_promotion
    assert rule.status == RuleStatus.REJECTED


def test_syntax_corruption_fails_shadow_eval():
    """If rule generates unbalanced quotes/parentheses, shadow eval fails with ACTION."""
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    rule = engine.learn_rule_candidate(
        name="syntax_corrupt_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric))",  # extra paren
        metadata=routine.metadata,
        dialect_constraint=DialectConstraint(
            source_dialect=DialectKind.ORACLE,
            target_dialect=DialectKind.POSTGRES,
        ),
        scope=NarrowScope(allowed_families={ObjectFamily.STORED_PROCEDURE}),
        rationale="Corrupt syntax",
        author_or_model="buggy-synthesizer",
    )

    shadow_res = engine.evaluate_shadow(rule, routine)

    assert shadow_res.applied
    assert not shadow_res.syntax_valid
    assert shadow_res.badge == CareBadge.ACTION
    assert ProofObligationKind.AST_WELL_FORMED in shadow_res.proofs_failed


def test_version_constraints_semantic_ranges():
    vc = VersionConstraint(min_version="12.2", max_version="16.5")

    assert vc.matches("12.2")
    assert vc.matches("14.0")
    assert vc.matches("16.5")
    assert vc.matches("15.9.1")
    assert not vc.matches("12.1")
    assert not vc.matches("11.2")
    assert not vc.matches("16.6")
    assert not vc.matches("17.0")

    vc_no_bounds = VersionConstraint()
    assert vc_no_bounds.matches("15.2")
    assert vc_no_bounds.matches(None)


def test_narrow_scope_forbidden_pattern_context_precondition():
    routine = _make_decimal_calc_routine()
    # Add autonomous transaction pragma to routine
    routine_with_pragma = routine.clone()
    routine_with_pragma.source_code = (
        "CREATE OR REPLACE PROCEDURE sales.calc_order_tax IS\n"
        "  PRAGMA AUTONOMOUS_TRANSACTION;\n"
        "BEGIN\n"
        "  NVL(p_amount * p_tax_rate, 0.00);\n"
        "END;\n"
    )

    scope = NarrowScope(
        allowed_families={ObjectFamily.STORED_PROCEDURE},
        context_preconditions=[
            ContextPrecondition(
                name="no_autonomous_transaction",
                forbidden_pattern=r"PRAGMA\s+AUTONOMOUS_TRANSACTION",
            )
        ],
    )

    # Compliant routine matches
    ok1, reason1 = scope.is_in_scope(routine.metadata, routine.source_code, {})
    assert ok1, reason1

    # Routine with forbidden pattern is rejected
    ok2, reason2 = scope.is_in_scope(routine_with_pragma.metadata, routine_with_pragma.source_code, {})
    assert not ok2
    assert "Matched forbidden context pattern" in reason2


def test_revoke_rule_in_engine():
    engine = RuleGeneralizationEngine()
    routine = _make_decimal_calc_routine()

    rule = engine.learn_rule_candidate(
        name="revocation_check",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=DialectConstraint(
            source_dialect=DialectKind.ORACLE,
            target_dialect=DialectKind.POSTGRES,
        ),
        scope=NarrowScope(allowed_families={ObjectFamily.STORED_PROCEDURE}),
        rationale="Revocation check",
        author_or_model="lead-orchestrator",
    )
    rule.status = RuleStatus.ACTIVE

    # Revoke
    engine.revoke_rule(rule.rule_id, reason="Explicit operator revocation")
    assert rule.status == RuleStatus.REVOKED
    assert rule.revoked_at_epoch is not None
    assert rule.revocation_reason == "Explicit operator revocation"

    # Apply rules will not apply a revoked rule
    res = engine.apply_rules(routine, rules=[rule])
    assert res.final_output == routine.baseline_sql
    assert res.rules_applied == []


def test_to_dict_serialization_completeness():
    routine = _make_decimal_calc_routine()
    engine = RuleGeneralizationEngine()

    rule = engine.learn_rule_candidate(
        name="serial_rule",
        before_sql="NVL(p_amount * p_tax_rate, 0.00)",
        fixed_sql="COALESCE(p_amount * p_tax_rate, 0.00::numeric)",
        metadata=routine.metadata,
        dialect_constraint=DialectConstraint(
            source_dialect=DialectKind.ORACLE,
            target_dialect=DialectKind.POSTGRES,
            target_version=VersionConstraint(min_version="14.0"),
        ),
        scope=NarrowScope(
            allowed_families={ObjectFamily.STORED_PROCEDURE},
            type_constraints=[TypeSignatureConstraint(required_scale=2)],
            context_preconditions=[ContextPrecondition(name="mult", required_pattern=r"\*")],
        ),
        rationale="Serialization validation",
        author_or_model="lead-orchestrator",
    )

    d = rule.to_dict()
    dumped = json.dumps(d)
    loaded = json.loads(dumped)
    assert loaded["rule_id"] == rule.rule_id
    assert loaded["dialect_constraint"]["target_version"]["min_version"] == "14.0"


def test_rule_generalization_rungs_coverage():
    with watched() as seen:
        # 1. admitted holds
        f1 = {
            "evaluated": True,
            "family_supported": True,
            "withheld_failures": 0,
            "shadow_divergence_count": 0,
            "conflicts_unresolved": 0,
            "rollback_validated": True,
        }
        c1 = decide_rule_generalization(f1)
        assert c1.rung.name == "admitted"

        # 2. admitted refuses, shadow_divergence_review refuses, rollback_untested_review refuses -> rejection_action holds
        f2 = {
            "evaluated": True,
            "family_supported": True,
            "withheld_failures": 1,
            "shadow_divergence_count": 0,
            "specificity_tie": False,
            "rollback_validated": True,
        }
        c2 = decide_rule_generalization(f2)
        assert c2.rung.name == "rejection_action"

        # 3. admitted refuses, shadow_divergence_review holds
        f3 = {
            "evaluated": True,
            "family_supported": True,
            "withheld_failures": 0,
            "shadow_divergence_count": 1,
            "conflicts_unresolved": 0,
            "rollback_validated": True,
        }
        c3 = decide_rule_generalization(f3)
        assert c3.rung.name == "shadow_divergence_review"

        # 4. admitted refuses, shadow_divergence_review refuses, rollback_untested_review holds
        f4 = {
            "evaluated": True,
            "family_supported": True,
            "withheld_failures": 0,
            "shadow_divergence_count": 0,
            "conflicts_unresolved": 0,
            "rollback_validated": False,
        }
        c4 = decide_rule_generalization(f4)
        assert c4.rung.name == "rollback_untested_review"


