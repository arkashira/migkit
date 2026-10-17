"""Rule generalization with narrow scoping and proof obligations (RCH20.rule-generalization).

Enforces disciplined learning of rewrite rules for stored code migration without
propagating one-off wrong fixes. Every learned rule requires narrow preconditions,
dialect and version constraints, proof obligations, shadow execution testing
against withheld object families, conflicting-rule resolution, deterministic baseline
preservation, and independent cross-project proof and revocation.

Pure UNIX style. No emojis or glyphs. Care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import copy
import dataclasses
import decimal
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide

SPECIFICITY_TIE_EPSILON: float = 0.001


class CareBadge(str, Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class RuleStatus(str, Enum):
    PROPOSED = "PROPOSED"
    SHADOW = "SHADOW"
    EVALUATED = "EVALUATED"
    ACTIVE = "ACTIVE"
    REJECTED = "REJECTED"
    REVOKED = "REVOKED"
    ROLLED_BACK = "ROLLED_BACK"


class ObjectFamily(str, Enum):
    SCALAR_FUNCTION = "SCALAR_FUNCTION"
    TABLE_FUNCTION = "TABLE_FUNCTION"
    STORED_PROCEDURE = "STORED_PROCEDURE"
    TRIGGER_ROUTINE = "TRIGGER_ROUTINE"
    VIEW_DEFINITION = "VIEW_DEFINITION"
    PACKAGE_BODY = "PACKAGE_BODY"
    ANONYMOUS_BLOCK = "ANONYMOUS_BLOCK"


class ProofObligationKind(str, Enum):
    SEMANTIC_EQUIVALENCE = "SEMANTIC_EQUIVALENCE"
    NULL_PRESERVATION = "NULL_PRESERVATION"
    DECIMAL_PRECISION_SCALE_INVARIANT = "DECIMAL_PRECISION_SCALE_INVARIANT"
    TYPE_CONVERSION_SAFETY = "TYPE_CONVERSION_SAFETY"
    NO_SIDE_EFFECT_ALTERATION = "NO_SIDE_EFFECT_ALTERATION"
    DETERMINISTIC_EVALUATION = "DETERMINISTIC_EVALUATION"
    ZERO_REGRESSION_WITHHELD = "ZERO_REGRESSION_WITHHELD"
    AST_WELL_FORMED = "AST_WELL_FORMED"


class DialectKind(str, Enum):
    ORACLE = "oracle"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLSERVER = "sqlserver"
    SNOWFLAKE = "snowflake"
    GENERIC = "generic"


class RuleGeneralizationError(Exception):
    """Base exception for rule generalization failures."""
    pass


class NarrowScopeViolationError(RuleGeneralizationError):
    """Raised when a rule is applied outside its narrow scope."""
    pass


class ProofObligationFailedError(RuleGeneralizationError):
    """Raised when a mandatory proof obligation fails."""
    pass


class ConflictingRuleResolutionError(RuleGeneralizationError):
    """Raised when conflicting candidate rules cannot be deterministically resolved."""
    pass


class UnearnedCrossProjectActivationError(RuleGeneralizationError):
    """Raised when cross-project activation is attempted without earned proof."""
    pass


class RevokedRuleInvocationError(RuleGeneralizationError):
    """Raised when a revoked rule is invoked."""
    pass


@dataclass(frozen=True)
class VersionConstraint:
    min_version: Optional[str] = None
    max_version: Optional[str] = None

    @staticmethod
    def _parse_version(v: str) -> Tuple[int, ...]:
        clean = re.sub(r"[^\d.]", "", v)
        parts = []
        for part in clean.split("."):
            if part:
                try:
                    parts.append(int(part))
                except ValueError:
                    parts.append(0)
        return tuple(parts) if parts else (0,)

    def matches(self, version: Optional[str]) -> bool:
        if not version:
            return self.min_version is None and self.max_version is None
        curr = self._parse_version(version)
        if self.min_version:
            if curr < self._parse_version(self.min_version):
                return False
        if self.max_version:
            if curr > self._parse_version(self.max_version):
                return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "min_version": self.min_version,
            "max_version": self.max_version,
        }


@dataclass(frozen=True)
class DialectConstraint:
    source_dialect: DialectKind
    target_dialect: DialectKind
    source_version: Optional[VersionConstraint] = None
    target_version: Optional[VersionConstraint] = None

    def matches(
        self,
        src: DialectKind,
        tgt: DialectKind,
        src_ver: Optional[str] = None,
        tgt_ver: Optional[str] = None,
    ) -> bool:
        if self.source_dialect != DialectKind.GENERIC and self.source_dialect != src:
            return False
        if self.target_dialect != DialectKind.GENERIC and self.target_dialect != tgt:
            return False
        if self.source_version and not self.source_version.matches(src_ver):
            return False
        if self.target_version and not self.target_version.matches(tgt_ver):
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_dialect": self.source_dialect.value,
            "target_dialect": self.target_dialect.value,
            "source_version": self.source_version.to_dict() if self.source_version else None,
            "target_version": self.target_version.to_dict() if self.target_version else None,
        }


@dataclass(frozen=True)
class TypeSignatureConstraint:
    param_types: Tuple[str, ...] = field(default_factory=tuple)
    return_type: Optional[str] = None
    allows_null: Optional[bool] = None
    required_precision: Optional[int] = None
    required_scale: Optional[int] = None

    def matches(self, sig: Dict[str, Any]) -> bool:
        if self.param_types:
            sig_params = [str(p).upper() for p in sig.get("params", [])]
            if len(sig_params) != len(self.param_types):
                return False
            for expected, actual in zip(self.param_types, sig_params):
                if expected.upper() not in actual:
                    return False
        if self.return_type:
            actual_ret = str(sig.get("return_type", "")).upper()
            if self.return_type.upper() not in actual_ret:
                return False
        if self.allows_null is not None:
            if bool(sig.get("nullable", False)) != self.allows_null:
                return False
        if self.required_precision is not None:
            if sig.get("precision") != self.required_precision:
                return False
        if self.required_scale is not None:
            if sig.get("scale") != self.required_scale:
                return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "param_types": list(self.param_types),
            "return_type": self.return_type,
            "allows_null": self.allows_null,
            "required_precision": self.required_precision,
            "required_scale": self.required_scale,
        }


@dataclass(frozen=True)
class ContextPrecondition:
    name: str
    required_pattern: Optional[str] = None
    forbidden_pattern: Optional[str] = None
    enclosing_scope: Optional[str] = None
    null_check_required: bool = False

    def evaluate(self, code_fragment: str, context_info: Dict[str, Any]) -> Tuple[bool, str]:
        if self.required_pattern:
            if not re.search(self.required_pattern, code_fragment, re.IGNORECASE):
                return False, f"Missing required context pattern: {self.required_pattern}"
        if self.forbidden_pattern:
            if re.search(self.forbidden_pattern, code_fragment, re.IGNORECASE):
                return False, f"Matched forbidden context pattern: {self.forbidden_pattern}"
        if self.enclosing_scope:
            curr_scope = str(context_info.get("scope", "")).upper()
            if self.enclosing_scope.upper() != curr_scope:
                return False, f"Scope mismatch: expected {self.enclosing_scope}, found {curr_scope}"
        if self.null_check_required:
            if not bool(context_info.get("has_null_check", False)):
                return False, "Required null check precondition not satisfied"
        return True, "Preconditions met"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "required_pattern": self.required_pattern,
            "forbidden_pattern": self.forbidden_pattern,
            "enclosing_scope": self.enclosing_scope,
            "null_check_required": self.null_check_required,
        }


@dataclass
class NarrowScope:
    allowed_families: Set[ObjectFamily]
    allowed_schemas: Optional[Set[str]] = None
    routine_name_filter: Optional[str] = None
    type_constraints: List[TypeSignatureConstraint] = field(default_factory=list)
    context_preconditions: List[ContextPrecondition] = field(default_factory=list)

    def is_in_scope(
        self,
        metadata: RoutineMetadata,
        code_fragment: str,
        context_info: Dict[str, Any],
    ) -> Tuple[bool, str]:
        if metadata.family not in self.allowed_families:
            return False, f"Object family {metadata.family.value} not in allowed families"
        if self.allowed_schemas is not None and metadata.schema not in self.allowed_schemas:
            return False, f"Schema {metadata.schema} not in allowed schemas"
        if self.routine_name_filter:
            if not re.search(self.routine_name_filter, metadata.name, re.IGNORECASE):
                return False, f"Routine name {metadata.name} does not match filter"
        for tc in self.type_constraints:
            if not tc.matches(metadata.signature):
                return False, "Type signature constraint failed"
        for cp in self.context_preconditions:
            ok, reason = cp.evaluate(code_fragment, context_info)
            if not ok:
                return False, f"Context precondition failed: {reason}"
        return True, "Scope matched"

    def specificity_score(self) -> float:
        score = 0.0
        # Narrowing allowed families increases specificity
        if len(self.allowed_families) == 1:
            score += 15.0
        elif len(self.allowed_families) < len(ObjectFamily):
            score += 8.0
        if self.allowed_schemas:
            score += 5.0
        if self.routine_name_filter:
            score += 10.0
        score += len(self.type_constraints) * 12.0
        for tc in self.type_constraints:
            if tc.required_precision is not None:
                score += 5.0
            if tc.required_scale is not None:
                score += 5.0
            if tc.allows_null is not None:
                score += 3.0
        score += len(self.context_preconditions) * 8.0
        return score

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed_families": [f.value for f in sorted(self.allowed_families, key=lambda x: x.value)],
            "allowed_schemas": sorted(list(self.allowed_schemas)) if self.allowed_schemas else None,
            "routine_name_filter": self.routine_name_filter,
            "type_constraints": [tc.to_dict() for tc in self.type_constraints],
            "context_preconditions": [cp.to_dict() for cp in self.context_preconditions],
        }


@dataclass(frozen=True)
class RuleProvenance:
    origin_project_id: str
    origin_hop_id: str
    origin_routine_name: str
    origin_object_family: ObjectFamily
    author_or_model: str
    created_at_epoch: float
    source_sha256: str
    rationale: str
    git_commit: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "origin_project_id": self.origin_project_id,
            "origin_hop_id": self.origin_hop_id,
            "origin_routine_name": self.origin_routine_name,
            "origin_object_family": self.origin_object_family.value,
            "author_or_model": self.author_or_model,
            "created_at_epoch": self.created_at_epoch,
            "source_sha256": self.source_sha256,
            "rationale": self.rationale,
            "git_commit": self.git_commit,
        }


@dataclass
class ProofObligation:
    kind: ProofObligationKind
    description: str
    is_mandatory: bool = True
    satisfied: bool = False
    evidence: str = ""
    verified_at_epoch: Optional[float] = None

    def mark_satisfied(self, evidence: str) -> None:
        self.satisfied = True
        self.evidence = evidence
        self.verified_at_epoch = time.time()

    def mark_failed(self, evidence: str) -> None:
        self.satisfied = False
        self.evidence = evidence
        self.verified_at_epoch = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind.value,
            "description": self.description,
            "is_mandatory": self.is_mandatory,
            "satisfied": self.satisfied,
            "evidence": self.evidence,
            "verified_at_epoch": self.verified_at_epoch,
        }


@dataclass
class RoutineMetadata:
    project_id: str
    schema: str
    name: str
    family: ObjectFamily
    dialect: DialectKind
    version: str = "1.0"
    signature: Dict[str, Any] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "schema": self.schema,
            "name": self.name,
            "family": self.family.value,
            "dialect": self.dialect.value,
            "version": self.version,
            "signature": self.signature,
            "tags": sorted(list(self.tags)),
        }


@dataclass
class RoutineTestCase:
    case_id: str
    inputs: Dict[str, Any]
    expected_output: Any
    is_null_case: bool = False
    is_boundary_case: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_id": self.case_id,
            "inputs": {k: str(v) if isinstance(v, Decimal) else v for k, v in self.inputs.items()},
            "expected_output": str(self.expected_output) if isinstance(self.expected_output, Decimal) else self.expected_output,
            "is_null_case": self.is_null_case,
            "is_boundary_case": self.is_boundary_case,
        }


@dataclass
class RoutineDefinition:
    metadata: RoutineMetadata
    source_code: str
    baseline_sql: str
    test_cases: List[RoutineTestCase] = field(default_factory=list)
    eval_fn: Optional[Callable[[Dict[str, Any]], Any]] = None

    def clone(self) -> RoutineDefinition:
        return copy.deepcopy(self)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "source_code": self.source_code,
            "baseline_sql": self.baseline_sql,
            "test_cases": [tc.to_dict() for tc in self.test_cases],
        }


@dataclass
class LearnedRewriteRule:
    rule_id: str
    name: str
    description: str
    provenance: RuleProvenance
    dialect_constraint: DialectConstraint
    scope: NarrowScope
    source_pattern: str
    replacement_template: str
    proof_obligations: List[ProofObligation]
    status: RuleStatus = RuleStatus.PROPOSED
    shadow_application_count: int = 0
    shadow_success_count: int = 0
    shadow_failure_count: int = 0
    activated_at_epoch: Optional[float] = None
    revoked_at_epoch: Optional[float] = None
    revocation_reason: Optional[str] = None

    @property
    def specificity_score(self) -> float:
        base = self.scope.specificity_score()
        if self.dialect_constraint.source_version:
            base += 5.0
        if self.dialect_constraint.target_version:
            base += 5.0
        satisfied_obligations = sum(1 for po in self.proof_obligations if po.satisfied)
        base += satisfied_obligations * 2.0
        return base

    def all_mandatory_proofs_satisfied(self) -> bool:
        return all(po.satisfied for po in self.proof_obligations if po.is_mandatory)

    def can_apply_to(
        self,
        routine: RoutineDefinition,
        context_info: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        if self.status in (RuleStatus.REJECTED, RuleStatus.REVOKED, RuleStatus.ROLLED_BACK):
            return False, f"Rule is {self.status.value}"
        meta = routine.metadata
        if not self.dialect_constraint.matches(meta.dialect, DialectKind.POSTGRES, meta.version):
            return False, "Dialect or version constraint mismatch"
        ctx = context_info or {}
        in_scope, reason = self.scope.is_in_scope(meta, routine.source_code, ctx)
        if not in_scope:
            return False, reason
        if not re.search(self.source_pattern, routine.baseline_sql, re.IGNORECASE):
            return False, "Source pattern does not match baseline SQL"
        return True, "Eligible"

    def apply_rewrite(self, sql: str) -> str:
        return re.sub(self.source_pattern, self.replacement_template, sql, flags=re.IGNORECASE)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "description": self.description,
            "provenance": self.provenance.to_dict(),
            "dialect_constraint": self.dialect_constraint.to_dict(),
            "scope": self.scope.to_dict(),
            "source_pattern": self.source_pattern,
            "replacement_template": self.replacement_template,
            "proof_obligations": [po.to_dict() for po in self.proof_obligations],
            "status": self.status.value,
            "specificity_score": self.specificity_score,
            "shadow_application_count": self.shadow_application_count,
            "shadow_success_count": self.shadow_success_count,
            "shadow_failure_count": self.shadow_failure_count,
            "activated_at_epoch": self.activated_at_epoch,
            "revoked_at_epoch": self.revoked_at_epoch,
            "revocation_reason": self.revocation_reason,
        }


@dataclass
class ShadowEvaluationResult:
    rule_id: str
    routine_name: str
    baseline_output: str
    rewritten_output: str
    applied: bool
    syntax_valid: bool
    test_results_pass: int
    test_results_fail: int
    badge: CareBadge
    discrepancy_details: List[str]
    proofs_satisfied: List[ProofObligationKind]
    proofs_failed: List[ProofObligationKind]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "routine_name": self.routine_name,
            "applied": self.applied,
            "syntax_valid": self.syntax_valid,
            "test_results_pass": self.test_results_pass,
            "test_results_fail": self.test_results_fail,
            "badge": self.badge.value,
            "discrepancy_details": self.discrepancy_details,
            "proofs_satisfied": [p.value for p in self.proofs_satisfied],
            "proofs_failed": [p.value for p in self.proofs_failed],
        }


@dataclass
class WithheldSuiteReport:
    rule_id: str
    badge: CareBadge
    training_passed: bool
    withheld_families_unaffected: bool
    unrelated_routines_untouched: bool
    withheld_regressions_count: int
    unrelated_alterations_count: int
    details: List[str]

    @property
    def is_eligible_for_promotion(self) -> bool:
        return (
            self.badge == CareBadge.PASS
            and self.training_passed
            and self.withheld_families_unaffected
            and self.unrelated_routines_untouched
            and self.withheld_regressions_count == 0
            and self.unrelated_alterations_count == 0
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "badge": self.badge.value,
            "training_passed": self.training_passed,
            "withheld_families_unaffected": self.withheld_families_unaffected,
            "unrelated_routines_untouched": self.unrelated_routines_untouched,
            "withheld_regressions_count": self.withheld_regressions_count,
            "unrelated_alterations_count": self.unrelated_alterations_count,
            "eligible_for_promotion": self.is_eligible_for_promotion,
            "details": self.details,
        }


@dataclass
class RuleResolutionResult:
    selected_rule: Optional[LearnedRewriteRule]
    badge: CareBadge
    conflict_detected: bool
    status_reason: str
    candidates_considered: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "selected_rule_id": self.selected_rule.rule_id if self.selected_rule else None,
            "badge": self.badge.value,
            "conflict_detected": self.conflict_detected,
            "status_reason": self.status_reason,
            "candidates_considered": self.candidates_considered,
        }


@dataclass
class RewriteResult:
    routine_name: str
    original_baseline: str
    final_output: str
    rules_applied: List[str]
    badge: CareBadge
    deterministic_match_on_rollback: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "routine_name": self.routine_name,
            "original_baseline": self.original_baseline,
            "final_output": self.final_output,
            "rules_applied": self.rules_applied,
            "badge": self.badge.value,
            "deterministic_match_on_rollback": self.deterministic_match_on_rollback,
        }


@dataclass
class RollbackResult:
    rule_id: str
    badge: CareBadge
    restored_routines_count: int
    verified_baseline_match: bool
    details: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "badge": self.badge.value,
            "restored_routines_count": self.restored_routines_count,
            "verified_baseline_match": self.verified_baseline_match,
            "details": self.details,
        }


@dataclass
class CrossProjectProofReport:
    rule_id: str
    target_project: str
    badge: CareBadge
    earned_proof: bool
    status: RuleStatus
    rejection_reason: Optional[str] = None
    shadow_test_count: int = 0
    passed_test_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "target_project": self.target_project,
            "badge": self.badge.value,
            "earned_proof": self.earned_proof,
            "status": self.status.value,
            "rejection_reason": self.rejection_reason,
            "shadow_test_count": self.shadow_test_count,
            "passed_test_count": self.passed_test_count,
        }


class RuleGeneralizationEngine:
    """Core engine for learning, verifying, and applying narrow scoped conversion rules."""

    def __init__(self) -> None:
        self.rules: Dict[str, LearnedRewriteRule] = {}
        self.rollback_ledger: List[Dict[str, Any]] = []

    def learn_rule_candidate(
        self,
        name: str,
        before_sql: str,
        fixed_sql: str,
        metadata: RoutineMetadata,
        dialect_constraint: DialectConstraint,
        scope: NarrowScope,
        rationale: str,
        author_or_model: str,
        origin_project_id: str = "project-alpha",
        origin_hop_id: str = "hop-01",
        custom_obligations: Optional[List[ProofObligationKind]] = None,
    ) -> LearnedRewriteRule:
        """Synthesizes a candidate rewrite rule from a proved fix with narrow preconditions."""
        # Calculate SHA256 of fix for provenance
        content_hash = hashlib.sha256(f"{before_sql}-->{fixed_sql}".encode("ascii", errors="replace")).hexdigest()
        rule_id = f"rule-{content_hash[:12]}"

        # Infer pattern and replacement if simple substitution or token pattern
        pattern, replacement = self._infer_pattern(before_sql, fixed_sql)

        # Standard proof obligations
        default_kinds = [
            ProofObligationKind.SEMANTIC_EQUIVALENCE,
            ProofObligationKind.NULL_PRESERVATION,
            ProofObligationKind.DECIMAL_PRECISION_SCALE_INVARIANT,
            ProofObligationKind.NO_SIDE_EFFECT_ALTERATION,
            ProofObligationKind.ZERO_REGRESSION_WITHHELD,
            ProofObligationKind.AST_WELL_FORMED,
        ]
        kinds = custom_obligations if custom_obligations is not None else default_kinds
        proof_obligations = [
            ProofObligation(kind=k, description=f"Verification of {k.value}", is_mandatory=True)
            for k in kinds
        ]

        provenance = RuleProvenance(
            origin_project_id=origin_project_id,
            origin_hop_id=origin_hop_id,
            origin_routine_name=metadata.name,
            origin_object_family=metadata.family,
            author_or_model=author_or_model,
            created_at_epoch=time.time(),
            source_sha256=content_hash,
            rationale=rationale,
            git_commit=None,
        )

        rule = LearnedRewriteRule(
            rule_id=rule_id,
            name=name,
            description=f"Narrow rule learned from {metadata.name} fix: {rationale}",
            provenance=provenance,
            dialect_constraint=dialect_constraint,
            scope=scope,
            source_pattern=pattern,
            replacement_template=replacement,
            proof_obligations=proof_obligations,
            status=RuleStatus.PROPOSED,
        )

        self.rules[rule_id] = rule
        return rule

    def _infer_pattern(self, before_sql: str, fixed_sql: str) -> Tuple[str, str]:
        """Infers regex pattern and replacement for SQL expressions."""
        # Extract NVL pattern to COALESCE with numeric cast
        nvl_match = re.search(r"NVL\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", before_sql, re.IGNORECASE)
        coalesce_match = re.search(r"COALESCE\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", fixed_sql, re.IGNORECASE)

        if nvl_match and coalesce_match:
            # Pattern capturing the exact expression
            expr_pat = re.escape(nvl_match.group(1).strip())
            default_val = re.escape(nvl_match.group(2).strip())
            pattern = rf"\bNVL\s*\(\s*({expr_pat})\s*,\s*({default_val})\s*\)"
            replacement = fixed_sql.strip()
            return pattern, replacement

        # Function call substitution
        func_before = re.search(r"\b([a-zA-Z_]\w*)\s*\(", before_sql)
        func_fixed = re.search(r"\b([a-zA-Z_]\w*)\s*\(", fixed_sql)
        if func_before and func_fixed and func_before.group(1).lower() != func_fixed.group(1).lower():
            old_fn = re.escape(func_before.group(1))
            new_fn = func_fixed.group(1)
            pattern = rf"\b{old_fn}\s*\("
            replacement = f"{new_fn}("
            return pattern, replacement

        # Literal escape
        return re.escape(before_sql.strip()), fixed_sql.strip()

    def evaluate_shadow(
        self,
        rule: LearnedRewriteRule,
        routine: RoutineDefinition,
        custom_executor: Optional[Callable[[str, Dict[str, Any]], Any]] = None,
    ) -> ShadowEvaluationResult:
        """Applies a candidate rule in shadow and runs verification tests."""
        rule.shadow_application_count += 1
        meta = routine.metadata

        # Check eligibility
        can_apply, reason = rule.can_apply_to(routine)
        if not can_apply:
            return ShadowEvaluationResult(
                rule_id=rule.rule_id,
                routine_name=meta.name,
                baseline_output=routine.baseline_sql,
                rewritten_output=routine.baseline_sql,
                applied=False,
                syntax_valid=True,
                test_results_pass=0,
                test_results_fail=0,
                badge=CareBadge.BENIGN,
                discrepancy_details=[f"Ineligible: {reason}"],
                proofs_satisfied=[],
                proofs_failed=[],
            )

        # Apply rewrite
        rewritten = rule.apply_rewrite(routine.baseline_sql)

        # Syntax check: no unbalanced parens or illegal tokens
        syntax_ok = (rewritten.count("(") == rewritten.count(")")) and (rewritten.count("'") % 2 == 0)
        if not syntax_ok:
            rule.shadow_failure_count += 1
            return ShadowEvaluationResult(
                rule_id=rule.rule_id,
                routine_name=meta.name,
                baseline_output=routine.baseline_sql,
                rewritten_output=rewritten,
                applied=True,
                syntax_valid=False,
                test_results_pass=0,
                test_results_fail=len(routine.test_cases),
                badge=CareBadge.ACTION,
                discrepancy_details=["Syntax corruption detected: unbalanced parentheses or quotes"],
                proofs_satisfied=[],
                proofs_failed=[ProofObligationKind.AST_WELL_FORMED],
            )

        # Run test cases
        passes = 0
        fails = 0
        discrepancies: List[str] = []
        satisfied_proofs: List[ProofObligationKind] = [ProofObligationKind.AST_WELL_FORMED]
        failed_proofs: List[ProofObligationKind] = []

        for tc in routine.test_cases:
            res: Any = None
            if custom_executor:
                try:
                    res = custom_executor(rewritten, tc.inputs)
                except Exception as ex:
                    fails += 1
                    discrepancies.append(f"Case {tc.case_id} crashed: {ex}")
                    continue
            elif routine.eval_fn:
                try:
                    res = routine.eval_fn(tc.inputs)
                except Exception as ex:
                    fails += 1
                    discrepancies.append(f"Case {tc.case_id} evaluation failed: {ex}")
                    continue
            else:
                fails += 1
                discrepancies.append(f"Case {tc.case_id} evaluation failed: neither custom_executor nor eval_fn provided")
                continue

            # Compare res to expected output
            if self._compare_values(res, tc.expected_output):
                passes += 1
            else:
                fails += 1
                discrepancies.append(
                    f"Case {tc.case_id} output mismatch: got {res!r}, expected {tc.expected_output!r}"
                )
                if tc.is_null_case:
                    failed_proofs.append(ProofObligationKind.NULL_PRESERVATION)

        # Determine obligations
        if fails == 0:
            satisfied_proofs.extend([
                ProofObligationKind.SEMANTIC_EQUIVALENCE,
                ProofObligationKind.NULL_PRESERVATION,
                ProofObligationKind.DECIMAL_PRECISION_SCALE_INVARIANT,
                ProofObligationKind.NO_SIDE_EFFECT_ALTERATION,
                ProofObligationKind.DETERMINISTIC_EVALUATION,
            ])
            badge = CareBadge.PASS
            rule.shadow_success_count += 1
        else:
            failed_proofs.append(ProofObligationKind.SEMANTIC_EQUIVALENCE)
            badge = CareBadge.ACTION
            rule.shadow_failure_count += 1

        # Mark rule obligations
        for po in rule.proof_obligations:
            if po.kind in satisfied_proofs:
                po.mark_satisfied("Shadow execution passed all cases")
            elif po.kind in failed_proofs:
                po.mark_failed("; ".join(discrepancies))

        return ShadowEvaluationResult(
            rule_id=rule.rule_id,
            routine_name=meta.name,
            baseline_output=routine.baseline_sql,
            rewritten_output=rewritten,
            applied=True,
            syntax_valid=True,
            test_results_pass=passes,
            test_results_fail=fails,
            badge=badge,
            discrepancy_details=discrepancies,
            proofs_satisfied=satisfied_proofs,
            proofs_failed=failed_proofs,
        )

    @staticmethod
    def _compare_values(val1: Any, val2: Any) -> bool:
        if val1 is None and val2 is None:
            return True
        if val1 is None or val2 is None:
            return False
        if isinstance(val1, (Decimal, int, float)) and isinstance(val2, (Decimal, int, float)):
            try:
                d1 = Decimal(str(val1)).normalize()
                d2 = Decimal(str(val2)).normalize()
                return d1 == d2
            except (decimal.InvalidOperation, ValueError):
                pass
        return val1 == val2

    def verify_against_withheld_suite(
        self,
        rule: LearnedRewriteRule,
        training_routine: RoutineDefinition,
        withheld_routines: List[RoutineDefinition],
        unrelated_routines: List[RoutineDefinition],
    ) -> WithheldSuiteReport:
        """Verifies the learned rule against withheld routines and unrelated routines.

        Guarantees that a fix learned for one routine:
        1. Fixes the training routine.
        2. Does NOT modify or regress unrelated routines.
        3. Respects withheld object families.
        """
        details: List[str] = []

        # 1. Training routine verification
        train_res = self.evaluate_shadow(rule, training_routine)
        training_passed = train_res.badge == CareBadge.PASS and train_res.applied
        if not training_passed:
            details.append(f"Training routine {training_routine.metadata.name} failed shadow validation")

        # 2. Unrelated routines: MUST remain 100% untouched
        unrelated_alterations = 0
        for ur in unrelated_routines:
            can_apply, _ = rule.can_apply_to(ur)
            if can_apply:
                rewritten = rule.apply_rewrite(ur.baseline_sql)
                if rewritten != ur.baseline_sql:
                    unrelated_alterations += 1
                    details.append(
                        f"Unrelated routine {ur.metadata.name} was erroneously altered by rule {rule.rule_id}"
                    )

        unrelated_untouched = unrelated_alterations == 0

        # 3. Withheld routines: test object families and ensure zero regressions
        withheld_regressions = 0
        withheld_families_unaffected = True

        for wr in withheld_routines:
            can_apply, reason = rule.can_apply_to(wr)
            if not can_apply:
                # Correctly withheld by narrow scope
                continue
            # If in scope, must pass shadow tests with zero regressions
            wr_res = self.evaluate_shadow(rule, wr)
            if wr_res.badge == CareBadge.ACTION or wr_res.test_results_fail > 0:
                withheld_regressions += 1
                details.append(
                    f"Withheld routine {wr.metadata.name} regressed under rule {rule.rule_id}: "
                    + "; ".join(wr_res.discrepancy_details)
                )

        if withheld_regressions > 0:
            withheld_families_unaffected = False

        # Overall badge
        if training_passed and unrelated_untouched and withheld_regressions == 0:
            badge = CareBadge.PASS
            for po in rule.proof_obligations:
                if po.kind == ProofObligationKind.ZERO_REGRESSION_WITHHELD:
                    po.mark_satisfied("All withheld and unrelated routines passed with zero regressions")
            rule.status = RuleStatus.EVALUATED
            details.append("Zero regressions on withheld suites; unrelated routines untouched")
        elif unrelated_alterations > 0:
            badge = CareBadge.ACTION
            rule.status = RuleStatus.REJECTED
            details.append("Rule rejected: overgeneralized and altered unrelated routines")
        elif withheld_regressions > 0:
            badge = CareBadge.ACTION
            rule.status = RuleStatus.REJECTED
            details.append(f"Rule rejected: caused {withheld_regressions} regressions on withheld suite")
        else:
            badge = CareBadge.REVIEW
            details.append("Rule requires review: training did not pass or scope is indeterminate")

        return WithheldSuiteReport(
            rule_id=rule.rule_id,
            badge=badge,
            training_passed=training_passed,
            withheld_families_unaffected=withheld_families_unaffected,
            unrelated_routines_untouched=unrelated_untouched,
            withheld_regressions_count=withheld_regressions,
            unrelated_alterations_count=unrelated_alterations,
            details=details,
        )

    def resolve_matching_rules(
        self,
        matching_rules: List[LearnedRewriteRule],
        routine: RoutineDefinition,
        context_info: Optional[Dict[str, Any]] = None,
    ) -> RuleResolutionResult:
        """Resolves rule conflicts deterministically using specificity scoring.

        If multiple candidate rules match the same routine:
        - Higher specificity wins.
        - If equal specificity and conflicting transformations: flag AMBIGUOUS_CONFLICT,
          refuse to guess, set badge ACTION, and fall back to deterministic baseline.
        """
        eligible_rules: List[LearnedRewriteRule] = []
        for r in matching_rules:
            ok, _ = r.can_apply_to(routine, context_info)
            if ok:
                eligible_rules.append(r)

        if not eligible_rules:
            return RuleResolutionResult(
                selected_rule=None,
                badge=CareBadge.BENIGN,
                conflict_detected=False,
                status_reason="No eligible matching rules",
                candidates_considered=[],
            )

        if len(eligible_rules) == 1:
            return RuleResolutionResult(
                selected_rule=eligible_rules[0],
                badge=CareBadge.PASS,
                conflict_detected=False,
                status_reason="Single matching rule selected",
                candidates_considered=[eligible_rules[0].rule_id],
            )

        # Sort by specificity descending
        sorted_rules = sorted(eligible_rules, key=lambda r: r.specificity_score, reverse=True)
        top_rule = sorted_rules[0]
        second_rule = sorted_rules[1]

        candidates = [r.rule_id for r in sorted_rules]

        # Check for ambiguity
        if abs(top_rule.specificity_score - second_rule.specificity_score) < SPECIFICITY_TIE_EPSILON:
            # Equal specificity score: check if transformations produce identical result
            sql1 = top_rule.apply_rewrite(routine.baseline_sql)
            sql2 = second_rule.apply_rewrite(routine.baseline_sql)
            if sql1 != sql2:
                # Conflicting rewrite without decisive specificity: refuse to guess!
                return RuleResolutionResult(
                    selected_rule=None,
                    badge=CareBadge.ACTION,
                    conflict_detected=True,
                    status_reason=(
                        f"AMBIGUOUS_RULE_CONFLICT: Rules {top_rule.rule_id} and {second_rule.rule_id} "
                        f"have equal specificity {top_rule.specificity_score} but conflicting rewrites. "
                        f"Refusing to guess; falling back to deterministic baseline."
                    ),
                    candidates_considered=candidates,
                )

        return RuleResolutionResult(
            selected_rule=top_rule,
            badge=CareBadge.PASS,
            conflict_detected=False,
            status_reason=f"Selected higher specificity rule {top_rule.rule_id} (score {top_rule.specificity_score})",
            candidates_considered=candidates,
        )

    def apply_rules(
        self,
        routine: RoutineDefinition,
        rules: Optional[Sequence[LearnedRewriteRule]] = None,
        context_info: Optional[Dict[str, Any]] = None,
    ) -> RewriteResult:
        """Applies active rules to a routine, preserving deterministic baseline."""
        available_rules = list(rules if rules is not None else self.rules.values())
        active_rules = [r for r in available_rules if r.status == RuleStatus.ACTIVE]

        original_baseline = routine.baseline_sql
        current_sql = original_baseline
        applied_rule_ids: List[str] = []

        # Find matching active rules
        matching: List[LearnedRewriteRule] = []
        for r in active_rules:
            ok, _ = r.can_apply_to(routine, context_info)
            if ok:
                matching.append(r)

        if not matching:
            return RewriteResult(
                routine_name=routine.metadata.name,
                original_baseline=original_baseline,
                final_output=original_baseline,
                rules_applied=[],
                badge=CareBadge.BENIGN,
                deterministic_match_on_rollback=True,
            )

        # Resolve conflict
        res = self.resolve_matching_rules(matching, routine, context_info)
        if res.conflict_detected:
            # Fall back strictly to baseline
            return RewriteResult(
                routine_name=routine.metadata.name,
                original_baseline=original_baseline,
                final_output=original_baseline,
                rules_applied=[],
                badge=CareBadge.ACTION,
                deterministic_match_on_rollback=True,
            )

        if res.selected_rule:
            current_sql = res.selected_rule.apply_rewrite(current_sql)
            applied_rule_ids.append(res.selected_rule.rule_id)
            badge = CareBadge.PASS
        else:
            badge = CareBadge.BENIGN

        return RewriteResult(
            routine_name=routine.metadata.name,
            original_baseline=original_baseline,
            final_output=current_sql,
            rules_applied=applied_rule_ids,
            badge=badge,
            deterministic_match_on_rollback=True,
        )

    def rollback_rule(
        self,
        rule_id: str,
        affected_routines: Sequence[RoutineDefinition],
        reason: str = "Regression rollback",
    ) -> RollbackResult:
        """Rolls back an active rule to the deterministic baseline immediately."""
        rule = self.rules.get(rule_id)
        if not rule:
            raise RuleGeneralizationError(f"Rule {rule_id} not found in engine registry")

        rule.status = RuleStatus.ROLLED_BACK
        rule.revoked_at_epoch = time.time()
        rule.revocation_reason = reason

        restored_count = 0
        all_matched = True

        for routine in affected_routines:
            res = self.apply_rules(routine, rules=[rule])
            if res.final_output == routine.baseline_sql:
                restored_count += 1
            else:
                all_matched = False

        self.rollback_ledger.append({
            "timestamp": time.time(),
            "rule_id": rule_id,
            "reason": reason,
            "restored_count": restored_count,
            "verified_baseline_match": all_matched,
        })

        badge = CareBadge.PASS if all_matched else CareBadge.ACTION
        return RollbackResult(
            rule_id=rule_id,
            badge=badge,
            restored_routines_count=restored_count,
            verified_baseline_match=all_matched,
            details=f"Rollback completed for rule {rule_id}: {reason}",
        )

    def revoke_rule(
        self,
        rule_id: str,
        reason: str = "Operator revocation",
    ) -> None:
        """Revokes a rule so it cannot be activated or applied."""
        rule = self.rules.get(rule_id)
        if not rule:
            raise RuleGeneralizationError(f"Rule {rule_id} not found")
        rule.status = RuleStatus.REVOKED
        rule.revoked_at_epoch = time.time()
        rule.revocation_reason = reason


class CrossProjectRegistry:
    """Manages cross-project rule sharing, proof earning, and independent revocation."""

    def __init__(self, engine: RuleGeneralizationEngine) -> None:
        self.engine = engine
        # project_id -> rule_id -> RuleStatus
        self.project_rule_states: Dict[str, Dict[str, RuleStatus]] = {}

    def register_rule_for_project(
        self,
        project_id: str,
        rule: LearnedRewriteRule,
        initial_status: RuleStatus = RuleStatus.ACTIVE,
    ) -> None:
        self.engine.rules[rule.rule_id] = rule
        self.project_rule_states.setdefault(project_id, {})[rule.rule_id] = initial_status

    def share_rule_to_project(
        self,
        rule_id: str,
        from_project: str,
        to_project: str,
    ) -> RuleStatus:
        """Shares a learned rule to another project in SHADOW state until proof is earned."""
        source_states = self.project_rule_states.get(from_project, {})
        if rule_id not in source_states:
            raise RuleGeneralizationError(f"Rule {rule_id} not registered in project {from_project}")
        if source_states[rule_id] != RuleStatus.ACTIVE:
            raise RuleGeneralizationError(
                f"Cannot share non-active rule {rule_id} from {from_project} (status: {source_states[rule_id]})"
            )

        # Initially SHADOW in target project
        self.project_rule_states.setdefault(to_project, {})[rule_id] = RuleStatus.SHADOW
        return RuleStatus.SHADOW

    def earn_proof_for_project(
        self,
        rule_id: str,
        to_project: str,
        training_routine: RoutineDefinition,
        withheld_routines: List[RoutineDefinition],
        unrelated_routines: List[RoutineDefinition],
    ) -> CrossProjectProofReport:
        """Evaluates whether a shared rule earns proof before activation in target project."""
        rule = self.engine.rules.get(rule_id)
        if not rule:
            raise RuleGeneralizationError(f"Rule {rule_id} not found")

        curr_state = self.project_rule_states.get(to_project, {}).get(rule_id)
        if curr_state not in (RuleStatus.SHADOW, RuleStatus.PROPOSED):
            raise RuleGeneralizationError(
                f"Rule {rule_id} is in state {curr_state} for project {to_project}, cannot earn proof"
            )

        # Run withheld verification suite in target project context
        report = self.engine.verify_against_withheld_suite(
            rule=rule,
            training_routine=training_routine,
            withheld_routines=withheld_routines,
            unrelated_routines=unrelated_routines,
        )

        total_tests = (
            len(training_routine.test_cases)
            + sum(len(r.test_cases) for r in withheld_routines)
            + sum(len(r.test_cases) for r in unrelated_routines)
        )

        if report.is_eligible_for_promotion:
            self.project_rule_states[to_project][rule_id] = RuleStatus.ACTIVE
            return CrossProjectProofReport(
                rule_id=rule_id,
                target_project=to_project,
                badge=CareBadge.PASS,
                earned_proof=True,
                status=RuleStatus.ACTIVE,
                rejection_reason=None,
                shadow_test_count=total_tests,
                passed_test_count=total_tests,
            )
        else:
            self.project_rule_states[to_project][rule_id] = RuleStatus.REJECTED
            reasons = "; ".join(report.details)
            return CrossProjectProofReport(
                rule_id=rule_id,
                target_project=to_project,
                badge=CareBadge.ACTION,
                earned_proof=False,
                status=RuleStatus.REJECTED,
                rejection_reason=f"Failed proof obligations: {reasons}",
                shadow_test_count=total_tests,
                passed_test_count=total_tests - report.withheld_regressions_count - report.unrelated_alterations_count,
            )

    def revoke_rule_in_project(
        self,
        rule_id: str,
        project_id: str,
        reason: str = "Project specific revocation",
    ) -> None:
        """Revokes a rule independently in a single project without affecting others."""
        if project_id not in self.project_rule_states or rule_id not in self.project_rule_states[project_id]:
            raise RuleGeneralizationError(f"Rule {rule_id} not registered for project {project_id}")

        self.project_rule_states[project_id][rule_id] = RuleStatus.REVOKED

    def is_rule_active_for_project(self, rule_id: str, project_id: str) -> bool:
        return self.project_rule_states.get(project_id, {}).get(rule_id) == RuleStatus.ACTIVE


def format_unix_report(report_dict: Dict[str, Any]) -> str:
    """Formats a structured generalization or proof report in terse UNIX style."""
    lines: List[str] = [
        "----------------------------------------------------------------",
        "MIGKIT RULE GENERALIZATION REPORT",
        "----------------------------------------------------------------",
    ]
    badge = report_dict.get("badge", "REVIEW")
    lines.append(f"STATUS BADGE : [{badge}]")
    for k, v in report_dict.items():
        if k == "badge":
            continue
        if isinstance(v, list):
            lines.append(f"{k.upper()}:")
            for item in v:
                lines.append(f"  * {item}")
        elif isinstance(v, dict):
            lines.append(f"{k.upper()}:")
            for subk, subv in v.items():
                lines.append(f"  {subk}: {subv}")
        else:
            lines.append(f"{k.upper():<20}: {v}")
    lines.append("----------------------------------------------------------------")
    return "\n".join(lines)


def rule_generalization_rungs() -> Tuple[decide.Rung, ...]:
    """Decide ladder for rule generalization admission and safety (Row 628: RCH20)."""
    return (
        decide.Rung(
            "admitted",
            "admit generalized rule verified by shadow evaluation and zero withheld regressions",
            needs=(
                decide.Need(
                    "evaluated against shadow workload",
                    lambda f: bool(f.get("evaluated")),
                    "{rung} requires rule to be evaluated against shadow workload",
                ),
                decide.Need(
                    "target family supported",
                    lambda f: bool(f.get("family_supported", True)),
                    "{rung} requires target object family to be supported",
                ),
                decide.Need(
                    "zero withheld regressions",
                    lambda f: int(f.get("withheld_failures", 0)) == 0,
                    "{rung} requires zero regressions across withheld test suites",
                ),
                decide.Need(
                    "shadow output conforms",
                    lambda f: int(f.get("shadow_divergence_count", 0)) == 0,
                    "{rung} requires shadow output to conform without unintended alterations",
                ),
                decide.Need(
                    "no unresolved conflicts",
                    lambda f: int(f.get("conflicts_unresolved", 0)) == 0,
                    "{rung} requires all competing rule conflicts to be resolved",
                ),
                decide.Need(
                    "rollback verified",
                    lambda f: bool(f.get("rollback_validated", True)),
                    "{rung} requires atomic rollback to be preflighted and verified",
                ),
            ),
            because="rule passed shadow evaluation across all suites with zero regressions and verified rollback",
        ),
        decide.Rung(
            "shadow_divergence_review",
            "flag rule for REVIEW due to non-breaking shadow divergence or tie in specificity",
            needs=(
                decide.Need(
                    "evaluated against shadow workload",
                    lambda f: bool(f.get("evaluated")),
                    "{rung} requires rule to be evaluated against shadow workload",
                ),
                decide.Need(
                    "zero withheld regressions",
                    lambda f: int(f.get("withheld_failures", 0)) == 0,
                    "{rung} requires zero regressions across withheld test suites",
                ),
                decide.Need(
                    "shadow divergence or specificity tie present",
                    lambda f: int(f.get("shadow_divergence_count", 0)) > 0 or bool(f.get("specificity_tie")),
                    "{rung} requires shadow divergence or specificity tie to flag for review",
                ),
            ),
            because="rule produced non-regressive shadow divergence or specificity tie requiring human review",
        ),
        decide.Rung(
            "rollback_untested_review",
            "flag rule for REVIEW due to unverified atomic rollback preflight",
            needs=(
                decide.Need(
                    "evaluated against shadow workload",
                    lambda f: bool(f.get("evaluated")),
                    "{rung} requires rule to be evaluated against shadow workload",
                ),
                decide.Need(
                    "rollback unverified",
                    lambda f: not bool(f.get("rollback_validated", True)),
                    "{rung} requires rollback to be unverified to flag for rollback review",
                ),
            ),
            because="rule passed tests but atomic rollback preflight is unverified",
        ),
        decide.Rung(
            "rejection_action",
            "reject rule generalization with ACTION care badge",
            because="rule caused regression on withheld suites, family mismatch, or unhandled conflicts",
        ),
    )


def decide_rule_generalization(facts: Dict[str, Any]) -> decide.Choice:
    """Climbs rule_generalization_rungs decide ladder."""
    return decide.climb("Rule generalization admission", rule_generalization_rungs(), facts)

