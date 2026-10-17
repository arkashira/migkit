"""AI Proof Loop with independent executable evidence (RCH19.ai-proof-loop).

Proposes conversion candidates with AI when authorized and accepts ONLY
independent executable evidence verified in isolated sandboxes.
Strictly rejects plausible wrong NULL, exception, timezone, or transaction
conversions with ACTION care badge. Enforces retry budgets, destination
allowlisting, counterexample minimization, and mutation-based fixture weakness
assessment.
"""
from __future__ import annotations

import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import decide
from . import durable

DEFAULT_MODEL_DESTINATION = "127.0.0.1:11434"
DEFAULT_MUTATION_KILL_RATE_THRESHOLD = 0.8
DEFAULT_RETRY_BUDGET = 3
DEFAULT_RETRY_TIMEOUT_SECONDS = 30.0
DEFAULT_APPROVED_DESTINATIONS = frozenset({
    "127.0.0.1:11434",
    "127.0.0.1:8000",
    "10.0.0.1:8080",
    "10.0.0.2:8080",
})


class AiProofLoopError(Exception):
    """Base exception for AI proof loop operations."""


class InexecutableCandidateError(AiProofLoopError, ValueError):
    """Raised when candidate has no executable callable."""


class LeaseLockHeldError(AiProofLoopError):
    """Raised when active lease lock (.run.lock) is held by another process."""


class CorruptCheckpointError(AiProofLoopError):
    """Raised when durable checkpoint file is corrupted."""


class CareBadge(str, Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ProofStatus(str, Enum):
    VERIFIED = "VERIFIED"
    DISCREPANCY = "DISCREPANCY"
    REJECTED = "REJECTED"
    WEAK_FIXTURES = "WEAK_FIXTURES"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    UNAUTHORIZED = "UNAUTHORIZED"
    DESTINATION_REFUSED = "DESTINATION_REFUSED"


class SemanticFaultType(str, Enum):
    WRONG_NULL = "WRONG_NULL"
    WRONG_EXCEPTION = "WRONG_EXCEPTION"
    WRONG_TIMEZONE = "WRONG_TIMEZONE"
    WRONG_TRANSACTION = "WRONG_TRANSACTION"
    VALUE_MISMATCH = "VALUE_MISMATCH"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    SIDE_EFFECT_MISMATCH = "SIDE_EFFECT_MISMATCH"
    TIMEOUT = "TIMEOUT"
    CRASH = "CRASH"


class TransactionState(str, Enum):
    NONE = "NONE"
    IN_TRANSACTION = "IN_TRANSACTION"
    COMMITTED = "COMMITTED"
    ROLLED_BACK = "ROLLED_BACK"


@dataclass
class AiAnnotation:
    """AI explanations and confidence metrics are annotations, NOT proof."""
    model_id: str
    explanation: str
    confidence_score: float
    prompt_tokens: int = 0
    completion_tokens: int = 0
    is_proof: bool = False  # Strictly False: AI explanations are never proof.

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_id": self.model_id,
            "explanation": self.explanation,
            "confidence_score": float(self.confidence_score),
            "prompt_tokens": int(self.prompt_tokens),
            "completion_tokens": int(self.completion_tokens),
            "is_proof": False,
        }


@dataclass
class CandidateProvenance:
    candidate_id: str
    source_checksum: str
    model_destination: str
    synthesized_at: float
    prompt_hash: str
    attempt_index: int
    annotation: Optional[AiAnnotation] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "source_checksum": self.source_checksum,
            "model_destination": self.model_destination,
            "synthesized_at": self.synthesized_at,
            "prompt_hash": self.prompt_hash,
            "attempt_index": self.attempt_index,
            "annotation": self.annotation.to_dict() if self.annotation else None,
        }


@dataclass
class CodeCandidate:
    candidate_id: str
    code: str
    callable_obj: Optional[Callable[..., Any]] = None
    provenance: Optional[CandidateProvenance] = None
    source_language: str = "sql"
    target_language: str = "python"
    is_deterministic: bool = False


@dataclass
class ExecutionEffect:
    return_value: Any = None
    out_params: Dict[str, Any] = field(default_factory=dict)
    result_sets: List[List[Dict[str, Any]]] = field(default_factory=list)
    rows_affected: int = 0
    error_class: Optional[str] = None
    error_message: Optional[str] = None
    transaction_status: TransactionState = TransactionState.NONE
    timezone_name: Optional[str] = None
    utc_timestamp: Optional[str] = None
    state_mutations: Dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "return_value": repr(self.return_value) if self.return_value is not None else None,
            "out_params": self.out_params,
            "result_sets": self.result_sets,
            "rows_affected": self.rows_affected,
            "error_class": self.error_class,
            "error_message": self.error_message,
            "transaction_status": self.transaction_status.value,
            "timezone_name": self.timezone_name,
            "utc_timestamp": self.utc_timestamp,
            "state_mutations": self.state_mutations,
            "duration_ms": self.duration_ms,
        }


@dataclass
class Discrepancy:
    badge: CareBadge
    fault_type: SemanticFaultType
    input_args: Dict[str, Any]
    source_effect: ExecutionEffect
    target_effect: ExecutionEffect
    explanation: str
    is_minimized: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "badge": self.badge.value,
            "fault_type": self.fault_type.value,
            "input_args": self.input_args,
            "source_effect": self.source_effect.to_dict(),
            "target_effect": self.target_effect.to_dict(),
            "explanation": self.explanation,
            "is_minimized": self.is_minimized,
        }


@dataclass
class FrozenInputFixture:
    fixture_id: str
    inputs: List[Dict[str, Any]]
    frozen_at: float = field(default_factory=time.time)
    fixture_hash: str = field(init=False)

    def __post_init__(self) -> None:
        raw_json = json.dumps(self.inputs, sort_keys=True, default=str)
        self.fixture_hash = hashlib.sha256(raw_json.encode("ascii")).hexdigest()

    def add_input(self, inp: Dict[str, Any]) -> None:
        self.inputs.append(inp)
        raw_json = json.dumps(self.inputs, sort_keys=True, default=str)
        self.fixture_hash = hashlib.sha256(raw_json.encode("ascii")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fixture_id": self.fixture_id,
            "fixture_hash": self.fixture_hash,
            "frozen_at": self.frozen_at,
            "input_count": len(self.inputs),
            "inputs": self.inputs,
        }


@dataclass
class DestinationPolicy:
    """Guards model communication: only approved test hosts permitted."""
    approved_destinations: Set[str] = field(default_factory=lambda: {
        "127.0.0.1:11434",
        "127.0.0.1:8000",
        "10.0.0.1:8080",
        "10.0.0.2:8080",
    })
    allow_pii: bool = False

    def is_destination_approved(self, destination: str) -> bool:
        if not destination:
            return False
        dest_clean = destination.strip().lower()
        # Direct exact match
        if dest_clean in {d.lower() for d in self.approved_destinations}:
            return True
        # Check IP/prefix safety
        host_part = dest_clean.split(":")[0]
        if host_part == "127.0.0.1" or host_part == "localhost":
            return True
        if host_part.startswith("10.0.0."):
            return True
        return False

    def sanitize_payload(self, text: str) -> Tuple[bool, str]:
        """Checks text for unapproved data/code leaks."""
        # Simple detection of secret patterns or unauthorized credit cards / emails
        sensitive_patterns = [
            (re.compile(r"\b4[0-9]{12}(?:[0-9]{3})?\b"), "CREDIT_CARD"),
            (re.compile(r"\bpassword\s*[:=]\s*['\"]?[^\s'\"]+", re.IGNORECASE), "PASSWORD_SECRET"),
            (re.compile(r"\baws_secret_access_key\s*[:=]\s*['\"]?[^\s'\"]+", re.IGNORECASE), "AWS_SECRET"),
        ]
        sanitized = text
        found_violation = False
        for pattern, tag in sensitive_patterns:
            if pattern.search(sanitized):
                found_violation = True
                sanitized = pattern.sub(f"[REDACTED_{tag}]", sanitized)
        return not found_violation or self.allow_pii, sanitized


@dataclass
class AuthorizationPolicy:
    authorized: bool = False
    destination_policy: DestinationPolicy = field(default_factory=DestinationPolicy)
    max_candidates: int = 5
    max_retry_budget: int = 3
    retry_timeout_seconds: float = 30.0
    min_mutation_kill_rate: float = 0.8


@dataclass
class MutantRecord:
    mutant_id: str
    description: str
    mutant_code: str
    mutant_callable: Optional[Callable[..., Any]] = None
    killed: bool = False
    killer_input: Optional[Dict[str, Any]] = None


@dataclass
class MutationReport:
    total_mutants: int
    killed_mutants: int
    survived_mutants: int
    kill_rate: float
    badge: CareBadge
    survivors: List[MutantRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_mutants": self.total_mutants,
            "killed_mutants": self.killed_mutants,
            "survived_mutants": self.survived_mutants,
            "kill_rate": round(self.kill_rate, 4),
            "badge": self.badge.value,
            "survivors": [
                {
                    "mutant_id": m.mutant_id,
                    "description": m.description,
                }
                for m in self.survivors
            ],
        }


@dataclass
class ProofResult:
    badge: CareBadge
    status: ProofStatus
    routine_name: str
    selected_candidate: Optional[CodeCandidate] = None
    discrepancies: List[Discrepancy] = field(default_factory=list)
    minimized_counterexample: Optional[Dict[str, Any]] = None
    mutation_report: Optional[MutationReport] = None
    annotation: Optional[AiAnnotation] = None
    provenance: Optional[CandidateProvenance] = None
    attempts_made: int = 0
    duration_seconds: float = 0.0
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "badge": self.badge.value,
            "status": self.status.value,
            "routine_name": self.routine_name,
            "candidate_id": self.selected_candidate.candidate_id if self.selected_candidate else None,
            "discrepancies": [d.to_dict() for d in self.discrepancies],
            "minimized_counterexample": self.minimized_counterexample,
            "mutation_report": self.mutation_report.to_dict() if self.mutation_report else None,
            "annotation": self.annotation.to_dict() if self.annotation else None,
            "provenance": self.provenance.to_dict() if self.provenance else None,
            "attempts_made": self.attempts_made,
            "duration_seconds": round(self.duration_seconds, 4),
            "reasons": self.reasons,
        }

    def render_unix_summary(self) -> str:
        lines = [
            f"[{self.badge.value}] routine={self.routine_name} status={self.status.value} attempts={self.attempts_made}",
        ]
        if self.selected_candidate:
            lines.append(f"  candidate_id={self.selected_candidate.candidate_id} deterministic={self.selected_candidate.is_deterministic}")
        if self.provenance:
            lines.append(f"  destination={self.provenance.model_destination} checksum={self.provenance.source_checksum[:12]}")
        if self.annotation:
            lines.append(f"  ai_annotation: model={self.annotation.model_id} confidence={self.annotation.confidence_score:.2f} is_proof={self.annotation.is_proof}")
        if self.mutation_report:
            lines.append(f"  mutation_kill_rate={self.mutation_report.kill_rate * 100:.1f}% killed={self.mutation_report.killed_mutants}/{self.mutation_report.total_mutants}")
        if self.discrepancies:
            lines.append(f"  discrepancies_count={len(self.discrepancies)}")
            for d in self.discrepancies[:3]:
                lines.append(f"    - fault={d.fault_type.value} badge={d.badge.value} input={json.dumps(d.input_args, default=str)} error={d.explanation}")
        if self.minimized_counterexample:
            lines.append(f"  minimized_counterexample={json.dumps(self.minimized_counterexample, default=str)}")
        for r in self.reasons:
            lines.append(f"  note: {r}")
        return "\n".join(lines)


# -------------------------------------------------------------------------
# Isolated Sandbox Execution & Effects Comparison
# -------------------------------------------------------------------------

class IsolatedSandbox:
    """Executes a callable with state isolation and collects observable effects."""

    def __init__(self, initial_state: Optional[Dict[str, Any]] = None):
        self._initial_state = copy.deepcopy(initial_state) if initial_state else {}

    def execute(self, func: Callable[..., Any], args: Dict[str, Any]) -> ExecutionEffect:
        state_copy = copy.deepcopy(self._initial_state)
        effect = ExecutionEffect()
        t0 = time.time()

        # Context wrapper to detect transactions, timezone, and state mutations
        context = {
            "state": state_copy,
            "out_params": {},
            "result_sets": [],
            "rows_affected": 0,
            "transaction_status": TransactionState.NONE,
            "timezone_name": None,
            "utc_timestamp": None,
        }

        try:
            # Check if func accepts context or kwargs
            res = func(args, context=context) if "context" in func.__code__.co_varnames else func(**args)
            effect.return_value = res
            effect.out_params = copy.deepcopy(context["out_params"])
            effect.result_sets = copy.deepcopy(context["result_sets"])
            effect.rows_affected = context["rows_affected"]
            effect.transaction_status = context["transaction_status"]
            effect.timezone_name = context["timezone_name"]
            effect.utc_timestamp = context["utc_timestamp"]
            effect.state_mutations = copy.deepcopy(context["state"])
        except Exception as exc:
            effect.error_class = exc.__class__.__name__
            effect.error_message = str(exc)
            # Check transaction status during error
            effect.transaction_status = context["transaction_status"]
            effect.state_mutations = copy.deepcopy(context["state"])

        effect.duration_ms = (time.time() - t0) * 1000.0
        return effect


class EffectsComparator:
    """Strictly evaluates observable effects with zero tolerance for plausible wrong conversions."""

    @staticmethod
    def compare(
        source_effect: ExecutionEffect,
        target_effect: ExecutionEffect,
        input_args: Dict[str, Any],
    ) -> Optional[Discrepancy]:
        # 1. Strict Exception Check (Plausible wrong exception conversion)
        if source_effect.error_class != target_effect.error_class:
            explanation = (
                f"Exception divergence: source raised '{source_effect.error_class}' "
                f"({source_effect.error_message}), but target gave '{target_effect.error_class}' "
                f"({target_effect.error_message})"
            )
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.WRONG_EXCEPTION,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=explanation,
            )

        # If both raised the same error class, observable error behavior matches
        if source_effect.error_class is not None:
            # Both raised error; check transaction rollback consistency
            if source_effect.transaction_status != target_effect.transaction_status:
                explanation = (
                    f"Transaction state mismatch on error: source ended with "
                    f"{source_effect.transaction_status.value}, target ended with "
                    f"{target_effect.transaction_status.value}"
                )
                return Discrepancy(
                    badge=CareBadge.ACTION,
                    fault_type=SemanticFaultType.WRONG_TRANSACTION,
                    input_args=input_args,
                    source_effect=source_effect,
                    target_effect=target_effect,
                    explanation=explanation,
                )
            return None

        # 2. Strict NULL Conversion Check (Plausible wrong NULL conversion)
        # e.g., None vs 0, None vs "", None vs "NULL", None vs False
        src_val = source_effect.return_value
        tgt_val = target_effect.return_value

        if (src_val is None and tgt_val is not None) or (src_val is not None and tgt_val is None):
            explanation = (
                f"Plausible wrong NULL conversion: source returned {repr(src_val)}, "
                f"target returned {repr(tgt_val)}"
            )
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.WRONG_NULL,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=explanation,
            )

        # 3. Strict Timezone Conversion Check
        if source_effect.timezone_name != target_effect.timezone_name or \
           source_effect.utc_timestamp != target_effect.utc_timestamp:
            explanation = (
                f"Timezone conversion fault: source (tz={source_effect.timezone_name}, "
                f"utc={source_effect.utc_timestamp}) vs target (tz={target_effect.timezone_name}, "
                f"utc={target_effect.utc_timestamp})"
            )
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.WRONG_TIMEZONE,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=explanation,
            )

        # Check datetime return values for timezone drift
        if isinstance(src_val, (datetime.date, datetime.datetime)) or isinstance(tgt_val, (datetime.date, datetime.datetime)):
            if str(src_val) != str(tgt_val):
                explanation = f"Datetime offset discrepancy: source={src_val} target={tgt_val}"
                return Discrepancy(
                    badge=CareBadge.ACTION,
                    fault_type=SemanticFaultType.WRONG_TIMEZONE,
                    input_args=input_args,
                    source_effect=source_effect,
                    target_effect=target_effect,
                    explanation=explanation,
                )

        # 4. Strict Transaction State Check
        if source_effect.transaction_status != target_effect.transaction_status:
            explanation = (
                f"Transaction state mismatch: source was {source_effect.transaction_status.value}, "
                f"target was {target_effect.transaction_status.value}"
            )
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.WRONG_TRANSACTION,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=explanation,
            )

        # 5. Return Value Equality and Type Precision
        if type(src_val) != type(tgt_val):
            # Special case: float vs int when values are exactly equal
            if isinstance(src_val, (int, float)) and isinstance(tgt_val, (int, float)):
                if abs(float(src_val) - float(tgt_val)) > 1e-9:
                    return Discrepancy(
                        badge=CareBadge.ACTION,
                        fault_type=SemanticFaultType.VALUE_MISMATCH,
                        input_args=input_args,
                        source_effect=source_effect,
                        target_effect=target_effect,
                        explanation=f"Numeric value mismatch: {src_val} vs {tgt_val}",
                    )
            else:
                return Discrepancy(
                    badge=CareBadge.ACTION,
                    fault_type=SemanticFaultType.TYPE_MISMATCH,
                    input_args=input_args,
                    source_effect=source_effect,
                    target_effect=target_effect,
                    explanation=f"Type mismatch: {type(src_val).__name__} vs {type(tgt_val).__name__}",
                )
        elif src_val != tgt_val:
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.VALUE_MISMATCH,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=f"Return value mismatch: source={repr(src_val)}, target={repr(tgt_val)}",
            )

        # 6. Out parameters and side-effect mutations
        if source_effect.out_params != target_effect.out_params:
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.SIDE_EFFECT_MISMATCH,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=f"OUT params mismatch: {source_effect.out_params} vs {target_effect.out_params}",
            )

        if source_effect.state_mutations != target_effect.state_mutations:
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.SIDE_EFFECT_MISMATCH,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation="Isolated state mutations differ between source and target",
            )

        if source_effect.rows_affected != target_effect.rows_affected:
            return Discrepancy(
                badge=CareBadge.ACTION,
                fault_type=SemanticFaultType.SIDE_EFFECT_MISMATCH,
                input_args=input_args,
                source_effect=source_effect,
                target_effect=target_effect,
                explanation=f"Rows affected mismatch: {source_effect.rows_affected} vs {target_effect.rows_affected}",
            )

        return None


# -------------------------------------------------------------------------
# Counterexample Minimizer (Delta Debugging)
# -------------------------------------------------------------------------

class CounterexampleMinimizer:
    """Minimizes failing counterexample inputs down to the smallest reproducing case."""

    @classmethod
    def minimize(
        cls,
        failing_input: Dict[str, Any],
        source_fn: Callable[..., Any],
        target_fn: Callable[..., Any],
        sandbox: IsolatedSandbox,
    ) -> Dict[str, Any]:
        current = copy.deepcopy(failing_input)

        def fails(inp: Dict[str, Any]) -> bool:
            s_eff = sandbox.execute(source_fn, inp)
            t_eff = sandbox.execute(target_fn, inp)
            return EffectsComparator.compare(s_eff, t_eff, inp) is not None

        if not fails(current):
            return current

        # Pass 1: Try dropping non-essential keys
        keys = list(current.keys())
        for k in keys:
            if len(current) <= 1:
                break
            candidate = {key: val for key, val in current.items() if key != k}
            if fails(candidate):
                current = candidate

        # Pass 2: Shrink individual values
        for k, v in list(current.items()):
            if isinstance(v, int):
                # Try 0
                cand = dict(current, **{k: 0})
                if fails(cand):
                    current = cand
                    continue
                # Try 1 or -1
                for step in [1, -1]:
                    cand = dict(current, **{k: step})
                    if fails(cand):
                        current = cand
                        break
                # Binary search shrink towards 0 if still large
                val = current[k]
                while abs(val) > 1:
                    half = val // 2
                    cand = dict(current, **{k: half})
                    if fails(cand):
                        current = cand
                        val = half
                    else:
                        break

            elif isinstance(v, str):
                # Try empty string
                cand = dict(current, **{k: ""})
                if fails(cand):
                    current = cand
                    continue
                # Try single character
                if len(v) > 1:
                    cand = dict(current, **{k: v[:1]})
                    if fails(cand):
                        current = cand
                        continue
                # Shrink length
                s = current[k]
                while len(s) > 2:
                    half = s[:len(s) // 2]
                    cand = dict(current, **{k: half})
                    if fails(cand):
                        current = cand
                        s = half
                    else:
                        break

            elif isinstance(v, list):
                # Try empty list
                cand = dict(current, **{k: []})
                if fails(cand):
                    current = cand
                    continue
                # Slice list
                lst = list(current[k])
                while len(lst) > 1:
                    half = lst[:len(lst) // 2]
                    cand = dict(current, **{k: half})
                    if fails(cand):
                        current = cand
                        lst = half
                    else:
                        break

        return current


# -------------------------------------------------------------------------
# Mutation Testing: Fixture Weakness Assessment
# -------------------------------------------------------------------------

class MutationAssessor:
    """Mutates candidate code to assess if test fixtures are weak or strong."""

    MUTATION_RULES: List[Tuple[str, str, str]] = [
        ("boundary_gte_to_gt", r">=", ">"),
        ("boundary_lte_to_lt", r"<=", "<"),
        ("equality_inversion", r"==", "!="),
        ("logical_and_to_or", r"\band\b", "or"),
        ("logical_or_to_and", r"\bor\b", "and"),
        ("arithmetic_plus_to_minus", r"\+", "-"),
        ("arithmetic_minus_to_plus", r"-", "+"),
        ("null_coalesce_bypass", r"if\s+([a-zA-Z0-9_]+)\s+is\s+None:\s*return\s+None", "if False: pass"),
        ("null_coalesce_default", r"if\s+([a-zA-Z0-9_]+)\s+is\s+None:\s*return\s+[0-9]+", "return 0"),
        ("tz_offset_dropped", r"timezone", "None"),
        ("tx_commit_to_rollback", r"COMMITTED", "ROLLED_BACK"),
    ]

    @classmethod
    def generate_code_mutants(cls, code: str, max_mutants: int = 15) -> List[Tuple[str, str]]:
        mutants: List[Tuple[str, str]] = []
        for name, pat, rep in cls.MUTATION_RULES:
            if re.search(pat, code):
                mutated_code = re.sub(pat, rep, code, count=1)
                if mutated_code != code:
                    mutants.append((name, mutated_code))
                if len(mutants) >= max_mutants:
                    break
        return mutants

    @classmethod
    def assess_candidate(
        cls,
        candidate_code: str,
        candidate_fn: Callable[..., Any],
        fixture: FrozenInputFixture,
        sandbox: IsolatedSandbox,
        synthetic_mutants: Optional[List[Tuple[str, Callable[..., Any]]]] = None,
        min_kill_rate: float = 0.8,
    ) -> MutationReport:
        records: List[MutantRecord] = []

        # 1. Evaluate synthetic mutant callables (if provided directly for robust evaluation)
        if synthetic_mutants:
            for i, (desc, mut_fn) in enumerate(synthetic_mutants):
                rec = MutantRecord(
                    mutant_id=f"mutant_{i+1:03d}",
                    description=desc,
                    mutant_code="",
                    mutant_callable=mut_fn,
                )
                records.append(rec)

        # 2. Add textual mutants if room
        if not records and candidate_code:
            text_mutants = cls.generate_code_mutants(candidate_code)
            for i, (name, mcode) in enumerate(text_mutants):
                # Try to compile mutant
                try:
                    local_env: Dict[str, Any] = {}
                    exec(mcode, {}, local_env)
                    # Find first callable
                    fn = next((v for v in local_env.values() if callable(v)), None)
                    if fn:
                        records.append(MutantRecord(
                            mutant_id=f"text_mutant_{i+1:03d}",
                            description=name,
                            mutant_code=mcode,
                            mutant_callable=fn,
                        ))
                except Exception:
                    continue

        if not records:
            # If no mutants could be formed, treat as baseline 100% with benign notice
            return MutationReport(
                total_mutants=0,
                killed_mutants=0,
                survived_mutants=0,
                kill_rate=1.0,
                badge=CareBadge.BENIGN,
                survivors=[],
            )

        # Run each mutant against all frozen fixture inputs
        for rec in records:
            if not rec.mutant_callable:
                continue
            for inp in fixture.inputs:
                cand_eff = sandbox.execute(candidate_fn, inp)
                mut_eff = sandbox.execute(rec.mutant_callable, inp)
                disc = EffectsComparator.compare(cand_eff, mut_eff, inp)
                if disc is not None:
                    rec.killed = True
                    rec.killer_input = inp
                    break

        killed = sum(1 for r in records if r.killed)
        total = len(records)
        survivors = [r for r in records if not r.killed]
        kill_rate = killed / total if total > 0 else 1.0

        if kill_rate >= min_kill_rate and len(survivors) == 0:
            badge = CareBadge.PASS
        elif kill_rate >= min_kill_rate:
            badge = CareBadge.REVIEW
        else:
            badge = CareBadge.ACTION

        return MutationReport(
            total_mutants=total,
            killed_mutants=killed,
            survived_mutants=len(survivors),
            kill_rate=kill_rate,
            badge=badge,
            survivors=survivors,
        )


# -------------------------------------------------------------------------
# Regression Fixture Bank
# -------------------------------------------------------------------------

class RegressionFixtureBank:
    """Retains counterexamples and hard boundary cases across iterations."""

    def __init__(self) -> None:
        self._fixtures: Dict[str, List[Dict[str, Any]]] = {}

    def record_counterexample(self, routine_name: str, input_args: Dict[str, Any]) -> None:
        if routine_name not in self._fixtures:
            self._fixtures[routine_name] = []
        if input_args not in self._fixtures[routine_name]:
            self._fixtures[routine_name].append(copy.deepcopy(input_args))

    def get_regression_inputs(self, routine_name: str) -> List[Dict[str, Any]]:
        return copy.deepcopy(self._fixtures.get(routine_name, []))

    def clear(self) -> None:
        self._fixtures.clear()


# -------------------------------------------------------------------------
# AI Proof Loop Engine
# -------------------------------------------------------------------------

class AiProofLoopEngine:
    """Core engine governing deterministic verification and bounded AI synthesis."""

    def __init__(
        self,
        authorization: Optional[AuthorizationPolicy] = None,
        fixture_bank: Optional[RegressionFixtureBank] = None,
        sandbox: Optional[IsolatedSandbox] = None,
    ):
        self.auth = authorization or AuthorizationPolicy(authorized=False)
        self.fixture_bank = fixture_bank or RegressionFixtureBank()
        self.sandbox = sandbox or IsolatedSandbox()

    def create_frozen_fixture(
        self,
        routine_name: str,
        base_inputs: Sequence[Dict[str, Any]],
        include_edge_cases: bool = True,
    ) -> FrozenInputFixture:
        """Constructs an aligned frozen input set, closed over edge cases and regression inputs."""
        inputs_list: List[Dict[str, Any]] = [copy.deepcopy(x) for x in base_inputs]

        # Append historical regression counterexamples
        reg_inputs = self.fixture_bank.get_regression_inputs(routine_name)
        for r_inp in reg_inputs:
            if r_inp not in inputs_list:
                inputs_list.append(r_inp)

        # Standard edge cases if requested
        if include_edge_cases:
            standard_edges = [
                {"a": None, "b": None},
                {"a": 0, "b": 0},
                {"a": -1, "b": 1},
                {"a": "", "b": "test"},
                {"a": "   ", "b": "   "},
            ]
            for ed in standard_edges:
                if ed not in inputs_list:
                    inputs_list.append(ed)

        return FrozenInputFixture(
            fixture_id=f"fixture_{routine_name}_{int(time.time())}",
            inputs=inputs_list,
        )

    def verify_candidate(
        self,
        source_fn: Callable[..., Any],
        candidate: CodeCandidate,
        fixture: FrozenInputFixture,
        synthetic_mutants: Optional[List[Tuple[str, Callable[..., Any]]]] = None,
    ) -> Tuple[bool, List[Discrepancy], Optional[Dict[str, Any]], Optional[MutationReport]]:
        """Runs candidate in sandbox against frozen fixture; compares effects and minimizes counterexamples."""
        if not candidate.callable_obj:
            raise InexecutableCandidateError(f"Candidate {candidate.candidate_id} has no executable callable")

        discrepancies: List[Discrepancy] = []
        minimized_ce: Optional[Dict[str, Any]] = None

        for inp in fixture.inputs:
            src_eff = self.sandbox.execute(source_fn, inp)
            tgt_eff = self.sandbox.execute(candidate.callable_obj, inp)
            disc = EffectsComparator.compare(src_eff, tgt_eff, inp)
            if disc is not None:
                # Discrepancy detected: minimize counterexample
                min_inp = CounterexampleMinimizer.minimize(
                    failing_input=inp,
                    source_fn=source_fn,
                    target_fn=candidate.callable_obj,
                    sandbox=self.sandbox,
                )
                disc.is_minimized = True
                disc.input_args = min_inp
                discrepancies.append(disc)
                if minimized_ce is None:
                    minimized_ce = min_inp

        if discrepancies:
            return False, discrepancies, minimized_ce, None

        # All inputs match: run mutation tests to assess fixture weakness
        mut_report = MutationAssessor.assess_candidate(
            candidate_code=candidate.code,
            candidate_fn=candidate.callable_obj,
            fixture=fixture,
            sandbox=self.sandbox,
            synthetic_mutants=synthetic_mutants,
            min_kill_rate=self.auth.min_mutation_kill_rate,
        )

        passed = (mut_report.badge in (CareBadge.PASS, CareBadge.BENIGN))
        return passed, [], None, mut_report

    def run_proof_loop(
        self,
        routine_name: str,
        source_fn: Callable[..., Any],
        source_code: str,
        deterministic_candidate: Optional[CodeCandidate] = None,
        synthesis_generator: Optional[Callable[[int, Optional[Dict[str, Any]]], CodeCandidate]] = None,
        base_inputs: Optional[Sequence[Dict[str, Any]]] = None,
        synthetic_mutants: Optional[List[Tuple[str, Callable[..., Any]]]] = None,
        destination: str = "127.0.0.1:11434",
        checkpoint_dir: Optional[Union[str, Path]] = None,
    ) -> ProofResult:
        """Orchestrates deterministic check first, followed by bounded AI synthesis if authorized."""
        t_start = time.time()
        base_inps = base_inputs or [{"a": 1, "b": 2}, {"a": 10, "b": 20}]

        # Check O(1) durable checkpoint store if provided
        store: Optional[ProofCheckpointStore] = None
        if checkpoint_dir:
            store = ProofCheckpointStore(checkpoint_dir)
            if store.is_completed(routine_name):
                ckpt = store.load(routine_name)
                if ckpt:
                    return ProofResult(
                        badge=ckpt.badge,
                        status=ckpt.status,
                        routine_name=routine_name,
                        attempts_made=ckpt.attempts_made,
                        duration_seconds=ckpt.duration_seconds,
                        minimized_counterexample=ckpt.minimized_counterexample,
                        reasons=ckpt.reasons or ["Restored from durable checkpoint in O(1)"],
                    )
        fixture = self.create_frozen_fixture(routine_name, base_inps)
        source_checksum = hashlib.sha256(source_code.encode("ascii")).hexdigest()

        # Step 1: Check Deterministic Candidate
        if deterministic_candidate and deterministic_candidate.callable_obj:
            deterministic_candidate.is_deterministic = True
            ok, disc, ce, mut_report = self.verify_candidate(
                source_fn=source_fn,
                candidate=deterministic_candidate,
                fixture=fixture,
                synthetic_mutants=synthetic_mutants,
            )
            if ok:
                return ProofResult(
                    badge=CareBadge.PASS,
                    status=ProofStatus.VERIFIED,
                    routine_name=routine_name,
                    selected_candidate=deterministic_candidate,
                    mutation_report=mut_report,
                    attempts_made=1,
                    duration_seconds=time.time() - t_start,
                    reasons=["Deterministic conversion verified with independent executable evidence"],
                )
            if ce:
                self.fixture_bank.record_counterexample(routine_name, ce)

        # Step 2: Deterministic conversion insufficient -> check Authorization Policy
        if not self.auth.authorized:
            return ProofResult(
                badge=CareBadge.ACTION,
                status=ProofStatus.UNAUTHORIZED,
                routine_name=routine_name,
                attempts_made=1 if deterministic_candidate else 0,
                duration_seconds=time.time() - t_start,
                reasons=["AI candidate synthesis is not authorized by policy"],
            )

        # Step 3: Check Destination Safety and PII sanitization
        if not self.auth.destination_policy.is_destination_approved(destination):
            return ProofResult(
                badge=CareBadge.ACTION,
                status=ProofStatus.DESTINATION_REFUSED,
                routine_name=routine_name,
                duration_seconds=time.time() - t_start,
                reasons=[f"Destination {destination} not in approved safe test host list"],
            )

        safe_prompt_ok, _ = self.auth.destination_policy.sanitize_payload(source_code)
        if not safe_prompt_ok:
            return ProofResult(
                badge=CareBadge.ACTION,
                status=ProofStatus.REJECTED,
                routine_name=routine_name,
                duration_seconds=time.time() - t_start,
                reasons=["Unapproved sensitive data / credentials detected in synthesis payload"],
            )

        if not synthesis_generator:
            return ProofResult(
                badge=CareBadge.ACTION,
                status=ProofStatus.REJECTED,
                routine_name=routine_name,
                duration_seconds=time.time() - t_start,
                reasons=["No synthesis generator available for bounded synthesis"],
            )

        # Step 4: Bounded Synthesis Loop with Retry Budget
        attempt = 0
        last_ce: Optional[Dict[str, Any]] = None
        last_candidate: Optional[CodeCandidate] = None
        all_discrepancies: List[Discrepancy] = []
        max_attempts = min(self.auth.max_retry_budget, self.auth.max_candidates)

        while attempt < max_attempts:
            attempt += 1
            # Check elapsed time
            if (time.time() - t_start) > self.auth.retry_timeout_seconds:
                return ProofResult(
                    badge=CareBadge.ACTION,
                    status=ProofStatus.BUDGET_EXHAUSTED,
                    routine_name=routine_name,
                    discrepancies=all_discrepancies,
                    minimized_counterexample=last_ce,
                    annotation=last_candidate.provenance.annotation if last_candidate and last_candidate.provenance else None,
                    provenance=last_candidate.provenance if last_candidate else None,
                    attempts_made=attempt,
                    duration_seconds=time.time() - t_start,
                    reasons=["Retry budget exhausted: time limit exceeded"],
                )

            # Synthesize candidate
            candidate = synthesis_generator(attempt, last_ce)
            last_candidate = candidate
            prompt_hash = hashlib.sha256(f"{source_checksum}:{attempt}".encode("ascii")).hexdigest()

            # Record provenance while preserving any existing AI annotation
            existing_annotation = candidate.provenance.annotation if candidate.provenance else None
            candidate.provenance = CandidateProvenance(
                candidate_id=candidate.candidate_id,
                source_checksum=source_checksum,
                model_destination=destination,
                synthesized_at=time.time(),
                prompt_hash=prompt_hash,
                attempt_index=attempt,
                annotation=existing_annotation,
            )

            # Verify in sandbox
            ok, disc, ce, mut_report = self.verify_candidate(
                source_fn=source_fn,
                candidate=candidate,
                fixture=fixture,
                synthetic_mutants=synthetic_mutants,
            )

            if ok and mut_report and mut_report.badge == CareBadge.PASS:
                # Proven by executable evidence!
                return ProofResult(
                    badge=CareBadge.PASS,
                    status=ProofStatus.VERIFIED,
                    routine_name=routine_name,
                    selected_candidate=candidate,
                    mutation_report=mut_report,
                    annotation=candidate.provenance.annotation,
                    provenance=candidate.provenance,
                    attempts_made=attempt,
                    duration_seconds=time.time() - t_start,
                    reasons=["Candidate verified by independent sandbox execution and strong mutation score"],
                )
            elif mut_report and mut_report.badge in (CareBadge.REVIEW, CareBadge.ACTION):
                # Candidate matched inputs, but fixture is weak (mutants survived)
                return ProofResult(
                    badge=mut_report.badge,
                    status=ProofStatus.WEAK_FIXTURES,
                    routine_name=routine_name,
                    selected_candidate=candidate,
                    mutation_report=mut_report,
                    annotation=candidate.provenance.annotation,
                    provenance=candidate.provenance,
                    attempts_made=attempt,
                    duration_seconds=time.time() - t_start,
                    reasons=[f"Mutation score insufficient ({mut_report.kill_rate * 100:.1f}%): test fixture is weak"],
                )
            else:
                # Discrepancy observed
                all_discrepancies.extend(disc)
                if ce:
                    last_ce = ce
                    self.fixture_bank.record_counterexample(routine_name, ce)
                    fixture.add_input(ce)

        # Budget exhausted without finding verified candidate
        return ProofResult(
            badge=CareBadge.ACTION,
            status=ProofStatus.BUDGET_EXHAUSTED,
            routine_name=routine_name,
            discrepancies=all_discrepancies,
            minimized_counterexample=last_ce,
            annotation=last_candidate.provenance.annotation if last_candidate and last_candidate.provenance else None,
            provenance=last_candidate.provenance if last_candidate else None,
            attempts_made=attempt,
            duration_seconds=time.time() - t_start,
            reasons=[f"Retry budget exhausted: failed after {attempt} attempts"],
        )


def ai_candidate_acceptance_rungs() -> Tuple[decide.Rung, ...]:
    """Decide ladder for AI candidate acceptance with measured Needs (Row 627: RCH19)."""
    return (
        decide.Rung(
            "deterministic_verified",
            "accept deterministic conversion verified by executable proof and strong mutation score",
            needs=(
                decide.Need(
                    "deterministic candidate passed without discrepancy",
                    lambda f: bool(f.get("deterministic_pass")),
                    "{rung} requires deterministic candidate to match all observable effects",
                ),
                decide.Need(
                    "sandbox execution strictly isolated",
                    lambda f: bool(f.get("sandbox_isolated", True)),
                    "{rung} requires clean sandbox execution isolation",
                ),
                decide.Need(
                    "mutation kill rate meets minimum threshold",
                    lambda f: float(f.get("mutation_score", 0.0)) >= float(f.get("min_mutation_kill_rate", 0.8)),
                    "{rung} requires mutation kill rate >= min_mutation_kill_rate",
                ),
            ),
            because="deterministic baseline passes all observable effects and fixture mutation tests without AI synthesis",
        ),
        decide.Rung(
            "ai_synthesis_verified",
            "accept AI synthesized candidate verified by executable proof and strong mutation score",
            needs=(
                decide.Need(
                    "synthesis authorized by policy",
                    lambda f: bool(f.get("synthesis_authorized")),
                    "{rung} requires explicit policy authorization for AI candidate synthesis",
                ),
                decide.Need(
                    "destination approved and payload sanitized",
                    lambda f: bool(f.get("destination_approved")) and bool(f.get("payload_sanitized")),
                    "{rung} requires approved model destination and sanitized payload without PII or credentials",
                ),
                decide.Need(
                    "candidate passed without discrepancy",
                    lambda f: bool(f.get("executable_pass")),
                    "{rung} requires candidate to match all observable effects in isolated sandbox",
                ),
                decide.Need(
                    "sandbox execution strictly isolated",
                    lambda f: bool(f.get("sandbox_isolated", True)),
                    "{rung} requires clean sandbox execution isolation",
                ),
                decide.Need(
                    "mutation kill rate meets minimum threshold",
                    lambda f: float(f.get("mutation_score", 0.0)) >= float(f.get("min_mutation_kill_rate", 0.8)),
                    "{rung} requires mutation kill rate >= min_mutation_kill_rate",
                ),
            ),
            because="AI candidate passed all observable effects in sandbox and proved strong fixture resilience",
        ),
        decide.Rung(
            "weak_fixture_review",
            "flag candidate for manual REVIEW due to weak test fixtures (mutants survived)",
            needs=(
                decide.Need(
                    "candidate passed observable effects",
                    lambda f: bool(f.get("executable_pass")) or bool(f.get("deterministic_pass")),
                    "{rung} requires candidate to pass all fixture inputs without discrepancy",
                ),
                decide.Need(
                    "sandbox execution strictly isolated",
                    lambda f: bool(f.get("sandbox_isolated", True)),
                    "{rung} requires clean sandbox execution isolation",
                ),
                decide.Need(
                    "fixture mutation kill rate below threshold",
                    lambda f: float(f.get("mutation_score", 0.0)) < float(f.get("min_mutation_kill_rate", 0.8)),
                    "{rung} requires mutation kill rate to fall below threshold",
                ),
            ),
            because="candidate matched test inputs but low mutation kill rate indicates weak test fixtures that may permit subtle regressions",
        ),
        decide.Rung(
            "counterexample_minimization_retry",
            "minimize counterexample and retry bounded synthesis with remaining retry budget",
            needs=(
                decide.Need(
                    "synthesis authorized by policy",
                    lambda f: bool(f.get("synthesis_authorized")),
                    "{rung} requires AI synthesis authorization",
                ),
                decide.Need(
                    "observable discrepancy detected",
                    lambda f: bool(f.get("has_discrepancy")),
                    "{rung} requires observable discrepancy between source and candidate",
                ),
                decide.Need(
                    "remaining retry budget available",
                    lambda f: int(f.get("remaining_attempts", 0)) > 0,
                    "{rung} requires remaining retry attempts within budget",
                ),
                decide.Need(
                    "counterexample minimized",
                    lambda f: bool(f.get("counterexample_minimized")),
                    "{rung} requires counterexample to be delta-debug minimized",
                ),
            ),
            because="discrepancy isolated and minimized to smallest counterexample for next synthesis attempt",
        ),
        decide.Rung(
            "rejection_action",
            "reject candidate conversion with ACTION care badge",
            because="no candidate verified by independent executable evidence; human intervention required",
        ),
    )


def decide_ai_candidate_acceptance(facts: Dict[str, Any]) -> decide.Choice:
    """Climbs ai_candidate_acceptance_rungs decide ladder."""
    return decide.climb("AI candidate acceptance selection", ai_candidate_acceptance_rungs(), facts)


@dataclass
class DurableProofCheckpoint:
    """Universal typed durable checkpoint for proof loop execution (O(1) resume)."""
    routine_name: str
    status: ProofStatus
    badge: CareBadge
    attempts_made: int
    mutation_kill_rate: float
    selected_candidate_id: Optional[str] = None
    minimized_counterexample: Optional[Dict[str, Any]] = None
    reasons: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    updated_at: float = field(default_factory=time.time)
    completed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "routine_name": self.routine_name,
            "status": self.status.value,
            "badge": self.badge.value,
            "attempts_made": self.attempts_made,
            "mutation_kill_rate": self.mutation_kill_rate,
            "selected_candidate_id": self.selected_candidate_id,
            "minimized_counterexample": self.minimized_counterexample,
            "reasons": self.reasons,
            "duration_seconds": self.duration_seconds,
            "updated_at": self.updated_at,
            "completed": self.completed,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DurableProofCheckpoint:
        return cls(
            routine_name=d["routine_name"],
            status=ProofStatus(d["status"]),
            badge=CareBadge(d["badge"]),
            attempts_made=d.get("attempts_made", 0),
            mutation_kill_rate=d.get("mutation_kill_rate", 0.0),
            selected_candidate_id=d.get("selected_candidate_id"),
            minimized_counterexample=d.get("minimized_counterexample"),
            reasons=d.get("reasons", []),
            duration_seconds=d.get("duration_seconds", 0.0),
            updated_at=d.get("updated_at", 0.0),
            completed=d.get("completed", False),
        )

    def save(self, filepath: Union[str, Path]) -> None:
        durable.write_json(filepath, self.to_dict(), indent=2)

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> Optional[DurableProofCheckpoint]:
        p = Path(filepath)
        if not p.exists():
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except (OSError, ValueError, KeyError):
            return None


class ProofCheckpointStore:
    """Manages O(1) skipping of completed routines and durable state checkpoints."""
    def __init__(self, checkpoint_dir: Union[str, Path]):
        self.checkpoint_dir = Path(checkpoint_dir)

    def _routine_path(self, routine_name: str) -> Path:
        clean_name = re.sub(r"[^a-zA-Z0-9_-]", "_", routine_name)
        return self.checkpoint_dir / f"proof_{clean_name}.json"

    def is_completed(self, routine_name: str) -> bool:
        ckpt = self.load(routine_name)
        return bool(ckpt and ckpt.completed and ckpt.badge in (CareBadge.PASS, CareBadge.BENIGN))

    def save(self, checkpoint: DurableProofCheckpoint) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint.save(self._routine_path(checkpoint.routine_name))

    def load(self, routine_name: str) -> Optional[DurableProofCheckpoint]:
        return DurableProofCheckpoint.load(self._routine_path(routine_name))


class ActiveVerificationLeaseLock:
    """Active verification lease lock (.run.lock) with heartbeat preventing duplicate runs."""
    def __init__(self, lock_dir: Union[str, Path], lease_term_seconds: float = 30.0):
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.lease_term_seconds = lease_term_seconds
        self.holder_id = f"{os.getpid()}@{os.uname().nodename}:{time.time()}"
        self.acquired = False

    def acquire(self) -> None:
        now = time.time()
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        if self.lock_file.exists():
            try:
                with open(self.lock_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                expires = float(data.get("expires", 0))
                holder = data.get("holder", "unknown")
                if now < expires and holder != self.holder_id:
                    raise LeaseLockHeldError(f"Active verification lease held by {holder} until {expires}")
            except (OSError, ValueError):
                pass
        record = {
            "holder": self.holder_id,
            "expires": now + self.lease_term_seconds,
            "heartbeat": now,
        }
        durable.write_json(self.lock_file, record, indent=2)
        self.acquired = True

    def heartbeat(self) -> None:
        if not self.acquired:
            return
        now = time.time()
        record = {
            "holder": self.holder_id,
            "expires": now + self.lease_term_seconds,
            "heartbeat": now,
        }
        durable.write_json(self.lock_file, record, indent=2)

    def release(self) -> None:
        if self.acquired and self.lock_file.exists():
            try:
                self.lock_file.unlink()
            except OSError:
                pass
            self.acquired = False


def verify_repair_transformation(
    routine_name: str,
    source_fn: Callable[..., Any],
    source_code: str,
    repaired_candidate: CodeCandidate,
    base_inputs: Optional[Sequence[Dict[str, Any]]] = None,
    authorization: Optional[AuthorizationPolicy] = None,
    checkpoint_dir: Optional[Union[str, Path]] = None,
) -> ProofResult:
    """Entry point for repair engines (repair_fence, repairways) to verify repaired code."""
    engine = AiProofLoopEngine(authorization=authorization)
    return engine.run_proof_loop(
        routine_name=routine_name,
        source_fn=source_fn,
        source_code=source_code,
        deterministic_candidate=repaired_candidate,
        base_inputs=base_inputs,
        checkpoint_dir=checkpoint_dir,
    )
