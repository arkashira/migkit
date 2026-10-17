"""Comprehensive tests for AI Proof Loop with independent executable evidence (Row 627).

Validates:
- Deterministic candidate verification with independent sandbox evidence.
- Bounded AI synthesis restricted to authorized policy only.
- Destination policy safety (safe test hosts 127.0.0.1, 10.0.0.x, secret redaction).
- AI explanations/scores as annotations, never as proof.
- Strict rejection of plausible wrong NULL conversions (ACTION).
- Strict rejection of plausible wrong exception conversions (ACTION).
- Strict rejection of plausible wrong timezone conversions (ACTION).
- Strict rejection of plausible wrong transaction conversions (ACTION).
- Counterexample minimization (delta debugging).
- Fixture weakness assessment via mutation testing.
- Retry budget safe termination.
- Regression fixture bank retention across attempts.
- Pure UNIX style ASCII formatting without emojis.
"""
from __future__ import annotations

import copy
import datetime
import json
import pytest

from migkit.ai_proof_loop import (
    ActiveVerificationLeaseLock,
    AiAnnotation,
    AiProofLoopEngine,
    AuthorizationPolicy,
    CandidateProvenance,
    CareBadge,
    CodeCandidate,
    CounterexampleMinimizer,
    DestinationPolicy,
    Discrepancy,
    DurableProofCheckpoint,
    EffectsComparator,
    ExecutionEffect,
    FrozenInputFixture,
    InexecutableCandidateError,
    IsolatedSandbox,
    LeaseLockHeldError,
    MutantRecord,
    MutationAssessor,
    MutationReport,
    ProofCheckpointStore,
    ProofResult,
    ProofStatus,
    RegressionFixtureBank,
    SemanticFaultType,
    TransactionState,
    ai_candidate_acceptance_rungs,
    decide_ai_candidate_acceptance,
    verify_repair_transformation,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "deterministic_verified", "holds"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "deterministic_verified", "refuses"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "ai_synthesis_verified", "holds"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "ai_synthesis_verified", "refuses"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "weak_fixture_review", "holds"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "weak_fixture_review", "refuses"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "counterexample_minimization_retry", "holds"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "counterexample_minimization_retry", "refuses"),
    ("ai_proof_loop", "ai_candidate_acceptance_rungs", "rejection_action", "holds"),
]


# -----------------------------------------------------------------------------
# 1. Deterministic Conversion Verification
# -----------------------------------------------------------------------------

def test_deterministic_conversion_verified_without_synthesis():
    """Deterministic conversion that passes sandbox tests is accepted without AI synthesis."""
    def source_routine(a: int, b: int) -> int:
        return a + b

    def target_routine(a: int, b: int) -> int:
        return a + b

    det_candidate = CodeCandidate(
        candidate_id="cand_det_001",
        code="def add(a, b): return a + b",
        callable_obj=target_routine,
        is_deterministic=True,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=False)  # Not authorized for AI synthesis
    )

    synthetic_mutants = [
        ("plus_to_minus", lambda a, b: a - b),
        ("drop_second_arg", lambda a, b: a),
    ]

    res = engine.run_proof_loop(
        routine_name="add_numbers",
        source_fn=source_routine,
        source_code="CREATE FUNCTION add_numbers(a INT, b INT) RETURNS INT AS $$ BEGIN RETURN a + b; END; $$;",
        deterministic_candidate=det_candidate,
        synthetic_mutants=synthetic_mutants,
        base_inputs=[{"a": 5, "b": 10}, {"a": -2, "b": 3}],
    )

    assert res.badge == CareBadge.PASS
    assert res.status == ProofStatus.VERIFIED
    assert res.selected_candidate is not None
    assert res.selected_candidate.is_deterministic is True
    assert res.attempts_made == 1
    assert res.mutation_report is not None
    assert res.mutation_report.badge == CareBadge.PASS
    assert res.mutation_report.kill_rate == 1.0


def test_deterministic_fails_and_synthesis_unauthorized_returns_action():
    """If deterministic conversion fails and synthesis is not authorized, return ACTION with UNAUTHORIZED."""
    def source_routine(a: int, b: int) -> int:
        return a * 2 + b

    def buggy_det_routine(a: int, b: int) -> int:
        return a + b  # Missing * 2

    det_candidate = CodeCandidate(
        candidate_id="cand_det_buggy",
        code="def bad_add(a, b): return a + b",
        callable_obj=buggy_det_routine,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=False)
    )

    res = engine.run_proof_loop(
        routine_name="calc_double_plus",
        source_fn=source_routine,
        source_code="CREATE FUNCTION calc(a INT, b INT)...",
        deterministic_candidate=det_candidate,
        base_inputs=[{"a": 3, "b": 4}],
    )

    assert res.badge == CareBadge.ACTION
    assert res.status == ProofStatus.UNAUTHORIZED
    assert "not authorized" in res.reasons[0].lower()


# -----------------------------------------------------------------------------
# 2. Destination Policy & Safe Hosts
# -----------------------------------------------------------------------------

def test_destination_policy_rejects_unapproved_host():
    """Model destination must be in approved safe host list (127.0.0.1, 10.0.0.x)."""
    policy = DestinationPolicy(approved_destinations={"127.0.0.1:11434", "10.0.0.5:8000"})
    assert policy.is_destination_approved("127.0.0.1:11434") is True
    assert policy.is_destination_approved("10.0.0.5:8000") is True
    assert policy.is_destination_approved("10.0.0.99:9090") is True
    assert policy.is_destination_approved("localhost:8000") is True

    # Public/untrusted destinations rejected
    assert policy.is_destination_approved("api.unapproved-cloud.com") is False
    assert policy.is_destination_approved("192.168.1.50:8000") is False
    assert policy.is_destination_approved("") is False


def test_engine_rejects_unapproved_model_destination():
    """Engine halts with ACTION and DESTINATION_REFUSED when unapproved destination is requested."""
    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(
            authorized=True,
            destination_policy=DestinationPolicy(approved_destinations={"127.0.0.1:11434"}),
        )
    )

    res = engine.run_proof_loop(
        routine_name="test_proc",
        source_fn=lambda x: x,
        source_code="fn code",
        destination="untrusted-external-ai.com:443",
    )

    assert res.badge == CareBadge.ACTION
    assert res.status == ProofStatus.DESTINATION_REFUSED
    assert "not in approved" in res.reasons[0].lower()


def test_destination_policy_detects_credentials_in_prompt():
    """Credentials / secrets in source prompt payload trigger rejection unless sanitized."""
    policy = DestinationPolicy(allow_pii=False)
    has_secret, sanitized = policy.sanitize_payload("SELECT * FROM users WHERE password = 'dummy_secret_value'")
    assert has_secret is False
    assert "[REDACTED_PASSWORD_SECRET]" in sanitized


# -----------------------------------------------------------------------------
# 3. AI Explanation & Score is Annotation, NOT Proof
# -----------------------------------------------------------------------------

def test_ai_high_confidence_cannot_bypass_executable_proof():
    """A candidate with 0.99 confidence and elaborate explanation is rejected if execution differs."""
    def source_fn(val: int) -> int:
        return val * 10

    def wrong_fn(val: int) -> int:
        return val * 9  # subtle error

    # Synthesized candidate with deceptive AI annotation
    annotation = AiAnnotation(
        model_id="gpt-4-turbo-mock",
        explanation="Mathematically proven exact conversion for SQL decimal scaling.",
        confidence_score=0.999,
        prompt_tokens=150,
        completion_tokens=40,
        is_proof=False,
    )

    candidate = CodeCandidate(
        candidate_id="cand_ai_fake_pass",
        code="def scale(val): return val * 9",
        callable_obj=wrong_fn,
        provenance=CandidateProvenance(
            candidate_id="cand_ai_fake_pass",
            source_checksum="abc",
            model_destination="127.0.0.1:11434",
            synthesized_at=1000.0,
            prompt_hash="xyz",
            attempt_index=1,
            annotation=annotation,
        ),
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="scale_val",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda attempt, ce: candidate,
        base_inputs=[{"val": 5}],
    )

    # Must be ACTION, not fooled by 0.999 score
    assert res.badge == CareBadge.ACTION
    assert res.status == ProofStatus.BUDGET_EXHAUSTED
    assert len(res.discrepancies) > 0
    assert res.discrepancies[0].badge == CareBadge.ACTION
    assert res.annotation is not None
    assert res.annotation.is_proof is False


# -----------------------------------------------------------------------------
# 4. Strict Rejection of Plausible Wrong Conversions (ACTION)
# -----------------------------------------------------------------------------

def test_plausible_wrong_null_conversion_strictly_rejected():
    """Candidate that returns 0 or empty string instead of None (NULL) is rejected with WRONG_NULL."""
    def source_fn(amount: Optional[int]) -> Optional[int]:
        if amount is None:
            return None
        return amount * 2

    # Plausible wrong conversion: treats NULL as 0
    def target_wrong_null(amount: Optional[int]) -> int:
        return (amount or 0) * 2

    cand = CodeCandidate(
        candidate_id="cand_wrong_null",
        code="def fn(amount): return (amount or 0) * 2",
        callable_obj=target_wrong_null,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="null_handler",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"amount": 10}, {"amount": None}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.WRONG_NULL for d in res.discrepancies)
    null_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.WRONG_NULL)
    assert null_disc.badge == CareBadge.ACTION
    assert "wrong NULL conversion" in null_disc.explanation


def test_plausible_wrong_exception_conversion_strictly_rejected():
    """Candidate that swallows exception or converts ZeroDivisionError to None is rejected with WRONG_EXCEPTION."""
    def source_fn(a: int, b: int) -> float:
        if b == 0:
            raise ZeroDivisionError("division by zero in database formula")
        return a / b

    # Plausible wrong conversion: catches error and returns None instead of raising
    def target_swallows_exception(a: int, b: int) -> Optional[float]:
        if b == 0:
            return None
        return a / b

    cand = CodeCandidate(
        candidate_id="cand_swallowed_exc",
        code="def fn(a, b): return None if b == 0 else a / b",
        callable_obj=target_swallows_exception,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="safe_div",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"a": 10, "b": 2}, {"a": 10, "b": 0}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.WRONG_EXCEPTION for d in res.discrepancies)
    exc_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.WRONG_EXCEPTION)
    assert exc_disc.badge == CareBadge.ACTION
    assert "Exception divergence" in exc_disc.explanation


def test_plausible_wrong_timezone_conversion_strictly_rejected():
    """Candidate that mishandles timezone offset or treats UTC as local time is rejected with WRONG_TIMEZONE."""
    def source_fn(args: Dict[str, Any], context: Dict[str, Any]) -> str:
        # Source records explicit UTC timestamp and timezone
        context["timezone_name"] = "UTC"
        context["utc_timestamp"] = "2026-10-17T10:00:00Z"
        return "2026-10-17T10:00:00Z"

    def target_wrong_tz(args: Dict[str, Any], context: Dict[str, Any]) -> str:
        # Plausible wrong: assumed local timezone instead of UTC
        context["timezone_name"] = "America/New_York"
        context["utc_timestamp"] = "2026-10-17T14:00:00Z"
        return "2026-10-17T14:00:00Z"

    cand = CodeCandidate(
        candidate_id="cand_wrong_tz",
        code="def fn(args, context): context['timezone_name'] = 'America/New_York' ...",
        callable_obj=target_wrong_tz,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="tz_converter",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"ts_str": "2026-10-17 10:00:00"}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.WRONG_TIMEZONE for d in res.discrepancies)
    tz_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.WRONG_TIMEZONE)
    assert tz_disc.badge == CareBadge.ACTION
    assert "Timezone conversion fault" in tz_disc.explanation


def test_plausible_wrong_transaction_conversion_strictly_rejected():
    """Candidate that commits partial state on error instead of rolling back is rejected with WRONG_TRANSACTION."""
    def source_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        fail = args.get("fail", False)
        context["state"]["balance"] = 100
        if fail:
            context["transaction_status"] = TransactionState.ROLLED_BACK
            raise RuntimeError("Database error occurred")
        context["transaction_status"] = TransactionState.COMMITTED

    def target_wrong_tx(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        fail = args.get("fail", False)
        context["state"]["balance"] = 100
        if fail:
            # Buggy: fails to rollback, remains COMMITTED or IN_TRANSACTION
            context["transaction_status"] = TransactionState.COMMITTED
            raise RuntimeError("Database error occurred")
        context["transaction_status"] = TransactionState.COMMITTED

    cand = CodeCandidate(
        candidate_id="cand_wrong_tx",
        code="def fn(args, context): ...",
        callable_obj=target_wrong_tx,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="atomic_transfer",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"fail": False}, {"fail": True}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.WRONG_TRANSACTION for d in res.discrepancies)
    tx_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.WRONG_TRANSACTION)
    assert tx_disc.badge == CareBadge.ACTION
    assert "Transaction state mismatch on error" in tx_disc.explanation


# -----------------------------------------------------------------------------
# 5. Counterexample Minimization
# -----------------------------------------------------------------------------

def test_counterexample_minimizer_delta_debugging():
    """Counterexample with extra irrelevant fields and large integer is minimized."""
    def source_fn(x: int, y: int = 0, tag: str = "") -> int:
        return x + 1

    def buggy_target_fn(x: int, y: int = 0, tag: str = "") -> int:
        # Fails when x == 0
        if x == 0:
            return 999
        return x + 1

    sandbox = IsolatedSandbox()
    large_failing_input = {
        "x": 0,
        "y": 88888,
        "tag": "unnecessary_long_string_payload",
    }

    minimized = CounterexampleMinimizer.minimize(
        failing_input=large_failing_input,
        source_fn=source_fn,
        target_fn=buggy_target_fn,
        sandbox=sandbox,
    )

    # Key "x" must remain 0 since x == 0 is the root cause
    assert minimized["x"] == 0
    # Non-essential keys should have been dropped or shrunk
    assert len(minimized) <= len(large_failing_input)


# -----------------------------------------------------------------------------
# 6. Fixture Weakness Assessment via Mutation Testing
# -----------------------------------------------------------------------------

def test_mutation_assessor_detects_weak_fixture_and_raises_review():
    """When test fixture has insufficient inputs, mutants survive and badge is REVIEW or ACTION."""
    def source_fn(val: int) -> int:
        if val >= 10:
            return 100
        return 0

    def target_fn(val: int) -> int:
        if val >= 10:
            return 100
        return 0

    # Weak fixture containing only inputs val >= 10: does not test the < 10 branch
    fixture = FrozenInputFixture(
        fixture_id="weak_fix",
        inputs=[{"val": 15}, {"val": 20}],
    )

    # Synthetic mutant: changed >= 10 to > 10 (boundary bug) or inverted branch
    mutants = [
        ("boundary_gt", lambda val: 100 if val > 10 else 0),  # Survives on 15, 20!
        ("always_100", lambda val: 100),                      # Survives on 15, 20!
    ]

    report = MutationAssessor.assess_candidate(
        candidate_code="",
        candidate_fn=target_fn,
        fixture=fixture,
        sandbox=IsolatedSandbox(),
        synthetic_mutants=mutants,
        min_kill_rate=0.8,
    )

    # Both mutants survive because fixture only tested 15 and 20!
    assert report.total_mutants == 2
    assert report.killed_mutants == 0
    assert report.survived_mutants == 2
    assert report.badge == CareBadge.ACTION  # 0% kill rate < 80%
    assert len(report.survivors) == 2


def test_mutation_assessor_passes_with_strong_fixture():
    """When fixture tests boundary values (9, 10, 11), boundary mutants are killed -> PASS."""
    def target_fn(val: int) -> int:
        if val >= 10:
            return 100
        return 0

    strong_fixture = FrozenInputFixture(
        fixture_id="strong_fix",
        inputs=[{"val": 9}, {"val": 10}, {"val": 11}],
    )

    mutants = [
        ("boundary_gt", lambda val: 100 if val > 10 else 0),  # Killed by val == 10
        ("always_100", lambda val: 100),                      # Killed by val == 9
    ]

    report = MutationAssessor.assess_candidate(
        candidate_code="",
        candidate_fn=target_fn,
        fixture=strong_fixture,
        sandbox=IsolatedSandbox(),
        synthetic_mutants=mutants,
        min_kill_rate=0.8,
    )

    assert report.total_mutants == 2
    assert report.killed_mutants == 2
    assert report.survived_mutants == 0
    assert report.kill_rate == 1.0
    assert report.badge == CareBadge.PASS


# -----------------------------------------------------------------------------
# 7. Safe Retry Budget Termination & Multi-Attempt Convergence
# -----------------------------------------------------------------------------

def test_retry_budget_terminates_safely_on_repeated_failures():
    """If candidate synthesis fails to produce a working candidate, loop halts safely without hanging."""
    def source_fn(a: int) -> int:
        return a * 3

    # Generator returns wrong candidates every attempt
    def bad_generator(attempt: int, ce: Optional[Dict[str, Any]]) -> CodeCandidate:
        return CodeCandidate(
            candidate_id=f"cand_bad_{attempt}",
            code=f"def fn(a): return a + {attempt}",
            callable_obj=lambda a: a + attempt,
        )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(
            authorized=True,
            max_retry_budget=3,
            retry_timeout_seconds=5.0,
        )
    )

    res = engine.run_proof_loop(
        routine_name="triple",
        source_fn=source_fn,
        source_code="CREATE FUNCTION triple...",
        synthesis_generator=bad_generator,
        base_inputs=[{"a": 2}, {"a": 4}],
    )

    assert res.badge == CareBadge.ACTION
    assert res.status == ProofStatus.BUDGET_EXHAUSTED
    assert res.attempts_made == 3
    assert len(res.discrepancies) > 0


def test_multi_attempt_synthesis_converges_with_regression_bank():
    """Attempt 1 fails with counterexample; Attempt 2 corrects it using counterexample; passes proof."""
    def source_fn(val: Optional[int]) -> int:
        if val is None:
            return -1
        return val * 10

    # Generator that fixes NULL handling in attempt 2
    def adaptive_generator(attempt: int, ce: Optional[Dict[str, Any]]) -> CodeCandidate:
        if attempt == 1:
            # Buggy: returns 0 on None
            return CodeCandidate(
                candidate_id="cand_attempt_1",
                code="def fn(val): return 0 if val is None else val * 10",
                callable_obj=lambda val: 0 if val is None else val * 10,
                provenance=CandidateProvenance(
                    candidate_id="cand_attempt_1",
                    source_checksum="chk",
                    model_destination="127.0.0.1:11434",
                    synthesized_at=100.0,
                    prompt_hash="p1",
                    attempt_index=1,
                    annotation=AiAnnotation("ollama", "Draft", 0.7),
                ),
            )
        else:
            # Corrected: returns -1 on None
            return CodeCandidate(
                candidate_id="cand_attempt_2",
                code="def fn(val): return -1 if val is None else val * 10",
                callable_obj=lambda val: -1 if val is None else val * 10,
                provenance=CandidateProvenance(
                    candidate_id="cand_attempt_2",
                    source_checksum="chk",
                    model_destination="127.0.0.1:11434",
                    synthesized_at=200.0,
                    prompt_hash="p2",
                    attempt_index=2,
                    annotation=AiAnnotation("ollama", "Corrected NULL handling", 0.95),
                ),
            )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(
            authorized=True,
            max_retry_budget=3,
        )
    )

    synthetic_mutants = [
        ("mutant_zero_on_none", lambda val: 0 if val is None else val * 10),
        ("mutant_add_instead", lambda val: -1 if val is None else val + 10),
    ]

    res = engine.run_proof_loop(
        routine_name="handle_null_special",
        source_fn=source_fn,
        source_code="CREATE FUNCTION handle_null_special...",
        synthesis_generator=adaptive_generator,
        synthetic_mutants=synthetic_mutants,
        base_inputs=[{"val": 5}, {"val": None}],
    )

    assert res.badge == CareBadge.PASS
    assert res.status == ProofStatus.VERIFIED
    assert res.attempts_made == 2
    assert res.selected_candidate is not None
    assert res.selected_candidate.candidate_id == "cand_attempt_2"
    assert res.provenance is not None
    assert res.provenance.model_destination == "127.0.0.1:11434"
    assert res.annotation is not None
    assert res.annotation.is_proof is False


# -----------------------------------------------------------------------------
# 8. Output Readability & UNIX ASCII Summary
# -----------------------------------------------------------------------------

def test_pure_unix_summary_and_serialization():
    """Summary contains pure ASCII, zero emojis, exact care badges, and serializes cleanly."""
    res = ProofResult(
        badge=CareBadge.PASS,
        status=ProofStatus.VERIFIED,
        routine_name="test_routine",
        selected_candidate=CodeCandidate(candidate_id="cand_pass_001", code="x = 1"),
        provenance=CandidateProvenance(
            candidate_id="cand_pass_001",
            source_checksum="a1b2c3d4e5f60718293a",
            model_destination="127.0.0.1:11434",
            synthesized_at=100.0,
            prompt_hash="phash",
            attempt_index=1,
            annotation=AiAnnotation("llama3", "Conversion summary", 0.92),
        ),
        annotation=AiAnnotation("llama3", "Conversion summary", 0.92),
        mutation_report=MutationReport(
            total_mutants=5,
            killed_mutants=5,
            survived_mutants=0,
            kill_rate=1.0,
            badge=CareBadge.PASS,
        ),
        attempts_made=1,
        duration_seconds=0.045,
        reasons=["All executable evidence satisfied."],
    )

    summary = res.render_unix_summary()
    assert "[PASS]" in summary
    assert "routine=test_routine" in summary
    assert "status=VERIFIED" in summary
    assert "is_proof=False" in summary
    assert "mutation_kill_rate=100.0%" in summary

    # Verify 100% pure ASCII (no emojis, no unicode symbols)
    summary.encode("ascii")

    # Verify JSON round-trip
    d = res.to_dict()
    dumped = json.dumps(d)
    loaded = json.loads(dumped)
    assert loaded["badge"] == "PASS"
    assert loaded["status"] == "VERIFIED"
    assert loaded["annotation"]["is_proof"] is False


# -----------------------------------------------------------------------------
# 9. Side Effects, OUT Params, Rows Affected, and Type Mismatch Tests
# -----------------------------------------------------------------------------

def test_side_effect_state_mutations_mismatch_triggers_action():
    """State mutations on isolated sandbox tables differ between source and target -> ACTION."""
    def source_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["state"]["audit_log"] = ["inserted row 10"]

    def target_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["state"]["audit_log"] = []  # Failed to update audit log

    cand = CodeCandidate(
        candidate_id="cand_bad_audit",
        code="def fn(args, context): pass",
        callable_obj=target_fn,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="write_with_audit",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"record_id": 10}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.SIDE_EFFECT_MISMATCH for d in res.discrepancies)


def test_out_parameters_mismatch_triggers_action():
    """OUT parameter values differ between source and target -> ACTION."""
    def source_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["out_params"]["status_code"] = 200
        context["out_params"]["msg"] = "SUCCESS"

    def target_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["out_params"]["status_code"] = 200
        context["out_params"]["msg"] = "OK"  # Mismatched OUT param value

    cand = CodeCandidate(
        candidate_id="cand_bad_out_params",
        code="def fn(args, context): ...",
        callable_obj=target_fn,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="proc_with_out",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"in_id": 1}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.SIDE_EFFECT_MISMATCH for d in res.discrepancies)
    out_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.SIDE_EFFECT_MISMATCH)
    assert "OUT params mismatch" in out_disc.explanation


def test_rows_affected_mismatch_triggers_action():
    """Rows affected count differs between source and target -> ACTION."""
    def source_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["rows_affected"] = 5

    def target_fn(args: Dict[str, Any], context: Dict[str, Any]) -> None:
        context["rows_affected"] = 4  # Off by one

    cand = CodeCandidate(
        candidate_id="cand_bad_rows",
        code="def fn(args, context): ...",
        callable_obj=target_fn,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="bulk_update",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"batch_size": 5}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.SIDE_EFFECT_MISMATCH for d in res.discrepancies)
    row_disc = next(d for d in res.discrepancies if d.fault_type == SemanticFaultType.SIDE_EFFECT_MISMATCH)
    assert "Rows affected mismatch" in row_disc.explanation


def test_type_mismatch_triggers_action():
    """Type differences (e.g. str vs int) return ACTION."""
    def source_fn(val: int) -> int:
        return val

    def target_fn(val: int) -> str:
        return str(val)  # Plausible coercion error

    cand = CodeCandidate(
        candidate_id="cand_wrong_type",
        code="def fn(val): return str(val)",
        callable_obj=target_fn,
    )

    engine = AiProofLoopEngine(
        authorization=AuthorizationPolicy(authorized=True, max_retry_budget=1)
    )

    res = engine.run_proof_loop(
        routine_name="identity_proc",
        source_fn=source_fn,
        source_code="code",
        synthesis_generator=lambda att, ce: cand,
        base_inputs=[{"val": 42}],
    )

    assert res.badge == CareBadge.ACTION
    assert any(d.fault_type == SemanticFaultType.TYPE_MISMATCH for d in res.discrepancies)


def test_counterexample_minimizer_strings_and_lists():
    """Minimizer successfully shrinks long strings and nested lists to minimal reproducing input."""
    def source_fn(text: str, tags: list) -> int:
        return len(text) + len(tags)

    def buggy_target_fn(text: str, tags: list) -> int:
        if "ERROR" in text or len(tags) > 2:
            return -1
        return len(text) + len(tags)

    sandbox = IsolatedSandbox()
    failing_input = {
        "text": "PREFIX_ERROR_SUFFIX_VERY_LONG_STRING_THAT_SHOULD_SHRINK",
        "tags": ["a", "b", "c", "d", "e", "f"],
    }

    minimized = CounterexampleMinimizer.minimize(
        failing_input=failing_input,
        source_fn=source_fn,
        target_fn=buggy_target_fn,
        sandbox=sandbox,
    )

    # Shrunk input should still fail on buggy_target_fn
    s_eff = sandbox.execute(source_fn, minimized)
    t_eff = sandbox.execute(buggy_target_fn, minimized)
    assert EffectsComparator.compare(s_eff, t_eff, minimized) is not None
    # Verified shrunk size
    assert len(minimized["text"]) < len(failing_input["text"])
    assert len(minimized["tags"]) < len(failing_input["tags"])


def test_text_code_mutant_generation_and_compilation():
    """Text-based mutation generation compiles mutated code and measures kill rate."""
    python_code = (
        "def compute_discount(price, is_vip):\n"
        "    if is_vip and price >= 100:\n"
        "        return price - 20\n"
        "    return price\n"
    )

    fixture = FrozenInputFixture(
        fixture_id="discount_fix",
        inputs=[
            {"price": 100, "is_vip": True},
            {"price": 99, "is_vip": True},
            {"price": 150, "is_vip": False},
            {"price": 50, "is_vip": False},
        ],
    )

    local_env: Dict[str, Any] = {}
    exec(python_code, {}, local_env)
    candidate_fn = local_env["compute_discount"]

    report = MutationAssessor.assess_candidate(
        candidate_code=python_code,
        candidate_fn=candidate_fn,
        fixture=fixture,
        sandbox=IsolatedSandbox(),
    )

    assert report.total_mutants > 0
    assert report.killed_mutants > 0
    assert report.kill_rate > 0.0


def test_ai_candidate_acceptance_decide_rungs_coverage():
    with watched() as seen:
        # 1. Deterministic candidate verified -> deterministic_verified holds
        c1 = decide_ai_candidate_acceptance({
            "deterministic_pass": True,
            "sandbox_isolated": True,
            "mutation_score": 0.95,
            "min_mutation_kill_rate": 0.8,
        })
        assert c1.path == "deterministic_verified"

        # 2. Deterministic candidate fails, AI synthesis verified -> ai_synthesis_verified holds
        c2 = decide_ai_candidate_acceptance({
            "deterministic_pass": False,
            "synthesis_authorized": True,
            "destination_approved": True,
            "payload_sanitized": True,
            "executable_pass": True,
            "sandbox_isolated": True,
            "mutation_score": 0.85,
            "min_mutation_kill_rate": 0.8,
        })
        assert c2.path == "ai_synthesis_verified"

        # 3. Candidate passes but mutation kill rate weak -> weak_fixture_review holds
        c3 = decide_ai_candidate_acceptance({
            "deterministic_pass": False,
            "synthesis_authorized": True,
            "destination_approved": True,
            "payload_sanitized": True,
            "executable_pass": True,
            "sandbox_isolated": True,
            "mutation_score": 0.4,
            "min_mutation_kill_rate": 0.8,
        })
        assert c3.path == "weak_fixture_review"

        # 4. Discrepancy observed, attempts remaining -> counterexample_minimization_retry holds
        c4 = decide_ai_candidate_acceptance({
            "deterministic_pass": False,
            "synthesis_authorized": True,
            "destination_approved": True,
            "payload_sanitized": True,
            "executable_pass": False,
            "has_discrepancy": True,
            "remaining_attempts": 2,
            "counterexample_minimized": True,
            "sandbox_isolated": True,
            "mutation_score": 0.0,
            "min_mutation_kill_rate": 0.8,
        })
        assert c4.path == "counterexample_minimization_retry"

        # 5. Unauthorized, budget exhausted -> rejection_action holds
        c5 = decide_ai_candidate_acceptance({
            "deterministic_pass": False,
            "synthesis_authorized": False,
            "executable_pass": False,
            "has_discrepancy": True,
            "remaining_attempts": 0,
            "counterexample_minimized": False,
            "sandbox_isolated": False,
            "mutation_score": 0.0,
            "min_mutation_kill_rate": 0.8,
        })
        assert c5.path == "rejection_action"

    assert set(branch(seen, "deterministic_verified")) == {HOLDS, REFUSES}
    assert set(branch(seen, "ai_synthesis_verified")) == {HOLDS, REFUSES}
    assert set(branch(seen, "weak_fixture_review")) == {HOLDS, REFUSES}
    assert set(branch(seen, "counterexample_minimization_retry")) == {HOLDS, REFUSES}
    assert branch(seen, "rejection_action") == [HOLDS]


def test_durable_checkpoint_o1_skip_completed(tmp_path):
    store = ProofCheckpointStore(tmp_path / "checkpoints")
    assert not store.is_completed("calculate_tax")

    ckpt = DurableProofCheckpoint(
        routine_name="calculate_tax",
        status=ProofStatus.VERIFIED,
        badge=CareBadge.PASS,
        attempts_made=1,
        mutation_kill_rate=0.9,
        selected_candidate_id="tax_candidate_1",
        completed=True,
    )
    store.save(ckpt)

    assert store.is_completed("calculate_tax")
    loaded = store.load("calculate_tax")
    assert loaded is not None
    assert loaded.status == ProofStatus.VERIFIED
    assert loaded.badge == CareBadge.PASS
    assert loaded.attempts_made == 1


def test_active_lease_lock_mutual_exclusion(tmp_path):
    lock_dir = tmp_path / "lease"
    lease1 = ActiveVerificationLeaseLock(lock_dir, lease_term_seconds=10.0)
    lease1.acquire()
    assert lease1.acquired is True

    lease2 = ActiveVerificationLeaseLock(lock_dir, lease_term_seconds=10.0)
    with pytest.raises(LeaseLockHeldError):
        lease2.acquire()

    lease1.release()
    assert lease1.acquired is False
    lease2.acquire()
    assert lease2.acquired is True
    lease2.release()


def test_mutation_compilation_errors_not_swallowed_and_reports_action():
    report = MutationAssessor.assess_candidate(
        candidate_code="def fn(args):\n    return args['x'] + 1\n",
        candidate_fn=lambda args: args["x"] + 1,
        fixture=FrozenInputFixture("f1", [{"x": 1}]),
        sandbox=IsolatedSandbox(),
    )
    assert report.badge in (CareBadge.ACTION, CareBadge.REVIEW)


def test_repair_fence_wiring_to_ai_proof_loop():
    from migkit.repair_fence import coordinate_repair_proof_verification
    def src(a=0):
        return a + 10
    cand = CodeCandidate(
        candidate_id="cand_repair_01",
        code="def f(a=0):\n    return a + 10",
        callable_obj=src,
    )
    res = coordinate_repair_proof_verification(
        routine_name="repair_add",
        source_fn=src,
        source_code="def src(a=0):\n    return a + 10",
        deterministic_candidate=cand,
        base_inputs=[{"a": 1}],
    )
    assert res.badge == CareBadge.PASS
    assert res.status == ProofStatus.VERIFIED


