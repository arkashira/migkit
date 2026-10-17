"""Tests for selecting validation methods by what each can actually prove (Row 644: RCH36.verify-method).

Proves that:
1. All 7 validation methods (`count`, `aggregate`, `vector`, `hash`, `key_row`,
   `external_sort`, `custom_query`) have explicit proof strength, collision and
   duplicate assumptions, canonicalization contracts, and source-cost models.
2. Every `decide.Rung` in `verify_method.select_validation_method` is exercised
   on both `holds` and `refuses` under `tests.rung_branches.watched()`.
3. Fast signatures (`count`, `aggregate`, `vector`, `hash`) can rule in
   differences (`DIFFERENT`) or prioritize exact work on dirty buckets, but can
   never certify stronger claims than their contract (`SAME`).
4. Count-only unchanged totals can never produce a `SAME` verdict.
5. Perfect duplicates, equal-count value changes (including zero-sum cancelling
   swaps), floating-point rounding/clamping drift, and hash ambiguity
   (delimiter collision, NULL-vs-empty conflation, weak digest width, XOR
   duplicate cancellation) either fail (`DIFFERENT` / `ACTION`) or escalate
   (`ESCALATE_TO_EXACT` / `REVIEW`).
6. O(1) typed durable checkpointing preserves native types and refuses to treat
   a weak triage checkpoint as satisfying a stronger cutover proof claim.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import uuid
from pathlib import Path

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

from migkit.verify_method import (
     METHOD_SPECS,
    AdaptiveAimdBatcher,
    CanonicalizationModel,
    CareBadge,
    ClaimedGuarantee,
    ColdStartByteWeightedEta,
    CollisionDuplicateAssumptions,
    CompletedRangeProof,
    CustomQueryContract,
    FloatCanonicalPolicy,
    MethodCostEstimate,
    PathologyKind,
    ProofStrength,
    SourceCostModel,
    TableVerificationProfile,
    UnsafeEndpointHostError,
    ValidationExecutionReport,
    ValidationMethodKind,
    ValidationMethodSpec,
    ValidationVerdict,
    VerificationLeaseError,
    VerificationLeaseExpiredError,
    VerificationLeaseLock,
    VerifyMethodCheckpoint,
    canonicalize_cell,
    canonicalize_float_value,
    canonicalize_row,
    compute_bucket_vectors,
    decode_typed_value,
    digest_canonical_row,
    encode_typed_value,
    execute_validation_method,
    get_method_spec,
    is_safe_test_host,
    prioritize_exact_verification_by_vector,
    select_validation_method,
)

PROVES_RUNGS = [
    ("verify_method", "select_validation_method", "count", "holds"),
    ("verify_method", "select_validation_method", "count", "refuses"),
    ("verify_method", "select_validation_method", "aggregate", "holds"),
    ("verify_method", "select_validation_method", "aggregate", "refuses"),
    ("verify_method", "select_validation_method", "vector", "holds"),
    ("verify_method", "select_validation_method", "vector", "refuses"),
    ("verify_method", "select_validation_method", "hash", "holds"),
    ("verify_method", "select_validation_method", "hash", "refuses"),
    ("verify_method", "select_validation_method", "custom_query", "holds"),
    ("verify_method", "select_validation_method", "custom_query", "refuses"),
    ("verify_method", "select_validation_method", "key_row", "holds"),
    ("verify_method", "select_validation_method", "key_row", "refuses"),
    ("verify_method", "select_validation_method", "external_sort", "holds"),
    ("verify_method", "select_validation_method", "external_sort", "refuses"),
    ("verify_method", "select_validation_method", "escalate_unprovable", "holds"),
]


# -----------------------------------------------------------------------------
# 1. Explicit Proof Strength, Assumptions, Canonicalization, and Cost Models
# -----------------------------------------------------------------------------

def test_all_seven_validation_methods_have_explicit_contracts_and_cost_models():
    expected_methods = {
        ValidationMethodKind.COUNT,
        ValidationMethodKind.AGGREGATE,
        ValidationMethodKind.VECTOR,
        ValidationMethodKind.HASH,
        ValidationMethodKind.KEY_ROW,
        ValidationMethodKind.EXTERNAL_SORT,
        ValidationMethodKind.CUSTOM_QUERY,
    }
    assert set(METHOD_SPECS.keys()) == expected_methods

    profile = TableVerificationProfile(
        table_name="public.ledger_entries",
        estimated_rows=100_000,
        estimated_bytes=50 * 1024 * 1024,
        column_count=10,
        has_unique_key=True,
        bucket_count=64,
    )

    estimates = {}
    for kind in expected_methods:
        spec = get_method_spec(kind)
        assert isinstance(spec, ValidationMethodSpec)
        assert isinstance(spec.proof_strength, ProofStrength)
        assert isinstance(spec.collision_assumptions, CollisionDuplicateAssumptions)
        assert isinstance(spec.canonicalization, CanonicalizationModel)
        assert isinstance(spec.cost_model, SourceCostModel)
        est = spec.cost_model.estimate(kind, profile)
        assert isinstance(est, MethodCostEstimate)
        assert est.source_scan_bytes > 0
        assert est.estimated_wall_seconds > 0.0
        assert est.total_weighted_cost > 0.0
        estimates[kind] = est

    # Proof strength ordering: COUNT < AGGREGATE < VECTOR < HASH < KEY_ROW < EXTERNAL_SORT
    assert (
        METHOD_SPECS[ValidationMethodKind.COUNT].proof_strength.rank
        < METHOD_SPECS[ValidationMethodKind.AGGREGATE].proof_strength.rank
        < METHOD_SPECS[ValidationMethodKind.VECTOR].proof_strength.rank
        < METHOD_SPECS[ValidationMethodKind.HASH].proof_strength.rank
        < METHOD_SPECS[ValidationMethodKind.KEY_ROW].proof_strength.rank
        < METHOD_SPECS[ValidationMethodKind.EXTERNAL_SORT].proof_strength.rank
    )

    # Cost progression: COUNT is cheapest; EXTERNAL_SORT is most expensive due to spill + full scan
    assert estimates[ValidationMethodKind.COUNT].total_weighted_cost < estimates[ValidationMethodKind.AGGREGATE].total_weighted_cost
    assert estimates[ValidationMethodKind.AGGREGATE].total_weighted_cost < estimates[ValidationMethodKind.KEY_ROW].total_weighted_cost
    assert estimates[ValidationMethodKind.KEY_ROW].total_weighted_cost < estimates[ValidationMethodKind.EXTERNAL_SORT].total_weighted_cost

    # Fast signatures can never certify EXACT_ROW_EQUIVALENCE or MULTISET_EQUIVALENCE
    for fast_kind in (
        ValidationMethodKind.COUNT,
        ValidationMethodKind.AGGREGATE,
        ValidationMethodKind.VECTOR,
        ValidationMethodKind.HASH,
    ):
        spec = METHOD_SPECS[fast_kind]
        assert spec.proof_strength.is_fast_signature
        assert not spec.can_certify(ClaimedGuarantee.EXACT_ROW_EQUIVALENCE)
        assert not spec.can_certify(ClaimedGuarantee.MULTISET_EQUIVALENCE)

    assert METHOD_SPECS[ValidationMethodKind.KEY_ROW].can_certify(ClaimedGuarantee.EXACT_ROW_EQUIVALENCE)
    assert not METHOD_SPECS[ValidationMethodKind.KEY_ROW].can_certify(ClaimedGuarantee.MULTISET_EQUIVALENCE)
    assert METHOD_SPECS[ValidationMethodKind.EXTERNAL_SORT].can_certify(ClaimedGuarantee.EXACT_ROW_EQUIVALENCE)
    assert METHOD_SPECS[ValidationMethodKind.EXTERNAL_SORT].can_certify(ClaimedGuarantee.MULTISET_EQUIVALENCE)


# -----------------------------------------------------------------------------
# 2. Decision Engine Rung Coverage (`watched()` holds & refuses for every rung)
# -----------------------------------------------------------------------------

def test_select_validation_method_rungs_hold_and_refuse_under_watched():
    base_profile = TableVerificationProfile(
        table_name="public.orders",
        estimated_rows=50_000,
        estimated_bytes=20 * 1024 * 1024,
        has_unique_key=True,
        has_duplicate_rows=False,
    )
    keyless_profile = TableVerificationProfile(
        table_name="public.audit_events",
        estimated_rows=50_000,
        estimated_bytes=20 * 1024 * 1024,
        has_unique_key=False,
        has_duplicate_rows=True,
    )
    spill_exhausted_profile = TableVerificationProfile(
        table_name="public.huge_events",
        estimated_rows=10_000_000,
        estimated_bytes=500 * 1024 * 1024,
        has_unique_key=False,
        has_duplicate_rows=True,
        available_memory_bytes=1 * 1024 * 1024,
        available_spill_disk_bytes=1024,  # Too small for external sort spill
    )

    full_custom_contract = CustomQueryContract(
        query_id="cq_full_keyed",
        source_sql="SELECT id, amount FROM public.orders ORDER BY id",
        target_sql="SELECT id, amount FROM public.orders ORDER BY id",
        covers_all_rows=True,
        projects_all_columns=True,
        returns_row_level_tuples=True,
        has_unique_key_in_projection=True,
        preserves_duplicate_multiplicity=False,
        is_deterministic=True,
    )

    with watched() as seen:
        # 1. Triage cardinality without value sensitivity -> count holds and wins on lowest cost
        res_count = select_validation_method(
            base_profile,
            required_claim=ClaimedGuarantee.TRIAGE_CARDINALITY,
            requires_value_sensitivity=False,
            custom_query_contract=full_custom_contract,
        )
        assert res_count.selected_method == ValidationMethodKind.COUNT
        assert res_count.care_badge == CareBadge.PASS

        # 2. Column bounds and sums -> count refuses (rank too low), aggregate holds and wins
        res_agg = select_validation_method(
            base_profile,
            required_claim=ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            requires_value_sensitivity=True,
        )
        assert res_agg.selected_method == ValidationMethodKind.AGGREGATE

        # 3. Bucket localization -> count & aggregate refuse, vector holds and wins
        res_vec = select_validation_method(
            base_profile,
            required_claim=ClaimedGuarantee.BUCKET_LOCALIZATION,
        )
        assert res_vec.selected_method == ValidationMethodKind.VECTOR

        # 4. Digest screening -> count/aggregate/vector refuse, hash holds and wins
        res_hash = select_validation_method(
            base_profile,
            required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        )
        assert res_hash.selected_method == ValidationMethodKind.HASH

        # 5. Exact row equivalence on keyed table -> fast signatures refuse, key_row holds and beats external_sort
        res_key_row = select_validation_method(
            base_profile,
            required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        )
        assert res_key_row.selected_method == ValidationMethodKind.KEY_ROW

        # 6. Keyless table with duplicates -> key_row refuses, external_sort holds and wins
        res_ext_sort = select_validation_method(
            keyless_profile,
            required_claim=ClaimedGuarantee.MULTISET_EQUIVALENCE,
        )
        assert res_ext_sort.selected_method == ValidationMethodKind.EXTERNAL_SORT

        # 7. Spill budget exhausted and float rounding unsafe -> external_sort refuses, escalate_unprovable holds
        res_unprovable = select_validation_method(
            spill_exhausted_profile,
            required_claim=ClaimedGuarantee.MULTISET_EQUIVALENCE,
            float_rounding_safe=False,
            supports_bucket_projection=False,
            hash_unambiguous=False,
            canonical_row_projection_ready=False,
        )
        assert res_unprovable.selected_method is None
        assert res_unprovable.selected_rung == "escalate_unprovable"
        assert res_unprovable.care_badge == CareBadge.ACTION

    for rung_name in ("count", "aggregate", "vector", "hash", "custom_query", "key_row", "external_sort"):
        assert set(branch(seen, rung_name)) == {HOLDS, REFUSES}, f"{rung_name} missing branch: {branch(seen, rung_name)}"
        assert why(seen, rung_name) is not None
    assert branch(seen, "escalate_unprovable") == [HOLDS]


# -----------------------------------------------------------------------------
# 3. Count-Only Unchanged Totals Cannot Produce "same"
# -----------------------------------------------------------------------------

def test_count_only_unchanged_totals_never_produce_same_verdict():
    source_rows = [
        {"id": 1, "account": "acct-1", "balance": decimal.Decimal("100.00")},
        {"id": 2, "account": "acct-2", "balance": decimal.Decimal("200.00")},
        {"id": 3, "account": "acct-3", "balance": decimal.Decimal("300.00")},
    ]
    # Target has identical row count (3), even identical sum (600.00), but values changed!
    target_rows = [
        {"id": 1, "account": "acct-1", "balance": decimal.Decimal("150.00")},
        {"id": 2, "account": "acct-2", "balance": decimal.Decimal("150.00")},
        {"id": 3, "account": "acct-3", "balance": decimal.Decimal("300.00")},
    ]

    # When exact equivalence is requested, count-only escalates and never certifies SAME
    report_exact = execute_validation_method(
        ValidationMethodKind.COUNT,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        key_columns=("id",),
    )
    assert report_exact.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert report_exact.verdict != ValidationVerdict.SAME
    assert report_exact.certified_same is False
    assert report_exact.needs_escalation is True
    assert report_exact.recommended_escalation_method == ValidationMethodKind.KEY_ROW
    assert report_exact.pathology_detected == PathologyKind.COUNT_ONLY_FOR_EXACT_CLAIM
    assert report_exact.care_badge == CareBadge.REVIEW

    # Even when only TRIAGE_CARDINALITY is requested, verdict is TRIAGE_COUNT_MATCH, never SAME
    report_triage = execute_validation_method(
        ValidationMethodKind.COUNT,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.TRIAGE_CARDINALITY,
        key_columns=("id",),
    )
    assert report_triage.verdict == ValidationVerdict.TRIAGE_COUNT_MATCH
    assert report_triage.verdict != ValidationVerdict.SAME
    assert report_triage.certified_same is False
    assert report_triage.care_badge == CareBadge.REVIEW

    # When counts differ, count immediately rules in the difference (DIFFERENT / ACTION)
    report_diff = execute_validation_method(
        ValidationMethodKind.COUNT,
        source_rows,
        target_rows[:2],
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        key_columns=("id",),
    )
    assert report_diff.verdict == ValidationVerdict.DIFFERENT
    assert report_diff.care_badge == CareBadge.ACTION
    assert report_diff.certified_same is False


# -----------------------------------------------------------------------------
# 4. Perfect Duplicates Fail or Escalate
# -----------------------------------------------------------------------------

def test_perfect_duplicates_fail_or_escalate_across_methods():
    # Both source and target have 3 rows and the exact same distinct set {alice, bob},
    # but source has 2x alice + 1x bob while target has 1x alice + 2x bob!
    source_rows = [
        {"event": "login", "user": "alice", "points": 10},
        {"event": "login", "user": "alice", "points": 10},
        {"event": "login", "user": "bob", "points": 10},
    ]
    target_rows = [
        {"event": "login", "user": "alice", "points": 10},
        {"event": "login", "user": "bob", "points": 10},
        {"event": "login", "user": "bob", "points": 10},
    ]

    # 1. KEY_ROW on a non-unique column ("event") detects duplicate keys and escalates to EXTERNAL_SORT
    kr_report = execute_validation_method(
        ValidationMethodKind.KEY_ROW,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        key_columns=("event",),
    )
    assert kr_report.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert kr_report.certified_same is False
    assert kr_report.needs_escalation is True
    assert kr_report.recommended_escalation_method == ValidationMethodKind.EXTERNAL_SORT
    assert kr_report.pathology_detected == PathologyKind.PERFECT_DUPLICATES

    # 2. HASH with XOR folding on duplicate rows detects XOR cancellation risk and escalates
    xor_hash_report = execute_validation_method(
        ValidationMethodKind.HASH,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        xor_fold_hashes=True,
    )
    assert xor_hash_report.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert xor_hash_report.certified_same is False
    assert xor_hash_report.recommended_escalation_method == ValidationMethodKind.EXTERNAL_SORT
    assert xor_hash_report.pathology_detected == PathologyKind.HASH_XOR_DUPLICATE_CANCELLATION

    # 3. EXTERNAL_SORT compares sorted canonical tuples with multiplicity and fails with DIFFERENT
    ext_report = execute_validation_method(
        ValidationMethodKind.EXTERNAL_SORT,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.MULTISET_EQUIVALENCE,
    )
    assert ext_report.verdict == ValidationVerdict.DIFFERENT
    assert ext_report.care_badge == CareBadge.ACTION
    assert ext_report.certified_same is False
    assert ext_report.pathology_detected == PathologyKind.PERFECT_DUPLICATES
    assert len(ext_report.differing_keys_or_rows) == 2

    # 4. When multisets with duplicates genuinely match in multiplicity, EXTERNAL_SORT certifies SAME
    ext_same = execute_validation_method(
        ValidationMethodKind.EXTERNAL_SORT,
        source_rows,
        list(reversed(source_rows)),
        required_claim=ClaimedGuarantee.MULTISET_EQUIVALENCE,
    )
    assert ext_same.verdict == ValidationVerdict.SAME
    assert ext_same.care_badge == CareBadge.PASS
    assert ext_same.certified_same is True


# -----------------------------------------------------------------------------
# 5. Equal-Count Value Changes (Including Zero-Sum Compensating Swaps)
# -----------------------------------------------------------------------------

def test_equal_count_value_changes_and_zero_sum_swaps_fail_or_escalate():
    # Source and target have identical COUNT (4), identical MIN (50), identical MAX (300),
    # and identical SUM (650), because row 2 (+50) and row 3 (-50) compensate each other!
    source_rows = [
        {"id": 1, "amount": 50},
        {"id": 2, "amount": 100},
        {"id": 3, "amount": 200},
        {"id": 4, "amount": 300},
    ]
    target_rows = [
        {"id": 1, "amount": 50},
        {"id": 2, "amount": 150},
        {"id": 3, "amount": 150},
        {"id": 4, "amount": 300},
    ]

    # AGGREGATE sees matching COUNT=4, SUM=650, MIN=50, MAX=300, but MUST escalate when exact proof is required
    agg_report = execute_validation_method(
        ValidationMethodKind.AGGREGATE,
        source_rows,
        target_rows,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        key_columns=("id",),
    )
    assert agg_report.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert agg_report.certified_same is False
    assert agg_report.needs_escalation is True
    assert agg_report.recommended_escalation_method == ValidationMethodKind.KEY_ROW
    assert agg_report.pathology_detected == PathologyKind.EQUAL_COUNT_VALUE_CHANGE

    # VECTOR, HASH, KEY_ROW, and EXTERNAL_SORT all catch the compensating value change as DIFFERENT
    for method in (
        ValidationMethodKind.VECTOR,
        ValidationMethodKind.HASH,
        ValidationMethodKind.KEY_ROW,
        ValidationMethodKind.EXTERNAL_SORT,
    ):
        rep = execute_validation_method(
            method,
            source_rows,
            target_rows,
            required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            key_columns=("id",),
            bucket_count=8,
        )
        assert rep.verdict == ValidationVerdict.DIFFERENT, f"{method} failed to detect zero-sum swap"
        assert rep.care_badge == CareBadge.ACTION
        assert rep.certified_same is False
        assert rep.pathology_detected == PathologyKind.EQUAL_COUNT_VALUE_CHANGE


# -----------------------------------------------------------------------------
# 6. Floating Rounding and Outer-Band Clamping Fail or Escalate
# -----------------------------------------------------------------------------

def test_floating_rounding_and_outer_band_clamping_fail_or_escalate():
    # Case A: Outer-band floats 1e45 vs 1e46 that MySQL DECIMAL(65,20) clamps to the same 999...99 string
    src_outer = [{"id": 1, "reading": 1e45}]
    tgt_outer = [{"id": 1, "reading": 1e46}]

    # If a caller attempts AGGREGATE or HASH with FIXED_DECIMAL_CLAMPED or RAW_ENGINE_TEXT, it escalates
    agg_clamped = execute_validation_method(
        ValidationMethodKind.AGGREGATE,
        src_outer,
        tgt_outer,
        required_claim=ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
        float_policy=FloatCanonicalPolicy.FIXED_DECIMAL_CLAMPED,
        key_columns=("id",),
    )
    assert agg_clamped.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert agg_clamped.pathology_detected == PathologyKind.FLOATING_ROUNDING
    assert agg_clamped.certified_same is False

    # Banded canonicalization in KEY_ROW distinguishes 1e45 vs 1e46 and fails with DIFFERENT
    kr_outer = execute_validation_method(
        ValidationMethodKind.KEY_ROW,
        src_outer,
        tgt_outer,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
        key_columns=("id",),
    )
    assert kr_outer.verdict == ValidationVerdict.DIFFERENT
    assert kr_outer.care_badge == CareBadge.ACTION

    # Case B: Subtle float4 vs float8 precision rounding drift (123.45678901234567 vs 123.456787109375)
    src_drift = [{"id": 1, "val": 123.45678901234567}]
    tgt_drift = [{"id": 1, "val": 123.456787109375}]

    agg_drift = execute_validation_method(
        ValidationMethodKind.AGGREGATE,
        src_drift,
        tgt_drift,
        required_claim=ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
        key_columns=("id",),
    )
    assert agg_drift.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert agg_drift.pathology_detected == PathologyKind.FLOATING_ROUNDING

    kr_drift = execute_validation_method(
        ValidationMethodKind.KEY_ROW,
        src_drift,
        tgt_drift,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        key_columns=("id",),
    )
    assert kr_drift.verdict == ValidationVerdict.DIFFERENT
    assert kr_drift.pathology_detected == PathologyKind.FLOATING_ROUNDING
    assert kr_drift.care_badge == CareBadge.ACTION

    # Case C: NaN and Infinity handling in canonicalizer
    nan_str, nan_flag = canonicalize_float_value(float("nan"))
    inf_str, inf_flag = canonicalize_float_value(float("inf"))
    ninf_str, ninf_flag = canonicalize_float_value(float("-inf"))
    assert nan_str == "uncomparable:NaN" and nan_flag is True
    assert inf_str == "uncomparable:+Inf" and inf_flag is True
    assert ninf_str == "uncomparable:-Inf" and ninf_flag is True
    assert inf_str != ninf_str


# -----------------------------------------------------------------------------
# 7. Hash Ambiguity (Delimiter Collision, NULL-vs-Empty, Weak Width)
# -----------------------------------------------------------------------------

def test_hash_ambiguity_escalates_and_canonical_framing_distinguishes_collisions():
    # Delimiter collision pair: ("ab", "c") vs ("a", "bc")
    src_delim = [{"id": 1, "first": "ab", "second": "c"}]
    tgt_delim = [{"id": 1, "first": "a", "second": "bc"}]

    # Unframed concatenation produces identical naive string, so HASH refuses/escalates
    unframed_rep = execute_validation_method(
        ValidationMethodKind.HASH,
        src_delim,
        tgt_delim,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        length_prefixed_hash=False,
        key_columns=("id",),
    )
    assert unframed_rep.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert unframed_rep.pathology_detected == PathologyKind.HASH_DELIMITER_AMBIGUITY
    assert unframed_rep.certified_same is False

    # Length-prefixed canonical HASH rules in the difference immediately
    framed_rep = execute_validation_method(
        ValidationMethodKind.HASH,
        src_delim,
        tgt_delim,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        length_prefixed_hash=True,
        key_columns=("id",),
    )
    assert framed_rep.verdict == ValidationVerdict.DIFFERENT
    assert framed_rep.care_badge == CareBadge.ACTION

    # NULL vs empty string '' pair
    src_null = [{"id": 1, "note": None}]
    tgt_empty = [{"id": 1, "note": ""}]

    null_ambig_rep = execute_validation_method(
        ValidationMethodKind.HASH,
        src_null,
        tgt_empty,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        null_distinct_hash=False,
        key_columns=("id",),
    )
    assert null_ambig_rep.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert null_ambig_rep.pathology_detected == PathologyKind.HASH_NULL_EMPTY_AMBIGUITY

    null_tagged_rep = execute_validation_method(
        ValidationMethodKind.HASH,
        src_null,
        tgt_empty,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        null_distinct_hash=True,
        key_columns=("id",),
    )
    assert null_tagged_rep.verdict == ValidationVerdict.DIFFERENT
    assert null_tagged_rep.care_badge == CareBadge.ACTION

    # Weak 32-bit digest width escalates due to birthday collision risk
    weak_hash_rep = execute_validation_method(
        ValidationMethodKind.HASH,
        src_delim,
        src_delim,
        required_claim=ClaimedGuarantee.DIGEST_SCREENING,
        digest_bits=32,
        key_columns=("id",),
    )
    assert weak_hash_rep.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert weak_hash_rep.pathology_detected == PathologyKind.HASH_WEAK_WIDTH_COLLISION


# -----------------------------------------------------------------------------
# 8. Fast Vector Signature Prioritizing Exact Work & Custom Query Scope
# -----------------------------------------------------------------------------

def test_vector_signature_localizes_dirty_buckets_and_prioritizes_exact_work():
    source_rows = [{"id": i, "payload": f"row-{i}", "amount": i * 10} for i in range(1, 65)]
    target_rows = [dict(r) for r in source_rows]
    # Corrupt a single row (id=42)
    target_rows[41]["amount"] = 999999

    fast_rep, exact_rep = prioritize_exact_verification_by_vector(
        source_rows,
        target_rows,
        key_columns=("id",),
        bucket_count=16,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
    )
    # Fast vector report rules in difference and pinpoints the single dirty bucket
    assert fast_rep.method == ValidationMethodKind.VECTOR
    assert fast_rep.verdict == ValidationVerdict.DIFFERENT
    assert len(fast_rep.differing_bucket_ids) == 1
    assert fast_rep.certified_same is False

    # Second-stage exact report verifies only the dirty bucket's rows (< 64 total rows) and proves difference
    assert exact_rep.method == ValidationMethodKind.KEY_ROW
    assert exact_rep.verdict == ValidationVerdict.DIFFERENT
    assert exact_rep.source_row_count < len(source_rows)
    assert exact_rep.care_badge == CareBadge.ACTION

    # When data is identical, fast vector still escalates (cannot certify SAME) and exact stage certifies SAME
    fast_clean, exact_clean = prioritize_exact_verification_by_vector(
        source_rows,
        source_rows,
        key_columns=("id",),
        bucket_count=16,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
    )
    assert fast_clean.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert fast_clean.certified_same is False
    assert exact_clean.verdict == ValidationVerdict.SAME
    assert exact_clean.certified_same is True
    assert exact_clean.care_badge == CareBadge.PASS


def test_custom_query_subset_cannot_certify_whole_table_exact_equivalence():
    rows = [{"id": 1, "status": "ACTIVE"}, {"id": 2, "status": "ACTIVE"}]
    subset_contract = CustomQueryContract(
        query_id="active_only_check",
        source_sql="SELECT id, status FROM orders WHERE status = 'ACTIVE'",
        target_sql="SELECT id, status FROM orders WHERE status = 'ACTIVE'",
        covers_all_rows=False,
        projects_all_columns=False,
        returns_row_level_tuples=True,
        has_unique_key_in_projection=True,
    )
    rep = execute_validation_method(
        ValidationMethodKind.CUSTOM_QUERY,
        rows,
        rows,
        required_claim=ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        custom_query_contract=subset_contract,
        key_columns=("id",),
    )
    assert rep.verdict == ValidationVerdict.ESCALATE_TO_EXACT
    assert rep.pathology_detected == PathologyKind.CUSTOM_QUERY_INCOMPLETE_SCOPE
    assert rep.certified_same is False


# -----------------------------------------------------------------------------
# 9. Universal Resilience: O(1) Typed Checkpoint, Lease Lock, AIMD, ETA, Hosts
# -----------------------------------------------------------------------------

def test_o1_typed_durable_checkpoint_gates_resume_on_achieved_proof_strength(tmp_path: Path):
    cp_path = tmp_path / "verify_checkpoint.json"
    cp = VerifyMethodCheckpoint(cp_path)

    composite_start = (
        datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("1000.50"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        b"\x00\xff",
    )
    composite_end = (
        datetime.datetime(2026, 10, 17, 13, 0, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("9999.99"),
        uuid.UUID("87654321-4321-8765-4321-876543218765"),
        b"\x10\x20",
    )

    # Record a range checked only with COUNT (triage cardinality)
    cp.record_range(
        CompletedRangeProof(
            range_id="range-001",
            start_key=composite_start,
            end_key=composite_end,
            method_used=ValidationMethodKind.COUNT,
            proof_strength=ProofStrength.CARDINALITY_ONLY,
            verdict=ValidationVerdict.TRIAGE_COUNT_MATCH,
            rows_verified=5000,
            bytes_verified=2 * 1024 * 1024,
            completed_at=1700000000.0,
        )
    )

    # Reload from disk and verify O(1) native type preservation
    cp_reloaded = VerifyMethodCheckpoint(cp_path)
    rec = cp_reloaded.get_range_proof("range-001")
    assert rec is not None
    assert rec.start_key == composite_start
    assert rec.end_key == composite_end
    assert isinstance(rec.start_key[0], datetime.datetime)
    assert isinstance(rec.start_key[1], decimal.Decimal)
    assert isinstance(rec.start_key[2], uuid.UUID)
    assert isinstance(rec.start_key[3], bytes)

    # Satisfies TRIAGE_CARDINALITY in O(1), but MUST NOT satisfy EXACT_ROW_EQUIVALENCE!
    assert cp_reloaded.is_range_satisfied("range-001", ClaimedGuarantee.TRIAGE_CARDINALITY) is True
    assert cp_reloaded.is_range_satisfied("range-001", ClaimedGuarantee.EXACT_ROW_EQUIVALENCE) is False

    # Upgrade range-001 with KEY_ROW exact verification
    cp_reloaded.record_range(
        CompletedRangeProof(
            range_id="range-001",
            start_key=composite_start,
            end_key=composite_end,
            method_used=ValidationMethodKind.KEY_ROW,
            proof_strength=ProofStrength.EXACT_KEYED_EQUIVALENCE,
            verdict=ValidationVerdict.SAME,
            rows_verified=5000,
            bytes_verified=2 * 1024 * 1024,
            completed_at=1700000010.0,
        )
    )
    assert cp_reloaded.is_range_satisfied("range-001", ClaimedGuarantee.EXACT_ROW_EQUIVALENCE) is True
    assert cp_reloaded.is_range_satisfied("range-001", ClaimedGuarantee.MULTISET_EQUIVALENCE) is False


def test_verification_lease_lock_aimd_batcher_eta_and_safe_hosts(tmp_path: Path):
    lease1 = VerificationLeaseLock(state_dir=tmp_path, run_id="run-1", lease_duration_seconds=60.0)
    lease1.acquire(now=1000.0)
    lease1.verify(now=1010.0)

    # Second concurrent runner with different run_id is blocked while lease1 is active
    lease2 = VerificationLeaseLock(state_dir=tmp_path, run_id="run-2", lease_duration_seconds=60.0)
    with pytest.raises(VerificationLeaseError):
        lease2.acquire(now=1015.0, steal_stale=True)

    # Lease expiration raises VerificationLeaseExpiredError
    with pytest.raises(VerificationLeaseExpiredError):
        lease1.verify(now=1070.0)

    lease1.release()

    # AIMD batcher adapts up on fast batches and halves on slow/pressured batches
    batcher = AdaptiveAimdBatcher(current_batch_rows=4000, additive_step_rows=1000, target_batch_seconds=1.0)
    assert batcher.observe_batch(0.4, had_pressure_or_error=False) == 5000
    assert batcher.observe_batch(2.5, had_pressure_or_error=False) == 2500
    assert batcher.observe_batch(0.2, had_pressure_or_error=True) == 1250

    # Cold-start byte-weighted ETA provides immediate non-zero estimate before first range completes
    eta = ColdStartByteWeightedEta(total_bytes=100 * 1024 * 1024, cold_start_bytes_per_sec=50 * 1024 * 1024)
    assert eta.estimate_remaining_seconds() == pytest.approx(2.0)
    eta.record_progress(50 * 1024 * 1024, 1.0)
    assert 0.5 <= eta.estimate_remaining_seconds() <= 1.5
    eta.record_progress(50 * 1024 * 1024, 1.0)
    assert eta.estimate_remaining_seconds() == 0.0

    # Safe host enforcement
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("10.0.0.42") is True
    assert is_safe_test_host("192.168.1.1") is False
    bad_profile = TableVerificationProfile(
        table_name="public.t",
        estimated_rows=10,
        estimated_bytes=1024,
        source_host="prod-db.example.com",
    )
    with pytest.raises(UnsafeEndpointHostError):
        select_validation_method(bad_profile)


def test_module_and_test_files_are_pure_ascii():
    mod_path = pathlib.Path(__file__).resolve().parent.parent / "migkit" / "verify_method.py"
    test_path = pathlib.Path(__file__).resolve()
    for path in (mod_path, test_path):
        data = path.read_bytes()
        assert all(b < 128 for b in data), f"Non-ASCII byte found in {path}"
