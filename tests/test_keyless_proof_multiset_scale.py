"""Tests for proving keyless multisets at scale (Row 615: RCH07.keyless-proof).

Proves:
1. Every decide.Rung in `keyless_proof.multiset_proof_rungs` and
   `keyless_proof.row_identity_rungs` is exercised on both `holds` and `refuses`
   under `tests.rung_branches.watched()`, and declared in `PROVES_RUNGS`.
2. Perfect duplicates with even-count cancellation (where XOR digests cancel to 0)
   cannot falsely pass across bucketed, spilled, or in-memory modes.
3. Reordered multisets with identical canonical multiplicities prove equal.
4. NULL vs empty string vs literal "N"/"NULL" and delimiter-in-column shifts
   ("a|b", "c" vs "a", "b|c") never collide or falsely pass.
5. Equal-count corruption (including same distinct set [A, A, B] vs [A, B, B])
   cannot falsely pass.
6. Billion-row scale and skewed-bucket spill scenarios route to external K-way
   merge-sort spill, coalesce across multiple disk runs, and detect spill
   corruption or disk exhaustion.
7. Deterministic keyless CDC/repair row-identity strategies mutate exact duplicate
   multiplicities without synthetic-key source mutation.
"""

from __future__ import annotations

import datetime
import decimal
import pathlib
import random
from typing import Any, Dict, List, Sequence, Tuple

import pytest

import tests.rung_branches
from migkit import decide, keyless_proof
from migkit.keyless_proof import (
    CARE_ACTION,
    CARE_PASS,
    CareBadge,
    ExternalMultisetSpillSorter,
    KeylessApplyReceipt,
    KeylessCanonicalizationError,
    KeylessChangeEvent,
    KeylessChangeOp,
    KeylessColumnSpec,
    KeylessDeterministicApplier,
    KeylessIdentityError,
    KeylessLocatorFenceError,
    KeylessMultisetVerifier,
    KeylessProofError,
    KeylessRowCanonicalizer,
    KeylessSpillError,
    KeylessTableContract,
    KeylessTargetTable,
    MultisetFingerprint,
    MultisetProofMode,
    RowIdentityMode,
    UnsafeEndpointError,
    choose_multiset_proof_strategy,
    choose_row_identity_strategy,
    derive_scale_facts,
    diff_coalesced_multiset_streams,
    multiset_proof_rungs,
    row_identity_rungs,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("keyless_proof", "multiset_proof_rungs", "sql_bucket_multiset_digest", "holds"),
    ("keyless_proof", "multiset_proof_rungs", "sql_bucket_multiset_digest", "refuses"),
    ("keyless_proof", "multiset_proof_rungs", "external_sort_spill_multiset", "holds"),
    ("keyless_proof", "multiset_proof_rungs", "external_sort_spill_multiset", "refuses"),
    ("keyless_proof", "multiset_proof_rungs", "in_memory_canonical_multiset", "holds"),
    ("keyless_proof", "multiset_proof_rungs", "in_memory_canonical_multiset", "refuses"),
    ("keyless_proof", "row_identity_rungs", "canonical_full_row_multiplicity", "holds"),
    ("keyless_proof", "row_identity_rungs", "canonical_full_row_multiplicity", "refuses"),
    ("keyless_proof", "row_identity_rungs", "fenced_physical_locator_verified", "holds"),
    ("keyless_proof", "row_identity_rungs", "fenced_physical_locator_verified", "refuses"),
    ("keyless_proof", "row_identity_rungs", "bucket_multiset_reconcile", "holds"),
    ("keyless_proof", "row_identity_rungs", "bucket_multiset_reconcile", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures & Helpers
# -----------------------------------------------------------------------------

def _audit_contract() -> KeylessTableContract:
    return KeylessTableContract(
        table_name="audit_events_keyless",
        columns=(
            KeylessColumnSpec("tenant_id", "integer", nullable=False),
            KeylessColumnSpec("event_kind", "text", nullable=True),
            KeylessColumnSpec("note", "text", nullable=True),
            KeylessColumnSpec("amount", "decimal", nullable=True),
        ),
        digest_salt="test-salt-rch07",
    )


# -----------------------------------------------------------------------------
# 1. Decision Rung Coverage (`watched()` + `PROVES_RUNGS`)
# -----------------------------------------------------------------------------

def test_multiset_proof_rungs_holds_and_refuses_under_watched() -> None:
    """Exercise both holds and refuses branches for every rung in `multiset_proof_rungs`."""
    with watched() as seen:
        # Case A: All 3 rungs hold (small table, server canonical hash, sufficient RAM & spill disk)
        c_all = choose_multiset_proof_strategy(
            {
                "synthetic_key_source_mutation": False,
                "xor_only_digest": False,
                "sample_only_proof": False,
                "server_canonical_hash": True,
                "estimated_rows": 10_000,
                "measured_row_bytes": 100,
                "available_ram_bytes": 64 * 1024 * 1024,
                "bucket_count": 16,
                "spill_free_bytes": 100 * 1024 * 1024,
            }
        )
        assert c_all.rung is not None
        assert c_all.rung.name == "sql_bucket_multiset_digest"

        # Case B: Billion-row skewed table exceeds bucket RAM and table RAM, so
        # sql_bucket_multiset_digest and in_memory_canonical_multiset refuse,
        # while external_sort_spill_multiset holds.
        c_spill = choose_multiset_proof_strategy(
            {
                "synthetic_key_source_mutation": False,
                "xor_only_digest": False,
                "sample_only_proof": False,
                "server_canonical_hash": True,
                "estimated_rows": 1_000_000_000,
                "measured_row_bytes": 128,
                "available_ram_bytes": 2 * 1024 * 1024 * 1024,  # 2 GiB RAM (~16.7M rows max)
                "bucket_count": 16,
                "measured_bucket_skew_factor": 4.0,  # max bucket ~250M rows > 16.7M limit
                "spill_free_bytes": 500 * 1024 * 1024 * 1024,  # 500 GiB disk >= 128 GB spill
            }
        )
        assert c_spill.rung is not None
        assert c_spill.rung.name == "external_sort_spill_multiset"

        # Case C: Spill disk also insufficient, and synthetic_key_source_mutation or xor_only_digest
        # refuses external_sort_spill_multiset as well.
        c_none = choose_multiset_proof_strategy(
            {
                "synthetic_key_source_mutation": False,
                "xor_only_digest": False,
                "sample_only_proof": False,
                "server_canonical_hash": False,
                "estimated_rows": 1_000_000_000,
                "measured_row_bytes": 128,
                "available_ram_bytes": 512 * 1024 * 1024,
                "spill_free_bytes": 1024 * 1024,  # only 1 MiB free disk
            }
        )
        assert c_none.rung is None

    for rung_name in (
        "sql_bucket_multiset_digest",
        "external_sort_spill_multiset",
        "in_memory_canonical_multiset",
    ):
        reached_branches = branch(seen, rung_name)
        assert HOLDS in reached_branches and REFUSES in reached_branches, (
            f"rung {rung_name} did not reach both holds and refuses: {reached_branches}"
        )
        assert why(seen, rung_name) is not None


def test_row_identity_rungs_holds_and_refuses_under_watched() -> None:
    """Exercise both holds and refuses branches for every rung in `row_identity_rungs`."""
    with watched() as seen:
        # Case A: All 3 row-identity rungs hold
        c_full = choose_row_identity_strategy(
            {
                "synthetic_key_source_mutation": False,
                "full_before_image": True,
                "supports_single_row_limit": True,
                "physical_locator_available": True,
                "locator_invalidated_by_movement": False,
                "verify_row_hash_on_locator": True,
                "transactional_bucket_replace": True,
                "bucket_reconcile_within_budget": True,
            }
        )
        assert c_full.rung is not None
        assert c_full.rung.name == "canonical_full_row_multiplicity"

        # Case B: Target lacks single-row limit on DELETE/UPDATE, but has fenced physical locator
        c_loc = choose_row_identity_strategy(
            {
                "synthetic_key_source_mutation": False,
                "full_before_image": True,
                "supports_single_row_limit": False,
                "physical_locator_available": True,
                "locator_invalidated_by_movement": False,
                "verify_row_hash_on_locator": True,
                "transactional_bucket_replace": False,
                "bucket_reconcile_within_budget": False,
            }
        )
        assert c_loc.rung is not None
        assert c_loc.rung.name == "fenced_physical_locator_verified"

        # Case C: Physical locator invalidated by movement; falls back to bucket_multiset_reconcile
        c_bucket = choose_row_identity_strategy(
            {
                "synthetic_key_source_mutation": False,
                "full_before_image": False,
                "supports_single_row_limit": False,
                "physical_locator_available": True,
                "locator_invalidated_by_movement": True,
                "verify_row_hash_on_locator": True,
                "transactional_bucket_replace": True,
                "bucket_reconcile_within_budget": True,
            }
        )
        assert c_bucket.rung is not None
        assert c_bucket.rung.name == "bucket_multiset_reconcile"

        # Case D: Synthetic-key source mutation requested -> all rungs refuse
        c_refuse_all = choose_row_identity_strategy(
            {
                "synthetic_key_source_mutation": True,
                "full_before_image": True,
                "supports_single_row_limit": True,
                "physical_locator_available": True,
                "locator_invalidated_by_movement": False,
                "verify_row_hash_on_locator": True,
                "transactional_bucket_replace": True,
                "bucket_reconcile_within_budget": True,
            }
        )
        assert c_refuse_all.rung is None

    for rung_name in (
        "canonical_full_row_multiplicity",
        "fenced_physical_locator_verified",
        "bucket_multiset_reconcile",
    ):
        reached_branches = branch(seen, rung_name)
        assert HOLDS in reached_branches and REFUSES in reached_branches, (
            f"rung {rung_name} did not reach both holds and refuses: {reached_branches}"
        )
        assert why(seen, rung_name) is not None


# -----------------------------------------------------------------------------
# 2. Acceptance: Perfect Duplicates & Even-Count Cancellation Cannot Falsely Pass
# -----------------------------------------------------------------------------

@pytest.mark.parametrize(
    "facts",
    [
        # sql_bucket_multiset_digest
        {
            "server_canonical_hash": True,
            "bucket_fits_memory": True,
            "memory_row_limit": 1000,
        },
        # external_sort_spill_multiset
        {
            "server_canonical_hash": False,
            "spill_disk_fits": True,
            "memory_row_limit": 2,
            "spill_free_bytes": 10 * 1024 * 1024,
        },
        # in_memory_canonical_multiset
        {
            "server_canonical_hash": False,
            "spill_disk_fits": False,
            "table_fits_memory": True,
            "memory_row_limit": 1000,
        },
    ],
)
def test_even_count_duplicate_cancellation_cannot_falsely_pass(
    facts: Dict[str, Any], tmp_path: pathlib.Path
) -> None:
    """Prove that even-multiplicity duplicates that cancel to 0 under XOR
    are caught with CareBadge.ACTION across all three proof strategies."""
    contract = _audit_contract()
    verifier = KeylessMultisetVerifier(contract)

    row_a = (10, "login", "dup-a", decimal.Decimal("12.50"))
    row_b = (20, "logout", "dup-b", decimal.Decimal("99.00"))
    row_anchor = (30, "ping", "anchor", decimal.Decimal("1.00"))

    # Under XOR: hash(row_a) ^ hash(row_a) ^ hash(row_anchor) == hash(row_anchor)
    # and        hash(row_b) ^ hash(row_b) ^ hash(row_anchor) == hash(row_anchor)
    # Both have count == 3 and identical XOR checksum!
    source_rows = [row_a, row_a, row_anchor]
    target_rows = [row_b, row_b, row_anchor]

    report = verifier.verify(source_rows, target_rows, facts, spill_dir=tmp_path)
    assert report.verified_equal is False
    assert report.care_badge == CareBadge.ACTION
    assert report.source_fingerprint.row_count == 3
    assert report.target_fingerprint.row_count == 3
    assert not report.source_fingerprint.matches(report.target_fingerprint)
    assert report.missing_from_target_count == 2
    assert report.extra_on_target_count == 2


def test_xor_only_and_sample_only_and_synthetic_key_are_refused() -> None:
    """Verify that XOR-only digests, sample-only checks, and synthetic-key source
    mutations are explicitly refused by the decision ladder."""
    contract = _audit_contract()
    verifier = KeylessMultisetVerifier(contract)
    rows = [(1, "a", "b", decimal.Decimal("1.0"))]

    for invalid_flag in ("xor_only_digest", "sample_only_proof", "synthetic_key_source_mutation"):
        facts = {
            invalid_flag: True,
            "server_canonical_hash": True,
            "bucket_fits_memory": True,
            "spill_disk_fits": True,
            "table_fits_memory": True,
        }
        with pytest.raises(KeylessProofError, match="no keyless multiset proof rung held"):
            verifier.verify(rows, rows, facts)


# -----------------------------------------------------------------------------
# 3. Acceptance: Reorder Invariance with Duplicates
# -----------------------------------------------------------------------------

def test_reordered_multiset_with_duplicates_proves_equal(tmp_path: pathlib.Path) -> None:
    """Reordering rows in any permutation preserves exact fingerprint and multiplicity proof."""
    contract = _audit_contract()
    verifier = KeylessMultisetVerifier(contract)

    base_rows = [
        (1, "click", None, decimal.Decimal("0.00")),
        (1, "click", None, decimal.Decimal("0.00")),
        (1, "click", None, decimal.Decimal("0.00")),
        (2, "view", "", decimal.Decimal("10.25")),
        (2, "view", "", decimal.Decimal("10.25")),
        (3, "purchase", "item|pipe", decimal.Decimal("500.00")),
        (4, None, "N", None),
    ]
    shuffled = list(base_rows)
    rng = random.Random(42)
    rng.shuffle(shuffled)
    assert shuffled != base_rows

    for mode_facts in (
        {"server_canonical_hash": True, "bucket_fits_memory": True, "memory_row_limit": 500},
        {
            "server_canonical_hash": False,
            "spill_disk_fits": True,
            "memory_row_limit": 2,
            "spill_free_bytes": 1024 * 1024,
        },
        {"server_canonical_hash": False, "spill_disk_fits": False, "table_fits_memory": True, "memory_row_limit": 500},
    ):
        report = verifier.verify(base_rows, shuffled, mode_facts, spill_dir=tmp_path)
        assert report.verified_equal is True
        assert report.care_badge == CareBadge.PASS
        assert report.source_fingerprint.matches(report.target_fingerprint)
        assert report.source_fingerprint.row_count == 7
        assert report.source_fingerprint.distinct_row_count == 4
        assert len(report.discrepancies) == 0


# -----------------------------------------------------------------------------
# 4. Acceptance: NULL / Empty String / Delimiter Canonicalization Contracts
# -----------------------------------------------------------------------------

def test_null_empty_string_and_delimiter_boundaries_never_collide() -> None:
    """Prove SQL NULL, empty string '', literal 'N', literal 'NULL', and pipe-delimited
    column shifts produce distinct canonical encodings and fail verification if swapped."""
    contract = _audit_contract()
    verifier = KeylessMultisetVerifier(contract)
    facts = {"server_canonical_hash": True, "bucket_fits_memory": True, "memory_row_limit": 100}

    # 1. NULL vs empty string ""
    r_null = [(1, None, "ok", decimal.Decimal("1.00"))]
    r_empty = [(1, "", "ok", decimal.Decimal("1.00"))]
    rep_null_empty = verifier.verify(r_null, r_empty, facts)
    assert rep_null_empty.verified_equal is False
    assert rep_null_empty.care_badge == CareBadge.ACTION

    # 2. NULL vs literal "N" (rowtext.NULL_LEN marker) vs literal "NULL"
    r_lit_n = [(1, "N", "ok", decimal.Decimal("1.00"))]
    r_lit_null = [(1, "NULL", "ok", decimal.Decimal("1.00"))]
    assert verifier.verify(r_null, r_lit_n, facts).verified_equal is False
    assert verifier.verify(r_null, r_lit_null, facts).verified_equal is False

    # 3. Column delimiter boundary shift: ("x|y", "z") vs ("x", "y|z")
    r_left = [(1, "x|y", "z", decimal.Decimal("1.00"))]
    r_right = [(1, "x", "y|z", decimal.Decimal("1.00"))]
    rep_delim = verifier.verify(r_left, r_right, facts)
    assert rep_delim.verified_equal is False
    assert rep_delim.care_badge == CareBadge.ACTION


def test_canonicalizer_rejects_schema_violations_and_uncomparable_values() -> None:
    """Verify strict schema arity, non-nullable enforcement, and uncomparable float rejection."""
    contract = KeylessTableContract(
        table_name="metrics_keyless",
        columns=(
            KeylessColumnSpec("host_id", "integer", nullable=False),
            KeylessColumnSpec("score", "float", nullable=True),
        ),
    )
    canon_helper = KeylessRowCanonicalizer(contract)

    # Non-nullable violation
    with pytest.raises(KeylessCanonicalizationError, match="non-nullable"):
        canon_helper.canonical_row_text((None, 1.5))

    # Arity mismatch
    with pytest.raises(KeylessCanonicalizationError, match="arity mismatch"):
        canon_helper.canonical_row_text((1, 1.5, "extra"))

    # Unknown column in dict row
    with pytest.raises(KeylessCanonicalizationError, match="undeclared columns"):
        canon_helper.canonical_row_text({"host_id": 1, "score": 1.5, "bogus": 2})

    # Uncomparable float (NaN / Inf)
    with pytest.raises(KeylessCanonicalizationError, match="uncomparable value"):
        canon_helper.canonical_row_text((1, float("nan")))


# -----------------------------------------------------------------------------
# 5. Acceptance: Equal-Count Corruption Cannot Falsely Pass
# -----------------------------------------------------------------------------

def test_equal_count_same_distinct_set_multiplicity_shift_detected(tmp_path: pathlib.Path) -> None:
    """Source=[A, A, B] (count=3, distinct={A,B}) vs Target=[A, B, B] (count=3, distinct={A,B})
    must fail verification and report exact multiplicity deltas (+1 for A, -1 for B)."""
    contract = _audit_contract()
    verifier = KeylessMultisetVerifier(contract)

    row_a = (100, "alpha", "same-set", decimal.Decimal("5.00"))
    row_b = (100, "beta", "same-set", decimal.Decimal("5.00"))

    src = [row_a, row_a, row_b]
    dst = [row_a, row_b, row_b]

    report = verifier.verify(
        src,
        dst,
        {
            "server_canonical_hash": False,
            "spill_disk_fits": True,
            "memory_row_limit": 2,
            "spill_free_bytes": 1024 * 1024,
        },
        spill_dir=tmp_path,
    )
    assert report.verified_equal is False
    assert report.care_badge == CareBadge.ACTION
    assert report.source_fingerprint.row_count == 3
    assert report.target_fingerprint.row_count == 3
    assert report.source_fingerprint.distinct_row_count == 2
    assert report.target_fingerprint.distinct_row_count == 2
    assert len(report.discrepancies) == 2

    by_event = {d.decoded_values[1]: d for d in report.discrepancies}
    assert by_event["alpha"].source_count == 2
    assert by_event["alpha"].target_count == 1
    assert by_event["beta"].source_count == 1
    assert by_event["beta"].target_count == 2


# -----------------------------------------------------------------------------
# 6. Acceptance: Billion-Row Scale & External Merge-Sort Spill Integrity
# -----------------------------------------------------------------------------

def test_billion_row_spill_multi_run_coalescing_and_corruption_detection(
    tmp_path: pathlib.Path,
) -> None:
    """Verify multi-run external K-way merge-sort coalesces duplicates across runs
    and detects tampered/truncated spill files or disk budget exhaustion."""
    contract = _audit_contract()
    canonicalizer = KeylessRowCanonicalizer(contract)

    # Generate 120 rows with duplicates across 30 distinct keys; force run_distinct_limit=4
    # so the sorter writes at least 8 sorted spill run files and merges them via heapq.merge.
    src_rows: List[Tuple[Any, ...]] = []
    for i in range(30):
        row = (i, f"evt_{i % 7}", f"payload_{i}", decimal.Decimal(f"{i}.50"))
        for _ in range((i % 4) + 1):
            src_rows.append(row)

    dst_rows = list(src_rows)
    random.Random(99).shuffle(src_rows)
    random.Random(101).shuffle(dst_rows)

    # Corrupt 1 duplicate instance in dst_rows while keeping total row count identical!
    corrupt_idx = dst_rows.index((15, "evt_1", "payload_15", decimal.Decimal("15.50")))
    dst_rows[corrupt_idx] = (15, "evt_1", "payload_15_corrupt", decimal.Decimal("15.50"))
    assert len(src_rows) == len(dst_rows)

    verifier = KeylessMultisetVerifier(contract)
    facts = {
        "server_canonical_hash": False,
        "estimated_rows": 1_000_000_000,
        "measured_row_bytes": 128,
        "available_ram_bytes": 512,  # forces memory_row_limit = 4 rows per spill run
        "spill_free_bytes": 500 * 1024 * 1024 * 1024,
    }
    report = verifier.verify(src_rows, dst_rows, facts, spill_dir=tmp_path)
    assert report.proof_mode == MultisetProofMode.EXTERNAL_SORT_SPILL_MULTISET
    assert report.spill_runs_written >= 12
    assert report.spill_bytes_written > 0
    assert report.verified_equal is False
    assert report.care_badge == CareBadge.ACTION
    assert report.missing_from_target_count == 1
    assert report.extra_on_target_count == 1

    # Verify spill file checksum tampering raises KeylessSpillError (never swallowed)
    sorter = ExternalMultisetSpillSorter(
        canonicalizer, run_distinct_limit=4, spill_dir=tmp_path / "tamper"
    )
    manifests, stream = sorter.spill_and_coalesce(src_rows[:12], tmp_path / "tamper")
    # Corrupt the first spill run file on disk (same byte length, flipped payload byte)
    first_file = manifests[0].file_path
    raw_bytes = bytearray(first_file.read_bytes())
    raw_bytes[-2] = ord("X") if raw_bytes[-2] != ord("X") else ord("Y")
    first_file.write_bytes(bytes(raw_bytes))

    with pytest.raises(KeylessSpillError):
        list(stream)

    # Verify disk exhaustion raises KeylessSpillError
    tiny_disk_sorter = ExternalMultisetSpillSorter(
        canonicalizer,
        run_distinct_limit=4,
        spill_dir=tmp_path / "full_disk",
        avail_disk_bytes=10,
    )
    with pytest.raises(KeylessSpillError, match="spill disk budget exhausted"):
        tiny_disk_sorter.spill_and_coalesce(src_rows[:10], tmp_path / "full_disk")


# -----------------------------------------------------------------------------
# 7. Acceptance: Deterministic Keyless CDC & Repair Row-Identity Strategies
# -----------------------------------------------------------------------------

def test_deterministic_keyless_cdc_single_duplicate_mutation_and_fenced_locator() -> None:
    """Prove that deleting or updating 1 duplicate of a 3x repeated keyless row leaves
    the remaining 2 duplicates intact, and that stale locators after compaction are refused."""
    contract = _audit_contract()
    dup_row = (7, "dup_event", "identical", decimal.Decimal("42.00"))
    other_row = (8, "other_event", "distinct", decimal.Decimal("1.00"))

    target = KeylessTargetTable(contract, [dup_row, dup_row, dup_row, other_row])
    applier = KeylessDeterministicApplier(target)

    # 1. Rung 1: canonical_full_row_multiplicity deletes 1 copy and updates 1 copy of dup_row
    updated_row = (7, "dup_event", "updated_once", decimal.Decimal("42.00"))
    receipt = applier.apply_changes(
        [
            KeylessChangeEvent(op=KeylessChangeOp.DELETE, before_row=dup_row, affected_multiplicity=1),
            KeylessChangeEvent(
                op=KeylessChangeOp.UPDATE,
                before_row=dup_row,
                after_row=updated_row,
                affected_multiplicity=1,
            ),
        ],
        {
            "synthetic_key_source_mutation": False,
            "full_before_image": True,
            "supports_single_row_limit": True,
        },
    )
    assert receipt.identity_mode == RowIdentityMode.CANONICAL_FULL_ROW_MULTIPLICITY
    assert receipt.care_badge == CareBadge.PASS
    assert receipt.resulting_row_count == 3

    # Verify exactly 1 original dup_row, 1 updated_row, and 1 other_row remain
    verifier = KeylessMultisetVerifier(contract)
    expected_after = [dup_row, updated_row, other_row]
    check = verifier.verify(
        expected_after,
        target.rows(),
        {"server_canonical_hash": True, "bucket_fits_memory": True, "memory_row_limit": 100},
    )
    assert check.verified_equal is True

    # 2. Rung 2: fenced_physical_locator_verified
    loc_new = target.insert_row(dup_row)
    loc_facts = {
        "synthetic_key_source_mutation": False,
        "full_before_image": True,
        "supports_single_row_limit": False,
        "physical_locator_available": True,
        "locator_invalidated_by_movement": False,
        "verify_row_hash_on_locator": True,
    }
    receipt_loc = applier.apply_changes(
        [
            KeylessChangeEvent(
                op=KeylessChangeOp.DELETE,
                before_row=dup_row,
                physical_locator=loc_new,
                fence_epoch=target.fence_epoch,
            )
        ],
        loc_facts,
    )
    assert receipt_loc.identity_mode == RowIdentityMode.FENCED_PHYSICAL_LOCATOR_VERIFIED
    assert receipt_loc.rows_deleted == 1

    # Simulate VACUUM / compaction moving rows -> old fence_epoch & locator must be refused
    loc_before_compact = target.insert_row(dup_row)
    old_epoch = target.fence_epoch
    target.compact_or_move_rows()
    with pytest.raises(KeylessLocatorFenceError, match="fence epoch mismatch"):
        applier.apply_changes(
            [
                KeylessChangeEvent(
                    op=KeylessChangeOp.DELETE,
                    before_row=dup_row,
                    physical_locator=loc_before_compact,
                    fence_epoch=old_epoch,
                )
            ],
            loc_facts,
        )

    # 3. Rung 3: bucket_multiset_reconcile
    canonicalizer = KeylessRowCanonicalizer(contract)
    h1, _ = canonicalizer.row_hashes(canonicalizer.canonical_row_text(dup_row))
    bucket_id = canonicalizer.bucket_for_hashes(h1, bucket_bits=4)
    bucket_facts = {
        "synthetic_key_source_mutation": False,
        "full_before_image": False,
        "supports_single_row_limit": False,
        "physical_locator_available": False,
        "transactional_bucket_replace": True,
        "bucket_reconcile_within_budget": True,
    }
    receipt_bkt = applier.apply_changes(
        [],
        bucket_facts,
        source_bucket_rows={bucket_id: [dup_row, dup_row, dup_row]},
        bucket_bits=4,
    )
    assert receipt_bkt.identity_mode == RowIdentityMode.BUCKET_MULTISET_RECONCILE
    assert receipt_bkt.rows_inserted == 3


def test_endpoint_host_validation_enforces_loopback_or_rfc1918_test_net() -> None:
    """Ensure only loopback or 10.0.0.x addresses are accepted."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.1")
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("")
