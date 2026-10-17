"""Select validation methods by what each can actually prove (Row 644: RCH36.verify-method).

Gives count, aggregate, vector, hash, key-row, external-sort, and custom-query
validation methods explicit proof strength, collision/duplicate assumptions,
canonicalization contracts, and source-cost models.

Key invariants enforced:
1. Fast signatures (`count`, `aggregate`, `vector`, `hash`) can rule in
   differences (`DIFFERENT`) or localize/prioritize exact work
   (`ESCALATE_TO_EXACT`), but can never certify stronger claims than their
   contract (specifically, count-only or summary-only unchanged totals can
   never emit a `SAME` exact-equivalence verdict).
2. Pathologies including perfect duplicates, equal-count value changes,
   floating-point rounding/representation drift, and hash ambiguity (delimiter
   collision, NULL-vs-empty conflation, weak digest bit width, or XOR
   duplicate cancellation) either fail (`DIFFERENT` / `ACTION`) or escalate
   (`ESCALATE_TO_EXACT` / `REVIEW`) to an exact method (`key_row` or
   `external_sort`).
3. Method selection runs through `migkit.decide` (`select_validation_method`)
   ranking eligible methods whose proof strength, key/multiset assumptions,
   canonicalization guarantees, and resource budgets hold by their estimated
   source and network cost.
4. Universal resilience: active verification lease lock (`.run.lock`), O(1)
   typed durable checkpoint resume (`datetime`, `Decimal`, `UUID`, composite
   tuple keys) that also verifies checkpointed proof strength meets the
   current claim, AIMD adaptive batch sizing, and cold-start byte-weighted ETA.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
import json
import math
import os
import socket
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Core Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


def is_safe_test_host(host: str) -> bool:
    """Validate that an endpoint host belongs to approved local or 10.0.0.x test ranges."""
    h = str(host or "").strip().lower()
    if h in SAFE_TEST_HOSTS:
        return True
    if h.startswith("10.0.0."):
        parts = h.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return True
    return False


class ValidationMethodKind(str, enum.Enum):
    COUNT = "count"
    AGGREGATE = "aggregate"
    VECTOR = "vector"
    HASH = "hash"
    KEY_ROW = "key_row"
    EXTERNAL_SORT = "external_sort"
    CUSTOM_QUERY = "custom_query"


class ProofStrength(str, enum.Enum):
    """Explicit lattice of what a validation method can mathematically prove."""
    UNPROVABLE = "unprovable"
    CARDINALITY_ONLY = "cardinality_only"
    PREDICATE_SUBSET_SUMMARY = "predicate_subset_summary"
    COLUMN_SUMMARY = "column_summary"
    BUCKET_SIGNATURE = "bucket_signature"
    AMBIGUOUS_SIGNATURE = "ambiguous_signature"
    DIGEST_EQUIVALENCE = "digest_equivalence"
    PREDICATE_SUBSET_EXACT = "predicate_subset_exact"
    EXACT_KEYED_EQUIVALENCE = "exact_keyed_equivalence"
    EXACT_MULTISET_EQUIVALENCE = "exact_multiset_equivalence"

    @property
    def rank(self) -> int:
        return _PROOF_STRENGTH_RANK[self]

    @property
    def is_exact_equivalence(self) -> bool:
        return self in (
            ProofStrength.EXACT_KEYED_EQUIVALENCE,
            ProofStrength.EXACT_MULTISET_EQUIVALENCE,
        )

    @property
    def is_fast_signature(self) -> bool:
        return self in (
            ProofStrength.CARDINALITY_ONLY,
            ProofStrength.PREDICATE_SUBSET_SUMMARY,
            ProofStrength.COLUMN_SUMMARY,
            ProofStrength.BUCKET_SIGNATURE,
            ProofStrength.AMBIGUOUS_SIGNATURE,
            ProofStrength.DIGEST_EQUIVALENCE,
        )


_PROOF_STRENGTH_RANK: Dict[ProofStrength, int] = {
    ProofStrength.UNPROVABLE: 0,
    ProofStrength.CARDINALITY_ONLY: 1,
    ProofStrength.PREDICATE_SUBSET_SUMMARY: 2,
    ProofStrength.COLUMN_SUMMARY: 3,
    ProofStrength.BUCKET_SIGNATURE: 4,
    ProofStrength.AMBIGUOUS_SIGNATURE: 4,
    ProofStrength.DIGEST_EQUIVALENCE: 5,
    ProofStrength.PREDICATE_SUBSET_EXACT: 6,
    ProofStrength.EXACT_KEYED_EQUIVALENCE: 7,
    ProofStrength.EXACT_MULTISET_EQUIVALENCE: 8,
}


class ClaimedGuarantee(str, enum.Enum):
    """What the caller or cutover gate asks the validation run to certify."""
    TRIAGE_CARDINALITY = "triage_cardinality"
    COLUMN_BOUNDS_AND_SUMS = "column_bounds_and_sums"
    BUCKET_LOCALIZATION = "bucket_localization"
    DIGEST_SCREENING = "digest_screening"
    SUBSET_PREDICATE_CHECK = "subset_predicate_check"
    EXACT_ROW_EQUIVALENCE = "exact_row_equivalence"
    MULTISET_EQUIVALENCE = "multiset_equivalence"

    @property
    def required_proof_strength(self) -> ProofStrength:
        return _CLAIM_TO_REQUIRED_STRENGTH[self]


_CLAIM_TO_REQUIRED_STRENGTH: Dict[ClaimedGuarantee, ProofStrength] = {
    ClaimedGuarantee.TRIAGE_CARDINALITY: ProofStrength.CARDINALITY_ONLY,
    ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS: ProofStrength.COLUMN_SUMMARY,
    ClaimedGuarantee.BUCKET_LOCALIZATION: ProofStrength.BUCKET_SIGNATURE,
    ClaimedGuarantee.DIGEST_SCREENING: ProofStrength.DIGEST_EQUIVALENCE,
    ClaimedGuarantee.SUBSET_PREDICATE_CHECK: ProofStrength.PREDICATE_SUBSET_EXACT,
    ClaimedGuarantee.EXACT_ROW_EQUIVALENCE: ProofStrength.EXACT_KEYED_EQUIVALENCE,
    ClaimedGuarantee.MULTISET_EQUIVALENCE: ProofStrength.EXACT_MULTISET_EQUIVALENCE,
}


class ValidationVerdict(str, enum.Enum):
    """Outcome of a validation execution.

    Note: `SAME` is strictly reserved for exact row/multiset equivalence
    certified by a method whose contract proves `EXACT_KEYED_EQUIVALENCE` or
    `EXACT_MULTISET_EQUIVALENCE`. Fast signatures can never produce `SAME`.
    """
    SAME = "same"
    DIFFERENT = "different"
    ESCALATE_TO_EXACT = "escalate_to_exact"
    TRIAGE_COUNT_MATCH = "triage_count_match"
    SIGNATURE_MATCH_NOT_SAME = "signature_match_not_same"
    SUBSET_MATCH_NOT_WHOLE = "subset_match_not_whole"


class PathologyKind(str, enum.Enum):
    NONE = "none"
    COUNT_ONLY_FOR_EXACT_CLAIM = "count_only_for_exact_claim"
    PERFECT_DUPLICATES = "perfect_duplicates"
    EQUAL_COUNT_VALUE_CHANGE = "equal_count_value_change"
    FLOATING_ROUNDING = "floating_rounding"
    HASH_DELIMITER_AMBIGUITY = "hash_delimiter_ambiguity"
    HASH_NULL_EMPTY_AMBIGUITY = "hash_null_empty_ambiguity"
    HASH_WEAK_WIDTH_COLLISION = "hash_weak_width_collision"
    HASH_XOR_DUPLICATE_CANCELLATION = "hash_xor_duplicate_cancellation"
    CUSTOM_QUERY_INCOMPLETE_SCOPE = "custom_query_incomplete_scope"


class FloatCanonicalPolicy(str, enum.Enum):
    NONE = "none"
    RAW_ENGINE_TEXT = "raw_engine_text"
    FIXED_DECIMAL_CLAMPED = "fixed_decimal_clamped"
    BANDED_EXACT_TEXT = "banded_exact_text"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class VerifyMethodError(RuntimeError):
    """Base error for validation method selection and proof-strength enforcement."""


class UnsafeEndpointHostError(VerifyMethodError, ValueError):
    """Raised when an endpoint host is outside approved safe test ranges."""


class ContractClaimViolationError(VerifyMethodError):
    """Raised when a fast signature or partial method attempts to certify a stronger claim."""


classVerificationLeaseError = VerifyMethodError


class VerificationLeaseError(VerifyMethodError):
    """Raised when a verification lease lock cannot be acquired or is lost."""


class VerificationLeaseExpiredError(VerificationLeaseError):
    """Raised when an active verification lease has expired."""


# -----------------------------------------------------------------------------
# Canonicalization, Collision/Duplicate Assumptions, and Cost Models
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class CanonicalizationModel:
    """Explicit canonicalization contract for a validation method."""
    requires_canonical_projection: bool
    float_policy: FloatCanonicalPolicy
    null_distinct_from_empty: bool
    length_prefixed_framing: bool
    timezone_normalized_utc: bool
    collation_deterministic_bytes: bool
    unicode_nfc_normalized: bool

    @property
    def is_hash_unambiguous(self) -> bool:
        return (
            self.requires_canonical_projection
            and self.float_policy == FloatCanonicalPolicy.BANDED_EXACT_TEXT
            and self.null_distinct_from_empty
            and self.length_prefixed_framing
            and self.timezone_normalized_utc
            and self.collation_deterministic_bytes
        )

    @property
    def is_float_rounding_safe(self) -> bool:
        return self.float_policy == FloatCanonicalPolicy.BANDED_EXACT_TEXT


@dataclasses.dataclass(frozen=True)
class CollisionDuplicateAssumptions:
    """Explicit collision and duplicate-handling assumptions of a validation method."""
    digest_bits: int
    collision_probability_bound: float
    handles_perfect_duplicates: bool
    handles_equal_count_value_changes: bool
    handles_zero_sum_cancelling_swaps: bool
    requires_unique_key: bool
    xor_cancellation_vulnerable: bool
    order_independent_fold: bool


@dataclasses.dataclass(frozen=True)
class TableVerificationProfile:
    """Observed table characteristics driving method eligibility and cost estimation."""
    table_name: str
    estimated_rows: int
    estimated_bytes: int
    column_count: int = 8
    has_unique_key: bool = True
    has_duplicate_rows: bool = False
    has_float_columns: bool = False
    has_nullable_text_columns: bool = True
    has_json_or_lob_columns: bool = False
    bucket_count: int = 64
    available_memory_bytes: int = 256 * 1024 * 1024
    available_spill_disk_bytes: int = 10 * 1024 * 1024 * 1024
    source_host: str = "127.0.0.1"
    target_host: str = "127.0.0.1"

    def validate_hosts(self) -> None:
        if not is_safe_test_host(self.source_host):
            raise UnsafeEndpointHostError(
                f"[ACTION] Unsafe source host {self.source_host!r}; only 127.0.0.1 or 10.0.0.x allowed"
            )
        if not is_safe_test_host(self.target_host):
            raise UnsafeEndpointHostError(
                f"[ACTION] Unsafe target host {self.target_host!r}; only 127.0.0.1 or 10.0.0.x allowed"
            )


@dataclasses.dataclass(frozen=True)
class MethodCostEstimate:
    """Estimated source, network, memory, and disk spill cost for a validation method."""
    method: ValidationMethodKind
    source_scan_bytes: int
    source_cpu_seconds: float
    network_transfer_bytes: int
    memory_working_set_bytes: int
    disk_spill_bytes: int
    estimated_wall_seconds: float
    total_weighted_cost: float


@dataclasses.dataclass(frozen=True)
class SourceCostModel:
    """Parametric source-load, network, and spill cost model for a validation method."""
    scan_byte_factor: float
    cpu_ns_per_row_per_col: float
    network_bytes_per_row: float
    fixed_network_bytes: int
    memory_factor_of_table: float
    spill_factor_of_table: float
    supports_index_only_scan: bool
    supports_range_pushdown: bool
    source_io_mb_per_sec: float = 250.0
    network_mb_per_sec: float = 125.0
    spill_io_mb_per_sec: float = 150.0

    def estimate(self, method: ValidationMethodKind, profile: TableVerificationProfile) -> MethodCostEstimate:
        rows = max(1, int(profile.estimated_rows))
        table_bytes = max(1, int(profile.estimated_bytes))
        cols = max(1, int(profile.column_count))

        if self.supports_index_only_scan and profile.has_unique_key:
            scan_bytes = int(table_bytes * min(self.scan_byte_factor, 0.15))
        else:
            scan_bytes = int(table_bytes * self.scan_byte_factor)

        cpu_seconds = (rows * cols * self.cpu_ns_per_row_per_col) / 1e9

        if method == ValidationMethodKind.VECTOR:
            net_bytes = self.fixed_network_bytes + max(1, profile.bucket_count) * 128
        else:
            net_bytes = self.fixed_network_bytes + int(rows * self.network_bytes_per_row)

        mem_bytes = min(
            int(table_bytes * self.memory_factor_of_table),
            max(64 * 1024, profile.available_memory_bytes),
        )
        raw_working_bytes = int(table_bytes * self.spill_factor_of_table)
        if self.spill_factor_of_table > 0.0:
            spill_bytes = max(0, raw_working_bytes - profile.available_memory_bytes)
            if method == ValidationMethodKind.EXTERNAL_SORT:
                # External sort requires staging canonical runs even when partially buffered
                spill_bytes = max(spill_bytes, int(table_bytes * 0.5))
        else:
            spill_bytes = 0

        io_sec = scan_bytes / (self.source_io_mb_per_sec * 1024.0 * 1024.0)
        net_sec = net_bytes / (self.network_mb_per_sec * 1024.0 * 1024.0)
        spill_sec = spill_bytes / (self.spill_io_mb_per_sec * 1024.0 * 1024.0)
        wall_sec = io_sec + cpu_seconds + net_sec + spill_sec

        # Weighted cost penalizes heavy source scan, network transfer, and disk spill
        weighted_cost = (
            io_sec * 1.5
            + cpu_seconds * 1.2
            + net_sec * 1.3
            + spill_sec * 2.0
        )
        return MethodCostEstimate(
            method=method,
            source_scan_bytes=scan_bytes,
            source_cpu_seconds=round(cpu_seconds, 6),
            network_transfer_bytes=net_bytes,
            memory_working_set_bytes=mem_bytes,
            disk_spill_bytes=spill_bytes,
            estimated_wall_seconds=round(wall_sec, 6),
            total_weighted_cost=round(weighted_cost, 6),
        )


@dataclasses.dataclass(frozen=True)
class CustomQueryContract:
    """Explicit contract for a custom-query validation method."""
    query_id: str
    source_sql: str
    target_sql: str
    covers_all_rows: bool = False
    projects_all_columns: bool = False
    returns_row_level_tuples: bool = False
    has_unique_key_in_projection: bool = False
    preserves_duplicate_multiplicity: bool = False
    uses_canonical_Initial_rendering: bool = True
    is_deterministic: bool = True

    @property
    def effective_proof_strength(self) -> ProofStrength:
        if not self.is_deterministic or not self.uses_canonical_Initial_rendering:
            return ProofStrength.UNPROVABLE
        if self.covers_all_rows and self.projects_all_columns and self.returns_row_level_tuples:
            if self.preserves_duplicate_multiplicity:
                return ProofStrength.EXACT_MULTISET_EQUIVALENCE
            if self.has_unique_key_in_projection:
                return ProofStrength.EXACT_KEYED_EQUIVALENCE
            return ProofStrength.DIGEST_EQUIVALENCE
        if self.returns_row_level_tuples:
            return ProofStrength.PREDICATE_SUBSET_EXACT
        return ProofStrength.PREDICATE_SUBSET_SUMMARY


@dataclasses.dataclass(frozen=True)
class ValidationMethodSpec:
    """Complete formal specification of a validation method and what it can prove."""
    method: ValidationMethodKind
    proof_strength: ProofStrength
    provable_claims: FrozenSet[ClaimedGuarantee]
    forbidden_claims: FrozenSet[ClaimedGuarantee]
    collision_assumptions: CollisionDuplicateAssumptions
    canonicalization: CanonicalizationModel
    cost_model: SourceCostModel
    can_rule_in_differences: bool = True
    can_prioritize_exact_work: bool = True

    def can_certify(self, claim: ClaimedGuarantee) -> bool:
        if claim in self.forbidden_claims:
            return False
        if claim not in self.provable_claims:
            return False
        return self.proof_strength.rank >= claim.required_proof_strength.rank


# -----------------------------------------------------------------------------
# Registry of the 7 Validation Method Specifications
# -----------------------------------------------------------------------------

METHOD_SPECS: Dict[ValidationMethodKind, ValidationMethodSpec] = {
    ValidationMethodKind.COUNT: ValidationMethodSpec(
        method=ValidationMethodKind.COUNT,
        proof_strength=ProofStrength.CARDINALITY_ONLY,
        provable_claims=frozenset({ClaimedGuarantee.TRIAGE_CARDINALITY}),
        forbidden_claims=frozenset({
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.BUCKET_LOCALIZATION,
            ClaimedGuarantee.DIGEST_SCREENING,
            ClaimedGuarantee.SUBSET_PREDICATE_CHECK,
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=0,
            collision_probability_bound=1.0,
            handles_perfect_duplicates=False,
            handles_equal_count_value_changes=False,
            handles_zero_sum_cancelling_swaps=False,
            requires_unique_key=False,
            xor_cancellation_vulnerable=False,
            order_independent_fold=True,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=False,
            float_policy=FloatCanonicalPolicy.NONE,
            null_distinct_from_empty=False,
            length_prefixed_framing=False,
            timezone_normalized_utc=False,
            collation_deterministic_bytes=False,
            unicode_nfc_normalized=False,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=0.10,
            cpu_ns_per_row_per_col=2.0,
            network_bytes_per_row=0.0,
            fixed_network_bytes=64,
            memory_factor_of_table=0.001,
            spill_factor_of_table=0.0,
            supports_index_only_scan=True,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=True,
    ),
    ValidationMethodKind.AGGREGATE: ValidationMethodSpec(
        method=ValidationMethodKind.AGGREGATE,
        proof_strength=ProofStrength.COLUMN_SUMMARY,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
        }),
        forbidden_claims=frozenset({
            ClaimedGuarantee.BUCKET_LOCALIZATION,
            ClaimedGuarantee.DIGEST_SCREENING,
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=64,
            collision_probability_bound=1e-6,
            handles_perfect_duplicates=False,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=False,
            requires_unique_key=False,
            xor_cancellation_vulnerable=True,
            order_independent_fold=True,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=False,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=1.0,
            cpu_ns_per_row_per_col=18.0,
            network_bytes_per_row=0.0,
            fixed_network_bytes=512,
            memory_factor_of_table=0.005,
            spill_factor_of_table=0.0,
            supports_index_only_scan=False,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=True,
    ),
    ValidationMethodKind.VECTOR: ValidationMethodSpec(
        method=ValidationMethodKind.VECTOR,
        proof_strength=ProofStrength.BUCKET_SIGNATURE,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.BUCKET_LOCALIZATION,
        }),
        forbidden_claims=frozenset({
            ClaimedGuarantee.DIGEST_SCREENING,
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=64,
            collision_probability_bound=2.0 ** -64,
            handles_perfect_duplicates=False,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=True,
            requires_unique_key=False,
            xor_cancellation_vulnerable=False,
            order_independent_fold=True,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=True,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=1.0,
            cpu_ns_per_row_per_col=28.0,
            network_bytes_per_row=0.0,
            fixed_network_bytes=4096,
            memory_factor_of_table=0.01,
            spill_factor_of_table=0.0,
            supports_index_only_scan=False,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=True,
    ),
    ValidationMethodKind.HASH: ValidationMethodSpec(
        method=ValidationMethodKind.HASH,
        proof_strength=ProofStrength.DIGEST_EQUIVALENCE,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.BUCKET_LOCALIZATION,
            ClaimedGuarantee.DIGEST_SCREENING,
        }),
        forbidden_claims=frozenset({
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=256,
            collision_probability_bound=2.0 ** -128,
            handles_perfect_duplicates=True,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=True,
            requires_unique_key=False,
            xor_cancellation_vulnerable=False,
            order_independent_fold=True,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=True,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=1.0,
            cpu_ns_per_row_per_col=45.0,
            network_bytes_per_row=0.5,
            fixed_network_bytes=2048,
            memory_factor_of_table=0.02,
            spill_factor_of_table=0.0,
            supports_index_only_scan=False,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=True,
    ),
    ValidationMethodKind.KEY_ROW: ValidationMethodSpec(
        method=ValidationMethodKind.KEY_ROW,
        proof_strength=ProofStrength.EXACT_KEYED_EQUIVALENCE,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.BUCKET_LOCALIZATION,
            ClaimedGuarantee.DIGEST_SCREENING,
            ClaimedGuarantee.SUBSET_PREDICATE_CHECK,
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
        }),
        forbidden_claims=frozenset({
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=0,
            collision_probability_bound=0.0,
            handles_perfect_duplicates=False,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=True,
            requires_unique_key=True,
            xor_cancellation_vulnerable=False,
            order_independent_fold=False,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=True,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=1.15,
            cpu_ns_per_row_per_col=55.0,
            network_bytes_per_row=128.0,
            fixed_network_bytes=1024,
            memory_factor_of_table=0.05,
            spill_factor_of_table=0.0,
            supports_index_only_scan=False,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=False,
    ),
    ValidationMethodKind.EXTERNAL_SORT: ValidationMethodSpec(
        method=ValidationMethodKind.EXTERNAL_SORT,
        proof_strength=ProofStrength.EXACT_MULTISET_EQUIVALENCE,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.BUCKET_LOCALIZATION,
            ClaimedGuarantee.DIGEST_SCREENING,
            ClaimedGuarantee.SUBSET_PREDICATE_CHECK,
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        forbidden_claims=frozenset(),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=0,
            collision_probability_bound=0.0,
            handles_perfect_duplicates=True,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=True,
            requires_unique_key=False,
            xor_cancellation_vulnerable=False,
            order_independent_fold=False,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=True,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=1.25,
            cpu_ns_per_row_per_col=95.0,
            network_bytes_per_row=140.0,
            fixed_network_bytes=2048,
            memory_factor_of_table=0.25,
            spill_factor_of_table=1.10,
            supports_index_only_scan=False,
            supports_range_pushdown=False,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=False,
    ),
    ValidationMethodKind.CUSTOM_QUERY: ValidationMethodSpec(
        method=ValidationMethodKind.CUSTOM_QUERY,
        proof_strength=ProofStrength.PREDICATE_SUBSET_EXACT,
        provable_claims=frozenset({
            ClaimedGuarantee.TRIAGE_CARDINALITY,
            ClaimedGuarantee.COLUMN_BOUNDS_AND_SUMS,
            ClaimedGuarantee.SUBSET_PREDICATE_CHECK,
        }),
        forbidden_claims=frozenset({
            ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
            ClaimedGuarantee.MULTISET_EQUIVALENCE,
        }),
        collision_assumptions=CollisionDuplicateAssumptions(
            digest_bits=0,
            collision_probability_bound=0.0,
            handles_perfect_duplicates=False,
            handles_equal_count_value_changes=True,
            handles_zero_sum_cancelling_swaps=True,
            requires_unique_key=False,
            xor_cancellation_vulnerable=False,
            order_independent_fold=False,
        ),
        canonicalization=CanonicalizationModel(
            requires_canonical_projection=True,
            float_policy=FloatCanonicalPolicy.BANDED_EXACT_TEXT,
            null_distinct_from_empty=True,
            length_prefixed_framing=True,
            timezone_normalized_utc=True,
            collation_deterministic_bytes=True,
            unicode_nfc_normalized=True,
        ),
        cost_model=SourceCostModel(
            scan_byte_factor=0.80,
            cpu_ns_per_row_per_col=50.0,
            network_bytes_per_row=64.0,
            fixed_network_bytes=1024,
            memory_factor_of_table=0.05,
            spill_factor_of_table=0.0,
            supports_index_only_scan=False,
            supports_range_pushdown=True,
        ),
        can_rule_in_differences=True,
        can_prioritize_exact_work=True,
    ),
}


def get_method_spec(method: Union[ValidationMethodKind, str]) -> ValidationMethodSpec:
    kind = ValidationMethodKind(method) if not isinstance(method, ValidationMethodKind) else method
    return METHOD_SPECS[kind]


# -----------------------------------------------------------------------------
# Canonical Value & Row Framing (Eliminating Float & Hash Ambiguities)
# -----------------------------------------------------------------------------

# Band boundaries matching migkit.canon: fixed decimal inside [1e-20, 1e45),
# shortest exact scientific notation outside that band.
FLOAT_DECIMAL_MIN = 1e-20
FLOAT_DECIMAL_MAX = 1e45


def canonicalize_float_value(val: float) -> Tuple[str, bool]:
    """Render a floating-point number into an engine-neutral canonical string.

    Returns `(canonical_str, has_non_associative_or_extreme_band)`.
    """
    if math.isnan(val):
        return "uncomparable:NaN", True
    if math.isinf(val):
        return ("uncomparable:+Inf" if val > 0 else "uncomparable:-Inf"), True
    if val == 0.0:
        return "0.00000000000000000000", False
    abs_val = abs(val)
    if abs_val < FLOAT_DECIMAL_MIN or abs_val >= FLOAT_DECIMAL_MAX:
        # Outer band: use exact repr and normalize exponent sign (`e+20` -> `e20`)
        raw = repr(float(val)).lower().replace("e+", "e")
        return f"band_outer:{raw}", True
    # Inner band: exact Decimal conversion rounded to 15 significant IEEE-754 digits
    dec = decimal.Decimal(f"{float(val):.15g}")
    return f"{dec:.20f}", False


def canonicalize_cell(val: Any, float_policy: FloatCanonicalPolicy = FloatCanonicalPolicy.BANDED_EXACT_TEXT) -> str:
    """Encode a single cell with explicit type tag, NULL discriminator, and length prefix."""
    if val is None:
        return "N:0:"
    if isinstance(val, bool):
        payload = "true" if val else "false"
        return f"B:{len(payload)}:{payload}"
    if isinstance(val, int):
        payload = str(val)
        return f"I:{len(payload)}:{payload}"
    if isinstance(val, decimal.Decimal):
        if val.is_nan():
            payload = "uncomparable:DecimalNaN"
        elif val.is_infinite():
            payload = "uncomparable:+DecimalInf" if val > 0 else "uncomparable:-DecimalInf"
        else:
            norm = val.normalize()
            payload = format(norm, "f")
        return f"D:{len(payload)}:{payload}"
    if isinstance(val, float):
        if float_policy == FloatCanonicalPolicy.RAW_ENGINE_TEXT:
            payload = str(val)
        elif float_policy == FloatCanonicalPolicy.FIXED_DECIMAL_CLAMPED:
            # Simulates MySQL DECIMAL(65,20) clamping above 1e45 and underflow below 1e-20
            if math.isnan(val) or math.isinf(val):
                payload = str(val)
            elif abs(val) >= FLOAT_DECIMAL_MAX:
                sign = "-" if val < 0 else ""
                payload = f"{sign}{'9' * 45}.{'9' * 20}"
            elif abs(val) < FLOAT_DECIMAL_MIN:
                payload = "0." + ("0" * 20)
            else:
                payload = f"{val:.20f}"
        else:
            payload, _ = canonicalize_float_value(val)
        return f"F:{len(payload)}:{payload}"
    if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
        if isinstance(val, datetime.datetime) and val.tzinfo is not None:
            val = val.astimezone(datetime.timezone.utc)
        payload = val.isoformat()
        return f"T:{len(payload)}:{payload}"
    if isinstance(val, uuid.UUID):
        payload = str(val).lower()
        return f"U:{len(payload)}:{payload}"
    if isinstance(val, (bytes, bytearray)):
        payload = bytes(val).hex()
        return f"X:{len(payload)}:{payload}"
    if isinstance(val, (dict, list)):
        payload = json.dumps(val, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return f"J:{len(payload)}:{payload}"
    payload = str(val)
    return f"S:{len(payload)}:{payload}"


def canonicalize_row(
    row: Union[Sequence[Any], Dict[str, Any]],
    columns: Optional[Sequence[str]] = None,
    *,
    length_prefixed: bool = True,
    null_distinct_from_empty: bool = True,
    float_policy: FloatCanonicalPolicy = FloatCanonicalPolicy.BANDED_EXACT_TEXT,
) -> str:
    """Render a row into a canonical string.

    When `length_prefixed=True` and `null_distinct_from_empty=True`, cell
    boundaries and NULLs are unambiguous (`("ab", "c") != ("a", "bc")` and
    `(None,) != ("",)`). When disabled (to model naive hash configurations),
    demonstrates why naive concatenation fails and triggers escalation.
    """
    if isinstance(row, dict):
        keys = list(columns) if columns is not None else sorted(row.keys())
        values = [row.get(k) for k in keys]
    else:
        values = list(row)

    if length_prefixed and null_distinct_from_empty:
        parts = [canonicalize_cell(v, float_policy=float_policy) for v in values]
        return "|".join(parts)

    # Naive unframed rendering used to detect/demonstrate hash ambiguity
    naive_parts: List[str] = []
    for v in values:
        if v is None:
            naive_parts.append("<NULL>" if null_distinct_from_empty else "")
        elif isinstance(v, float):
            if float_policy == FloatCanonicalPolicy.BANDED_EXACT_TEXT:
                s, _ = canonicalize_float_value(v)
                naive_parts.append(s)
            else:
                naive_parts.append(str(v))
        else:
            naive_parts.append(str(v))
    return "|".join(naive_parts) if length_prefixed else "".join(naive_parts)


def digest_canonical_row(canonical_row: str, digest_bits: int = 256) -> str:
    """Compute a hex digest of a canonical row with explicit bit-width."""
    raw = hashlib.sha256(canonical_row.encode("utf-8")).digest()
    if digest_bits <= 32:
        return raw[:4].hex()
    if digest_bits <= 64:
        return raw[:8].hex()
    if digest_bits <= 128:
        return raw[:16].hex()
    return raw.hex()


# -----------------------------------------------------------------------------
# Decision Engine Rung Ladder (`select_validation_method`)
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class MethodSelectionResult:
    """Result of climbing the validation method decision ladder."""
    selected_method: Optional[ValidationMethodKind]
    selected_rung: str
    required_claim: ClaimedGuarantee
    required_proof_strength: ProofStrength
    achieved_proof_strength: ProofStrength
    cost_estimate: Optional[MethodCostEstimate]
    care_badge: CareBadge
    reason: str
    choice: decide.Choice


def select_validation_method(
    profile: TableVerificationProfile,
    required_claim: ClaimedGuarantee = ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
    *,
    requires_value_sensitivity: bool = True,
    float_rounding_safe: bool = True,
    has_duplicate_cancellation_risk: bool = False,
    supports_bucket_projection: bool = True,
    hash_unambiguous: bool = True,
    custom_query_contract: Optional[CustomQueryContract] = None,
    canonical_row_projection_ready: bool = True,
) -> MethodSelectionResult:
    """Select a validation method via `migkit.decide` based on what each can actually prove.

    Every rung states its proof-strength, duplicate/collision, canonicalization,
    and resource budget needs explicitly. Among all rungs whose needs hold,
    `decide.climb` picks the lowest-cost method.
    """
    profile.validate_hosts()
    req_strength = required_claim.required_proof_strength
    req_rank = req_strength.rank

    # Multiplicity is preserved for hash only when there are no duplicate rows
    # or when hash folding uses multiset counters rather than XOR cancellation.
    multiplicity_preserved = (
        (profile.has_unique_key and not profile.has_duplicate_rows)
        or not has_duplicate_cancellation_risk
    )
    has_strict_unique_key = bool(profile.has_unique_key and not profile.has_duplicate_rows)

    cq_present = custom_query_contract is not None and custom_query_contract.is_deterministic
    cq_strength = (
        custom_query_contract.effective_proof_strength
        if custom_query_contract is not None
        else ProofStrength.UNPROVABLE
    )
    cq_meets_rank = (
        cq_present
        and cq_strength.rank >= req_rank
        and (
            required_claim not in (ClaimedGuarantee.EXACT_ROW_EQUIVALENCE, ClaimedGuarantee.MULTISET_EQUIVALENCE)
            or (custom_query_contract is not None and custom_query_contract.covers_all_rows and custom_query_contract.projects_all_columns)
        )
    )

    estimates: Dict[ValidationMethodKind, MethodCostEstimate] = {
        kind: spec.cost_model.estimate(kind, profile)
        for kind, spec in METHOD_SPECS.items()
    }
    ext_sort_est = estimates[ValidationMethodKind.EXTERNAL_SORT]
    spill_within_budget = (
        ext_sort_est.disk_spill_bytes <= profile.available_spill_disk_bytes
        and canonical_row_projection_ready
    )

    rungs = (
        decide.Rung(
            "count",
            "table or range row count check (COUNT(*)) for cardinality triage only",
            needs=(
                decide.Need(
                    "claim requires at most cardinality screening",
                    lambda f: f["req_rank"] <= ProofStrength.CARDINALITY_ONLY.rank,
                    "count-only cannot prove value equivalence, column summaries, or multiset identity",
                ),
                decide.Need(
                    "caller does not require equal-count value change sensitivity",
                    lambda f: not f["requires_value_sensitivity"],
                    "count-only is blind to equal-count value changes and compensated duplicates",
                ),
            ),
            because="cheapest metadata or index-only scan when only row cardinality triage is requested",
        ),
        decide.Rung(
            "aggregate",
            "fused column aggregate check (COUNT, SUM, MIN, MAX) for summary screening",
            needs=(
                decide.Need(
                    "claim requires at most column summary strength",
                    lambda f: f["req_rank"] <= ProofStrength.COLUMN_SUMMARY.rank,
                    "column aggregates cannot prove row-level or multiset equivalence",
                ),
                decide.Need(
                    "floating-point columns are safe from non-associative summation rounding drift",
                    lambda f: f["float_rounding_safe"],
                    "floating-point rounding drift makes aggregate SUM/AVG ambiguous across engines",
                ),
                decide.Need(
                    "no duplicate XOR cancellation or zero-sum swap exposure",
                    lambda f: not f["has_duplicate_cancellation_risk"],
                    "duplicate XOR cancellation or compensating swaps can mask row changes in aggregates",
                ),
            ),
            because="single fused scan summarizes numeric bounds and totals without streaming rows",
        ),
        decide.Rung(
            "vector",
            "partitioned bucket vector signature check for difference localization",
            needs=(
                decide.Need(
                    "claim requires at most bucket signature strength",
                    lambda f: f["req_rank"] <= ProofStrength.BUCKET_SIGNATURE.rank,
                    "bucket vector signatures localize dirty ranges but cannot certify exact equivalence",
                ),
                decide.Need(
                    "both endpoints support deterministic bucket projection",
                    lambda f: f["supports_bucket_projection"],
                    "endpoints lack deterministic bucket projection",
                ),
                decide.Need(
                    "canonical float projection before bucket folding",
                    lambda f: f["float_rounding_safe"],
                    "uncanonicalized floats cause false bucket divergence or rounding masking",
                ),
            ),
            because="narrows differences to specific buckets in one pass to prioritize exact verification",
        ),
        decide.Rung(
            "hash",
            "canonical length-prefixed cryptographic range/row digest screening",
            needs=(
                decide.Need(
                    "claim requires at most digest equivalence screening",
                    lambda f: f["req_rank"] <= ProofStrength.DIGEST_EQUIVALENCE.rank,
                    "fast hash signatures cannot certify exact row or multiset equivalence on their own",
                ),
                decide.Need(
                    "hash framing is unambiguous (length-prefixed, NULL-tagged, >=128-bit, banded floats)",
                    lambda f: f["hash_unambiguous"] and f["float_rounding_safe"],
                    "hash has delimiter, NULL-vs-empty, float rounding, or weak bit-width ambiguity",
                ),
                decide.Need(
                    "duplicate row multiplicity is preserved without XOR cancellation",
                    lambda f: f["multiplicity_preserved"],
                    "duplicate rows cancel under order-independent XOR hash folding",
                ),
            ),
            because="strong cryptographic screening with minimal network bytes before exact row verification",
        ),
        decide.Rung(
            "custom_query",
            "contract-scoped custom query validation",
            needs=(
                decide.Need(
                    "a deterministic custom query contract is provided",
                    lambda f: f["cq_present"],
                    "no deterministic custom query contract was provided",
                ),
                decide.Need(
                    "custom query contract covers required proof strength, rows, and columns",
                    lambda f: f["cq_meets_rank"],
                    "custom query contract covers only a predicate subset or summary below required proof rank",
                ),
            ),
            because="executes operator-specified domain query when its contract matches required proof strength",
        ),
        decide.Rung(
            "key_row",
            "key-ordered canonical row-by-row exact comparison over unique key ranges",
            needs=(
                decide.Need(
                    "claim requires at most exact keyed row equivalence",
                    lambda f: f["req_rank"] <= ProofStrength.EXACT_KEYED_EQUIVALENCE.rank,
                    "key-row comparison requires unique keys and cannot prove keyless multiset multiplicity",
                ),
                decide.Need(
                    "table has an enforced unique key without duplicate keys",
                    lambda f: f["has_strict_unique_key"],
                    "table is keyless or contains duplicate rows requiring external sort multiset proof",
                ),
                decide.Need(
                    "canonical row projection and banded float rendering are ready",
                    lambda f: f["canonical_row_projection_ready"] and f["float_rounding_safe"],
                    "canonical row projection is unavailable or float rounding is uncanonicalized",
                ),
            ),
            because="proves exact row-by-row equivalence in O(1) memory without external disk sort spill",
        ),
        decide.Rung(
            "external_sort",
            "deterministic external merge-sort over canonicalized full-row tuples with multiplicity count",
            needs=(
                decide.Need(
                    "external sort spill fits within available disk budget and canonical projection is ready",
                    lambda f: f["spill_within_budget"] and f["float_rounding_safe"],
                    "external sort spill exceeds disk budget or canonical float projection is unsafe",
                ),
            ),
            because="proves exact multiset equivalence including keyless tables and perfect duplicate multiplicities",
        ),
        decide.Rung(
            "escalate_unprovable",
            "refuse certification and escalate when no method can prove the requested guarantee",
            needs=(),
            because="never certify a stronger claim than available validation contracts can prove",
        ),
    )

    facts = decide.Facts({
        "req_rank": req_rank,
        "requires_value_sensitivity": bool(requires_value_sensitivity),
        "float_rounding_safe": bool(float_rounding_safe),
        "has_duplicate_cancellation_risk": bool(has_duplicate_cancellation_risk),
        "supports_bucket_projection": bool(supports_bucket_projection),
        "hash_unambiguous": bool(hash_unambiguous),
        "multiplicity_preserved": bool(multiplicity_preserved),
        "cq_present": bool(cq_present),
        "cq_meets_rank": bool(cq_meets_rank),
        "has_strict_unique_key": bool(has_strict_unique_key),
        "canonical_row_projection_ready": bool(canonical_row_projection_ready),
        "spill_within_budget": bool(spill_within_budget),
    })

    costs: Dict[str, float] = {
        "count": estimates[ValidationMethodKind.COUNT].total_weighted_cost,
        "aggregate": estimates[ValidationMethodKind.AGGREGATE].total_weighted_cost,
        "vector": estimates[ValidationMethodKind.VECTOR].total_weighted_cost,
        "hash": estimates[ValidationMethodKind.HASH].total_weighted_cost,
        "custom_query": estimates[ValidationMethodKind.CUSTOM_QUERY].total_weighted_cost,
        "key_row": estimates[ValidationMethodKind.KEY_ROW].total_weighted_cost,
        "external_sort": estimates[ValidationMethodKind.EXTERNAL_SORT].total_weighted_cost,
        "escalate_unprovable": 1e12,
    }

    choice = decide.climb("validation method by proof strength", rungs, facts, costs=costs)
    chosen_name = choice.rung.name if choice.rung else "escalate_unprovable"

    if chosen_name == "escalate_unprovable":
        return MethodSelectionResult(
            selected_method=None,
            selected_rung="escalate_unprovable",
            required_claim=required_claim,
            required_proof_strength=req_strength,
            achieved_proof_strength=ProofStrength.UNPROVABLE,
            cost_estimate=None,
            care_badge=CareBadge.ACTION,
            reason=(
                f"[ACTION] No validation method can prove {required_claim.value} "
                f"(required={req_strength.value}) under current table/canonicalization constraints"
            ),
            choice=choice,
        )

    method_kind = ValidationMethodKind(chosen_name)
    spec = METHOD_SPECS[method_kind]
    achieved = (
         custom_query_contract.effective_proof_strength
        if method_kind == ValidationMethodKind.CUSTOM_QUERY and custom_query_contract is not None
        else spec.proof_strength
    )
    est = estimates[method_kind]
    badge = CareBadge.PASS if achieved.rank >= req_rank else CareBadge.REVIEW
    return MethodSelectionResult(
        selected_method=method_kind,
        selected_rung=chosen_name,
        required_claim=required_claim,
        required_proof_strength=req_strength,
        achieved_proof_strength=achieved,
        cost_estimate=est,
        care_badge=badge,
        reason=(
            f"[{badge.value}] Selected {method_kind.value} (proves={achieved.value}, "
            f"required={req_strength.value}, cost={est.total_weighted_cost:.4f}, "
            f"wall={est.estimated_wall_seconds:.4f}s)"
        ),
        choice=choice,
    )


# -----------------------------------------------------------------------------
# Execution Engine & Adversarial Pathology Verifier
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class BucketSignature:
    """Per-bucket signature used by `VECTOR` validation to localize dirty ranges."""
    bucket_id: int
    row_count: int
    numeric_sum: decimal.Decimal
    min_canonical: Optional[str]
    max_canonical: Optional[str]
    multiset_digest: str


@dataclasses.dataclass(frozen=True)
class ValidationExecutionReport:
    """Outcome of running a validation method against source and target datasets."""
    method: ValidationMethodKind
    required_claim: ClaimedGuarantee
    achieved_proof_strength: ProofStrength
    verdict: ValidationVerdict
    care_badge: CareBadge
    certified_same: bool
    needs_escalation: bool
    recommended_escalation_method: Optional[ValidationMethodKind]
    pathology_detected: PathologyKind
    source_row_count: int
    target_row_count: int
    differing_bucket_ids: Tuple[int, ...] = ()
    differing_keys_or_rows: Tuple[str, ...] = ()
    summary_message: str = ""


def _detect_float_rounding_pathology(
    source_rows: Sequence[Dict[str, Any]],
    target_rows: Sequence[Dict[str, Any]],
    float_policy: FloatCanonicalPolicy,
) -> Tuple[bool, List[str]]:
    """Detect floating-point rounding drift, outer-band clamping, or non-associative sum drift."""
    anomalies: List[str] = []
    for side_name, rows in (("source", source_rows), ("target", target_rows)):
        for idx, r in enumerate(rows):
            for col, val in sorted(r.items()):
                if isinstance(val, float):
                    if math.isnan(val) or math.isinf(val):
                        anomalies.append(f"{side_name}[{idx}].{col}={val!r}")
                    elif float_policy == FloatCanonicalPolicy.FIXED_DECIMAL_CLAMPED and (
                        abs(val) >= FLOAT_DECIMAL_MAX or (val != 0.0 and abs(val) < FLOAT_DECIMAL_MIN)
                    ):
                        anomalies.append(f"{side_name}[{idx}].{col}={val!r} clamped in fixed decimal")
                    elif float_policy == FloatCanonicalPolicy.RAW_ENGINE_TEXT:
                        # Raw engine text is vulnerable to 1e20 vs 1e+20 and precision drift
                        anomalies.append(f"{side_name}[{idx}].{col}={val!r} uncanonicalized float text")

    # Also detect when naive float sum matches or drifts due to non-associativity or tiny rounding
    if len(source_rows) == len(target_rows):
        for idx, (sr, tr) in enumerate(zip(source_rows, target_rows)):
            for col in sorted(set(sr.keys()) | set(tr.keys())):
                sv, tv = sr.get(col), tr.get(col)
                if isinstance(sv, float) and isinstance(tv, float):
                    sc, _ = canonicalize_float_value(sv)
                    tc, _ = canonicalize_float_value(tv)
                    if sc != tc:
                        # Check if they look close under loose float32/64 rounding
                        if not (math.isnan(sv) or math.isnan(tv) or math.isinf(sv) or math.isinf(tv)):
                            rel_diff = abs(sv - tv) / max(abs(sv), abs(tv), 1e-300)
                            if 0.0 < rel_diff < 1e-5:
                                anomalies.append(
                                    f"row[{idx}].{col} float rounding drift: source={sc} vs target={tc} (rel={rel_diff:.3e})"
                                )
    return (len(anomalies) > 0), anomalies


def execute_validation_method(
    method: ValidationMethodKind,
    source_rows: Sequence[Dict[str, Any]],
    target_rows: Sequence[Dict[str, Any]],
    *,
    required_claim: ClaimedGuarantee = ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
    key_columns: Sequence[str] = (),
    columns: Optional[Sequence[str]] = None,
    bucket_count: int = 16,
    float_policy: FloatCanonicalPolicy = FloatCanonicalPolicy.BANDED_EXACT_TEXT,
    length_prefixed_hash: bool = True,
    null_distinct_hash: bool = True,
    digest_bits: int = 256,
    xor_fold_hashes: bool = False,
    custom_query_contract: Optional[CustomQueryContract] = None,
) -> ValidationExecutionReport:
    """Execute the specified validation method and enforce proof-strength contracts.

    Fast signatures (`COUNT`, `AGGREGATE`, `VECTOR`, `HASH`) can rule IN
    differences (`ValidationVerdict.DIFFERENT`) or localize/escalate work
    (`ValidationVerdict.ESCALATE_TO_EXACT`), and can NEVER return
    `ValidationVerdict.SAME` (`certified_same=True`).
    """
    spec = METHOD_SPECS[method]
    req_strength = required_claim.required_proof_strength
    src_n = len(source_rows)
    tgt_n = len(target_rows)
    all_cols = (
        list(columns)
        if columns is not None
        else sorted({k for r in list(source_rows) + list(target_rows) for k in r.keys()})
    )
    fallback_exact = (
        ValidationMethodKind.KEY_ROW
        if (len(key_columns) > 0)
        else ValidationMethodKind.EXTERNAL_SORT
    )

    # 1. COUNT validation
    if method == ValidationMethodKind.COUNT:
        if src_n != tgt_n:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.CARDINALITY_ONLY,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=(f"row_count:source={src_n}!=target={tgt_n}",),
                summary_message=(
                    f"[ACTION] count ruled in difference: source={src_n} vs target={tgt_n}"
                ),
            )
        # Counts match! Count-only unchanged totals can NEVER produce SAME.
        if req_strength.rank > ProofStrength.CARDINALITY_ONLY.rank:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.CARDINALITY_ONLY,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.COUNT_ONLY_FOR_EXACT_CLAIM,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] count matched ({src_n} rows) but count-only cannot certify "
                    f"{required_claim.value}; escalating to {fallback_exact.value}"
                ),
            )
        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.CARDINALITY_ONLY,
            verdict=ValidationVerdict.TRIAGE_COUNT_MATCH,
            care_badge=CareBadge.REVIEW,
            certified_same=False,
            needs_escalation=False,
            recommended_escalation_method=fallback_exact,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message=(
                f"[REVIEW] triage count matched ({src_n} rows); exact row equivalence is not certified"
            ),
        )

    # 2. AGGREGATE validation
    if method == ValidationMethodKind.AGGREGATE:
        if src_n != tgt_n:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.COLUMN_SUMMARY,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=(f"row_count:source={src_n}!=target={tgt_n}",),
                summary_message=f"[ACTION] aggregate count mismatch: source={src_n} vs target={tgt_n}",
            )

        has_float_issue, float_notes = _detect_float_rounding_pathology(source_rows, target_rows, float_policy)
        if has_float_issue:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.FLOATING_ROUNDING,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=tuple(float_notes),
                summary_message=(
                    f"[REVIEW] aggregate detected floating-point rounding/representation risk "
                    f"({float_notes[0]}); escalating to {fallback_exact.value}"
                ),
            )

        # Compute column-level exact Decimal sums and canonical min/max
        col_diffs: List[str] = []
        for col in all_cols:
            s_vals = [r.get(col) for r in source_rows if r.get(col) is not None]
            t_vals = [r.get(col) for r in target_rows if r.get(col) is not None]
            if len(s_vals) != len(t_vals):
                col_diffs.append(f"non_null_count({col}):{len(s_vals)}!={len(t_vals)}")
                continue
            s_canon = [canonicalize_cell(v, float_policy=float_policy) for v in s_vals]
            t_canon = [canonicalize_cell(v, float_policy=float_policy) for v in t_vals]
            if s_canon and (min(s_canon) != min(t_canon) or max(s_canon) != max(t_canon)):
                col_diffs.append(f"minmax({col}):({min(s_canon)},{max(s_canon)})!=({min(t_canon)},{max(t_canon)})")
            # Numeric sum check
            if all(isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool) for v in s_vals + t_vals):
                s_sum = sum((decimal.Decimal(str(v)) for v in s_vals), decimal.Decimal(0))
                t_sum = sum((decimal.Decimal(str(v)) for v in t_vals), decimal.Decimal(0))
                if s_sum != t_sum:
                    col_diffs.append(f"sum({col}):{s_sum}!={t_sum}")

        if col_diffs:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.COLUMN_SUMMARY,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.EQUAL_COUNT_VALUE_CHANGE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=tuple(col_diffs),
                summary_message=f"[ACTION] aggregate ruled in column difference: {', '.join(col_diffs)}",
            )

        # Aggregates matched, but cannot rule out zero-sum cancelling swaps or duplicate shifts
        if req_strength.rank > ProofStrength.COLUMN_SUMMARY.rank:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.COLUMN_SUMMARY,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.EQUAL_COUNT_VALUE_CHANGE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] aggregate summaries matched on {src_n} rows, but aggregates cannot "
                    f"rule out compensating value swaps or duplicate shifts; escalating to {fallback_exact.value}"
                ),
            )

        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.COLUMN_SUMMARY,
            verdict=ValidationVerdict.SIGNATURE_MATCH_NOT_SAME,
            care_badge=CareBadge.REVIEW,
            certified_same=False,
            needs_escalation=False,
            recommended_escalation_method=fallback_exact,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message="[REVIEW] column summary matched within contract; exact equivalence not claimed",
        )

    # 3. VECTOR validation (bucketed signatures to localize differences and prioritize exact work)
    if method == ValidationMethodKind.VECTOR:
        num_buckets = max(1, int(bucket_count))
        src_buckets = compute_bucket_vectors(
            source_rows, key_columns=key_columns, columns=all_cols, bucket_count=num_buckets, float_policy=float_policy
        )
        tgt_buckets = compute_bucket_vectors(
            target_rows, key_columns=key_columns, columns=all_cols, bucket_count=num_buckets, float_policy=float_policy
        )
        dirty_buckets: List[int] = []
        for b_id in range(num_buckets):
            if src_buckets[b_id] != tgt_buckets[b_id]:
                dirty_buckets.append(b_id)

        if dirty_buckets:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.BUCKET_SIGNATURE,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=(
                    PathologyKind.EQUAL_COUNT_VALUE_CHANGE if src_n == tgt_n else PathologyKind.NONE
                ),
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_bucket_ids=tuple(dirty_buckets),
                summary_message=(
                    f"[ACTION] vector signature ruled in differences in {len(dirty_buckets)}/{num_buckets} "
                    f"buckets {dirty_buckets}; prioritizing {fallback_exact.value} on dirty buckets"
                ),
            )

        if req_strength.rank > ProofStrength.BUCKET_SIGNATURE.rank:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.BUCKET_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] all {num_buckets} bucket vectors matched, but fast vector signatures "
                    f"cannot certify {required_claim.value}; escalating to {fallback_exact.value}"
                ),
            )

        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.BUCKET_SIGNATURE,
            verdict=ValidationVerdict.SIGNATURE_MATCH_NOT_SAME,
            care_badge=CareBadge.REVIEW,
            certified_same=False,
            needs_escalation=False,
            recommended_escalation_method=fallback_exact,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message=f"[REVIEW] all {num_buckets} bucket signatures matched; exact equivalence not claimed",
        )

    # 4. HASH validation
    if method == ValidationMethodKind.HASH:
        # Check for hash contract ambiguities first!
        if not length_prefixed_hash:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.HASH_DELIMITER_AMBIGUITY,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] unframed concatenation hash is vulnerable to delimiter collision "
                    f"('ab'+'c' == 'a'+'bc'); escalating to {fallback_exact.value}"
                ),
            )
        if not null_distinct_hash:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.HASH_NULL_EMPTY_AMBIGUITY,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] hash conflates NULL with empty string ''; escalating to {fallback_exact.value}"
                ),
            )
        if digest_bits < 128:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.HASH_WEAK_WIDTH_COLLISION,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] {digest_bits}-bit hash is below 128-bit collision floor; "
                    f"escalating to {fallback_exact.value}"
                ),
            )
        has_float_issue, float_notes = _detect_float_rounding_pathology(source_rows, target_rows, float_policy)
        if has_float_issue and float_policy != FloatCanonicalPolicy.BANDED_EXACT_TEXT:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.FLOATING_ROUNDING,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=tuple(float_notes),
                summary_message=(
                    f"[REVIEW] hash over non-banded float representation risks rounding mismatch; "
                    f"escalating to {fallback_exact.value}"
                ),
            )

        src_canons = [
            canonicalize_row(
                r,
                all_cols,
                length_prefixed=length_prefixed_hash,
                null_distinct_from_empty=null_distinct_hash,
                float_policy=float_policy,
            )
            for r in source_rows
        ]
        tgt_canons = [
            canonicalize_row(
                r,
                all_cols,
                length_prefixed=length_prefixed_hash,
                null_distinct_from_empty=null_distinct_hash,
                float_policy=float_policy,
            )
            for r in target_rows
        ]

        # Check if XOR folding is requested on data with duplicate rows
        has_dups = (len(set(src_canons)) < len(src_canons)) or (len(set(tgt_canons)) < len(tgt_canons))
        if xor_fold_hashes and has_dups:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.AMBIGUOUS_SIGNATURE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=ValidationMethodKind.EXTERNAL_SORT,
                pathology_detected=PathologyKind.HASH_XOR_DUPLICATE_CANCELLATION,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    "[REVIEW] XOR hash folding cancels even-multiplicity duplicate rows "
                    "(h ^ h == 0); escalating to external_sort"
                ),
            )

        src_digests = sorted(digest_canonical_row(c, digest_bits=digest_bits) for c in src_canons)
        tgt_digests = sorted(digest_canonical_row(c, digest_bits=digest_bits) for c in tgt_canons)

        if src_digests != tgt_digests:
            pathology = PathologyKind.NONE
            if src_n == tgt_n:
                if set(src_canons) == set(tgt_canons) and src_canons != tgt_canons:
                    pathology = PathologyKind.PERFECT_DUPLICATES
                elif has_float_issue:
                    pathology = PathologyKind.FLOATING_ROUNDING
                else:
                    pathology = PathologyKind.EQUAL_COUNT_VALUE_CHANGE
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.DIGEST_EQUIVALENCE,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=fallback_exact,
                pathology_detected=pathology,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=("digest_mismatch",),
                summary_message=f"[ACTION] canonical digest ruled in difference ({pathology.value})",
            )

        if req_strength.rank > ProofStrength.DIGEST_EQUIVALENCE.rank:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.DIGEST_EQUIVALENCE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] 256-bit canonical digests matched ({src_n} rows), but digest "
                    f"screening cannot certify {required_claim.value}; escalating to {fallback_exact.value}"
                ),
            )

        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.DIGEST_EQUIVALENCE,
            verdict=ValidationVerdict.SIGNATURE_MATCH_NOT_SAME,
            care_badge=CareBadge.REVIEW,
            certified_same=False,
            needs_escalation=False,
            recommended_escalation_method=fallback_exact,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message=f"[REVIEW] canonical digest screening matched ({src_n} rows)",
        )

    # 5. CUSTOM_QUERY validation
    if method == ValidationMethodKind.CUSTOM_QUERY:
        contract = custom_query_contract or CustomQueryContract(
            query_id="default_custom",
            source_sql="SELECT * FROM tbl",
            target_sql="SELECT * FROM tbl",
            covers_all_rows=False,
            projects_all_columns=False,
            returns_row_level_tuples=True,
        )
        eff_strength = contract.effective_proof_strength
        src_canons = sorted(canonicalize_row(r, all_cols, float_policy=float_policy) for r in source_rows)
        tgt_canons = sorted(canonicalize_row(r, all_cols, float_policy=float_policy) for r in target_rows)
        if src_canons != tgt_canons:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=eff_strength,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.EQUAL_COUNT_VALUE_CHANGE if src_n == tgt_n else PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=("custom_query_result_mismatch",),
                summary_message=f"[ACTION] custom query {contract.query_id} ruled in difference",
            )
        if eff_strength.rank < req_strength.rank:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=eff_strength,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=fallback_exact,
                pathology_detected=PathologyKind.CUSTOM_QUERY_INCOMPLETE_SCOPE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    f"[REVIEW] custom query {contract.query_id} matched on its projection/subset "
                    f"({eff_strength.value}), but cannot certify {required_claim.value}; "
                    f"escalating to {fallback_exact.value}"
                ),
            )
        is_exact = eff_strength.is_exact_equivalence
        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=eff_strength,
            verdict=ValidationVerdict.SAME if is_exact else ValidationVerdict.SUBSET_MATCH_NOT_WHOLE,
            care_badge=CareBadge.PASS if is_exact else CareBadge.REVIEW,
            certified_same=is_exact,
            needs_escalation=False,
            recommended_escalation_method=None if is_exact else fallback_exact,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message=(
                f"[{'PASS' if is_exact else 'REVIEW'}] custom query {contract.query_id} "
                f"verified {eff_strength.value}"
            ),
        )

    # 6. KEY_ROW validation (Key-ordered exact canonical row-by-row comparison)
    if method == ValidationMethodKind.KEY_ROW:
        if not key_columns:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.UNPROVABLE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=ValidationMethodKind.EXTERNAL_SORT,
                pathology_detected=PathologyKind.PERFECT_DUPLICATES,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    "[REVIEW] key_row cannot run without key_columns; escalating to external_sort"
                ),
            )

        src_by_key: Dict[str, str] = {}
        tgt_by_key: Dict[str, str] = {}
        dup_keys: List[str] = []

        for r in source_rows:
            k_str = canonicalize_row({k: r.get(k) for k in key_columns}, key_columns, float_policy=float_policy)
            if k_str in src_by_key:
                dup_keys.append(f"source_dup_key:{k_str}")
            src_by_key[k_str] = canonicalize_row(r, all_cols, float_policy=float_policy)

        for r in target_rows:
            k_str = canonicalize_row({k: r.get(k) for k in key_columns}, key_columns, float_policy=float_policy)
            if k_str in tgt_by_key:
                dup_keys.append(f"target_dup_key:{k_str}")
            tgt_by_key[k_str] = canonicalize_row(r, all_cols, float_policy=float_policy)

        if dup_keys:
            # Duplicate keys violate key_row's uniqueness assumption -> escalate to external_sort!
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.UNPROVABLE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=ValidationMethodKind.EXTERNAL_SORT,
                pathology_detected=PathologyKind.PERFECT_DUPLICATES,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=tuple(dup_keys),
                summary_message=(
                    f"[REVIEW] duplicate keys detected ({dup_keys[0]}); key_row uniqueness "
                    f"assumption violated, escalating to external_sort"
                ),
            )

        all_keys = sorted(set(src_by_key.keys()) | set(tgt_by_key.keys()))
        diffs: List[str] = []
        for k in all_keys:
            sv = src_by_key.get(k)
            tv = tgt_by_key.get(k)
            if sv is None:
                diffs.append(f"missing_on_source:{k}")
            elif tv is None:
                diffs.append(f"missing_on_target:{k}")
            elif sv != tv:
                diffs.append(f"value_mismatch:{k}:source={sv}!=target={tv}")

        if diffs:
            has_float_issue, _ = _detect_float_rounding_pathology(source_rows, target_rows, float_policy)
            pathology = (
                PathologyKind.FLOATING_ROUNDING
                if has_float_issue
                else (PathologyKind.EQUAL_COUNT_VALUE_CHANGE if src_n == tgt_n else PathologyKind.NONE)
            )
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.EXACT_KEYED_EQUIVALENCE,
                verdict=ValidationVerdict.DIFFERENT,
                care_badge=CareBadge.ACTION,
                certified_same=False,
                needs_escalation=False,
                recommended_escalation_method=None,
                pathology_detected=pathology,
                source_row_count=src_n,
                target_row_count=tgt_n,
                differing_keys_or_rows=tuple(diffs),
                summary_message=f"[ACTION] key_row proved {len(diffs)} exact row difference(s): {diffs[0]}",
            )

        if req_strength == ProofStrength.EXACT_MULTISET_EQUIVALENCE:
            return ValidationExecutionReport(
                method=method,
                required_claim=required_claim,
                achieved_proof_strength=ProofStrength.EXACT_KEYED_EQUIVALENCE,
                verdict=ValidationVerdict.ESCALATE_TO_EXACT,
                care_badge=CareBadge.REVIEW,
                certified_same=False,
                needs_escalation=True,
                recommended_escalation_method=ValidationMethodKind.EXTERNAL_SORT,
                pathology_detected=PathologyKind.NONE,
                source_row_count=src_n,
                target_row_count=tgt_n,
                summary_message=(
                    "[REVIEW] key_row proved keyed equivalence, but multiset equivalence was requested; "
                    "escalating to external_sort"
                ),
            )

        return ValidationExecutionReport(
            method=method,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.EXACT_KEYED_EQUIVALENCE,
            verdict=ValidationVerdict.SAME,
            care_badge=CareBadge.PASS,
            certified_same=True,
            needs_escalation=False,
            recommended_escalation_method=None,
            pathology_detected=PathologyKind.NONE,
            source_row_count=src_n,
            target_row_count=tgt_n,
            summary_message=f"[PASS] key_row certified exact keyed equivalence across {src_n} rows",
        )

    # 7. EXTERNAL_SORT validation (Deterministic sort-merge over canonical tuples with multiplicity)
    src_multiset = _build_sorted_multiset(source_rows, all_cols, float_policy=float_policy)
    tgt_multiset = _build_sorted_multiset(target_rows, all_cols, float_policy=float_policy)

    if src_multiset != tgt_multiset:
        src_dict = dict(src_multiset)
        tgt_dict = dict(tgt_multiset)
        all_tuples = sorted(set(src_dict.keys()) | set(tgt_dict.keys()))
        multiset_diffs: List[str] = []
        for tup in all_tuples:
            sc = src_dict.get(tup, 0)
            tc = tgt_dict.get(tup, 0)
            if sc != tc:
                multiset_diffs.append(f"tuple={tup}:source_mult={sc}!=target_mult={tc}")

        # Determine exact pathology
        has_float_issue, _ = _detect_float_rounding_pathology(source_rows, target_rows, float_policy)
        if set(src_dict.keys()) == set(tgt_dict.keys()):
            pathology = PathologyKind.PERFECT_DUPLICATES
        elif has_float_issue:
            pathology = PathologyKind.FLOATING_ROUNDING
        elif src_n == tgt_n:
            pathology = PathologyKind.EQUAL_COUNT_VALUE_CHANGE
        else:
            pathology = PathologyKind.NONE

        return ValidationExecutionReport(
            method=ValidationMethodKind.EXTERNAL_SORT,
            required_claim=required_claim,
            achieved_proof_strength=ProofStrength.EXACT_MULTISET_EQUIVALENCE,
            verdict=ValidationVerdict.DIFFERENT,
            care_badge=CareBadge.ACTION,
            certified_same=False,
            needs_escalation=False,
            recommended_escalation_method=None,
            pathology_detected=pathology,
            source_row_count=src_n,
            target_row_count=tgt_n,
            differing_keys_or_rows=tuple(multiset_diffs),
            summary_message=(
                f"[ACTION] external_sort proved {len(multiset_diffs)} multiset difference(s) "
                f"({pathology.value}): {multiset_diffs[0]}"
            ),
        )

    return ValidationExecutionReport(
        method=ValidationMethodKind.EXTERNAL_SORT,
        required_claim=required_claim,
        achieved_proof_strength=ProofStrength.EXACT_MULTISET_EQUIVALENCE,
        verdict=ValidationVerdict.SAME,
        care_badge=CareBadge.PASS,
        certified_same=True,
        needs_escalation=False,
        recommended_escalation_method=None,
        pathology_detected=PathologyKind.NONE,
        source_row_count=src_n,
        target_row_count=tgt_n,
        summary_message=f"[PASS] external_sort certified exact multiset equivalence across {src_n} rows",
    )


def _build_sorted_multiset(
    rows: Sequence[Dict[str, Any]],
    columns: Sequence[str],
    *,
    float_policy: FloatCanonicalPolicy = FloatCanonicalPolicy.BANDED_EXACT_TEXT,
) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for r in rows:
        c = canonicalize_row(r, columns, float_policy=float_policy)
        counts[c] = counts.get(c, 0) + 1
    return sorted(counts.items(), key=lambda kv: kv[0])


def compute_bucket_vectors(
    rows: Sequence[Dict[str, Any]],
    *,
    key_columns: Sequence[str] = (),
    columns: Optional[Sequence[str]] = None,
    bucket_count: int = 16,
    float_policy: FloatCanonicalPolicy = FloatCanonicalPolicy.BANDED_EXACT_TEXT,
) -> Dict[int, BucketSignature]:
    """Compute per-bucket vector signatures (`count`, `numeric_sum`, `min`, `max`, `multiset_digest`)."""
    num_buckets = max(1, int(bucket_count))
    all_cols = (
        list(columns)
        if columns is not None
        else sorted({k for r in rows for k in r.keys()})
    )
    bucket_rows: Dict[int, List[Tuple[str, Dict[str, Any]]]] = {b: [] for b in range(num_buckets)}

    for r in rows:
        routing_cols = list(key_columns) if key_columns else all_cols
        routing_canon = canonicalize_row(
            {k: r.get(k) for k in routing_cols}, routing_cols, float_policy=float_policy
        )
        full_canon = canonicalize_row(r, all_cols, float_policy=float_policy)
        b_hash = int(hashlib.sha256(routing_canon.encode("utf-8")).hexdigest()[:8], 16)
        b_id = b_hash % num_buckets
        bucket_rows[b_id].append((full_canon, r))

    result: Dict[int, BucketSignature] = {}
    for b_id in range(num_buckets):
        items = bucket_rows[b_id]
        if not items:
            result[b_id] = BucketSignature(
                bucket_id=b_id,
                row_count=0,
                numeric_sum=decimal.Decimal(0),
                min_canonical=None,
                max_canonical=None,
                multiset_digest="empty",
            )
            continue
        canons = [c for c, _ in items]
        num_sum = decimal.Decimal(0)
        for _, row_dict in items:
            for col in all_cols:
                v = row_dict.get(col)
                if isinstance(v, (int, decimal.Decimal)) and not isinstance(v, bool):
                    num_sum += decimal.Decimal(str(v))
                elif isinstance(v, float) and not (math.isnan(v) or math.isinf(v)):
                    num_sum += decimal.Decimal(f"{v:.15g}")
        # Multiplicity-preserving digest over sorted canonical rows in the bucket
        multiset_counts: Dict[str, int] = {}
        for c in canons:
            multiset_counts[c] = multiset_counts.get(c, 0) + 1
        folded = "|".join(f"{ mult }*{ c }" for c, mult in sorted(multiset_counts.items()))
        digest = hashlib.sha256(folded.encode("utf-8")).hexdigest()[:16]
        result[b_id] = BucketSignature(
            bucket_id=b_id,
            row_count=len(items),
            numeric_sum=num_sum,
            min_canonical=min(canons),
            max_canonical=max(canons),
            multiset_digest=digest,
        )
    return result


def prioritize_exact_verification_by_vector(
    source_rows: Sequence[Dict[str, Any]],
    target_rows: Sequence[Dict[str, Any]],
    *,
    key_columns: Sequence[str] = (),
    columns: Optional[Sequence[str]] = None,
    bucket_count: int = 16,
    required_claim: ClaimedGuarantee = ClaimedGuarantee.EXACT_ROW_EQUIVALENCE,
) -> Tuple[ValidationExecutionReport, ValidationExecutionReport]:
    """Run fast `VECTOR` screening to localize dirty buckets, then escalate to exact proof.

    Proves two invariants:
    1. When `VECTOR` finds dirty buckets, exact verification (`KEY_ROW` or
       `EXTERNAL_SORT`) inspects dirty buckets first to surface differences
       immediately.
    2. Even when `VECTOR` finds 0 dirty buckets, `VECTOR` alone returns
       `ESCALATE_TO_EXACT` (`certified_same=False`), and only the second-stage
       exact verifier can certify `ValidationVerdict.SAME`.
    """
    fast_report = execute_validation_method(
        ValidationMethodKind.VECTOR,
        source_rows,
        target_rows,
        required_claim=required_claim,
        key_columns=key_columns,
        columns=columns,
        bucket_count=bucket_count,
    )
    exact_method = (
        ValidationMethodKind.KEY_ROW
        if (key_columns and required_claim != ClaimedGuarantee.MULTISET_EQUIVALENCE)
        else ValidationMethodKind.EXTERNAL_SORT
    )

    if fast_report.differing_bucket_ids:
        # Partition rows and verify dirty buckets first!
        dirty_set = set(fast_report.differing_bucket_ids)
        all_cols = (
            list(columns)
            if columns is not None
            else sorted({k for r in list(source_rows) + list(target_rows) for k in r.keys()})
        )
        routing_cols = list(key_columns) if key_columns else all_cols

        def _in_dirty(r: Dict[str, Any]) -> bool:
            rc = canonicalize_row({k: r.get(k) for k in routing_cols}, routing_cols)
            b_id = int(hashlib.sha256(rc.encode("utf-8")).hexdigest()[:8], 16) % max(1, bucket_count)
            return b_id in dirty_set

        src_dirty = [r for r in source_rows if _in_dirty(r)]
        tgt_dirty = [r for r in target_rows if _in_dirty(r)]
        exact_report = execute_validation_method(
            exact_method,
            src_dirty,
            tgt_dirty,
            required_claim=required_claim,
            key_columns=key_columns,
            columns=columns,
        )
        return fast_report, exact_report

    # All buckets matched in fast signature; escalate to full exact proof to certify SAME
    exact_report = execute_validation_method(
        exact_method,
        source_rows,
        target_rows,
        required_claim=required_claim,
        key_columns=key_columns,
        columns=columns,
    )
    return fast_report, exact_report


# -----------------------------------------------------------------------------
# Universal Resilience: O(1) Typed Durable Checkpoint & Active Lease Lock
# -----------------------------------------------------------------------------

def encode_typed_value(val: Any) -> Any:
    """Recursively encode native Python types (datetime, date, Decimal, UUID, bytes, tuple) for JSON."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "value": val.isoformat()}
    if isinstance(val, datetime.date):
        return {"__type__": "date", "value": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "value": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "value": str(val)}
    if isinstance(val, (bytes, bytearray)):
        return {"__type__": "bytes", "value": bytes(val).hex()}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "value": [encode_typed_value(x) for x in val]}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    if isinstance(val, dict):
        return {str(k): encode_typed_value(v) for k, v in val.items()}
    return val


def decode_typed_value(obj: Any) -> Any:
    """Recursively restore native Python types from JSON-decoded structures."""
    if isinstance(obj, dict):
        t = obj.get("__type__")
        if t == "datetime":
            return datetime.datetime.fromisoformat(obj["value"])
        if t == "date":
            return datetime.date.fromisoformat(obj["value"])
        if t == "decimal":
            return decimal.Decimal(obj["value"])
        if t == "uuid":
            return uuid.UUID(obj["value"])
        if t == "bytes":
            return bytes.fromhex(obj["value"])
        if t == "tuple":
            return tuple(decode_typed_value(x) for x in obj["value"])
        return {k: decode_typed_value(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decode_typed_value(x) for x in obj]
    return obj


@dataclasses.dataclass(frozen=True)
class CompletedRangeProof:
    """Durable record of a verified key/bucket range and the proof strength achieved."""
    range_id: str
    start_key: Any
    end_key: Any
    method_used: ValidationMethodKind
    proof_strength: ProofStrength
    verdict: ValidationVerdict
    rows_verified: int
    bytes_verified: int
    completed_at: float


class VerifyMethodCheckpoint:
    """O(1) typed durable checkpoint for validation ranges, gated on achieved proof strength."""

    def __init__(self, checkpoint_path: Path) -> None:
        self.checkpoint_path = Path(checkpoint_path)
        self._completed: Dict[str, CompletedRangeProof] = {}
        if self.checkpoint_path.exists():
            self.load()

    def load(self) -> None:
        raw = json.loads(self.checkpoint_path.read_text(encoding="ascii"))
        entries = raw.get("completed_ranges", {})
        loaded: Dict[str, CompletedRangeProof] = {}
        for rid, item in entries.items():
            loaded[rid] = CompletedRangeProof(
                range_id=rid,
                start_key=decode_typed_value(item["start_key"]),
                end_key=decode_typed_value(item["end_key"]),
                method_used=ValidationMethodKind(item["method_used"]),
                proof_strength=ProofStrength(item["proof_strength"]),
                verdict=ValidationVerdict(item["verdict"]),
                rows_verified=int(item["rows_verified"]),
                bytes_verified=int(item["bytes_verified"]),
                completed_at=float(item["completed_at"]),
            )
        self._completed = loaded

    def save(self) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "completed_ranges": {
                rid: {
                    "start_key": encode_typed_value(rec.start_key),
                    "end_key": encode_typed_value(rec.end_key),
                    "method_used": rec.method_used.value,
                    "proof_strength": rec.proof_strength.value,
                    "verdict": rec.verdict.value,
                    "rows_verified": rec.rows_verified,
                    "bytes_verified": rec.bytes_verified,
                    "completed_at": rec.completed_at,
                }
                for rid, rec in self._completed.items()
            },
        }
        durable.write_json(self.checkpoint_path, payload)

    def record_range(self, proof: CompletedRangeProof) -> None:
        self._completed[proof.range_id] = proof
        self.save()

    def is_range_satisfied(self, range_id: str, required_claim: ClaimedGuarantee) -> bool:
        """O(1) check whether `range_id` was already verified at >= required proof strength."""
        rec = self._completed.get(range_id)
        if rec is None:
            return False
        if rec.verdict == ValidationVerdict.DIFFERENT:
            return False
        if required_claim in (ClaimedGuarantee.EXACT_ROW_EQUIVALENCE, ClaimedGuarantee.MULTISET_EQUIVALENCE):
            if rec.verdict != ValidationVerdict.SAME:
                return False
        return rec.proof_strength.rank >= required_claim.required_proof_strength.rank

    def get_range_proof(self, range_id: str) -> Optional[CompletedRangeProof]:
        return self._completed.get(range_id)

    @property
    def completed_count(self) -> int:
        return len(self._completed)


@dataclasses.dataclass
class VerificationLeaseLock:
    """Mutual-exclusion active verification lease lock (`.run.lock`) with heartbeat."""
    state_dir: Path
    run_id: str
    lease_duration_seconds: float = 300.0
    stale_heartbeat_seconds: float = 30.0
    host: str = dataclasses.field(default_factory=socket.gethostname)
    pid: int = dataclasses.field(default_factory=os.getpid)
    issued_at: float = 0.0
    heartbeat_at: float = 0.0
    expires_at: float = 0.0
    _held: bool = False

    @property
    def lock_path(self) -> Path:
        return self.state_dir / ".run.lock"

    @property
    def guard_path(self) -> Path:
        return self.state_dir / ".run.lock.guard"

    def acquire(self, now: Optional[float] = None, steal_stale: bool = True) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        ts = float(now if now is not None else time.time())
        guard_fd = os.open(str(self.guard_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(guard_fd, fcntl.LOCK_EX)
            if self.lock_path.exists():
                try:
                    existing = json.loads(self.lock_path.read_text(encoding="ascii"))
                except Exception:
                    existing = None
                if isinstance(existing, dict):
                    ex_run = existing.get("run_id")
                    ex_pid = existing.get("pid")
                    ex_host = existing.get("host")
                    ex_hb = float(existing.get("heartbeat_at", 0.0))
                    ex_exp = float(existing.get("expires_at", 0.0))
                    same_owner = (ex_run == self.run_id and ex_pid == self.pid and ex_host == self.host)
                    is_stale = (ts - ex_hb) > self.stale_heartbeat_seconds or ts > ex_exp
                    if not same_owner and (not steal_stale or not is_stale):
                        raise VerificationLeaseError(
                            f"[ACTION] Active verification lease held by {ex_host}:{ex_pid} "
                            f"(run_id={ex_run}) on {self.lock_path}"
                        )
            self.issued_at = ts
            self.heartbeat_at = ts
            self.expires_at = ts + self.lease_duration_seconds
            durable.write_json(
                self.lock_path,
                {
                    "run_id": self.run_id,
                    "host": self.host,
                    "pid": self.pid,
                    "issued_at": self.issued_at,
                    "heartbeat_at": self.heartbeat_at,
                    "expires_at": self.expires_at,
                },
            )
            self._held = True
        finally:
            fcntl.flock(guard_fd, fcntl.LOCK_UN)
            os.close(guard_fd)

    def heartbeat(self, now: Optional[float] = None, extend: bool = True) -> None:
        ts = float(now if now is not None else time.time())
        self.verify(now=ts)
        self.heartbeat_at = ts
        if extend:
            self.expires_at = ts + self.lease_duration_seconds
        durable.write_json(
            self.lock_path,
            {
                "run_id": self.run_id,
                "host": self.host,
                "pid": self.pid,
                "issued_at": self.issued_at,
                "heartbeat_at": self.heartbeat_at,
                "expires_at": self.expires_at,
            },
        )

    def verify(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        if not self._held or not self.lock_path.exists():
            raise VerificationLeaseError(
                f"[ACTION] Run {self.run_id} does not hold an active lease at {self.lock_path}"
            )
        existing = json.loads(self.lock_path.read_text(encoding="ascii"))
        if existing.get("run_id") != self.run_id or int(existing.get("pid", -1)) != self.pid:
            raise VerificationLeaseError(
                f"[ACTION] Lease {self.lock_path} was stolen by {existing.get('host')}:{existing.get('pid')}"
            )
        if ts > self.expires_at or ts > float(existing.get("expires_at", 0.0)):
            raise VerificationLeaseExpiredError(
                f"[ACTION] Verification lease {self.run_id} expired at {self.expires_at:.3f} (now={ts:.3f})"
            )

    def release(self) -> None:
        if self.lock_path.exists():
            self.lock_path.unlink(missing_ok=True)
        self._held = False


@dataclasses.dataclass
class AdaptiveAimdBatcher:
    """Dynamic AIMD batch sizing controller for range/row verification."""
    current_batch_rows: int = 5000
    min_batch_rows: int = 250
    max_batch_rows: int = 100000
    additive_step_rows: int = 2500
    multiplicative_beta: float = 0.5
    target_batch_seconds: float = 1.0

    def observe_batch(self, elapsed_seconds: float, had_pressure_or_error: bool = False) -> int:
        if had_pressure_or_error or elapsed_seconds > self.target_batch_seconds * 1.5:
            self.current_batch_rows = max(
                self.min_batch_rows,
                int(self.current_batch_rows * self.multiplicative_beta),
            )
        else:
            self.current_batch_rows = min(
                self.max_batch_rows,
                self.current_batch_rows + self.additive_step_rows,
            )
        return self.current_batch_rows


@dataclasses.dataclass
class ColdStartByteWeightedEta:
    """Instant cold-start byte-weighted ETA estimator blended with observed throughput."""
    total_bytes: int
    cold_start_bytes_per_sec: float = 100.0 * 1024.0 * 1024.0
    verified_bytes: int = 0
    elapsed_seconds: float = 0.0

    def record_progress(self, delta_bytes: int, delta_seconds: float) -> None:
        self.verified_bytes = min(self.total_bytes, self.verified_bytes + max(0, int(delta_bytes)))
        self.elapsed_seconds += max(0.0, float(delta_seconds))

    def estimate_remaining_seconds(self) -> float:
        remaining = max(0, self.total_bytes - self.verified_bytes)
        if remaining == 0:
            return 0.0
        cold_rate = max(1.0, self.cold_start_bytes_per_sec)
        if self.verified_bytes <= 0 or self.elapsed_seconds <= 0.0:
            return remaining / cold_rate
        observed_rate = self.verified_bytes / self.elapsed_seconds
        progress_fraction = min(1.0, self.verified_bytes / max(1, self.total_bytes))
        blended_rate = (progress_fraction * observed_rate) + ((1.0 - progress_fraction) * cold_rate)
        return remaining / max(1.0, blended_rate)
