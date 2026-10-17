"""Tests for cross-engine semantic negotiation before fast-path ranking (Row 597: SMART-N14.semantic-negotiation).

Proves:
1. Every decide.Rung in `semantic_negotiation.py` is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in
   `PROVES_RUNGS`.
2. Cross-engine representability for decimal/unsigned boundaries, timestamp/timezone/DST,
   collation/Unicode, JSON, NULL, uniqueness, generated columns, counters, isolation,
   and DDL effects.
3. Targeted boundary-value and metamorphic testing distinguishing identical raw bytes
   from approved semantic equivalence.
4. Row equality cannot pass broken constraints, rounding, or transaction effects;
   lossy mappings require an explicit approved rule; no-PK multiplicity and
   cross-table commit semantics are preserved.
5. Universal typed durable checkpointing in O(1) time complexity, mutual exclusion hop
   leases, adaptive AIMD batching, cold-start ETA, and external tool wrappers.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import pathlib
import time
import uuid
from typing import Any, Dict

import pytest

import tests.rung_branches
from migkit import decide
from migkit.semantic_negotiation import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    AIMDBatchController,
    ApprovedSemanticRule,
    CareBadge,
    ColumnSemanticSpec,
    CrossTableCommitSpec,
    InstantETAEstimator,
    MerkleBucketDiff,
    MultisetVerifier,
    SemanticCheckpointStore,
    SemanticCheckpointUnit,
    SemanticConstraintBreachError,
    SemanticDomain,
    SemanticEquivalenceKind,
    SemanticNegotiationError,
    SemanticToolWrapper,
    TableSemanticSpec,
    UnsafeEndpointError,
    acquire_semantic_negotiation_lease,
    collation_unicode_rungs,
    compare_cross_engine_values,
    cross_table_commit_rungs,
    evaluate_table_semantic_negotiation,
    fast_path_admission_rungs,
    json_structured_rungs,
    keyless_multiplicity_rungs,
    negotiate_collation_unicode,
    negotiate_cross_table_commit,
    negotiate_fast_path_admission,
    negotiate_json_structured,
    negotiate_keyless_multiplicity,
    negotiate_null_semantics,
    negotiate_schema_effects,
    negotiate_temporal_timezone,
    negotiate_type_representation,
    null_semantics_rungs,
    schema_effects_rungs,
    temporal_timezone_rungs,
    type_representation_rungs,
    validate_safe_endpoint,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    # type_representation_rungs
    ("semantic_negotiation", "type_representation_rungs", "strict_lossless_representation", "holds"),
    ("semantic_negotiation", "type_representation_rungs", "strict_lossless_representation", "refuses"),
    ("semantic_negotiation", "type_representation_rungs", "lossless_widened_representation", "holds"),
    ("semantic_negotiation", "type_representation_rungs", "lossless_widened_representation", "refuses"),
    ("semantic_negotiation", "type_representation_rungs", "approved_lossy_transformation", "holds"),
    ("semantic_negotiation", "type_representation_rungs", "approved_lossy_transformation", "refuses"),
    ("semantic_negotiation", "type_representation_rungs", "refuse_unrepresentable_type", "holds"),
    ("semantic_negotiation", "type_representation_rungs", "refuse_unrepresentable_type", "refuses"),

    # temporal_timezone_rungs
    ("semantic_negotiation", "temporal_timezone_rungs", "utc_normalized_timestamptz", "holds"),
    ("semantic_negotiation", "temporal_timezone_rungs", "utc_normalized_timestamptz", "refuses"),
    ("semantic_negotiation", "temporal_timezone_rungs", "explicit_offset_reconciliation", "holds"),
    ("semantic_negotiation", "temporal_timezone_rungs", "explicit_offset_reconciliation", "refuses"),
    ("semantic_negotiation", "temporal_timezone_rungs", "bounded_subsecond_adaptation", "holds"),
    ("semantic_negotiation", "temporal_timezone_rungs", "bounded_subsecond_adaptation", "refuses"),
    ("semantic_negotiation", "temporal_timezone_rungs", "refuse_ambiguous_temporal_mapping", "holds"),
    ("semantic_negotiation", "temporal_timezone_rungs", "refuse_ambiguous_temporal_mapping", "refuses"),

    # collation_unicode_rungs
    ("semantic_negotiation", "collation_unicode_rungs", "exact_binary_codepoint_equivalence", "holds"),
    ("semantic_negotiation", "collation_unicode_rungs", "exact_binary_codepoint_equivalence", "refuses"),
    ("semantic_negotiation", "collation_unicode_rungs", "canonical_unicode_nfc_collation", "holds"),
    ("semantic_negotiation", "collation_unicode_rungs", "canonical_unicode_nfc_collation", "refuses"),
    ("semantic_negotiation", "collation_unicode_rungs", "approved_pad_space_relaxation", "holds"),
    ("semantic_negotiation", "collation_unicode_rungs", "approved_pad_space_relaxation", "refuses"),
    ("semantic_negotiation", "collation_unicode_rungs", "refuse_conflicting_collation_or_charset", "holds"),
    ("semantic_negotiation", "collation_unicode_rungs", "refuse_conflicting_collation_or_charset", "refuses"),

    # json_structured_rungs
    ("semantic_negotiation", "json_structured_rungs", "canonical_json_semantic_equivalence", "holds"),
    ("semantic_negotiation", "json_structured_rungs", "canonical_json_semantic_equivalence", "refuses"),
    ("semantic_negotiation", "json_structured_rungs", "lossless_json_container_conversion", "holds"),
    ("semantic_negotiation", "json_structured_rungs", "lossless_json_container_conversion", "refuses"),
    ("semantic_negotiation", "json_structured_rungs", "approved_json_text_fallback", "holds"),
    ("semantic_negotiation", "json_structured_rungs", "approved_json_text_fallback", "refuses"),
    ("semantic_negotiation", "json_structured_rungs", "refuse_lossy_json_mutation", "holds"),
    ("semantic_negotiation", "json_structured_rungs", "refuse_lossy_json_mutation", "refuses"),

    # null_semantics_rungs
    ("semantic_negotiation", "null_semantics_rungs", "strict_sql_standard_null_distinct", "holds"),
    ("semantic_negotiation", "null_semantics_rungs", "strict_sql_standard_null_distinct", "refuses"),
    ("semantic_negotiation", "null_semantics_rungs", "reconciled_empty_string_null_mapping", "holds"),
    ("semantic_negotiation", "null_semantics_rungs", "reconciled_empty_string_null_mapping", "refuses"),
    ("semantic_negotiation", "null_semantics_rungs", "refuse_incompatible_null_semantics", "holds"),
    ("semantic_negotiation", "null_semantics_rungs", "refuse_incompatible_null_semantics", "refuses"),

    # keyless_multiplicity_rungs
    ("semantic_negotiation", "keyless_multiplicity_rungs", "multiset_frequency_hash_verification", "holds"),
    ("semantic_negotiation", "keyless_multiplicity_rungs", "multiset_frequency_hash_verification", "refuses"),
    ("semantic_negotiation", "keyless_multiplicity_rungs", "synthetic_surrogate_row_identity", "holds"),
    ("semantic_negotiation", "keyless_multiplicity_rungs", "synthetic_surrogate_row_identity", "refuses"),
    ("semantic_negotiation", "keyless_multiplicity_rungs", "refuse_unkeyed_set_collapsing", "holds"),
    ("semantic_negotiation", "keyless_multiplicity_rungs", "refuse_unkeyed_set_collapsing", "refuses"),

    # cross_table_commit_rungs
    ("semantic_negotiation", "cross_table_commit_rungs", "synchronized_snapshot_isolation", "holds"),
    ("semantic_negotiation", "cross_table_commit_rungs", "synchronized_snapshot_isolation", "refuses"),
    ("semantic_negotiation", "cross_table_commit_rungs", "quiesced_consistent_freeze", "holds"),
    ("semantic_negotiation", "cross_table_commit_rungs", "quiesced_consistent_freeze", "refuses"),
    ("semantic_negotiation", "cross_table_commit_rungs", "refuse_inconsistent_cross_table_read", "holds"),
    ("semantic_negotiation", "cross_table_commit_rungs", "refuse_inconsistent_cross_table_read", "refuses"),

    # schema_effects_rungs
    ("semantic_negotiation", "schema_effects_rungs", "reconciled_generated_and_counter_state", "holds"),
    ("semantic_negotiation", "schema_effects_rungs", "reconciled_generated_and_counter_state", "refuses"),
    ("semantic_negotiation", "schema_effects_rungs", "deferred_foreign_key_and_trigger_suppression", "holds"),
    ("semantic_negotiation", "schema_effects_rungs", "deferred_foreign_key_and_trigger_suppression", "refuses"),
    ("semantic_negotiation", "schema_effects_rungs", "refuse_unmanaged_schema_effects", "holds"),
    ("semantic_negotiation", "schema_effects_rungs", "refuse_unmanaged_schema_effects", "refuses"),

    # fast_path_admission_rungs
    ("semantic_negotiation", "fast_path_admission_rungs", "admit_lossless_fast_path", "holds"),
    ("semantic_negotiation", "fast_path_admission_rungs", "admit_lossless_fast_path", "refuses"),
    ("semantic_negotiation", "fast_path_admission_rungs", "admit_with_semantic_adaptation_filter", "holds"),
    ("semantic_negotiation", "fast_path_admission_rungs", "admit_with_semantic_adaptation_filter", "refuses"),
    ("semantic_negotiation", "fast_path_admission_rungs", "fallback_to_verified_row_by_row", "holds"),
    ("semantic_negotiation", "fast_path_admission_rungs", "fallback_to_verified_row_by_row", "refuses"),
    ("semantic_negotiation", "fast_path_admission_rungs", "refuse_fast_path_semantic_breach", "holds"),
    ("semantic_negotiation", "fast_path_admission_rungs", "refuse_fast_path_semantic_breach", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures & Helpers
# -----------------------------------------------------------------------------

def _sample_approved_rules():
    now = datetime.datetime.now(datetime.timezone.utc)
    return [
        ApprovedSemanticRule(
            rule_id="rule_dec_scale",
            domain=SemanticDomain.DECIMAL_UNSIGNED,
            source_pattern="decimal",
            target_pattern="decimal",
            transformation_name="round_half_even",
            max_loss_tolerance=0.005,
            approved_by="dba_admin",
            approved_at=now,
            reason="Approved minor scale reduction from 4 to 2 decimals",
        ),
        ApprovedSemanticRule(
            rule_id="rule_subsecond",
            domain=SemanticDomain.TEMPORAL_TIMEZONE,
            source_pattern="timestamp",
            target_pattern="datetime",
            transformation_name="truncate_microseconds_to_millis",
            max_loss_tolerance=0.001,
            approved_by="lead_architect",
            approved_at=now,
            reason="Approved 3-digit millisecond truncation",
        ),
        ApprovedSemanticRule(
            rule_id="rule_pad_space",
            domain=SemanticDomain.COLLATION_UNICODE,
            source_pattern="char",
            target_pattern="varchar",
            transformation_name="rstrip_padding",
            max_loss_tolerance=0.0,
            approved_by="lead_architect",
            approved_at=now,
            reason="Approved trailing space stripping under pad collation",
        ),
        ApprovedSemanticRule(
            rule_id="rule_json_text",
            domain=SemanticDomain.JSON_STRUCTURED,
            source_pattern="json",
            target_pattern="text",
            transformation_name="canonical_json_serializer",
            max_loss_tolerance=0.0,
            approved_by="lead_architect",
            approved_at=now,
            reason="Approved canonical JSON text serialization",
        ),
        ApprovedSemanticRule(
            rule_id="rule_empty_null",
            domain=SemanticDomain.NULL_SEMANTICS,
            source_pattern="empty_string",
            target_pattern="null",
            transformation_name="oracle_empty_string_to_null",
            max_loss_tolerance=0.0,
            approved_by="compliance_officer",
            approved_at=now,
            reason="Approved Oracle empty-string to NULL equivalence for legacy compatibility",
        ),
    ]


# -----------------------------------------------------------------------------
# Test 1: Exercise All 32 Rungs on Both HOLDS and REFUSES
# -----------------------------------------------------------------------------

def test_every_semantic_negotiation_rung_holds_and_refuses():
    """Verify that every decide.Rung ladder is exercised on both branches."""
    with watched() as seen:
        # 1. Type representation rungs
        # Holds strict_lossless
        c1 = decide.climb("type test", type_representation_rungs(), decide.Facts({
            "source_types_lossless": True,
            "no_unsigned_overflow": True,
            "no_decimal_truncation": True,
        }))
        assert c1.path == "strict_lossless_representation"

        # Refuses strict_lossless, holds lossless_widened
        c2 = decide.climb("type test", type_representation_rungs(), decide.Facts({
            "source_types_lossless": False,
            "widened_capacity_available": True,
            "widened_storage_budget_ok": True,
        }))
        assert c2.path == "lossless_widened_representation"

        # Refuses strict & widened, holds approved_lossy
        c3 = decide.climb("type test", type_representation_rungs(), decide.Facts({
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "has_approved_lossy_rule": True,
            "loss_within_tolerance": True,
        }))
        assert c3.path == "approved_lossy_transformation"

        # Refuses all three, holds refuse_unrepresentable_type
        c4 = decide.climb("type test", type_representation_rungs(), decide.Facts({
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "has_approved_lossy_rule": False,
            "must_block_unrepresentable_type": True,
        }))
        assert c4.path == "refuse_unrepresentable_type"

        # Refuses all four
        c5 = decide.climb("type test", type_representation_rungs(), decide.Facts({
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "has_approved_lossy_rule": False,
            "must_block_unrepresentable_type": False,
        }))
        assert c5.path is None

        # 2. Temporal timezone rungs
        # Holds utc_normalized
        t1 = decide.climb("temporal test", temporal_timezone_rungs(), decide.Facts({
            "both_support_timestamptz": True,
            "subsecond_precision_preserved": True,
        }))
        assert t1.path == "utc_normalized_timestamptz"

        # Holds explicit_offset
        t2 = decide.climb("temporal test", temporal_timezone_rungs(), decide.Facts({
            "both_support_timestamptz": False,
            "explicit_offset_declared": True,
            "no_dst_ambiguity": True,
        }))
        assert t2.path == "explicit_offset_reconciliation"

        # Holds bounded_subsecond
        t3 = decide.climb("temporal test", temporal_timezone_rungs(), decide.Facts({
            "both_support_timestamptz": False,
            "explicit_offset_declared": False,
            "subsecond_truncation_approved": True,
        }))
        assert t3.path == "bounded_subsecond_adaptation"

        # Holds refuse_ambiguous
        t4 = decide.climb("temporal test", temporal_timezone_rungs(), decide.Facts({
            "both_support_timestamptz": False,
            "explicit_offset_declared": False,
            "subsecond_truncation_approved": False,
            "must_block_ambiguous_temporal": True,
        }))
        assert t4.path == "refuse_ambiguous_temporal_mapping"

        # Refuses all
        t5 = decide.climb("temporal test", temporal_timezone_rungs(), decide.Facts({
            "both_support_timestamptz": False,
            "explicit_offset_declared": False,
            "subsecond_truncation_approved": False,
            "must_block_ambiguous_temporal": False,
        }))
        assert t5.path is None

        # 3. Collation unicode rungs
        # Holds exact_binary
        col1 = decide.climb("col test", collation_unicode_rungs(), decide.Facts({
            "identical_binary_and_pad": True,
            "four_byte_utf8_matched": True,
        }))
        assert col1.path == "exact_binary_codepoint_equivalence"

        # Holds canonical_unicode_nfc
        col2 = decide.climb("col test", collation_unicode_rungs(), decide.Facts({
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": True,
            "collation_preserves_unique": True,
        }))
        assert col2.path == "canonical_unicode_nfc_collation"

        # Holds approved_pad_space
        col3 = decide.climb("col test", collation_unicode_rungs(), decide.Facts({
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": False,
            "approved_pad_rule_registered": True,
            "no_trailing_space_collisions": True,
        }))
        assert col3.path == "approved_pad_space_relaxation"

        # Holds refuse_conflicting
        col4 = decide.climb("col test", collation_unicode_rungs(), decide.Facts({
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": False,
            "approved_pad_rule_registered": False,
            "must_block_collation_conflict": True,
        }))
        assert col4.path == "refuse_conflicting_collation_or_charset"

        # Refuses all
        col5 = decide.climb("col test", collation_unicode_rungs(), decide.Facts({
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": False,
            "approved_pad_rule_registered": False,
            "must_block_collation_conflict": False,
        }))
        assert col5.path is None

        # 4. JSON structured rungs
        # Holds canonical_json
        j1 = decide.climb("json test", json_structured_rungs(), decide.Facts({
            "canonical_json_normalizer_active": True,
            "no_json_numeric_precision_loss": True,
        }))
        assert j1.path == "canonical_json_semantic_equivalence"

        # Holds lossless_json_container
        j2 = decide.climb("json test", json_structured_rungs(), decide.Facts({
            "canonical_json_normalizer_active": False,
            "json_container_lossless": True,
        }))
        assert j2.path == "lossless_json_container_conversion"

        # Holds approved_json_text_fallback
        j3 = decide.climb("json test", json_structured_rungs(), decide.Facts({
            "canonical_json_normalizer_active": False,
            "json_container_lossless": False,
            "approved_json_text_rule": True,
        }))
        assert j3.path == "approved_json_text_fallback"

        # Holds refuse_lossy_json
        j4 = decide.climb("json test", json_structured_rungs(), decide.Facts({
            "canonical_json_normalizer_active": False,
            "json_container_lossless": False,
            "approved_json_text_rule": False,
            "must_block_lossy_json": True,
        }))
        assert j4.path == "refuse_lossy_json_mutation"

        # Refuses all
        j5 = decide.climb("json test", json_structured_rungs(), decide.Facts({
            "canonical_json_normalizer_active": False,
            "json_container_lossless": False,
            "approved_json_text_rule": False,
            "must_block_lossy_json": False,
        }))
        assert j5.path is None

        # 5. Null semantics rungs
        # Holds strict_sql_standard
        n1 = decide.climb("null test", null_semantics_rungs(), decide.Facts({
            "empty_string_distinct_from_null": True,
            "standard_null_unique_handling": True,
        }))
        assert n1.path == "strict_sql_standard_null_distinct"

        # Holds reconciled_empty_string
        n2 = decide.climb("null test", null_semantics_rungs(), decide.Facts({
            "empty_string_distinct_from_null": False,
            "approved_empty_string_null_rule": True,
            "not_null_safe_under_conversion": True,
        }))
        assert n2.path == "reconciled_empty_string_null_mapping"

        # Holds refuse_incompatible_null
        n3 = decide.climb("null test", null_semantics_rungs(), decide.Facts({
            "empty_string_distinct_from_null": False,
            "approved_empty_string_null_rule": False,
            "must_block_null_incompatibility": True,
        }))
        assert n3.path == "refuse_incompatible_null_semantics"

        # Refuses all
        n4 = decide.climb("null test", null_semantics_rungs(), decide.Facts({
            "empty_string_distinct_from_null": False,
            "approved_empty_string_null_rule": False,
            "must_block_null_incompatibility": False,
        }))
        assert n4.path is None

        # 6. Keyless multiplicity rungs
        # Holds multiset_frequency_hash
        m1 = decide.climb("mult test", keyless_multiplicity_rungs(), decide.Facts({
            "multiset_frequency_hashing_enabled": True,
            "multiplicity_count_matched": True,
        }))
        assert m1.path == "multiset_frequency_hash_verification"

        # Holds synthetic_surrogate
        m2 = decide.climb("mult test", keyless_multiplicity_rungs(), decide.Facts({
            "multiset_frequency_hashing_enabled": False,
            "surrogate_row_id_available": True,
            "chunking_preserves_duplicates": True,
        }))
        assert m2.path == "synthetic_surrogate_row_identity"

        # Holds refuse_unkeyed_set
        m3 = decide.climb("mult test", keyless_multiplicity_rungs(), decide.Facts({
            "multiset_frequency_hashing_enabled": False,
            "surrogate_row_id_available": False,
            "must_block_set_collapsing": True,
        }))
        assert m3.path == "refuse_unkeyed_set_collapsing"

        # Refuses all
        m4 = decide.climb("mult test", keyless_multiplicity_rungs(), decide.Facts({
            "multiset_frequency_hashing_enabled": False,
            "surrogate_row_id_available": False,
            "must_block_set_collapsing": False,
        }))
        assert m4.path is None

        # 7. Cross-table commit rungs
        # Holds synchronized_snapshot
        comm1 = decide.climb("commit test", cross_table_commit_rungs(), decide.Facts({
            "synchronized_snapshot_active": True,
            "foreign_keys_referentially_valid": True,
        }))
        assert comm1.path == "synchronized_snapshot_isolation"

        # Holds quiesced_freeze
        comm2 = decide.climb("commit test", cross_table_commit_rungs(), decide.Facts({
            "synchronized_snapshot_active": False,
            "group_quiescence_locked": True,
            "cross_table_positions_reconciled": True,
        }))
        assert comm2.path == "quiesced_consistent_freeze"

        # Holds refuse_inconsistent
        comm3 = decide.climb("commit test", cross_table_commit_rungs(), decide.Facts({
            "synchronized_snapshot_active": False,
            "group_quiescence_locked": False,
            "must_block_inconsistent_cross_table": True,
        }))
        assert comm3.path == "refuse_inconsistent_cross_table_read"

        # Refuses all
        comm4 = decide.climb("commit test", cross_table_commit_rungs(), decide.Facts({
            "synchronized_snapshot_active": False,
            "group_quiescence_locked": False,
            "must_block_inconsistent_cross_table": False,
        }))
        assert comm4.path is None

        # 8. Schema effects rungs
        # Holds reconciled_generated
        sch1 = decide.climb("schema test", schema_effects_rungs(), decide.Facts({
            "generated_columns_safely_handled": True,
            "counter_high_water_mark_safe": True,
        }))
        assert sch1.path == "reconciled_generated_and_counter_state"

        # Holds deferred_fk_trigger
        sch2 = decide.climb("schema test", schema_effects_rungs(), decide.Facts({
            "generated_columns_safely_handled": False,
            "triggers_and_fks_managed": True,
            "post_load_integrity_check_planned": True,
        }))
        assert sch2.path == "deferred_foreign_key_and_trigger_suppression"

        # Holds refuse_unmanaged
        sch3 = decide.climb("schema test", schema_effects_rungs(), decide.Facts({
            "generated_columns_safely_handled": False,
            "triggers_and_fks_managed": False,
            "must_block_unmanaged_schema_effects": True,
        }))
        assert sch3.path == "refuse_unmanaged_schema_effects"

        # Refuses all
        sch4 = decide.climb("schema test", schema_effects_rungs(), decide.Facts({
            "generated_columns_safely_handled": False,
            "triggers_and_fks_managed": False,
            "must_block_unmanaged_schema_effects": False,
        }))
        assert sch4.path is None

        # 9. Fast path admission rungs
        # Holds admit_lossless
        fp1 = decide.climb("fp test", fast_path_admission_rungs(), decide.Facts({
            "all_semantics_lossless_or_approved": True,
            "no_unapproved_lossy_rules": True,
            "multiplicity_and_commit_preserved": True,
        }))
        assert fp1.path == "admit_lossless_fast_path"

        # Holds admit_adaptation_filter
        fp2 = decide.climb("fp test", fast_path_admission_rungs(), decide.Facts({
            "all_semantics_lossless_or_approved": False,
            "pushdown_adapter_active": True,
            "lossy_adaptations_approved": True,
        }))
        assert fp2.path == "admit_with_semantic_adaptation_filter"

        # Holds fallback_row_by_row
        fp3 = decide.climb("fp test", fast_path_admission_rungs(), decide.Facts({
            "all_semantics_lossless_or_approved": False,
            "pushdown_adapter_active": False,
            "row_by_row_available": True,
            "row_by_row_sla_acceptable": True,
        }))
        assert fp3.path == "fallback_to_verified_row_by_row"

        # Holds refuse_fast_path
        fp4 = decide.climb("fp test", fast_path_admission_rungs(), decide.Facts({
            "all_semantics_lossless_or_approved": False,
            "pushdown_adapter_active": False,
            "row_by_row_available": False,
            "must_block_fast_path_breach": True,
        }))
        assert fp4.path == "refuse_fast_path_semantic_breach"

        # Refuses all
        fp5 = decide.climb("fp test", fast_path_admission_rungs(), decide.Facts({
            "all_semantics_lossless_or_approved": False,
            "pushdown_adapter_active": False,
            "row_by_row_available": False,
            "must_block_fast_path_breach": False,
        }))
        assert fp5.path is None

    # Verify that all 32 rungs reached both HOLDS and REFUSES
    for module, ladder, rung, branch_kind in PROVES_RUNGS:
        branches = branch(seen, rung)
        assert branch_kind in branches, f"Rung '{rung}' did not reach branch '{branch_kind}' (reached: {branches})"


# -----------------------------------------------------------------------------
# Test 2: Decimal & Unsigned Boundaries
# -----------------------------------------------------------------------------

def test_decimal_and_unsigned_boundaries():
    """Verify unsigned integer overflow protection and decimal scale loss prevention."""
    rules = _sample_approved_rules()

    # 1. Unsigned bigint into signed bigint without widening risks overflow -> refuses strict_lossless
    choice = negotiate_type_representation(
        source_type="bigint unsigned",
        target_type="bigint",
        facts_input={"is_unsigned": True},
        approved_rules=rules,
    )
    # Refuses strict lossless because unsigned overflow can occur
    assert choice.path != "strict_lossless_representation"

    # With widened type available (e.g. numeric), picks lossless widened
    choice_widened = negotiate_type_representation(
        source_type="bigint unsigned",
        target_type="numeric",
        facts_input={
            "source_types_lossless": False,
            "widened_capacity_available": True,
            "widened_storage_budget_ok": True,
        },
        approved_rules=rules,
    )
    assert choice_widened.path == "lossless_widened_representation"

    # Decimal scale truncation: source scale 4 into target scale 2
    # Within tolerance of approved rule -> approved_lossy_transformation
    choice_lossy = negotiate_type_representation(
        source_type="decimal(18,4)",
        target_type="decimal(18,2)",
        facts_input={
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "source_scale": 4,
            "target_scale": 2,
            "observed_loss": 0.003,
        },
        approved_rules=rules,
    )
    assert choice_lossy.path == "approved_lossy_transformation"

    # Without approved rule -> refuses unrepresentable type
    choice_refuse = negotiate_type_representation(
        source_type="decimal(18,4)",
        target_type="decimal(18,2)",
        facts_input={
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "source_scale": 4,
            "target_scale": 2,
            "observed_loss": 0.05,
        },
        approved_rules=(),
    )
    assert choice_refuse.path == "refuse_unrepresentable_type"


# -----------------------------------------------------------------------------
# Test 3: Timestamp, Timezone, and DST Transition Ambiguity
# -----------------------------------------------------------------------------

def test_timestamp_timezone_and_dst():
    """Verify UTC normalization, subsecond microsecond precision, and DST protection."""
    rules = _sample_approved_rules()

    # Timezone-aware timestamp on both ends -> utc_normalized_timestamptz
    choice_tz = negotiate_temporal_timezone(
        source_temporal="timestamp with time zone",
        target_temporal="timestamptz",
        facts_input={"both_support_timestamptz": True, "subsecond_precision_preserved": True},
        approved_rules=rules,
    )
    assert choice_tz.path == "utc_normalized_timestamptz"

    # Subsecond precision truncation (microsecond 6-digit to millisecond 3-digit)
    choice_sub = negotiate_temporal_timezone(
        source_temporal="timestamp(6)",
        target_temporal="datetime(3)",
        facts_input={
            "both_support_timestamptz": False,
            "explicit_offset_declared": False,
            "source_subsecond_precision": 6,
            "target_subsecond_precision": 3,
            "subsecond_truncation_approved": True,
        },
        approved_rules=rules,
    )
    assert choice_sub.path == "bounded_subsecond_adaptation"

    # DST transition ambiguity present without explicit offset -> refuse_ambiguous_temporal_mapping
    choice_dst = negotiate_temporal_timezone(
        source_temporal="datetime",
        target_temporal="datetime",
        facts_input={
            "both_support_timestamptz": False,
            "explicit_offset_declared": False,
            "dst_ambiguity_present": True,
            "subsecond_truncation_approved": False,
            "must_block_ambiguous_temporal": True,
        },
        approved_rules=(),
    )
    assert choice_dst.path == "refuse_ambiguous_temporal_mapping"


# -----------------------------------------------------------------------------
# Test 4: Collation, Unicode NFC, and Trailing Space Pad Semantics
# -----------------------------------------------------------------------------

def test_collation_unicode_and_pad_spaces():
    """Verify binary collation, Unicode NFC normalization, and trailing space padding."""
    rules = _sample_approved_rules()

    # Exact binary collation
    choice_bin = negotiate_collation_unicode(
        source_collation="utf8mb4_bin",
        target_collation="utf8mb4_bin",
        facts_input={"identical_binary_and_pad": True, "four_byte_utf8_matched": True},
        approved_rules=rules,
    )
    assert choice_bin.path == "exact_binary_codepoint_equivalence"

    # Unicode NFC normalization
    choice_nfc = negotiate_collation_unicode(
        source_collation="utf8mb4_general_ci",
        target_collation="utf8mb4_0900_ai_ci",
        facts_input={
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": True,
            "collation_preserves_unique": True,
        },
        approved_rules=rules,
    )
    assert choice_nfc.path == "canonical_unicode_nfc_collation"

    # Pad space vs NO PAD trailing space relaxation
    choice_pad = negotiate_collation_unicode(
        source_collation="utf8mb4_bin",
        target_collation="utf8mb4_nopad_bin",
        facts_input={
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": False,
            "approved_pad_rule_registered": True,
            "no_trailing_space_collisions": True,
        },
        approved_rules=rules,
    )
    assert choice_pad.path == "approved_pad_space_relaxation"

    # Conflicting collation causing duplicate collision in unique column -> refuse
    choice_conflict = negotiate_collation_unicode(
        source_collation="utf8mb4_bin",
        target_collation="utf8mb4_general_ci",
        facts_input={
            "identical_binary_and_pad": False,
            "unicode_nfc_compatible": False,
            "approved_pad_rule_registered": False,
            "must_block_collation_conflict": True,
        },
        approved_rules=(),
    )
    assert choice_conflict.path == "refuse_conflicting_collation_or_charset"


# -----------------------------------------------------------------------------
# Test 5: JSON Semantic Equivalence vs Large Integer Truncation
# -----------------------------------------------------------------------------

def test_json_structured_semantics():
    """Verify JSON canonical equivalence independent of key-ordering and 64-bit int preservation."""
    rules = _sample_approved_rules()

    # Canonical equivalence with active key-order normalizer
    choice_json = negotiate_json_structured(
        source_json_spec="jsonb",
        target_json_spec="json",
        facts_input={
            "canonical_json_normalizer_active": True,
            "no_json_numeric_precision_loss": True,
        },
        approved_rules=rules,
    )
    assert choice_json.path == "canonical_json_semantic_equivalence"

    # Large integers exceeding 53 bits in JSON -> refuse lossy mutation
    choice_large_int = negotiate_json_structured(
        source_json_spec="jsonb",
        target_json_spec="json",
        facts_input={
            "canonical_json_normalizer_active": True,
            "has_large_json_integers": True,
            "json_container_lossless": False,
            "approved_json_text_rule": False,
            "must_block_lossy_json": True,
        },
        approved_rules=(),
    )
    assert choice_large_int.path == "refuse_lossy_json_mutation"


# -----------------------------------------------------------------------------
# Test 6: NULL Semantics & Oracle Empty-String Conflation
# -----------------------------------------------------------------------------

def test_null_semantics_and_empty_strings():
    """Verify SQL standard NULL distinctness and explicit approved Oracle empty string rule."""
    rules = _sample_approved_rules()

    # Strict SQL standard where empty string != NULL
    choice_strict = negotiate_null_semantics(
        source_null_spec="standard_null",
        target_null_spec="standard_null",
        facts_input={"empty_string_distinct_from_null": True, "standard_null_unique_handling": True},
        approved_rules=rules,
    )
    assert choice_strict.path == "strict_sql_standard_null_distinct"

    # Engine conflates empty string with NULL (Oracle) with approved rule
    choice_reconciled = negotiate_null_semantics(
        source_null_spec="oracle_empty_string",
        target_null_spec="standard_null",
        facts_input={
            "engine_conflates_empty_string_null": True,
            "approved_empty_string_null_rule": True,
            "not_null_safe_under_conversion": True,
        },
        approved_rules=rules,
    )
    assert choice_reconciled.path == "reconciled_empty_string_null_mapping"

    # Conflates empty string with NULL without approved rule or breaches NOT NULL -> refuse
    choice_refuse = negotiate_null_semantics(
        source_null_spec="oracle_empty_string",
        target_null_spec="standard_null",
        facts_input={
            "engine_conflates_empty_string_null": True,
            "approved_empty_string_null_rule": False,
            "must_block_null_incompatibility": True,
        },
        approved_rules=(),
    )
    assert choice_refuse.path == "refuse_incompatible_null_semantics"


# -----------------------------------------------------------------------------
# Test 7: Keyless Multiplicity & Cross-Table Commit Semantics
# -----------------------------------------------------------------------------

def test_keyless_multiplicity_and_commit_semantics():
    """Verify exact duplicate multiplicity preservation and synchronized snapshot commit."""
    # Keyless table with duplicates: multiset verifier
    verifier = MultisetVerifier()
    row1 = (1, "alpha", 100.5)
    row2 = (2, "beta", 200.0)

    # Source has 2 copies of row1, 1 of row2
    verifier.record_source_tuple(row1)
    verifier.record_source_tuple(row1)
    verifier.record_source_tuple(row2)

    # Target has matching duplicates
    verifier.record_target_tuple(row1)
    verifier.record_target_tuple(row1)
    verifier.record_target_tuple(row2)

    ok, badge, msg = verifier.verify_multiplicity()
    assert ok is True
    assert badge == CareBadge.PASS

    # Now simulate naive set difference collapsing duplicate row1 to 1 copy
    bad_verifier = MultisetVerifier()
    bad_verifier.record_source_tuple(row1)
    bad_verifier.record_source_tuple(row1)
    bad_verifier.record_target_tuple(row1)  # Only 1 copy on target!

    bad_ok, bad_badge, bad_msg = bad_verifier.verify_multiplicity()
    assert bad_ok is False
    assert bad_badge == CareBadge.ACTION
    assert "keyless multiplicity breach" in bad_msg

    # Keyless table decision ladder
    col = ColumnSemanticSpec(column_name="payload", source_type="text", target_type="text")
    keyless_table = TableSemanticSpec(
        table_name="event_logs",
        columns=(col,),
        primary_key=(),
        unique_constraints=(),
    )
    choice_mult = negotiate_keyless_multiplicity(
        keyless_table,
        facts_input={"enable_frequency_hashing": True, "duplicate_counts_identical": True},
    )
    assert choice_mult.path == "multiset_frequency_hash_verification"

    # Cross-table commit ladder
    cs = CrossTableCommitSpec(
        group_id="orders_and_items",
        tables=("orders", "order_items"),
        cross_table_foreign_keys=(("order_items", "orders"),),
        shared_snapshot_required=True,
    )
    commit_choice = negotiate_cross_table_commit(
        cs,
        facts_input={"has_synchronized_snapshot": True, "orphaned_foreign_keys_present": False},
    )
    assert commit_choice.path == "synchronized_snapshot_isolation"

    # Cross-table independent read without snapshot -> refuse
    bad_commit = negotiate_cross_table_commit(
        cs,
        facts_input={
            "has_synchronized_snapshot": False,
            "group_quiescence_locked": False,
            "must_block_inconsistent_cross_table": True,
        },
    )
    assert bad_commit.path == "refuse_inconsistent_cross_table_read"


# -----------------------------------------------------------------------------
# Test 8: Metamorphic & Boundary Value Comparisons (Bytes vs Equivalence)
# -----------------------------------------------------------------------------

def test_metamorphic_and_boundary_value_comparisons():
    """Verify that row equality distinguishes raw bytes from approved semantic equivalence."""
    rules = _sample_approved_rules()

    # 1. JSON key ordering: different string bytes, but canonically equivalent!
    col_json = ColumnSemanticSpec(column_name="doc", source_type="json", target_type="json")
    val_s = '{"b": 2, "a": 1}'
    val_t = '{"a": 1, "b": 2}'
    ok_j, kind_j, msg_j = compare_cross_engine_values(val_s, val_t, col_json, approved_rules=rules)
    assert ok_j is True
    assert kind_j == SemanticEquivalenceKind.CANONICAL_SEMANTIC_EQUIVALENCE

    # 2. Decimal: 10.50 vs 10.5
    col_dec = ColumnSemanticSpec(column_name="price", source_type="decimal(10,2)", target_type="decimal(10,1)")
    ok_d, kind_d, msg_d = compare_cross_engine_values(
        decimal.Decimal("10.50"), decimal.Decimal("10.5"), col_dec, approved_rules=rules
    )
    assert ok_d is True
    assert kind_d in (
        SemanticEquivalenceKind.STRICT_IDENTICAL_BYTES,
        SemanticEquivalenceKind.CANONICAL_SEMANTIC_EQUIVALENCE,
    )

    # 3. Timestamp UTC normalization: different local string representation, identical instant
    col_ts = ColumnSemanticSpec(column_name="ts", source_type="timestamptz", target_type="timestamptz")
    t_utc = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    t_plus7 = datetime.datetime(
        2026, 10, 17, 19, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=7))
    )
    ok_t, kind_t, msg_t = compare_cross_engine_values(t_utc, t_plus7, col_ts, approved_rules=rules)
    assert ok_t is True
    assert kind_t == SemanticEquivalenceKind.CANONICAL_SEMANTIC_EQUIVALENCE

    # 4. Trailing pad spaces: 'data   ' vs 'data' under approved pad rule
    col_txt = ColumnSemanticSpec(column_name="tag", source_type="char(10)", target_type="varchar(10)")
    ok_p, kind_p, msg_p = compare_cross_engine_values("data   ", "data", col_txt, approved_rules=rules)
    assert ok_p is True
    assert kind_p == SemanticEquivalenceKind.APPROVED_LOSSY_EQUIVALENCE

    # 5. Empty string vs NULL: with rule vs without rule
    col_null = ColumnSemanticSpec(column_name="opt", source_type="varchar(10)", target_type="varchar(10)")
    ok_null_app, kind_null_app, _ = compare_cross_engine_values("", None, col_null, approved_rules=rules)
    assert ok_null_app is True
    assert kind_null_app == SemanticEquivalenceKind.APPROVED_LOSSY_EQUIVALENCE

    ok_null_noapp, kind_null_noapp, _ = compare_cross_engine_values("", None, col_null, approved_rules=())
    assert ok_null_noapp is False
    assert kind_null_noapp == SemanticEquivalenceKind.INCOMPATIBLE_CONSTRAINT_BREACH


# -----------------------------------------------------------------------------
# Test 9: End-to-End Table Negotiation & Fast Path Admission
# -----------------------------------------------------------------------------

def test_evaluate_table_semantic_negotiation_lossless_vs_breach():
    """Verify full table semantic evaluation admitting fast path or refusing on breach."""
    rules = _sample_approved_rules()

    col1 = ColumnSemanticSpec(column_name="id", source_type="bigint", target_type="bigint")
    col2 = ColumnSemanticSpec(column_name="created_at", source_type="timestamptz", target_type="timestamptz")
    col3 = ColumnSemanticSpec(column_name="payload", source_type="json", target_type="json")

    src_table = TableSemanticSpec(
        table_name="users",
        columns=(col1, col2, col3),
        primary_key=("id",),
        unique_constraints=(("id",),),
    )
    tgt_table = TableSemanticSpec(
        table_name="users",
        columns=(col1, col2, col3),
        primary_key=("id",),
        unique_constraints=(("id",),),
    )

    # 1. Clean lossless table -> PASS, admit lossless fast path
    verdict_clean = evaluate_table_semantic_negotiation(
        src_table,
        tgt_table,
        approved_rules=rules,
        facts_override={
            "source_types_lossless": True,
            "no_unsigned_overflow": True,
            "no_decimal_truncation": True,
            "both_support_timestamptz": True,
            "subsecond_precision_preserved": True,
            "identical_binary_and_pad": True,
            "four_byte_utf8_matched": True,
            "canonical_json_normalizer_active": True,
            "no_json_numeric_precision_loss": True,
            "empty_string_distinct_from_null": True,
            "standard_null_unique_handling": True,
            "multiset_frequency_hashing_enabled": True,
            "multiplicity_count_matched": True,
            "synchronized_snapshot_active": True,
            "foreign_keys_referentially_valid": True,
            "generated_columns_safely_handled": True,
            "counter_high_water_mark_safe": True,
        },
    )
    assert verdict_clean.admitted is True
    assert verdict_clean.care_badge == CareBadge.PASS
    assert verdict_clean.fast_path_choice.path == "admit_lossless_fast_path"

    # 2. Table with semantic breach (e.g. unhandled duplicate keys or unapproved loss) -> ACTION, refuse fast path
    verdict_breach = evaluate_table_semantic_negotiation(
        src_table,
        tgt_table,
        approved_rules=(),
        facts_override={
            "source_types_lossless": False,
            "widened_capacity_available": False,
            "has_approved_lossy_rule": False,
            "must_block_unrepresentable_type": True,
            "all_semantics_lossless_or_approved": False,
            "pushdown_adapter_active": False,
            "row_by_row_available": False,
            "must_block_fast_path_breach": True,
        },
    )
    assert verdict_breach.admitted is False
    assert verdict_breach.care_badge == CareBadge.ACTION
    assert verdict_breach.fast_path_choice.path == "refuse_fast_path_semantic_breach"


# -----------------------------------------------------------------------------
# Test 10: Durable O(1) Checkpoints, Lease, AIMD Controller & Merkle Diffs
# -----------------------------------------------------------------------------

def test_checkpoint_lease_aimd_and_merkle_diff(tmp_path: pathlib.Path):
    """Verify typed checkpoint resume in O(1), active lease, AIMD batching, and Merkle diffs."""
    # 1. Typed durable checkpoint store
    ckpt_path = tmp_path / "semantic_checkpoints.json"
    store = SemanticCheckpointStore(ckpt_path)

    unit_id = f"chunk_{uuid.uuid4()}"
    unit = SemanticCheckpointUnit(
        unit_id=unit_id,
        table_name="orders",
        min_cursor=1000,
        max_cursor=5000,
        verified_row_count=4001,
        multiplicity_hash="sha256_mock_hash",
        completed_at=datetime.datetime.now(datetime.timezone.utc),
    )
    store.record_unit(unit)

    # O(1) retrieval
    assert store.is_completed_o1(unit_id) is True
    assert store.is_completed_o1("non_existent_chunk") is False

    retrieved = store.get_unit(unit_id)
    assert retrieved is not None
    assert retrieved.table_name == "orders"
    assert retrieved.min_cursor == 1000
    assert retrieved.max_cursor == 5000

    # Reload store from disk
    store_reloaded = SemanticCheckpointStore(ckpt_path)
    assert store_reloaded.is_completed_o1(unit_id) is True

    # 2. Mutual exclusion active lease
    lock_file = tmp_path / "semantic.run.lock"
    lease_obj = acquire_semantic_negotiation_lease(lock_file, term_seconds=30.0)
    assert lease_obj is not None
    lease_obj.release()

    # 3. AIMD Batch controller
    aimd = AIMDBatchController(current_batch_size=1000, additive_step=500, multiplicative_factor=0.5)
    size1 = aimd.on_success()
    assert size1 == 1500
    size2 = aimd.on_congestion_or_drift()
    assert size2 == 750

    # 4. Instant cold-start ETA estimator
    eta_est = InstantETAEstimator(total_bytes=100_000_000, default_bytes_per_sec=25_000_000.0)
    rem_sec = eta_est.update(processed_bytes=20_000_000, elapsed_seconds=0.5)
    assert rem_sec >= 0.0

    # 5. Merkle bucket diff
    merkle = MerkleBucketDiff(bucket_count=8)
    merkle.add_source("key1", "hash_a")
    merkle.add_target("key1", "hash_a")
    merkle.add_source("key2", "hash_b")
    merkle.add_target("key2", "hash_c")  # Differs!

    diffs, badge = merkle.compute_root_diffs()
    assert len(diffs) > 0
    assert badge == CareBadge.ACTION

    # 6. Third-party tool wrapper & safe endpoint validation
    ok_tool, tool_badge, tool_msg = SemanticToolWrapper.execute_tool_check(
        tool_name="datafold",
        source_endpoint="127.0.0.1",
        target_endpoint="10.0.0.5",
    )
    assert ok_tool is True
    assert tool_badge == CareBadge.PASS

    # Unsafe host check
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("8.8.8.8")  # Public IP rejected!
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("db.external.com")  # External domain rejected!
