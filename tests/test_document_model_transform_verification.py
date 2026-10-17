"""Tests for Row 636: RCH28.document-model (Verify normalized-to-document transformations after updates)."""
from __future__ import annotations

import copy
import datetime
import decimal
import os
import uuid
from typing import Any, Dict, List

import pytest

from migkit.document_model import (
    ActiveVerificationLeaseError,
    CareBadge,
    ChangeOp,
    ConcurrencyMode,
    ConcurrentSourceAssumptions,
    DefectCategory,
    DeletePropagationPolicy,
    DocumentMappingSpec,
    DocumentTransformVerifier,
    DocumentVerificationError,
    DocumentVerifyCheckpoint,
    EmbeddedRelationRule,
    FieldPathRule,
    MeasuredTargetShape,
    OrderSemantics,
    RelationMultiplicity,
    RelationalMutationEvent,
    TypeConversionKind,
    TypeConversionSpec,
    VerificationFetchStrategy,
    VerificationLeaseLock,
    select_verification_strategy,
    decide_document_verification_strategy,
)
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("document_model", "document_verification_strategy_rungs", "indexed_lookup", "holds"),
    ("document_model", "document_verification_strategy_rungs", "indexed_lookup", "refuses"),
    ("document_model", "document_verification_strategy_rungs", "batched_fetch", "holds"),
    ("document_model", "document_verification_strategy_rungs", "batched_fetch", "refuses"),
    ("document_model", "document_verification_strategy_rungs", "stream_compare", "holds"),
]


# ---------------------------------------------------------------------------
# Fixtures & Sample Relational-to-Document Schema

# ---------------------------------------------------------------------------


def _build_order_mapping_spec(
    item_order_semantics: OrderSemantics = OrderSemantics.SEMANTIC_ORDERED,
    customer_delete_policy: DeletePropagationPolicy = DeletePropagationPolicy.NULLIFY_REFERENCE,
) -> DocumentMappingSpec:
    return DocumentMappingSpec(
        mapping_id="rel-orders-to-mongo-orders",
        version="1.0.0",
        root_entity="orders",
        root_source_pk="order_id",
        target_collection="orders_docs",
        target_root_key_path="_id",
        root_field_mappings=[
            FieldPathRule("order_id", "_id", TypeConversionSpec(TypeConversionKind.IDENTITY), required=True),
            FieldPathRule(
                "order_uuid",
                "metadata.trace_uuid",
                TypeConversionSpec(TypeConversionKind.UUID_TO_STRING),
            ),
            FieldPathRule(
                "created_at",
                "metadata.created_iso",
                TypeConversionSpec(TypeConversionKind.DATETIME_TO_ISO8601),
            ),
            FieldPathRule(
                "total_amount",
                "billing.total_decimal",
                TypeConversionSpec(TypeConversionKind.DECIMAL_TO_DECIMAL128_STR),
            ),
            FieldPathRule(
                "discount_rate",
                "billing.discount_rate",
                TypeConversionSpec(TypeConversionKind.DECIMAL_TO_FLOAT_TOLERANT, float_tolerance=1e-4),
            ),
            FieldPathRule(
                "shipping_zip",
                "shipping.address.postal_code",
                TypeConversionSpec(TypeConversionKind.IDENTITY),
            ),
            FieldPathRule(
                "is_expedited",
                "shipping.expedited",
                TypeConversionSpec(TypeConversionKind.INT_TO_BOOL),
            ),
            FieldPathRule(
                "Optional_note",
                "metadata.note",
                TypeConversionSpec(TypeConversionKind.NULL_TO_ABSENT),
            ),
        ],
        embedded_relations=[
            # N:1 reference lookup: orders.customer_id -> customers.customer_id embedded at "customer"
            EmbeddedRelationRule(
                rule_id="embed-customer-ref",
                parent_entity="orders",
                child_entity="customers",
                parent_key="customer_id",
                child_foreign_key="customer_id",
                child_identity_key="customer_id",
                target_path="customer",
                multiplicity=RelationMultiplicity.MANY_TO_ONE,
                delete_policy=customer_delete_policy,
                field_mappings=[
                    FieldPathRule("customer_id", "id"),
                    FieldPathRule("email", "contact.email"),
                    FieldPathRule("tier", "tier"),
                ],
            ),
            # 1:N child array: order_items embedded at "items", semantically ordered by line_seq
            EmbeddedRelationRule(
                rule_id="embed-order-items",
                parent_entity="orders",
                child_entity="order_items",
                parent_key="order_id",
                child_foreign_key="order_id",
                child_identity_key="item_id",
                target_path="items",
                multiplicity=RelationMultiplicity.ONE_TO_MANY,
                order_semantics=item_order_semantics,
                order_by_fields=["line_seq", "item_id"],
                delete_policy=DeletePropagationPolicy.REMOVE_ELEMENT,
                field_mappings=[
                    FieldPathRule("item_id", "item_id"),
                    FieldPathRule("line_seq", "seq"),
                    FieldPathRule("sku", "sku"),
                    FieldPathRule("qty", "qty"),
                    FieldPathRule(
                        "unit_price",
                        "unit_cents",
                        TypeConversionSpec(TypeConversionKind.DECIMAL_TO_CENTS_INT),
                    ),
                ],
            ),
            # 2nd-level 1:N child-of-child array: item_allocations embedded inside each item at "allocations"
            EmbeddedRelationRule(
                rule_id="embed-item-allocations",
                parent_entity="order_items",
                child_entity="item_allocations",
                parent_key="item_id",
                child_foreign_key="item_id",
                child_identity_key="alloc_id",
                target_path="allocations",
                parent_array_path="items",
                multiplicity=RelationMultiplicity.ONE_TO_MANY,
                order_semantics=OrderSemantics.SEMANTIC_ORDERED,
                order_by_fields=["alloc_id"],
                field_mappings=[
                    FieldPathRule("alloc_id", "alloc_id"),
                    FieldPathRule("warehouse", "warehouse"),
                ],
            ),
        ],
    )


def _sample_normalized_tables() -> Dict[str, List[Dict[str, Any]]]:
    u1 = uuid.UUID("11111111-2222-3333-4444-555555555555")
    u2 = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    return {
        "customers": [
            {"customer_id": "CUST-1", "email": "alice@example.org", "tier": "GOLD"},
            {"customer_id": "CUST-2", "email": "bob@example.org", "tier": "STANDARD"},
        ],
        "orders": [
            {
                "order_id": "ORD-100",
                "customer_id": "CUST-1",
                "order_uuid": u1,
                "created_at": datetime.datetime(2026, 5, 1, 12, 0, 0, tzinfo=datetime.timezone.utc),
                "total_amount": decimal.Decimal("149.95"),
                "discount_rate": decimal.Decimal("0.0500"),
                "shipping_zip": "10110",
                "is_expedited": 1,
                "Optional_note": None,
            },
            {
                "order_id": "ORD-200",
                "customer_id": "CUST-1",
                "order_uuid": u2,
                "created_at": datetime.datetime(2026, 5, 2, 15, 30, 0, tzinfo=datetime.timezone.utc),
                "total_amount": decimal.Decimal("49.50"),
                "discount_rate": decimal.Decimal("0.0000"),
                "shipping_zip": "50200",
                "is_expedited": 0,
                "Optional_note": "leave at gate",
            },
        ],
        "order_items": [
            {
                "item_id": "ITM-1",
                "order_id": "ORD-100",
                "line_seq": 1,
                "sku": "SKU-A",
                "qty": 2,
                "unit_price": decimal.Decimal("50.00"),
            },
            {
                "item_id": "ITM-2",
                "order_id": "ORD-100",
                "line_seq": 2,
                "sku": "SKU-B",
                "qty": 1,
                "unit_price": decimal.Decimal("49.95"),
            },
            {
                "item_id": "ITM-3",
                "order_id": "ORD-200",
                "line_seq": 1,
                "sku": "SKU-C",
                "qty": 1,
                "unit_price": decimal.Decimal("49.50"),
            },
        ],
        "item_allocations": [
            {"alloc_id": "AL-10", "item_id": "ITM-1", "warehouse": "WH-EAST"},
            {"alloc_id": "AL-11", "item_id": "ITM-1", "warehouse": "WH-WEST"},
            {"alloc_id": "AL-20", "item_id": "ITM-2", "warehouse": "WH-NORTH"},
        ],
    }


# ---------------------------------------------------------------------------
# 1. Snapshot Projection, Embedding, Multiplicity, and Type Conversions
# ---------------------------------------------------------------------------


def test_clean_snapshot_projection_and_verification_passes() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)

    expected_docs = verifier.projector.project_snapshot(tables)
    assert len(expected_docs) == 2

    res = verifier.verify_snapshot(tables, copy.deepcopy(expected_docs))
    assert res.badge == CareBadge.PASS
    assert res.is_clean()
    assert res.collection_count_matches is True
    assert res.expected_collection_count == 2
    assert res.actual_collection_count == 2
    assert len(res.defects) == 0

    report = res.format_unix_report()
    assert "DOCUMENT SNAPSHOT VERIFICATION: [PASS]" in report
    assert "Zero snapshot defects detected." in report


def test_type_conversion_mismatch_and_tolerated_coercion_drift() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)
    target_docs = verifier.projector.project_snapshot(tables)

    # 1. Within-tolerance float drift on discount_rate -> REVIEW (not silent PASS, not ACTION)
    target_docs[0]["billing"]["discount_rate"] = 0.05004
    res_review = verifier.verify_snapshot(tables, target_docs)
    assert res_review.badge == CareBadge.REVIEW
    assert any(d.category == DefectCategory.TOLERATED_COERCION_DRIFT for d in res_review.defects)

    # 2. Type mismatch (string instead of int on unit_cents, or explicit null when NULL_TO_ABSENT required)
    target_docs_bad = verifier.projector.project_snapshot(tables)
    target_docs_bad[0]["items"][0]["unit_cents"] = "5000"  # wrong type: str instead of int
    target_docs_bad[0]["metadata"]["note"] = None  # NULL_TO_ABSENT violated: explicit null present
    res_action = verifier.verify_snapshot(tables, target_docs_bad)
    assert res_action.badge == CareBadge.ACTION
    categories = {d.category for d in res_action.defects}
    assert DefectCategory.TYPE_CONVERSION_MISMATCH in categories or DefectCategory.VALUE_MISMATCH in categories


# ---------------------------------------------------------------------------
# 2. Equal Collection Counts with Wrong Path, Missing Child, or Reordered Array -> ACTION
# ---------------------------------------------------------------------------


def test_equal_collection_counts_with_wrong_nested_path_fails_with_action() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)
    target_docs = verifier.projector.project_snapshot(tables)

    # Misplace shipping.address.postal_code to shipping.postal_code while keeping 2 documents
    postal = target_docs[0]["shipping"]["address"].pop("postal_code")
    target_docs[0]["shipping"]["postal_code"] = postal

    res = verifier.verify_snapshot(tables, target_docs)
    assert res.collection_count_matches is True
    assert res.expected_collection_count == 2
    assert res.actual_collection_count == 2
    assert res.badge == CareBadge.ACTION
    path_defects = [d for d in res.defects if d.category == DefectCategory.PATH_IDENTITY_MISMATCH]
    assert len(path_defects) >= 1
    assert any("shipping.address.postal_code" in d.path for d in path_defects)
    assert any("shipping.postal_code" in d.reason for d in path_defects)


def test_equal_collection_counts_with_missing_embedded_child_fails_with_action() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)
    target_docs = verifier.projector.project_snapshot(tables)

    # Remove ITM-2 from ORD-100.items; collection still has 2 root documents
    target_docs[0]["items"] = [it for it in target_docs[0]["items"] if it["item_id"] != "ITM-2"]

    res = verifier.verify_snapshot(tables, target_docs)
    assert res.collection_count_matches is True
    assert res.badge == CareBadge.ACTION
    missing_defects = [d for d in res.defects if d.category == DefectCategory.MISSING_EMBEDDED_CHILD]
    mult_defects = [d for d in res.defects if d.category == DefectCategory.NESTED_MULTIPLICITY_MISMATCH]
    assert len(missing_defects) == 1
    assert "ITM-2" in missing_defects[0].path
    assert len(mult_defects) == 1


def test_equal_collection_counts_with_reordered_semantic_array_fails_with_action() -> None:
    spec_ordered = _build_order_mapping_spec(item_order_semantics=OrderSemantics.SEMANTIC_ORDERED)
    tables = _sample_normalized_tables()
    verifier_ordered = DocumentTransformVerifier(spec_ordered)
    target_docs = verifier_ordered.projector.project_snapshot(tables)

    # Reverse the order of items in ORD-100 ([ITM-2, ITM-1] instead of [ITM-1, ITM-2])
    # Both collection count (2) and array length (2) match, and every item's fields are identical!
    target_docs[0]["items"] = list(reversed(target_docs[0]["items"]))

    res = verifier_ordered.verify_snapshot(tables, target_docs)
    assert res.collection_count_matches is True
    assert len(target_docs[0]["items"]) == 2
    assert res.badge == CareBadge.ACTION
    order_defects = [d for d in res.defects if d.category == DefectCategory.SEMANTIC_ORDER_VIOLATION]
    assert len(order_defects) == 1
    assert order_defects[0].root_key == "ORD-100"
    assert order_defects[0].expected_value == ["ITM-1", "ITM-2"]
    assert order_defects[0].actual_value == ["ITM-2", "ITM-1"]

    # Contrast: when array is declared UNORDERED_KEYED, the same reordered array passes cleanly
    spec_unordered = _build_order_mapping_spec(item_order_semantics=OrderSemantics.UNORDERED_KEYED)
    verifier_unordered = DocumentTransformVerifier(spec_unordered)
    res_unordered = verifier_unordered.verify_snapshot(tables, target_docs)
    assert res_unordered.badge == CareBadge.PASS
    assert len(res_unordered.defects) == 0


# ---------------------------------------------------------------------------
# 3. Target Shape Measurement & Access Strategy Selection
# ---------------------------------------------------------------------------


def test_select_verification_strategy_from_measured_target_shape() -> None:
    # 1. Small number of changed root keys with index -> INDEXED_LOOKUP
    shape_point = MeasuredTargetShape(
        collection_name="orders_docs",
        total_documents=100_000,
        avg_document_bytes=2048,
        changed_root_keys_count=12,
        has_root_key_index=True,
    )
    plan_point = select_verification_strategy(shape_point)
    assert plan_point.strategy == VerificationFetchStrategy.INDEXED_LOOKUP
    assert plan_point.batch_size == 1
    assert plan_point.estimated_round_trips == 12
    assert plan_point.used_index == "_id"

    # 2. Moderate number of changed root keys (500 out of 100,000 = 0.5%) with index -> BATCHED_FETCH
    shape_batch = MeasuredTargetShape(
        collection_name="orders_docs",
        total_documents=100_000,
        avg_document_bytes=4096,
        changed_root_keys_count=500,
        has_root_key_index=True,
    )
    plan_batch = select_verification_strategy(shape_batch)
    assert plan_batch.strategy == VerificationFetchStrategy.BATCHED_FETCH
    assert plan_batch.batch_size > 1
    assert plan_batch.estimated_round_trips >= 1

    # 3. Large change ratio (45,000 out of 100,000 = 45%) -> STREAM_COMPARE
    shape_stream_high_ratio = MeasuredTargetShape(
        collection_name="orders_docs",
        total_documents=100_000,
        avg_document_bytes=2048,
        changed_root_keys_count=45_000,
        has_root_key_index=True,
    )
    plan_stream_ratio = select_verification_strategy(shape_stream_high_ratio)
    assert plan_stream_ratio.strategy == VerificationFetchStrategy.STREAM_COMPARE

    # 4. No index available even for 5 changed keys -> STREAM_COMPARE (avoids O(N*K) collection scans)
    shape_no_index = MeasuredTargetShape(
        collection_name="orders_docs",
        total_documents=50_000,
        avg_document_bytes=1024,
        changed_root_keys_count=5,
        has_root_key_index=False,
        has_embedded_path_index=False,
    )
    plan_no_index = select_verification_strategy(shape_no_index)
    assert plan_no_index.strategy == VerificationFetchStrategy.STREAM_COMPARE
    assert plan_no_index.used_index == "collection_scan_cursor"

    # 5. Embedded reference update with embedded path index
    shape_emb_idx = MeasuredTargetShape(
        collection_name="orders_docs",
        total_documents=80_000,
        avg_document_bytes=2048,
        changed_root_keys_count=0,
        affected_embedded_refs_count=8,
        has_root_key_index=False,
        has_embedded_path_index=True,
    )
    plan_emb = select_verification_strategy(shape_emb_idx)
    assert plan_emb.strategy == VerificationFetchStrategy.INDEXED_LOOKUP
    assert plan_emb.used_index == "embedded_path_idx"


# ---------------------------------------------------------------------------
# 4. Update & Delete Propagation for Embedded References vs Snapshot Equivalence
# ---------------------------------------------------------------------------


def test_embedded_reference_fanout_update_propagation_and_partial_fanout_detection() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)

    # CUST-1 is embedded in BOTH ORD-100 and ORD-200
    mutations = [
        RelationalMutationEvent(
            seq=1,
            entity="customers",
            op=ChangeOp.UPDATE,
            before={"customer_id": "CUST-1", "email": "alice@example.org", "tier": "GOLD"},
            after={"customer_id": "CUST-1", "email": "alice.new@example.org", "tier": "PLATINUM"},
            lsn=500,
        )
    ]

    _, expected_after_docs, _ = verifier.projector.apply_mutations(tables, mutations)

    # Case A: Both ORD-100 and ORD-200 properly propagated the CUST-1 reference update -> PASS
    prop_ok = verifier.verify_update_delete_propagation(tables, mutations, copy.deepcopy(expected_after_docs))
    assert prop_ok.badge == CareBadge.PASS
    assert prop_ok.total_mutations_evaluated == 1
    assert prop_ok.total_effects_expected == 1
    assert len(prop_ok.propagation_defects) == 0

    # Case B: Partial fan-out failure (ORD-100 updated, but ORD-200 still has stale CUST-1 email/tier)
    partial_target_docs = copy.deepcopy(expected_after_docs)
    partial_target_docs[1]["customer"] = {
        "id": "CUST-1",
        "contact": {"email": "alice@example.org"},
        "tier": "GOLD",
    }
    prop_fail = verifier.verify_update_delete_propagation(tables, mutations, partial_target_docs)
    assert prop_fail.badge == CareBadge.ACTION
    categories = {d.category for d in prop_fail.propagation_defects}
    assert DefectCategory.STALE_EMBEDDED_REFERENCE in categories
    assert DefectCategory.INCOMPLETE_REFERENCE_FANOUT in categories
    report = prop_fail.format_unix_report()
    assert "EMBEDDED UPDATE/DELETE PROPAGATION VERIFICATION: [ACTION]" in report
    assert "INCOMPLETE_REFERENCE_FANOUT" in report


def test_embedded_child_delete_and_reparent_propagation() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)

    # Mutation 1: Delete ITM-2 from ORD-100
    # Mutation 2: Reparent ITM-3 from ORD-200 to ORD-100 (with line_seq=3)
    mutations = [
        RelationalMutationEvent(
            seq=10,
            entity="order_items",
            op=ChangeOp.DELETE,
            before={
                "item_id": "ITM-2",
                "order_id": "ORD-100",
                "line_seq": 2,
                "sku": "SKU-B",
                "qty": 1,
                "unit_price": decimal.Decimal("49.95"),
            },
        ),
        RelationalMutationEvent(
            seq=11,
            entity="order_items",
            op=ChangeOp.UPDATE,
            before={
                "item_id": "ITM-3",
                "order_id": "ORD-200",
                "line_seq": 1,
                "sku": "SKU-C",
                "qty": 1,
                "unit_price": decimal.Decimal("49.50"),
            },
            after={
                "item_id": "ITM-3",
                "order_id": "ORD-100",
                "line_seq": 3,
                "sku": "SKU-C",
                "qty": 2,
                "unit_price": decimal.Decimal("49.50"),
            },
        ),
    ]

    _, expected_after, _ = verifier.projector.apply_mutations(tables, mutations)
    res_clean = verifier.verify_update_delete_propagation(tables, mutations, copy.deepcopy(expected_after))
    assert res_clean.badge == CareBadge.PASS

    # Now simulate broken CDC pipeline where:
    # - Deleted ITM-2 was not removed from ORD-100 (orphan child)
    # - Reparented ITM-3 was added to ORD-100 but NOT removed from old parent ORD-200
    broken_docs = copy.deepcopy(expected_after)
    broken_docs[0]["items"].append(
        {"item_id": "ITM-2", "seq": 2, "sku": "SKU-B", "qty": 1, "unit_cents": 4995, "allocations": []}
    )
    broken_docs[1]["items"].append(
        {"item_id": "ITM-3", "seq": 1, "sku": "SKU-C", "qty": 1, "unit_cents": 4950, "allocations": []}
    )

    res_broken = verifier.verify_update_delete_propagation(tables, mutations, broken_docs)
    assert res_broken.badge == CareBadge.ACTION
    cats = {d.category for d in res_broken.propagation_defects}
    assert DefectCategory.ORPHAN_EMBEDDED_CHILD in cats
    assert DefectCategory.REPARENT_PROPAGATION_FAILURE in cats


def test_reference_delete_policies_nullify_cascade_and_retain_historical() -> None:
    tables = _sample_normalized_tables()
    del_cust_mutation = [
        RelationalMutationEvent(
            seq=1,
            entity="customers",
            op=ChangeOp.DELETE,
            before={"customer_id": "CUST-1", "email": "alice@example.org", "tier": "GOLD"},
        )
    ]

    # 1. NULLIFY_REFERENCE policy: customer field must become None
    spec_nullify = _build_order_mapping_spec(customer_delete_policy=DeletePropagationPolicy.NULLIFY_REFERENCE)
    v_nullify = DocumentTransformVerifier(spec_nullify)
    _, exp_nullify, _ = v_nullify.projector.apply_mutations(tables, del_cust_mutation)
    assert exp_nullify[0]["customer"] is None
    assert v_nullify.verify_update_delete_propagation(tables, del_cust_mutation, exp_nullify).badge == CareBadge.PASS

    # If target failed to nullify deleted reference -> ACTION
    stale_docs = v_nullify.projector.project_snapshot(tables)
    res_stale = v_nullify.verify_update_delete_propagation(tables, del_cust_mutation, stale_docs)
    assert res_stale.badge == CareBadge.ACTION
    assert any(d.category == DefectCategory.DELETE_PROPAGATION_FAILURE for d in res_stale.propagation_defects)

    # 2. CASCADE_DELETE_DOCUMENT policy: orders referencing deleted CUST-1 must be deleted
    spec_cascade = _build_order_mapping_spec(customer_delete_policy=DeletePropagationPolicy.CASCADE_DELETE_DOCUMENT)
    v_cascade = DocumentTransformVerifier(spec_cascade)
    _, exp_cascade, _ = v_cascade.projector.apply_mutations(tables, del_cust_mutation)
    assert len(exp_cascade) == 0
    assert v_cascade.verify_update_delete_propagation(tables, del_cust_mutation, []).badge == CareBadge.PASS
    assert v_cascade.verify_update_delete_propagation(tables, del_cust_mutation, stale_docs).badge == CareBadge.ACTION

    # 3. RETAIN_HISTORICAL_SNAPSHOT policy: retaining embedded customer snapshot after delete is BENIGN
    spec_retain = _build_order_mapping_spec(
        customer_delete_policy=DeletePropagationPolicy.RETAIN_HISTORICAL_SNAPSHOT
    )
    v_retain = DocumentTransformVerifier(spec_retain)
    _, exp_retain, _ = v_retain.projector.apply_mutations(tables, del_cust_mutation)
    res_retain = v_retain.verify_update_delete_propagation(tables, del_cust_mutation, exp_retain)
    assert res_retain.badge == CareBadge.BENIGN
    assert res_retain.is_clean()
    assert any(d.category == DefectCategory.HISTORICAL_SNAPSHOT_RETAINED for d in res_retain.propagation_defects)


# ---------------------------------------------------------------------------
# 5. Explicit Concurrent-Source Assumptions
# ---------------------------------------------------------------------------


def test_explicit_concurrent_source_assumptions_enforcement() -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)
    docs = verifier.projector.project_snapshot(tables)

    # 1. QUIESCED_SOURCE violated by in-flight root keys -> ACTION
    conc_quiesced_violated = ConcurrentSourceAssumptions(
        mode=ConcurrencyMode.QUIESCED_SOURCE,
        in_flight_root_keys={"ORD-100"},
    )
    res1 = verifier.verify_snapshot(tables, docs, concurrency=conc_quiesced_violated)
    assert res1.badge == CareBadge.ACTION
    assert any(d.category == DefectCategory.CONCURRENT_ASSUMPTION_VIOLATION for d in res1.defects)

    # 2. WATERMARK_ALIGNED_CDC where target LSN lags behind source watermark -> ACTION
    conc_lagging_lsn = ConcurrentSourceAssumptions(
        mode=ConcurrencyMode.WATERMARK_ALIGNED_CDC,
        source_watermark_lsn=1050,
        target_applied_lsn=980,
        max_allowed_lag_ms=500.0,
        observed_lag_ms=120.0,
    )
    res2 = verifier.verify_snapshot(tables, docs, concurrency=conc_lagging_lsn)
    assert res2.badge == CareBadge.ACTION

    # 3. LIVE_CONCURRENT_WINDOW with in-flight key exclusion downgrades drift on ORD-100 to REVIEW
    drifted_docs = copy.deepcopy(docs)
    drifted_docs[0]["shipping"]["address"]["postal_code"] = "99999"
    conc_live_window = ConcurrentSourceAssumptions(
        mode=ConcurrencyMode.LIVE_CONCURRENT_WINDOW,
        in_flight_root_keys={"ORD-100"},
        max_allowed_lag_ms=1000.0,
        observed_lag_ms=250.0,
        allow_in_flight_window_exclusion=True,
        explicit_notes=["Order ORD-100 modified by active checkout session tx-991"],
    )
    res3 = verifier.verify_snapshot(tables, drifted_docs, concurrency=conc_live_window)
    assert res3.badge == CareBadge.REVIEW
    assert all(d.badge == CareBadge.REVIEW for d in res3.defects)
    assert any("ASSUMPTION: Order ORD-100" in f for f in res3.concurrency_findings)


# ---------------------------------------------------------------------------
# 6. O(1) Typed Durable Checkpoint Resume & Active Lease Lock (.run.lock)
# ---------------------------------------------------------------------------


def test_o1_typed_durable_checkpoint_and_active_lease_lock(tmp_path: Any) -> None:
    spec = _build_order_mapping_spec()
    tables = _sample_normalized_tables()
    verifier = DocumentTransformVerifier(spec)
    docs = verifier.projector.project_snapshot(tables)

    ck_file = str(tmp_path / "doc_verify_ck.json")
    composite_cursor = (
        decimal.Decimal("149.95"),
        datetime.datetime(2026, 5, 1, 12, 0, tzinfo=datetime.timezone.utc),
        uuid.UUID("11111111-2222-3333-4444-555555555555"),
    )
    ck = DocumentVerifyCheckpoint(mapping_checksum=spec.checksum())
    ck.mark_root_key_completed("ORD-100", cursor_key=composite_cursor)
    ck.save(ck_file)

    loaded_ck = DocumentVerifyCheckpoint.load(ck_file, expected_checksum=spec.checksum())
    assert loaded_ck.is_root_key_completed("ORD-100") is True
    assert loaded_ck.is_root_key_completed("ORD-200") is False
    assert loaded_ck.last_cursor_key == composite_cursor
    assert isinstance(loaded_ck.last_cursor_key[0], decimal.Decimal)
    assert isinstance(loaded_ck.last_cursor_key[1], datetime.datetime)
    assert isinstance(loaded_ck.last_cursor_key[2], uuid.UUID)

    # Verify with checkpoint: ORD-100 is skipped in O(1), only ORD-200 is verified
    res = verifier.verify_snapshot(tables, docs, checkpoint=loaded_ck)
    assert res.badge == CareBadge.PASS
    assert res.skipped_via_checkpoint_count == 1
    assert res.verified_documents_count == 1
    assert loaded_ck.is_root_key_completed("ORD-200") is True

    # Verify mutual exclusion lease lock (.run.lock)
    lock_dir = str(tmp_path)
    with VerificationLeaseLock(lock_dir, owner_id="worker-1", ttl_seconds=60.0) as lock1:
        lock1.heartbeat()
        lock2 = VerificationLeaseLock(lock_dir, owner_id="worker-2", ttl_seconds=60.0)
        with pytest.raises(ActiveVerificationLeaseError):
            lock2.acquire()

    # After context exit, worker-2 can acquire the lock cleanly
    with VerificationLeaseLock(lock_dir, owner_id="worker-2", ttl_seconds=60.0):
        assert os.path.exists(os.path.join(lock_dir, ".run.lock"))


def test_pure_ascii_and_zero_clutter_compliance() -> None:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    target_files = [
        os.path.join(repo_root, "migkit", "document_model.py"),
        os.path.abspath(__file__),
    ]
    for path in target_files:
        with open(path, "rb") as f:
            raw = f.read()
        non_ascii = [b for b in raw if b > 127]
        assert not non_ascii, f"Non-ASCII bytes found in {path}"


def test_document_verification_strategy_rungs_reachability():
    """Prove all branches of document_verification_strategy_rungs under watched()."""
    with watched():
        # 1. indexed_lookup holds
        c1 = decide_document_verification_strategy({
            "has_usable_index": True,
            "selective_keys": True,
            "small_document_footprint": True,
        })
        assert c1.rung.name == "indexed_lookup"

        # 2. indexed_lookup refuses, batched_fetch holds
        c2 = decide_document_verification_strategy({
            "has_usable_index": True,
            "selective_keys": False,
            "moderate_working_set": True,
        })
        assert c2.rung.name == "batched_fetch"

        # 3. batched_fetch refuses, stream_compare holds
        c3 = decide_document_verification_strategy({
            "has_usable_index": False,
        })
        assert c3.rung.name == "stream_compare"

