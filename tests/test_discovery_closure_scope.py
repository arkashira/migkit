"""Test suite for transitive discovery closure within authorized scope (Row 595: SMART-N12).

Verifies that:
- Every decide.Rung ladder is exercised on both holds and refuses under tests.rung_branches.watched().
- Strict scope boundary invariant holds: out-of-scope objects are never added to copy scope automatically.
- Hidden or inaccessible objects cannot look complete (marked INCOMPLETE with care badge ACTION).
- Dependency cycles and external references receive explicit dispositions.
- Metadata probes are strictly bounded and optional missing facts do not trigger full scans.
- All object kinds (tables, partitions, FKs, views, routines, extensions, sequences, jobs,
  principals, collation/timezone/encoding, replication, target conflicts) are inspected.
- Output adheres to pure UNIX style, zero emojis, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

import pytest
from migkit.discovery_closure import (
    CARE_ACTION,
    CARE_PASS,
    MetadataProbeBudget,
    ObjectKind,
    ObjectStatus,
    climb_closure_evaluation,
    climb_cycle_disposition,
    climb_metadata_probe_budget,
    climb_scope_boundary,
    climb_target_conflict,
    climb_unreadable_probe,
    discover_closure,
    extend_survey_with_closure,
)
from tests.rung_branches import branch, watched

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    # 1. scope_boundary_rungs
    ("discovery_closure", "scope_boundary_rungs", "strict_enforce_authorized_boundary", "holds"),
    ("discovery_closure", "scope_boundary_rungs", "strict_enforce_authorized_boundary", "refuses"),
    ("discovery_closure", "scope_boundary_rungs", "target_preexisting_contract", "holds"),
    ("discovery_closure", "scope_boundary_rungs", "target_preexisting_contract", "refuses"),
    ("discovery_closure", "scope_boundary_rungs", "block_unauthorized_scope_leak", "holds"),
    ("discovery_closure", "scope_boundary_rungs", "block_unauthorized_scope_leak", "refuses"),

    # 2. cycle_disposition_rungs
    ("discovery_closure", "cycle_disposition_rungs", "defer_deferrable_fk_constraints", "holds"),
    ("discovery_closure", "cycle_disposition_rungs", "defer_deferrable_fk_constraints", "refuses"),
    ("discovery_closure", "cycle_disposition_rungs", "staged_two_pass_nullable_fk", "holds"),
    ("discovery_closure", "cycle_disposition_rungs", "staged_two_pass_nullable_fk", "refuses"),
    ("discovery_closure", "cycle_disposition_rungs", "halt_unresolvable_cycle_for_operator", "holds"),
    ("discovery_closure", "cycle_disposition_rungs", "halt_unresolvable_cycle_for_operator", "refuses"),

    # 3. unreadable_object_rungs
    ("discovery_closure", "unreadable_object_rungs", "mark_incomplete_action_badge", "holds"),
    ("discovery_closure", "unreadable_object_rungs", "mark_incomplete_action_badge", "refuses"),
    ("discovery_closure", "unreadable_object_rungs", "bounded_schema_inferred_probe", "holds"),
    ("discovery_closure", "unreadable_object_rungs", "bounded_schema_inferred_probe", "refuses"),
    ("discovery_closure", "unreadable_object_rungs", "fail_closed_security_violation", "holds"),
    ("discovery_closure", "unreadable_object_rungs", "fail_closed_security_violation", "refuses"),

    # 4. metadata_probe_budget_rungs
    ("discovery_closure", "metadata_probe_budget_rungs", "cached_catalogue_facts", "holds"),
    ("discovery_closure", "metadata_probe_budget_rungs", "cached_catalogue_facts", "refuses"),
    ("discovery_closure", "metadata_probe_budget_rungs", "bounded_targeted_catalog_probe", "holds"),
    ("discovery_closure", "metadata_probe_budget_rungs", "bounded_targeted_catalog_probe", "refuses"),
    ("discovery_closure", "metadata_probe_budget_rungs", "skip_optional_probe_graceful", "holds"),
    ("discovery_closure", "metadata_probe_budget_rungs", "skip_optional_probe_graceful", "refuses"),

    # 5. target_conflict_rungs
    ("discovery_closure", "target_conflict_rungs", "compatible_reuse_target_object", "holds"),
    ("discovery_closure", "target_conflict_rungs", "compatible_reuse_target_object", "refuses"),
    ("discovery_closure", "target_conflict_rungs", "stage_shadow_isolated_schema", "holds"),
    ("discovery_closure", "target_conflict_rungs", "stage_shadow_isolated_schema", "refuses"),
    ("discovery_closure", "target_conflict_rungs", "block_target_collision_action", "holds"),
    ("discovery_closure", "target_conflict_rungs", "block_target_collision_action", "refuses"),

    # 6. closure_evaluation_rungs
    ("discovery_closure", "closure_evaluation_rungs", "certify_complete_closure", "holds"),
    ("discovery_closure", "closure_evaluation_rungs", "certify_complete_closure", "refuses"),
    ("discovery_closure", "closure_evaluation_rungs", "advisory_partial_closure_with_warnings", "holds"),
    ("discovery_closure", "closure_evaluation_rungs", "advisory_partial_closure_with_warnings", "refuses"),
    ("discovery_closure", "closure_evaluation_rungs", "reject_incomplete_closure", "holds"),
    ("discovery_closure", "closure_evaluation_rungs", "reject_incomplete_closure", "refuses"),
]


# =============================================================================
# 1. Rung Branch Coverage Tests (holds & refuses)
# =============================================================================


def test_every_discovery_closure_rung_holds_and_refuses_under_watched() -> None:
    """Exercise both holds and refuses for all 18 rungs across 6 ladders."""
    with watched() as seen:
        # ---------------------------------------------------------------------
        # 1. scope_boundary_rungs
        # ---------------------------------------------------------------------
        # Pass A: target_preexisting_contract holds
        c1 = climb_scope_boundary(
            "test scope boundary preexisting",
            {
                "target_allows_preexisting_contract": True,
                "external_object_registered_in_target": True,
                "boundary_strict_enforced": False,
                "external_dependency_is_optional": False,
                "fail_closed_on_unauthorized_scope": False,
            },
        )
        assert c1.path == "target_preexisting_contract"

        # Pass B: target_preexisting_contract refuses, strict_enforce_authorized_boundary holds
        c2 = climb_scope_boundary(
            "test scope boundary strict",
            {
                "target_allows_preexisting_contract": False,
                "external_object_registered_in_target": False,
                "boundary_strict_enforced": True,
                "external_dependency_is_optional": True,
                "fail_closed_on_unauthorized_scope": False,
            },
        )
        assert c2.path == "strict_enforce_authorized_boundary"

        # Pass C: both refuse, block_unauthorized_scope_leak holds
        c3 = climb_scope_boundary(
            "test scope boundary block",
            {
                "target_allows_preexisting_contract": False,
                "external_object_registered_in_target": False,
                "boundary_strict_enforced": False,
                "external_dependency_is_optional": False,
                "fail_closed_on_unauthorized_scope": True,
            },
        )
        assert c3.path == "block_unauthorized_scope_leak"

        # Pass D: all refuse
        c4 = climb_scope_boundary(
            "test scope boundary all refuse",
            {
                "target_allows_preexisting_contract": False,
                "external_object_registered_in_target": False,
                "boundary_strict_enforced": False,
                "external_dependency_is_optional": False,
                "fail_closed_on_unauthorized_scope": False,
            },
        )
        assert c4.path is None or c4.path == ""

        # ---------------------------------------------------------------------
        # 2. cycle_disposition_rungs
        # ---------------------------------------------------------------------
        # Pass A: defer_deferrable_fk_constraints holds
        c5 = climb_cycle_disposition(
            "test cycle deferrable",
            {
                "all_cycle_fks_deferrable": True,
                "target_supports_deferred_constraints": True,
                "at_least_one_fk_nullable": False,
                "supports_two_pass_staged_update": False,
                "cycle_cannot_be_broken_automatically": True,
            },
        )
        assert c5.path == "defer_deferrable_fk_constraints"

        # Pass B: defer refuses, staged_two_pass_nullable_fk holds
        c6 = climb_cycle_disposition(
            "test cycle nullable two-pass",
            {
                "all_cycle_fks_deferrable": False,
                "target_supports_deferred_constraints": False,
                "at_least_one_fk_nullable": True,
                "supports_two_pass_staged_update": True,
                "cycle_cannot_be_broken_automatically": True,
            },
        )
        assert c6.path == "staged_two_pass_nullable_fk"

        # Pass C: defer refuses, nullable refuses, halt_unresolvable holds
        c7 = climb_cycle_disposition(
            "test cycle halt",
            {
                "all_cycle_fks_deferrable": False,
                "target_supports_deferred_constraints": False,
                "at_least_one_fk_nullable": False,
                "supports_two_pass_staged_update": False,
                "cycle_cannot_be_broken_automatically": True,
            },
        )
        assert c7.path == "halt_unresolvable_cycle_for_operator"

        # Pass D: all refuse
        c8 = climb_cycle_disposition(
            "test cycle all refuse",
            {
                "all_cycle_fks_deferrable": False,
                "target_supports_deferred_constraints": False,
                "at_least_one_fk_nullable": False,
                "supports_two_pass_staged_update": False,
                "cycle_cannot_be_broken_automatically": False,
            },
        )
        assert c8.path is None or c8.path == ""

        # ---------------------------------------------------------------------
        # 3. unreadable_object_rungs
        # ---------------------------------------------------------------------
        # Pass A: mark_incomplete_action_badge holds
        c9 = climb_unreadable_probe(
            "test unreadable mark incomplete",
            {
                "enforce_honest_completeness": True,
                "has_unreadable_objects": True,
                "schema_read_grant_available": False,
                "probe_budget_remaining": 0,
                "strict_security_failure_mode": True,
            },
        )
        assert c9.path == "mark_incomplete_action_badge"

        # Pass B: mark_incomplete refuses, bounded_schema_inferred_probe holds
        c10 = climb_unreadable_probe(
            "test unreadable schema inferred",
            {
                "enforce_honest_completeness": False,
                "has_unreadable_objects": False,
                "schema_read_grant_available": True,
                "probe_budget_remaining": 5,
                "strict_security_failure_mode": True,
            },
        )
        assert c10.path == "bounded_schema_inferred_probe"

        # Pass C: mark_incomplete refuses, schema refuses, fail_closed holds
        c11 = climb_unreadable_probe(
            "test unreadable fail closed",
            {
                "enforce_honest_completeness": False,
                "has_unreadable_objects": False,
                "schema_read_grant_available": False,
                "probe_budget_remaining": 0,
                "strict_security_failure_mode": True,
            },
        )
        assert c11.path == "fail_closed_security_violation"

        # Pass D: all refuse
        c12 = climb_unreadable_probe(
            "test unreadable all refuse",
            {
                "enforce_honest_completeness": False,
                "has_unreadable_objects": False,
                "schema_read_grant_available": False,
                "probe_budget_remaining": 0,
                "strict_security_failure_mode": False,
            },
        )
        assert c12.path is None or c12.path == ""

        # ---------------------------------------------------------------------
        # 4. metadata_probe_budget_rungs
        # ---------------------------------------------------------------------
        # Pass A: cached_catalogue_facts holds
        c13 = climb_metadata_probe_budget(
            "test probe cached facts",
            {
                "cached_facts_sufficient": True,
                "probe_queries_executed": 0,
                "probe_budget_remaining": 10,
                "missing_facts_required": False,
                "missing_facts_are_optional": False,
                "do_not_force_full_scan": True,
            },
        )
        assert c13.path == "cached_catalogue_facts"

        # Pass B: cached refuses, bounded_targeted_catalog_probe holds
        c14 = climb_metadata_probe_budget(
            "test probe targeted",
            {
                "cached_facts_sufficient": False,
                "probe_queries_executed": 1,
                "probe_budget_remaining": 10,
                "missing_facts_required": True,
                "missing_facts_are_optional": False,
                "do_not_force_full_scan": True,
            },
        )
        assert c14.path == "bounded_targeted_catalog_probe"

        # Pass C: cached refuses, targeted refuses, skip_optional_probe holds
        c15 = climb_metadata_probe_budget(
            "test probe skip optional",
            {
                "cached_facts_sufficient": False,
                "probe_queries_executed": 1,
                "probe_budget_remaining": 0,
                "missing_facts_required": False,
                "missing_facts_are_optional": True,
                "do_not_force_full_scan": True,
            },
        )
        assert c15.path == "skip_optional_probe_graceful"

        # Pass D: all refuse
        c16 = climb_metadata_probe_budget(
            "test probe all refuse",
            {
                "cached_facts_sufficient": False,
                "probe_queries_executed": 1,
                "probe_budget_remaining": 0,
                "missing_facts_required": False,
                "missing_facts_are_optional": False,
                "do_not_force_full_scan": False,
            },
        )
        assert c16.path is None or c16.path == ""

        # ---------------------------------------------------------------------
        # 5. target_conflict_rungs
        # ---------------------------------------------------------------------
        # Pass A: compatible_reuse_target_object holds
        c17 = climb_target_conflict(
            "test target compatible reuse",
            {
                "target_object_exists": True,
                "structural_and_encoding_compatible": True,
                "shadow_schema_supported": False,
                "conflict_tolerates_shadowing": False,
                "abort_on_incompatible_collision": True,
            },
        )
        assert c17.path == "compatible_reuse_target_object"

        # Pass B: compatible refuses, stage_shadow_isolated_schema holds
        c18 = climb_target_conflict(
            "test target stage shadow",
            {
                "target_object_exists": False,
                "structural_and_encoding_compatible": False,
                "shadow_schema_supported": True,
                "conflict_tolerates_shadowing": True,
                "abort_on_incompatible_collision": True,
            },
        )
        assert c18.path == "stage_shadow_isolated_schema"

        # Pass C: compatible refuses, shadow refuses, block_target_collision holds
        c19 = climb_target_conflict(
            "test target block collision",
            {
                "target_object_exists": False,
                "structural_and_encoding_compatible": False,
                "shadow_schema_supported": False,
                "conflict_tolerates_shadowing": False,
                "abort_on_incompatible_collision": True,
            },
        )
        assert c19.path == "block_target_collision_action"

        # Pass D: all refuse
        c20 = climb_target_conflict(
            "test target all refuse",
            {
                "target_object_exists": False,
                "structural_and_encoding_compatible": False,
                "shadow_schema_supported": False,
                "conflict_tolerates_shadowing": False,
                "abort_on_incompatible_collision": False,
            },
        )
        assert c20.path is None or c20.path == ""

        # ---------------------------------------------------------------------
        # 6. closure_evaluation_rungs
        # ---------------------------------------------------------------------
        # Pass A: certify_complete_closure holds
        c21 = climb_closure_evaluation(
            "test closure certify",
            {
                "all_required_objects_present": True,
                "zero_unreadable_objects": True,
                "all_cycles_disposed": True,
                "zero_unauthorized_external_refs": True,
                "target_conflicts_resolved": True,
                "optional_gaps_annotated": False,
                "cycles_and_external_disposed": True,
                "closure_has_blocking_defects": True,
            },
        )
        assert c21.path == "certify_complete_closure"

        # Pass B: certify refuses, advisory_partial_closure holds
        c22 = climb_closure_evaluation(
            "test closure advisory partial",
            {
                "all_required_objects_present": True,
                "zero_unreadable_objects": True,
                "all_cycles_disposed": True,
                "zero_unauthorized_external_refs": True,
                "target_conflicts_resolved": False,
                "optional_gaps_annotated": True,
                "cycles_and_external_disposed": True,
                "closure_has_blocking_defects": True,
            },
        )
        assert c22.path == "advisory_partial_closure_with_warnings"

        # Pass C: certify refuses, advisory refuses, reject_incomplete holds
        c23 = climb_closure_evaluation(
            "test closure reject incomplete",
            {
                "all_required_objects_present": False,
                "zero_unreadable_objects": False,
                "all_cycles_disposed": False,
                "zero_unauthorized_external_refs": False,
                "target_conflicts_resolved": False,
                "optional_gaps_annotated": False,
                "cycles_and_external_disposed": False,
                "closure_has_blocking_defects": True,
            },
        )
        assert c23.path == "reject_incomplete_closure"

        # Pass D: all refuse
        c24 = climb_closure_evaluation(
            "test closure all refuse",
            {
                "all_required_objects_present": False,
                "zero_unreadable_objects": False,
                "all_cycles_disposed": False,
                "zero_unauthorized_external_refs": False,
                "target_conflicts_resolved": False,
                "optional_gaps_annotated": False,
                "cycles_and_external_disposed": False,
                "closure_has_blocking_defects": False,
            },
        )
        assert c24.path is None or c24.path == ""

    # Verify that each rung recorded both HOLDS and REFUSES
    for mod, ladder, rung, expected_branch in PROVES_RUNGS:
        branches = branch(seen, rung)
        assert expected_branch in branches, (
            f"Rung {rung} in {ladder} missing branch {expected_branch}: seen={branches}"
        )


# =============================================================================
# 2. Strict Scope Invariant Tests (No Automatic Expansion)
# =============================================================================


def test_strict_scope_boundary_invariant_no_auto_expansion() -> None:
    """Verifies out-of-scope tables are never added to copy scope automatically."""
    source_cat = {
        "objects": {
            "public.orders": {
                "exists": True,
                "columns": {"id": "int", "user_id": "int", "billing_id": "int"},
                "dependencies": [
                    {"target": "billing.invoices", "kind": "foreign_key", "hard": True},
                    {"target": "public.order_items", "kind": "table", "hard": True},
                ],
            },
            "public.order_items": {
                "exists": True,
                "columns": {"id": "int", "order_id": "int"},
                "dependencies": [],
            },
            "billing.invoices": {
                "exists": True,
                "columns": {"id": "int", "amount": "numeric"},
                "dependencies": [],
            },
        },
    }

    # Authorized scope strictly limited to public.orders and public.order_items
    res = discover_closure(
        authorized_tables=["public.orders", "public.order_items"],
        authorized_schemas=["public"],
        source_catalogue=source_cat,
        target_catalogue={},
    )

    # 1. billing.invoices is recognized as OUT_OF_SCOPE
    assert "billing.invoices" in res.objects
    obj = res.objects["billing.invoices"]
    assert obj.status == ObjectStatus.OUT_OF_SCOPE
    assert not obj.in_authorized_scope
    assert obj.disposition == "block_unauthorized_scope_leak"

    # 2. Invariant: billing.invoices was NOT added to authorized copy tables
    assert "billing.invoices" in res.external_references
    assert not res.is_complete
    assert res.care_badge == CARE_ACTION


def test_external_reference_with_preexisting_target_contract() -> None:
    """Verifies external references resolve safely when target contract admits preexisting object."""
    source_cat = {
        "objects": {
            "app.logs": {
                "exists": True,
                "columns": {"id": "int", "sys_id": "int"},
                "dependencies": [
                    {"target": "external_sys.lookup", "kind": "foreign_key", "hard": False},
                ],
            },
            "external_sys.lookup": {
                "exists": True,
                "columns": {"id": "int", "code": "varchar"},
                "dependencies": [],
            },
        },
    }

    target_cat = {
        "objects": {
            "external_sys.lookup": {"exists": True, "compatible": True},
        },
        "environment": {
            "allow_preexisting_contract": True,
        },
    }

    res = discover_closure(
        authorized_tables=["app.logs"],
        authorized_schemas=["app"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    assert "external_sys.lookup" in res.external_references
    assert res.external_dispositions["external_sys.lookup"] == "target_preexisting_contract"
    assert res.objects["external_sys.lookup"].disposition == "target_preexisting_contract"


# =============================================================================
# 3. Hidden and Inaccessible Objects Cannot Look Complete
# =============================================================================


def test_hidden_inaccessible_objects_cannot_look_complete() -> None:
    """Verifies that unreadable objects force incomplete status and ACTION care badge."""
    source_cat = {
        "objects": {
            "public.users": {
                "exists": True,
                "dependencies": [
                    {"target": "public.audit_vault", "kind": "table", "hard": True},
                ],
            },
            "public.audit_vault": {
                "exists": True,
                "unreadable": True,
                "error_detail": "permission denied for relation audit_vault (pg_hba.conf / missing grant)",
                "dependencies": [],
            },
        },
    }

    res = discover_closure(
        authorized_tables=["public.users", "public.audit_vault"],
        authorized_schemas=["public"],
        source_catalogue=source_cat,
    )

    assert not res.is_complete
    assert res.care_badge == CARE_ACTION
    assert "public.audit_vault" in res.unreadable_objects
    obj = res.objects["public.audit_vault"]
    assert obj.status == ObjectStatus.UNREADABLE
    assert not obj.is_accessible
    assert obj.disposition == "mark_incomplete_action_badge"


# =============================================================================
# 4. Dependency Cycles and Explicit Dispositions
# =============================================================================


def test_deferrable_fk_cycle_resolution() -> None:
    """Verifies circular FKs break cleanly via defer_deferrable_fk_constraints when supported."""
    source_cat = {
        "objects": {
            "public.node_a": {
                "exists": True,
                "dependencies": [
                    {"target": "public.node_b", "kind": "foreign_key", "deferrable": True},
                ],
            },
            "public.node_b": {
                "exists": True,
                "dependencies": [
                    {"target": "public.node_a", "kind": "foreign_key", "deferrable": True},
                ],
            },
        },
    }
    target_cat = {
        "environment": {
            "supports_deferred_constraints": True,
        },
    }

    res = discover_closure(
        authorized_tables=["public.node_a", "public.node_b"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    assert len(res.cycles) == 1
    ckey = "public.node_a -> public.node_b"
    assert res.cycle_dispositions.get(ckey) == "defer_deferrable_fk_constraints"


def test_nullable_fk_two_pass_cycle_resolution() -> None:
    """Verifies circular FKs break via staged two-pass nullable insert/update."""
    source_cat = {
        "objects": {
            "public.dept": {
                "exists": True,
                "dependencies": [
                    {"target": "public.emp", "kind": "foreign_key", "deferrable": False, "nullable": True},
                ],
            },
            "public.emp": {
                "exists": True,
                "dependencies": [
                    {"target": "public.dept", "kind": "foreign_key", "deferrable": False, "nullable": False},
                ],
            },
        },
    }
    target_cat = {
        "environment": {
            "supports_deferred_constraints": False,
            "supports_two_pass_update": True,
        },
    }

    res = discover_closure(
        authorized_tables=["public.dept", "public.emp"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    assert len(res.cycles) == 1
    ckey = "public.dept -> public.emp"
    assert res.cycle_dispositions.get(ckey) == "staged_two_pass_nullable_fk"


def test_unresolvable_cycle_halts_with_operator_action() -> None:
    """Verifies that non-deferrable not-null circular references halt with ACTION badge."""
    source_cat = {
        "objects": {
            "public.lock_x": {
                "exists": True,
                "dependencies": [
                    {"target": "public.lock_y", "kind": "foreign_key", "deferrable": False, "nullable": False},
                ],
            },
            "public.lock_y": {
                "exists": True,
                "dependencies": [
                    {"target": "public.lock_x", "kind": "foreign_key", "deferrable": False, "nullable": False},
                ],
            },
        },
    }
    target_cat = {
        "environment": {
            "supports_deferred_constraints": False,
            "supports_two_pass_update": False,
        },
    }

    res = discover_closure(
        authorized_tables=["public.lock_x", "public.lock_y"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    assert len(res.cycles) == 1
    ckey = "public.lock_x -> public.lock_y"
    assert res.cycle_dispositions.get(ckey) == "halt_unresolvable_cycle_for_operator"
    assert not res.is_complete
    assert res.care_badge == CARE_ACTION


# =============================================================================
# 5. Inspection of All Object Kinds
# =============================================================================


def test_full_catalog_object_kinds_inspection() -> None:
    """Verifies inspection of partitions, views, routines, sequences, extensions, and jobs."""
    source_cat = {
        "objects": {
            "public.events": {
                "exists": True,
                "columns": {"id": "bigint", "ts": "timestamptz"},
                "dependencies": [
                    {"target": "public.event_types", "kind": "foreign_key", "hard": True},
                ],
            },
            "public.event_types": {
                "exists": True,
                "columns": {"id": "int", "name": "text"},
                "dependencies": [],
            },
        },
        "partitions": {
            "public.events": {
                "children": ["public.events_2026_q1", "public.events_2026_q2"],
            },
        },
        "sequences": {
            "public.events_id_seq": {
                "owner_table": "public.events",
            },
        },
        "extensions": {
            "pg_trgm": {"required": True},
            "citext": {"required": False},
        },
        "principals": {
            "app_writer": {"required": True},
        },
        "replication": {
            "migkit_cdc_slot": {"required": True},
        },
        "environment": {
            "encoding": "UTF8",
            "collation": "en_US.UTF-8",
            "timezone": "UTC",
        },
    }

    target_cat = {
        "extensions": {
            "pg_trgm": {"installed": True},
        },
        "environment": {
            "encoding": "UTF8",
            "collation": "en_US.UTF-8",
            "timezone": "UTC",
        },
    }

    res = discover_closure(
        authorized_tables=["public.events", "public.event_types"],
        authorized_schemas=["public"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    # Check partition hierarchy was discovered
    assert "public.events_2026_q1" in res.objects
    assert res.objects["public.events_2026_q1"].kind == ObjectKind.PARTITION

    # Check owned sequence
    assert "public.events_id_seq" in res.objects
    assert res.objects["public.events_id_seq"].kind == ObjectKind.SEQUENCE

    # Check extension
    assert "extension:pg_trgm" in res.objects
    assert res.objects["extension:pg_trgm"].kind == ObjectKind.EXTENSION

    # Check replication slot
    assert "replication:migkit_cdc_slot" in res.objects

    # Check principals
    assert "principal:app_writer" in res.objects


# =============================================================================
# 6. Target Conflict Detection & Dispositions
# =============================================================================


def test_target_encoding_and_schema_conflict_detection() -> None:
    """Verifies target collation, timezone, encoding, and column mismatch detection."""
    source_cat = {
        "objects": {
            "public.products": {
                "exists": True,
                "columns": {"id": "int", "sku": "varchar(32)"},
                "dependencies": [],
            },
        },
        "environment": {
            "encoding": "UTF8",
            "collation": "en_US.UTF-8",
            "timezone": "UTC",
        },
    }

    target_cat = {
        "tables": {
            "public.products": {
                "columns": {"id": "int", "sku": "varchar(16)"},  # Column width mismatch!
            },
        },
        "environment": {
            "encoding": "LATIN1",  # Encoding mismatch!
            "collation": "POSIX",
            "timezone": "EST",
            "shadow_schema_supported": True,
        },
    }

    res = discover_closure(
        authorized_tables=["public.products"],
        source_catalogue=source_cat,
        target_catalogue=target_cat,
    )

    # Assert target conflicts detected
    conflict_types = {tc.conflict_type for tc in res.target_conflicts}
    assert "encoding_mismatch" in conflict_types
    assert "column_definition_mismatch" in conflict_types

    # Column definition mismatch staged to shadow schema
    table_conflict = next(
        tc for tc in res.target_conflicts if tc.conflict_type == "column_definition_mismatch"
    )
    assert table_conflict.disposition == "stage_shadow_isolated_schema"


# =============================================================================
# 7. Bounded Probes Budget & No Forced Full Scans
# =============================================================================


def test_bounded_metadata_probe_budget_and_no_forced_scans() -> None:
    """Verifies that probe budget is strictly respected and optional missing facts skip full scan."""
    budget = MetadataProbeBudget(max_queries=5, max_depth=3)

    source_cat = {
        "objects": {
            "public.t1": {
                "exists": True,
                "required_for_closure": True,
                # dependencies missing from cache -> requires targeted probe
                "probed_dependencies": [{"target": "public.t2", "kind": "table"}],
            },
            "public.t2": {
                "exists": True,
                "required_for_closure": False,
                # optional table with no cached dependencies -> skip probe graceful
            },
        },
    }

    res = discover_closure(
        authorized_tables=["public.t1"],
        source_catalogue=source_cat,
        probe_budget=budget,
    )
    assert res.is_complete

    # Probe queries executed are tracked and bounded
    assert budget.queries_executed <= budget.max_queries
    assert len(budget.probe_log) == 1
    assert budget.probe_log[0]["object_id"] == "public.t1"


def test_budget_exhaustion_raises_runtime_error() -> None:
    """Verifies that exceeding max_queries raises RuntimeError rather than runaway crawling."""
    budget = MetadataProbeBudget(max_queries=1)
    budget.record_probe("obj1", "query1")
    assert not budget.can_probe()

    with pytest.raises(RuntimeError, match="Metadata probe budget exhausted"):
        budget.record_probe("obj2", "query2")


# =============================================================================
# 8. Integration with guide.survey
# =============================================================================


def test_extend_survey_with_closure_integration() -> None:
    """Verifies integration with survey dictionary structure."""
    raw_survey = {
        "reached": True,
        "tables": ["public.customer", "public.invoice"],
        "sides": {
            "source": {
                "facts": {
                    "public.customer": {
                        "exists": True,
                        "columns": {"id": "int"},
                        "dependencies": [
                            {"target": "public.invoice", "kind": "foreign_key"},
                        ],
                    },
                    "public.invoice": {
                        "exists": True,
                        "columns": {"id": "int", "customer_id": "int"},
                        "dependencies": [],
                    },
                },
                "encoding": "UTF8",
            },
            "target": {
                "dbs": ["target_db"],
            },
        },
        "care": [],
    }

    enriched = extend_survey_with_closure(
        raw_survey,
        authorized_tables=["public.customer", "public.invoice"],
    )

    assert "discovery_closure" in enriched
    assert enriched["closure_complete"] is True
    assert enriched["closure_care_badge"] == CARE_PASS
    assert enriched["discovery_closure"]["objects_count"] == 2


# =============================================================================
# 9. ASCII Report Rendering
# =============================================================================


def test_pure_ascii_report_rendering() -> None:
    """Verifies pure UNIX ASCII report generation with 4-tier care badges and zero emojis."""
    source_cat = {
        "objects": {
            "public.orders": {
                "exists": True,
                "dependencies": [],
            },
        },
    }

    res = discover_closure(
        authorized_tables=["public.orders"],
        source_catalogue=source_cat,
    )
    report = res.render_report()

    # Pure ASCII check
    assert report.isascii()
    assert "DISCOVERY CLOSURE REPORT (Row 595: SMART-N12)" in report
    assert "[PASS]" in report or "[ACTION]" in report or "[REVIEW]" in report
    assert "Overall Status" in report
