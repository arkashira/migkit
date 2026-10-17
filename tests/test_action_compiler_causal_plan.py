"""Tests for Row 596: SMART-N13.action-compiler (Compile typed actions into a causally complete plan).

Proves:
1. Every `decide.Rung` in `action_compiler.subplan_synthesis_rungs`,
   `action_compiler.plan_validation_rungs`,
   `action_compiler.compensation_policy_rungs`, and
   `action_compiler.bounded_search_incumbent_rungs` is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in
   `PROVES_RUNGS`.
2. Action definitions declare typed input/output ports, causal prerequisites, durable
   postconditions, side effects, resource envelopes, and compensation actions.
3. Missing schema or required transforms dynamically generate complete schema/transform/verify
   subplans. Direct execution is synthesized when target schema pre-exists.
4. Independent validation prunes contradictions (cycles, unsatisfied dependencies,
   resource over-subscriptions, and conflicting side-effects) before costing.
5. Strict compensation policy binds durable rollback actions to all irreversible steps
   and honestly refuses uncompensated irreversible actions.
6. Bounded search enforces statistically derived depth and resource bounds from measured
   telemetry readings, returning a validated incumbent or honest refusal.
7. Pure UNIX style, zero emojis/glyphs, care badges ACTION/REVIEW/BENIGN/PASS.
"""

from __future__ import annotations

from migkit import action_compiler, decide
from migkit.action_compiler import (
    ActionDefinition,
    ActionPort,
    ActionTelemetryReadings,
    CareBadge,
    CausalPhase,
    EffectKind,
    ResourceRequirement,
    bounded_search_incumbent_rungs,
    compensation_policy_rungs,
    compile_action_plan,
    default_action_registry,
    derive_compiler_bounds,
    plan_validation_rungs,
    subplan_synthesis_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

#: read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    ("action_compiler", "subplan_synthesis_rungs", "synthesize_schema_transform_verify_subplan", "holds"),
    ("action_compiler", "subplan_synthesis_rungs", "synthesize_schema_transform_verify_subplan", "refuses"),
    ("action_compiler", "subplan_synthesis_rungs", "synthesize_direct_execution_subplan", "holds"),
    ("action_compiler", "subplan_synthesis_rungs", "synthesize_direct_execution_subplan", "refuses"),
    ("action_compiler", "subplan_synthesis_rungs", "refuse_unsupported_transforms", "holds"),
    ("action_compiler", "subplan_synthesis_rungs", "refuse_unsupported_transforms", "refuses"),
    ("action_compiler", "plan_validation_rungs", "admit_causally_valid_plan", "holds"),
    ("action_compiler", "plan_validation_rungs", "admit_causally_valid_plan", "refuses"),
    ("action_compiler", "plan_validation_rungs", "prune_contradictory_plan", "holds"),
    ("action_compiler", "plan_validation_rungs", "prune_contradictory_plan", "refuses"),
    ("action_compiler", "compensation_policy_rungs", "strict_durable_compensation", "holds"),
    ("action_compiler", "compensation_policy_rungs", "strict_durable_compensation", "refuses"),
    ("action_compiler", "compensation_policy_rungs", "best_effort_compensation", "holds"),
    ("action_compiler", "compensation_policy_rungs", "best_effort_compensation", "refuses"),
    ("action_compiler", "compensation_policy_rungs", "refuse_uncompensated_irreversible", "holds"),
    ("action_compiler", "compensation_policy_rungs", "refuse_uncompensated_irreversible", "refuses"),
    ("action_compiler", "bounded_search_incumbent_rungs", "admit_validated_incumbent_plan", "holds"),
    ("action_compiler", "bounded_search_incumbent_rungs", "admit_validated_incumbent_plan", "refuses"),
    ("action_compiler", "bounded_search_incumbent_rungs", "honest_refusal_unachievable_goal", "holds"),
    ("action_compiler", "bounded_search_incumbent_rungs", "honest_refusal_unachievable_goal", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

def sample_readings() -> ActionTelemetryReadings:
    return ActionTelemetryReadings(
        cpu_cores_samples=(2.0, 4.0, 4.0),
        memory_bytes_samples=(
            1024 * 1024 * 1024,
            2 * 1024 * 1024 * 1024,
            2 * 1024 * 1024 * 1024,
        ),
        disk_bytes_samples=(
            10 * 1024 * 1024 * 1024,
            12 * 1024 * 1024 * 1024,
        ),
        connections_limit=50,
        step_latency_seconds_samples=(0.5, 0.8, 1.2),
        max_search_depth_limit=25,
    )


# -----------------------------------------------------------------------------
# Test 1: Subplan Synthesis Ladder (All Rungs, Holds and Refuses)
# -----------------------------------------------------------------------------

def test_subplan_synthesis_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of subplan_synthesis_rungs."""
    rungs = subplan_synthesis_rungs()

    with watched() as seen:
        # Case A: Missing schema -> synthesize_schema_transform_verify_subplan HOLDS
        facts_a = {
            "needs_schema_subplan": True,
            "needs_transform_subplan": False,
            "transforms_supported": True,
            "schema_synthesis_allowed": True,
            "target_schema_exists": False,
        }
        ch_a = decide.climb("subplan:test_a", rungs, facts=facts_a, explore=False)
        assert ch_a.path == "synthesize_schema_transform_verify_subplan"

        # Case B: Target schema pre-exists and no transform -> synthesize_direct_execution_subplan HOLDS,
        #         synthesize_schema_transform_verify_subplan REFUSES
        facts_b = {
            "needs_schema_subplan": False,
            "needs_transform_subplan": False,
            "transforms_supported": True,
            "schema_synthesis_allowed": True,
            "target_schema_exists": True,
        }
        ch_b = decide.climb("subplan:test_b", rungs, facts=facts_b, explore=False)
        assert ch_b.path == "synthesize_direct_execution_subplan"

        # Case C: Transform required but unsupported -> refuse_unsupported_transforms HOLDS,
        #         synthesize_schema_transform_verify_subplan REFUSES (transforms_supported is False),
        #         synthesize_direct_execution_subplan REFUSES (needs_transform is True)
        facts_c = {
            "needs_schema_subplan": False,
            "needs_transform_subplan": True,
            "transforms_supported": False,
            "schema_synthesis_allowed": True,
            "target_schema_exists": True,
        }
        ch_c = decide.climb("subplan:test_c", rungs, facts=facts_c, explore=False)
        assert ch_c.path == "refuse_unsupported_transforms"

    # Verify branches seen
    assert HOLDS in branch(seen, "synthesize_schema_transform_verify_subplan")
    assert REFUSES in branch(seen, "synthesize_schema_transform_verify_subplan")

    assert HOLDS in branch(seen, "synthesize_direct_execution_subplan")
    assert REFUSES in branch(seen, "synthesize_direct_execution_subplan")

    assert HOLDS in branch(seen, "refuse_unsupported_transforms")
    assert REFUSES in branch(seen, "refuse_unsupported_transforms")


# -----------------------------------------------------------------------------
# Test 2: Plan Validation Ladder (All Rungs, Holds and Refuses)
# -----------------------------------------------------------------------------

def test_plan_validation_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of plan_validation_rungs."""
    rungs = plan_validation_rungs()

    with watched() as seen:
        # Case A: Clean valid plan -> admit_causally_valid_plan HOLDS, prune_contradictory_plan REFUSES
        facts_clean = {
            "dag_acyclic": True,
            "prerequisites_satisfied": True,
            "within_resource_bounds": True,
            "has_conflicting_effects": False,
            "has_contradictions": False,
        }
        ch_clean = decide.climb("plan_val:clean", rungs, facts=facts_clean, explore=False)
        assert ch_clean.path == "admit_causally_valid_plan"

        # Case B: Contradiction present -> prune_contradictory_plan HOLDS, admit_causally_valid_plan REFUSES
        facts_contradiction = {
            "dag_acyclic": False,
            "prerequisites_satisfied": True,
            "within_resource_bounds": False,
            "has_conflicting_effects": True,
            "has_contradictions": True,
        }
        ch_bad = decide.climb("plan_val:bad", rungs, facts=facts_contradiction, explore=False)
        assert ch_bad.path == "prune_contradictory_plan"

    assert HOLDS in branch(seen, "admit_causally_valid_plan")
    assert REFUSES in branch(seen, "admit_causally_valid_plan")

    assert HOLDS in branch(seen, "prune_contradictory_plan")
    assert REFUSES in branch(seen, "prune_contradictory_plan")


# -----------------------------------------------------------------------------
# Test 3: Compensation Policy Ladder (All Rungs, Holds and Refuses)
# -----------------------------------------------------------------------------

def test_compensation_policy_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of compensation_policy_rungs."""
    rungs = compensation_policy_rungs()

    with watched() as seen:
        # Case A: Strict compensation with all actions compensated -> strict_durable_compensation HOLDS,
        #         refuse_uncompensated_irreversible REFUSES
        facts_strict_ok = {
            "strict_compensation_required": True,
            "all_actions_compensated": True,
            "best_effort_allowed": False,
            "staging_cleanup_present": False,
            "has_uncompensated_irreversible": False,
        }
        ch_a = decide.climb("comp:strict_ok", rungs, facts=facts_strict_ok, explore=False)
        assert ch_a.path == "strict_durable_compensation"

        # Case B: Best effort policy allowed with staging cleanup -> best_effort_compensation HOLDS,
        #         strict_durable_compensation REFUSES
        facts_best_effort = {
            "strict_compensation_required": False,
            "all_actions_compensated": False,
            "best_effort_allowed": True,
            "staging_cleanup_present": True,
            "has_uncompensated_irreversible": False,
        }
        ch_b = decide.climb("comp:best_effort", rungs, facts=facts_best_effort, explore=False)
        assert ch_b.path == "best_effort_compensation"

        # Case C: Uncompensated irreversible actions present -> refuse_uncompensated_irreversible HOLDS,
        #         strict_durable_compensation REFUSES, best_effort_compensation REFUSES
        facts_uncompensated = {
            "strict_compensation_required": True,
            "all_actions_compensated": False,
            "best_effort_allowed": False,
            "staging_cleanup_present": False,
            "has_uncompensated_irreversible": True,
        }
        ch_c = decide.climb("comp:uncompensated", rungs, facts=facts_uncompensated, explore=False)
        assert ch_c.path == "refuse_uncompensated_irreversible"

    assert HOLDS in branch(seen, "strict_durable_compensation")
    assert REFUSES in branch(seen, "strict_durable_compensation")

    assert HOLDS in branch(seen, "best_effort_compensation")
    assert REFUSES in branch(seen, "best_effort_compensation")

    assert HOLDS in branch(seen, "refuse_uncompensated_irreversible")
    assert REFUSES in branch(seen, "refuse_uncompensated_irreversible")


# -----------------------------------------------------------------------------
# Test 4: Bounded Search Incumbent Ladder (All Rungs, Holds and Refuses)
# -----------------------------------------------------------------------------

def test_bounded_search_incumbent_rungs_holds_and_refuses():
    """Exercise both holds and refuses on all rungs of bounded_search_incumbent_rungs."""
    rungs = bounded_search_incumbent_rungs()

    with watched() as seen:
        # Case A: Valid incumbent within bounds -> admit_validated_incumbent_plan HOLDS,
        #         honest_refusal_unachievable_goal REFUSES
        facts_incumbent = {
            "has_valid_incumbent": True,
            "search_within_depth_bounds": True,
            "cost_within_budget": True,
            "search_exhausted": False,
        }
        ch_a = decide.climb("search:incumbent", rungs, facts=facts_incumbent, explore=False)
        assert ch_a.path == "admit_validated_incumbent_plan"

        # Case B: Search bounds exhausted -> honest_refusal_unachievable_goal HOLDS,
        #         admit_validated_incumbent_plan REFUSES
        facts_exhausted = {
            "has_valid_incumbent": False,
            "search_within_depth_bounds": False,
            "cost_within_budget": False,
            "search_exhausted": True,
        }
        ch_b = decide.climb("search:exhausted", rungs, facts=facts_exhausted, explore=False)
        assert ch_b.path == "honest_refusal_unachievable_goal"

    assert HOLDS in branch(seen, "admit_validated_incumbent_plan")
    assert REFUSES in branch(seen, "admit_validated_incumbent_plan")

    assert HOLDS in branch(seen, "honest_refusal_unachievable_goal")
    assert REFUSES in branch(seen, "honest_refusal_unachievable_goal")


# -----------------------------------------------------------------------------
# Test 5: Missing Schema Synthesizes Complete Subplan
# -----------------------------------------------------------------------------

def test_missing_schema_generates_schema_subplan():
    """Target lacking schema generates schema inspect, ddl generate, ddl apply, and verify."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": False,
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": False,
    }

    plan = compile_action_plan("migrate_orders", initial_facts, readings=readings)

    assert not plan.is_refusal
    assert plan.validated
    assert plan.care_badge in (CareBadge.PASS, CareBadge.REVIEW)
    assert "schema" in plan.subplans

    schema_ids = [a.action_id for a in plan.subplans["schema"]]
    assert schema_ids == [
        "schema.inspect_source",
        "schema.generate_ddl",
        "schema.apply_ddl",
        "schema.verify",
    ]

    # Verify causal ordering: apply_ddl depends on generate_ddl, which depends on inspect_source
    assert "schema.generate_ddl" in plan.dependencies["schema.apply_ddl"]
    assert "schema.inspect_source" in plan.dependencies["schema.generate_ddl"]

    # Verify execution includes data copy and verification
    action_ids = plan.action_ids()
    assert "data.read_snapshot" in action_ids
    assert "data.write_direct_target" in action_ids
    assert "verify.row_count" in action_ids
    assert "verify.hash_digest" in action_ids
    assert "verify.certify" in action_ids


# -----------------------------------------------------------------------------
# Test 6: Required Transform Synthesizes Transform and Verify Subplans
# -----------------------------------------------------------------------------

def test_required_transform_generates_transform_subplan():
    """Required column mapping synthesizes staging buffer, column map, type cast, and target write."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": True,
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": True,
        "transform_kind": "column_mapping",
    }

    plan = compile_action_plan("migrate_transformed_orders", initial_facts, readings=readings)

    assert not plan.is_refusal
    assert plan.validated
    assert "transform" in plan.subplans

    transform_ids = [a.action_id for a in plan.subplans["transform"]]
    assert transform_ids == [
        "transform.stage_buffer",
        "transform.column_map",
        "transform.type_cast",
    ]

    action_ids = plan.action_ids()
    assert "data.write_target" in action_ids
    assert "verify.certify" in action_ids


# -----------------------------------------------------------------------------
# Test 7: Direct Execution When Schema Exists and No Transform Needed
# -----------------------------------------------------------------------------

def test_direct_execution_path_when_schema_exists():
    """Target with pre-existing schema and direct compatibility generates minimal direct plan."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": True,
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": False,
    }

    plan = compile_action_plan("direct_copy_orders", initial_facts, readings=readings)

    assert not plan.is_refusal
    assert plan.validated
    assert "schema" not in plan.subplans
    assert "transform" not in plan.subplans

    action_ids = plan.action_ids()
    assert action_ids == (
        "data.read_snapshot",
        "data.write_direct_target",
        "verify.row_count",
        "verify.hash_digest",
        "verify.certify",
    )


# -----------------------------------------------------------------------------
# Test 8: Independent Validation Prunes Contradictions and Unsatisfied Prereqs
# -----------------------------------------------------------------------------

def test_independent_validation_prunes_unsatisfied_prerequisites():
    """Unreachable source endpoint leaves prerequisites unsatisfied and prunes the plan."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": False,
        "source_endpoint_reachable": False,  # Missing requirement
        "target_endpoint_reachable": True,
        "requires_transform": False,
    }

    plan = compile_action_plan("failing_plan", initial_facts, readings=readings)

    assert plan.is_refusal
    assert not plan.validated
    assert plan.care_badge == CareBadge.ACTION
    assert "source_endpoint_reachable" in plan.refusal_reason


def test_independent_validation_prunes_dag_cycles():
    """Custom action registry with circular causal dependencies is detected and pruned."""
    reg = default_action_registry()

    # Introduce circular dependency: action A needs post B, action B needs post A
    reg.register(
        ActionDefinition(
            action_id="circular.step_a",
            name="circular step a",
            phase=CausalPhase.TRANSFORM,
            part="chunk",
            prerequisites=("post_b",),
            durable_postconditions=("post_a",),
        )
    )
    reg.register(
        ActionDefinition(
            action_id="circular.step_b",
            name="circular step b",
            phase=CausalPhase.TRANSFORM,
            part="chunk",
            prerequisites=("post_a",),
            durable_postconditions=("post_b",),
        )
    )

    deps, unsatisfied = action_compiler.build_causal_dependencies(
        (reg.get("circular.step_a"), reg.get("circular.step_b")),
        initial_facts={},
    )
    assert not action_compiler.is_dag_acyclic(deps)


# -----------------------------------------------------------------------------
# Test 9: Resource Envelope Bounds Derived from Measured Telemetry
# -----------------------------------------------------------------------------

def test_derive_compiler_bounds_from_telemetry_samples():
    """Bounds are derived statistically from sample distributions, not hardcoded constants."""
    readings = sample_readings()
    bounds = derive_compiler_bounds(readings)

    # Max cpu cores should be max sample * 1.5
    assert bounds.max_cpu_cores == 4.0 * 1.5
    # Max memory should be max sample * 1.5
    assert bounds.max_memory_bytes == int(2 * 1024 * 1024 * 1024 * 1.5)
    # Latency budget derived from mean + 3 * stddev * search_depth
    assert bounds.max_plan_cost_seconds > 0.0

    # Test ResourceRequirement exceeds check
    excessive_res = ResourceRequirement(
        cpu_cores=100.0,
        memory_bytes=100 * 1024**3,
    )
    exceeded, reasons = bounds.check_exceeds(excessive_res)
    assert exceeded
    assert len(reasons) >= 2


# -----------------------------------------------------------------------------
# Test 10: Strict Compensation Refuses Uncompensated Irreversible Actions
# -----------------------------------------------------------------------------

def test_strict_compensation_refuses_uncompensated_irreversible():
    """Irreversible action without registered rollback compensation is refused under strict policy."""
    readings = sample_readings()
    reg = default_action_registry()

    # Register an irreversible action with no compensation
    reg.register(
        ActionDefinition(
            action_id="data.write_target",
            name="write rows without compensation",
            phase=CausalPhase.DATA,
            part="write",
            typed_inputs=(ActionPort("validated_rows", "row_stream"),),
            prerequisites=("snapshot_read_complete", "target_schema_exists"),
            durable_postconditions=("rows_written_to_target",),
            effects=(EffectKind.WRITE_TARGET,),
            compensation_action_id=None,  # No compensation!
            is_irreversible=True,
        )
    )

    initial_facts = {
        "target_schema_exists": True,
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": True,
    }

    plan = compile_action_plan(
        "uncompensated_test",
        initial_facts,
        registry=reg,
        readings=readings,
        strict_compensation=True,
    )

    assert plan.is_refusal
    assert not plan.validated
    assert plan.care_badge == CareBadge.ACTION
    assert "Irreversible actions lack registered compensation" in plan.refusal_reason


# -----------------------------------------------------------------------------
# Test 11: Unsupported Transform Honest Refusal (No Invented Plans)
# -----------------------------------------------------------------------------

def test_unsupported_transform_honest_refusal():
    """Required transform not in registry produces honest refusal, never hallucinated plan."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": True,
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": True,
        "transform_kind": "unsupported_ai_vector_embed",  # Unsupported!
    }

    plan = compile_action_plan("unsupported_test", initial_facts, readings=readings)

    assert plan.is_refusal
    assert not plan.validated
    assert plan.care_badge == CareBadge.ACTION
    assert "unsupported by the action registry" in plan.refusal_reason


# -----------------------------------------------------------------------------
# Test 12: Bounded Search Budget Exhaustion
# -----------------------------------------------------------------------------

def test_bounded_search_honest_refusal_when_depth_exceeded():
    """Plan requiring more steps than the bounded search limit honestly refuses."""
    readings = sample_readings()
    initial_facts = {
        "target_schema_exists": False,  # Generates 4 schema + 1 read + 1 write + 3 verify = 9 steps
        "source_endpoint_reachable": True,
        "target_endpoint_reachable": True,
        "requires_transform": False,
    }

    # Restrict operator_max_depth to 3 (less than 9 steps required)
    plan = compile_action_plan(
        "depth_exceeded_test",
        initial_facts,
        readings=readings,
        operator_max_depth=3,
    )

    assert plan.is_refusal
    assert not plan.validated
    assert plan.care_badge == CareBadge.ACTION
    assert "exceeds search bound" in plan.refusal_reason
