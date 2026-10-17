import pytest
from migkit.rollback_set import (
    Effect,
    EffectDependencyDAG,
    CyclicDependencyError,
    EffectNotFoundError,
    PostconditionStatus,
    PostconditionCheckResult,
    CompensatingActionType,
    CompensatingOperation,
    ForwardSafetyStatus,
    ForwardSafetyResult,
    RollbackSet,
    RollbackSetResolver,
    generate_compensating_operation,
    verify_forward_safety,
)


def test_effect_immutability_and_identity():
    eff1 = Effect(
        effect_id="eff_01",
        target_entity="table:users",
        operation_type="DDL_ALTER",
        owner="migration_42",
        tags=("v1.0", "sprint_1"),
        depends_on=(),
        forward_payload="ALTER TABLE users ADD COLUMN phone VARCHAR(32);",
        reverse_payload="ALTER TABLE users DROP COLUMN phone;",
        postconditions=(("column_exists", "phone"),),
    )
    assert eff1.effect_id == "eff_01"
    assert eff1.target_entity == "table:users"
    assert "v1.0" in eff1.tags

    with pytest.raises(Exception):
        eff1.effect_id = "eff_mutated"  # type: ignore


def test_dag_dependency_and_cycle_detection():
    dag = EffectDependencyDAG()

    eff_a = Effect(effect_id="A", target_entity="table:users", operation_type="CREATE")
    eff_b = Effect(effect_id="B", target_entity="table:orders", operation_type="CREATE", depends_on=("A",))
    eff_c = Effect(effect_id="C", target_entity="table:shipments", operation_type="CREATE", depends_on=("B",))

    dag.add_effect(eff_a)
    dag.add_effect(eff_b)
    dag.add_effect(eff_c)

    assert dag.get_dependencies("B") == frozenset({"A"})
    assert dag.get_dependents("A") == frozenset({"B"})
    assert dag.get_transitive_dependents("A") == frozenset({"B", "C"})
    assert dag.get_transitive_prerequisites("C") == frozenset({"A", "B"})

    # Cycle detection
    eff_cycle = Effect(effect_id="cycle", target_entity="table:users", operation_type="ALTER", depends_on=("C",))
    dag.add_effect(eff_cycle)
    with pytest.raises(CyclicDependencyError):
        dag.add_dependency("A", "cycle")


def test_dag_independence_check():
    dag = EffectDependencyDAG()
    dag.add_effect(Effect(effect_id="A", target_entity="table:users", operation_type="CREATE"))
    dag.add_effect(Effect(effect_id="B", target_entity="table:users", operation_type="ALTER", depends_on=("A",)))
    dag.add_effect(Effect(effect_id="X", target_entity="table:audit_logs", operation_type="CREATE"))
    dag.add_effect(Effect(effect_id="Y", target_entity="table:audit_logs", operation_type="INSERT", depends_on=("X",)))

    assert dag.is_independent("A", "X") is True
    assert dag.is_independent("B", "Y") is True
    assert dag.is_independent("A", "B") is False
    assert dag.is_independent("X", "Y") is False


def test_dag_topological_and_reverse_ordering():
    dag = EffectDependencyDAG()
    dag.add_effect(Effect(effect_id="root", target_entity="table:t", operation_type="CREATE"))
    dag.add_effect(Effect(effect_id="mid1", target_entity="table:t", operation_type="ALTER", depends_on=("root",)))
    dag.add_effect(Effect(effect_id="mid2", target_entity="table:t", operation_type="ALTER", depends_on=("root",)))
    dag.add_effect(Effect(effect_id="leaf", target_entity="table:t", operation_type="ALTER", depends_on=("mid1", "mid2")))

    topo = dag.topological_sort()
    assert topo.index("root") < topo.index("mid1") < topo.index("leaf")
    assert topo.index("root") < topo.index("mid2") < topo.index("leaf")

    rev = dag.reverse_topological_sort()
    assert rev.index("leaf") < rev.index("mid1") < rev.index("root")
    assert rev.index("leaf") < rev.index("mid2") < rev.index("root")


def test_tag_boundary_and_preservation_of_unrelated_work():
    dag = EffectDependencyDAG()
    # Scenario:
    # 1. Base table orders (E1)
    # 2. Tag release-1.0
    # 3. Alpha feature: add discount col (E2, depends on E1)
    # 4. Unrelated Beta feature: create audit table (E3, independent)
    # 5. Tag release-1.1
    # 6. Alpha feature 2: add index on discount (E4, depends on E2)
    # 7. Unrelated Beta feature 2: insert audit entry (E5, depends on E3)
    dag.add_effect(Effect(effect_id="E1", target_entity="table:orders", operation_type="CREATE", tags=("release-1.0",)))
    dag.add_effect(Effect(effect_id="E2", target_entity="col:orders.discount", operation_type="ALTER", depends_on=("E1",)))
    dag.add_effect(Effect(effect_id="E3", target_entity="table:audit_logs", operation_type="CREATE", tags=("release-1.1",)))
    dag.add_effect(Effect(effect_id="E4", target_entity="idx:orders_discount", operation_type="CREATE", depends_on=("E2",)))
    dag.add_effect(Effect(effect_id="E5", target_entity="row:audit_logs:1", operation_type="INSERT", depends_on=("E3",)))

    resolver = RollbackSetResolver(dag)

    # Targeted rollback of Feature Alpha (E2)
    plan = resolver.resolve_by_effect_ids(["E2"])

    assert plan.target_effect_ids == frozenset({"E2"})
    assert plan.cascading_effect_ids == frozenset({"E4"})
    assert "E4" in plan.rollback_order
    assert "E2" in plan.rollback_order
    # Dependent E4 must be rolled back before prerequisite E2
    assert plan.rollback_order.index("E4") < plan.rollback_order.index("E2")

    # Unrelated work must be preserved
    assert "E1" in plan.independent_effect_ids
    assert "E3" in plan.independent_effect_ids
    assert "E5" in plan.independent_effect_ids
    assert "E3" not in plan.rollback_order
    assert "E5" not in plan.rollback_order
    assert plan.allowed is True
    assert plan.badge == "PASS"


def test_tag_targeted_rollback():
    dag = EffectDependencyDAG()
    dag.add_effect(Effect(effect_id="E1", target_entity="table:orders", operation_type="CREATE", tags=("v1.0",)))
    dag.add_effect(Effect(effect_id="E2", target_entity="table:users", operation_type="CREATE", tags=("v1.1",)))
    dag.add_effect(Effect(effect_id="E3", target_entity="col:users.age", operation_type="ALTER", tags=("v1.1",), depends_on=("E2",)))
    dag.add_effect(Effect(effect_id="E4", target_entity="table:metrics", operation_type="CREATE", tags=("v1.2",)))

    resolver = RollbackSetResolver(dag)
    # Rollback tag v1.1
    plan = resolver.resolve_by_tag("v1.1")

    assert plan.target_effect_ids == frozenset({"E2", "E3"})
    assert plan.cascading_effect_ids == frozenset()
    assert "E4" in plan.independent_effect_ids
    assert "E1" in plan.independent_effect_ids
    assert plan.rollback_order.index("E3") < plan.rollback_order.index("E2")


def test_postcondition_check_satisfied():
    dag = EffectDependencyDAG()
    eff = Effect(
        effect_id="eff_add_col",
        target_entity="col:users.tier",
        operation_type="ALTER",
        postconditions=(("tier_type", "VARCHAR(32)"), ("tier_default", "bronze")),
    )
    dag.add_effect(eff)

    target_state = {
        "col:users.tier": {
            "tier_type": "VARCHAR(32)",
            "tier_default": "bronze",
        }
    }

    resolver = RollbackSetResolver(dag)
    plan = resolver.resolve_by_effect_ids(["eff_add_col"], target_state=target_state)

    assert plan.allowed is True
    assert len(plan.postcondition_results) == 1
    assert plan.postcondition_results[0].status == PostconditionStatus.SATISFIED
    assert plan.postcondition_results[0].passed is True


def test_postcondition_check_prevents_accidental_rollback_of_concurrent_writes():
    dag = EffectDependencyDAG()
    eff = Effect(
        effect_id="eff_price_update",
        target_entity="row:products:100",
        operation_type="UPDATE",
        postconditions=(("price", 19.99), ("status", "ACTIVE")),
        reverse_payload="UPDATE products SET price = 10.00 WHERE id = 100;",
    )
    dag.add_effect(eff)

    # An external / concurrent writer modified the price to 25.00 and status to DISCOUNTED
    concurrent_target_state = {
        "row:products:100": {
            "price": 25.00,
            "status": "DISCOUNTED",
        }
    }

    resolver = RollbackSetResolver(dag)
    plan = resolver.resolve_by_effect_ids(["eff_price_update"], target_state=concurrent_target_state)

    assert plan.allowed is False
    assert plan.badge == "ACTION"
    assert len(plan.postcondition_results) == 1
    res = plan.postcondition_results[0]
    assert res.status == PostconditionStatus.CONCURRENT_OVERWRITE
    assert res.passed is False
    assert "concurrent unrelated writes prevented" in res.details


def test_postcondition_distinguishes_in_dag_cascading_effects_from_concurrent_writes():
    dag = EffectDependencyDAG()
    # E1 created column with default 'bronze'
    eff1 = Effect(
        effect_id="E1",
        target_entity="col:users.tier",
        operation_type="ALTER",
        postconditions=(("tier_default", "bronze"),),
        reverse_payload="ALTER TABLE users DROP COLUMN tier;",
    )
    # E2 modified default to 'silver' and depends on E1
    eff2 = Effect(
        effect_id="E2",
        target_entity="col:users.tier",
        operation_type="ALTER",
        depends_on=("E1",),
        postconditions=(("tier_default", "silver"),),
        reverse_payload="ALTER TABLE users ALTER COLUMN tier SET DEFAULT 'bronze';",
    )
    dag.add_effect(eff1)
    dag.add_effect(eff2)

    # Current target state reflects E2's changes ('silver')
    target_state = {
        "col:users.tier": {
            "tier_default": "silver",
        }
    }

    resolver = RollbackSetResolver(dag)
    # We target E1 for rollback. E2 is resolved as cascading dependent!
    plan = resolver.resolve_by_effect_ids(["E1"], target_state=target_state)

    assert plan.target_effect_ids == frozenset({"E1"})
    assert plan.cascading_effect_ids == frozenset({"E2"})
    assert plan.allowed is True
    assert plan.badge == "PASS"

    status_by_id = {r.effect_id: r.status for r in plan.postcondition_results}
    assert status_by_id["E2"] == PostconditionStatus.SATISFIED
    assert status_by_id["E1"] == PostconditionStatus.MODIFIED_BY_DEPENDENT


def test_surgical_rollback_forward_safety_verification():
    dag = EffectDependencyDAG()
    eff1 = Effect(
        effect_id="E1",
        target_entity="table:customers",
        operation_type="CREATE",
        reverse_payload="DROP TABLE customers;",
    )
    eff2 = Effect(
        effect_id="E2",
        target_entity="col:customers.email",
        operation_type="ALTER",
        depends_on=("E1",),
        reverse_payload="ALTER TABLE customers DROP COLUMN email;",
    )
    eff_independent = Effect(
        effect_id="E_ind",
        target_entity="table:billing",
        operation_type="CREATE",
        reverse_payload="DROP TABLE billing;",
    )
    dag.add_effect(eff1)
    dag.add_effect(eff2)
    dag.add_effect(eff_independent)

    resolver = RollbackSetResolver(dag)
    plan = resolver.resolve_by_effect_ids(["E2"])

    assert plan.allowed is True
    assert plan.forward_safety.is_safe is True
    assert len(plan.compensating_operations) == 1
    comp = plan.compensating_operations[0]
    assert comp.effect_id == "E2"
    assert comp.action_type == CompensatingActionType.REVERSE_DDL
    assert comp.transactional is True


def test_forward_safety_flags_unsafe_destructive_operations():
    dag = EffectDependencyDAG()
    eff = Effect(
        effect_id="E_drop",
        target_entity="table:customers",
        operation_type="CREATE",
        reverse_payload="DROP TABLE customers CASCADE;",
    )
    dag.add_effect(eff)

    comp_ops = [
        CompensatingOperation(
            operation_id="comp_1",
            effect_id="E_drop",
            target_entity="table:customers",
            action_type=CompensatingActionType.REVERSE_DDL,
            payload="DROP TABLE customers CASCADE;",
            is_destructive=True,
            preserves_concurrent_data=False,
            transactional=False,
        )
    ]

    report = verify_forward_safety(
        rollback_effects=[eff],
        independent_effects=[],
        compensating_ops=comp_ops,
    )
    assert report.is_safe is False
    assert report.badge == "ACTION"
    assert any("destructive" in v for v in report.violations)


def test_dangling_dependency_in_independent_effect_triggers_action():
    eff1 = Effect(effect_id="E1", target_entity="table:a", operation_type="CREATE")
    eff2 = Effect(effect_id="E2", target_entity="table:b", operation_type="CREATE", depends_on=("E1",))

    # If somehow E1 is in rollback set while E2 is considered independent:
    report = verify_forward_safety(
        rollback_effects=[eff1],
        independent_effects=[eff2],
        compensating_ops=[],
    )
    assert report.is_safe is False
    assert report.badge == "ACTION"
    assert any("dangling dependency" in v for v in report.violations)


def test_unknown_effect_raises_not_found():
    dag = EffectDependencyDAG()
    resolver = RollbackSetResolver(dag)
    with pytest.raises(EffectNotFoundError):
        resolver.resolve_by_effect_ids(["nonexistent_id"])


def test_empty_target_returns_empty_plan():
    dag = EffectDependencyDAG()
    resolver = RollbackSetResolver(dag)
    plan = resolver.resolve_by_effect_ids([])
    assert plan.target_effect_ids == frozenset()
    assert plan.cascading_effect_ids == frozenset()
    assert plan.rollback_order == ()
    assert plan.allowed is True
    assert plan.badge == "PASS"
