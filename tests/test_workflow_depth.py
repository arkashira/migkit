"""Workflow depth contracts, phase invalidation and hypothesis investigation (backlog P0-WORKFLOW-DEPTH)."""
import pytest
from migkit import workflow


def test_phase_advancement_requires_prerequisites_and_evidence():
    st = workflow.WorkflowState(hop_name="test_hop", goal="cutover", scope=("users", "orders"))
    # Advance to plan without assess evidence -> fails
    ok, why = st.advance_phase("plan")
    assert not ok
    assert "prerequisite" in why or "missing" in why

    # Record assess evidence and complete assess
    st.record_evidence("assess", "endpoints_reachable", {"source": True, "target": True})
    st.record_evidence("assess", "schema_inventory", {"tables": ["users", "orders"]})
    st.completed_phases.append("assess")

    # Advance to plan: needs plan required evidence
    st.record_evidence("plan", "capacity_measured", {"vcpus": 4})
    st.record_evidence("plan", "strategy_selected", {"ladder": "streaming"})
    ok, why = st.advance_phase("plan")
    assert ok
    assert st.current_phase == "plan"


def test_schema_drift_invalidates_downstream_evidence():
    st = workflow.WorkflowState(hop_name="test_hop", goal="copy", scope=("orders",),
                                schema_fingerprint="fp_v1")
    st.record_evidence("verify", "checksum_matrix", {"hash": "abcd"}, schema_fp="fp_v1")
    assert "checksum_matrix" in st.evidence

    # Schema changes on source (e.g. alter table add column)
    invalidated = st.check_invalidation(new_schema_fingerprint="fp_v2")
    assert "checksum_matrix" in invalidated
    assert "checksum_matrix" not in st.evidence
    assert len(st.invalidated) == 1
    assert st.invalidated[0]["reason"] == "schema fingerprint changed"


def test_investigation_branches_transient_vs_configuration_vs_defect():
    # 1. Concurrent writes causing transient count difference
    res1 = workflow.investigate("count_mismatch", {
        "replication_lag_seconds": 12,
        "uncommitted_writes": True,
    })
    assert res1["cause"] == "concurrent_writes"
    assert res1["verdict"] == "transient_divergence"
    assert res1["action"] == "wait_drain_or_fence_writes"

    # 2. Timezone drift causing hash mismatch on naive datetimes
    res2 = workflow.investigate("hash_divergence", {
        "src_timezone": "UTC",
        "dst_timezone": "America/New_York",
        "has_naive_datetimes": True,
    })
    assert res2["cause"] == "timezone_drift"
    assert res2["verdict"] == "configuration_drift"

    # 3. Persistent data defect
    res3 = workflow.investigate("hash_divergence", {
        "sample_diff": {"pk=42": {"src": "val1", "dst": "val2"}},
    })
    assert res3["cause"] == "data_defect"
    assert res3["verdict"] == "application_defect"
    assert res3["action"] == "execute_repair_ladder"

    # 4. Unknown finding
    res4 = workflow.investigate("unheard_anomaly", {})
    assert res4["verdict"] == "inconclusive"


def test_validate_safety_dependency_ordering():
    semantics = workflow.SemanticsModel(
        scope=("orders", "order_items"),
        foreign_keys=(("order_items", "orders"),),  # order_items depends on orders
    )
    # Valid schedule: parent first
    ok, msg = workflow.validate_safety(["orders", "order_items"], semantics)
    assert ok
    assert "satisfied" in msg

    # Invalid schedule: child scheduled before parent
    bad, why = workflow.validate_safety(["order_items", "orders"], semantics)
    assert not bad
    assert "dependency ordering violation" in why

