"""Tests for conflict resolution decide rungs, ConflictResolver wiring, and UnresolvedConflictError (Row 622: RCH14).

Proves that:
1. conflict_resolution_rungs covers all 15 branches under watched().
2. decide ladder eliminates if-chains and evaluates measured Need facts.
3. ConflictResolver.resolve_batch handles normal progression, cyclic echo suppression, and CRDT commutative merges.
4. Unresolved conflicts raise UnresolvedConflictError with ACTION care badge.
5. tailctl.apply_unit integrates ConflictResolver into the apply path.
"""

from __future__ import annotations

import pytest

from migkit import decide, tailctl
from migkit.conflict_semantics import (
    BeforeImage,
    CareBadge,
    ColumnMergePolicy,
    ConflictAuditLedger,
    ConflictResolver,
    ConflictType,
    MergeAlgebraKind,
    OriginIdentity,
    TableConflictPolicy,
    UnresolvedConflictError,
    conflict_resolution_rungs,
    decide_conflict_resolution,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("conflict_semantics", "conflict_resolution_rungs", "cyclic_echo_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "cyclic_echo_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "tombstone_delete_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "tombstone_delete_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "tombstone_reinsert_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "tombstone_reinsert_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "clean_insert_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "clean_insert_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "causal_vector_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "causal_vector_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "serial_before_image_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "serial_before_image_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "commutative_column_merge_resolution", "holds"),
    ("conflict_semantics", "conflict_resolution_rungs", "commutative_column_merge_resolution", "refuses"),
    ("conflict_semantics", "conflict_resolution_rungs", "unresolved_conflict_audit", "holds"),
]


def test_conflict_resolution_decide_rungs_coverage():
    with watched() as seen:
        # 1. Cyclic echo holds -> cyclic_echo_resolution holds
        c1 = decide_conflict_resolution({"is_cyclic_echo": True})
        assert c1.path == "cyclic_echo_resolution"

        # 2. Delete holds -> cyclic_echo refuses, tombstone_delete holds
        c2 = decide_conflict_resolution({"is_cyclic_echo": False, "is_delete": True})
        assert c2.path == "tombstone_delete_resolution"

        # 3. Tombstone reinsert holds -> tombstone_delete refuses, tombstone_reinsert holds
        c3 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": True,
            "within_horizon": True,
        })
        assert c3.path == "tombstone_reinsert_resolution"

        # 4. Clean insert holds -> tombstone_reinsert refuses (has_tombstone False), clean_insert holds
        c4 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": False,
            "is_clean_insert": True,
        })
        assert c4.path == "clean_insert_resolution"

        # 5. Causal vector holds -> clean_insert refuses, causal_vector holds
        c5 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": False,
            "is_clean_insert": False,
            "has_version_vectors": True,
            "causally_ordered": True,
        })
        assert c5.path == "causal_vector_resolution"

        # 6. Serial before image holds -> causal_vector refuses, serial_before_image holds
        c6 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": False,
            "is_clean_insert": False,
            "has_version_vectors": False,
            "has_before_image": True,
            "before_image_matches": True,
        })
        assert c6.path == "serial_before_image_resolution"

        # 7. Commutative column merge holds -> serial_before_image refuses, commutative_column_merge holds
        c7 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": False,
            "is_clean_insert": False,
            "has_version_vectors": False,
            "has_before_image": False,
            "can_commutative_merge": True,
        })
        assert c7.path == "commutative_column_merge_resolution"

        # 8. All refuse -> commutative_column_merge refuses, unresolved_conflict_audit holds
        c8 = decide_conflict_resolution({
            "is_cyclic_echo": False,
            "is_delete": False,
            "has_tombstone": False,
            "is_clean_insert": False,
            "has_version_vectors": False,
            "has_before_image": False,
            "can_commutative_merge": False,
        })
        assert c8.path == "unresolved_conflict_audit"

    assert set(branch(seen, "cyclic_echo_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "tombstone_delete_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "tombstone_reinsert_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "clean_insert_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "causal_vector_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "serial_before_image_resolution")) == {HOLDS, REFUSES}
    assert set(branch(seen, "commutative_column_merge_resolution")) == {HOLDS, REFUSES}
    assert branch(seen, "unresolved_conflict_audit") == [HOLDS]


def test_conflict_resolver_batch_and_unresolved_error():
    resolver = ConflictResolver(node_id="node-1")
    policy = TableConflictPolicy(
        table_name="orders",
        require_before_image=True,
    )
    resolver.register_policy("orders", policy)

    # 1. Clean insert resolves cleanly
    changes = [
        {"table": "orders", "key": {"id": 10}, "row": {"id": 10, "amount": 100}, "type": "insert"},
    ]
    resolved = resolver.resolve_batch(changes)
    assert len(resolved) == 1
    assert resolved[0]["row"] == {"id": 10, "amount": 100}

    # 2. Cyclic echo is suppressed from batch
    changes_echo = [
        {"table": "orders", "key": {"id": 10}, "row": {"id": 10, "amount": 100}, "type": "update", "origin": "node-1"},
    ]
    resolved_echo = resolver.resolve_batch(changes_echo)
    assert len(resolved_echo) == 0

    # 3. Unresolved conflict (absent before image on update) raises UnresolvedConflictError
    changes_unresolved = [
        {
            "table": "orders",
            "key": {"id": 10},
            "row": {"id": 10, "amount": 200},
            "local_row": {"id": 10, "amount": 100},
            "type": "update",
            "origin": "node-2",
        },
    ]
    with pytest.raises(UnresolvedConflictError) as excinfo:
        resolver.resolve_batch(changes_unresolved)
    assert "Unresolved conflict on orders" in str(excinfo.value)
    assert excinfo.value.audit_record.care_badge == CareBadge.ACTION.value


def test_tailctl_apply_unit_with_conflict_resolver():
    resolver = ConflictResolver(node_id="target-node")
    policy = TableConflictPolicy(
        table_name="counters",
        columns={
            "hits": ColumnMergePolicy(column_name="hits", algebra=MergeAlgebraKind.COMMUTATIVE_PN_COUNTER, data_type="int"),
        },
    )
    resolver.register_policy("counters", policy)

    class FakeEngine:
        def __init__(self, cr):
            self.conflict_resolver = cr
            self.name = "postgres"

        def apply_columns(self, side, db, cols):
            raise tailctl.NotColumns("records")

    eng = FakeEngine(resolver)

    # Incoming commutative update
    changes = [
        {
            "table": "counters",
            "key": {"id": 1},
            "row": {"id": 1, "hits": 15},
            "local_row": {"id": 1, "hits": 12},
            "before": {"id": 1, "hits": 10},
            "type": "update",
            "origin": "source-node",
        },
    ]

    applied_changes = []
    tailctl.apply_unit(eng, "main", 1000, changes, lambda c: applied_changes.extend(c) or len(c))

    assert len(applied_changes) == 1
    # 10 before + (12-10 local delta) + (15-10 incoming delta) = 17
    assert applied_changes[0]["row"]["hits"] == 17
