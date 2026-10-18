import json
import os
import tempfile
from pathlib import Path
import pytest

from migkit.backfill_race import (
    CDCEvent,
    CoverageStrategy,
    HistoricalCoverageObligation,
    ObligationStatus,
    ReconciliationResult,
    SchemaEpoch,
    SchemaEpochTracker,
    BackfillRaceReconciler,
    choose_coverage_strategy,
    compare_watermarks,
    normalize_watermark,
    decide_backfill_coverage,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("backfill_race", "backfill_coverage_rungs", "selective_key_range_backfill", "holds"),
    ("backfill_race", "backfill_coverage_rungs", "selective_key_range_backfill", "refuses"),
    ("backfill_race", "backfill_coverage_rungs", "shadow_table_resnapshot", "holds"),
    ("backfill_race", "backfill_coverage_rungs", "shadow_table_resnapshot", "refuses"),
    ("backfill_race", "backfill_coverage_rungs", "dual_read_reconciliation", "holds"),
    ("backfill_race", "backfill_coverage_rungs", "dual_read_reconciliation", "refuses"),
    ("backfill_race", "backfill_coverage_rungs", "drain_and_reprobe_schema", "holds"),
    ("backfill_race", "backfill_coverage_rungs", "drain_and_reprobe_schema", "refuses"),
    ("backfill_race", "backfill_coverage_rungs", "refuse_backfill_race", "holds"),
]


def test_schema_epoch_tracker_registration_and_gap_detection():
    tracker = SchemaEpochTracker()
    ep1 = tracker.register_schema("users", ["id", "name"], watermark=100)
    assert ep1.epoch == 1
    assert ep1.relation == "users"
    assert "name" in ep1.columns
    assert len(tracker.get_pending_obligations("users")) == 0

    ep2 = tracker.register_schema("users", ["id", "name", "email"], watermark=150)
    assert ep2.epoch == 2
    assert "email" in ep2.columns

    obs = tracker.get_pending_obligations("users")
    assert len(obs) == 1
    assert obs[0].relation == "users"
    assert obs[0].columns == ["email"]
    assert obs[0].from_epoch == 1
    assert obs[0].epoch == 2
    assert obs[0].status == ObligationStatus.PENDING

    gaps = tracker.get_gaps("users")
    assert len(gaps) == 1
    assert gaps[0].relation == "users"
    assert gaps[0].from_epoch == 1
    assert gaps[0].to_epoch == 2
    assert "email" in gaps[0].missing_columns
    assert gaps[0].start_watermark == 100
    assert gaps[0].end_watermark == 150


def test_observed_gap_on_delayed_discovery_with_events():
    tracker = SchemaEpochTracker()
    tracker.register_schema("orders", ["id", "customer_id", "total"], watermark=200)

    ev1 = CDCEvent(
        event_id="ev-1",
        relation="orders",
        op="UPDATE",
        key=42,
        after={"id": 42, "customer_id": 10, "total": 99.5, "discount": 10.0},
        watermark=210,
    )
    gap = tracker.observe_event(ev1)
    assert gap is not None
    assert "discount" in gap.missing_columns
    assert gap.start_watermark == 200
    assert gap.end_watermark == 210

    gaps = tracker.get_gaps("orders")
    assert len(gaps) == 1

    tracker.register_schema("orders", ["id", "customer_id", "total", "discount"], watermark=220)
    obs = tracker.get_pending_obligations("orders")
    assert len(obs) == 1
    assert "discount" in obs[0].columns


def test_coverage_strategy_selection():
    tracker = SchemaEpochTracker()
    tracker.register_schema("items", ["id", "sku"], watermark=10)
    tracker.register_schema("items", ["id", "sku", "category"], watermark=20)

    obs = tracker.get_pending_obligations("items")[0]

    strat_selective = choose_coverage_strategy(
        "items",
        obs,
        table_stats={"total_rows": 100000, "affected_rows": 50},
    )
    assert strat_selective == CoverageStrategy.SELECTIVE_BACKFILL

    strat_shadow = choose_coverage_strategy(
        "items",
        obs,
        table_stats={"total_rows": 100000, "affected_rows": 40000},
    )
    assert strat_shadow == CoverageStrategy.SHADOW_RESNAPSHOT

    strat_override = choose_coverage_strategy(
        "items",
        obs,
        config={"strategy": "shadow_resnapshot"},
    )
    assert strat_override == CoverageStrategy.SHADOW_RESNAPSHOT


def test_add_column_update_delayed_discovery_restart_convergence(tmp_path):
    state_file = tmp_path / "backfill_race_state.json"

    source_db = {
        "users": {
            1: {"id": 1, "name": "Alice"},
            2: {"id": 2, "name": "Bob"},
            3: {"id": 3, "name": "Charlie"},
        },
        "inventory": {
            101: {"item_id": 101, "qty": 50},
            102: {"item_id": 102, "qty": 80},
        },
    }

    target_db = {
        "users": {
            1: {"id": 1, "name": "Alice"},
            2: {"id": 2, "name": "Bob"},
            3: {"id": 3, "name": "Charlie"},
        },
        "inventory": {
            101: {"item_id": 101, "qty": 50},
            102: {"item_id": 102, "qty": 80},
        },
    }

    tracker = SchemaEpochTracker()
    tracker.register_schema("users", ["id", "name"], watermark=100)
    tracker.register_schema("inventory", ["item_id", "qty"], watermark=100)

    reconciler = BackfillRaceReconciler(
        tracker=tracker,
        target_store=target_db,
        state_path=state_file,
    )

    source_db["users"][1]["email"] = "alice@example.com"
    source_db["users"][2]["email"] = "bob@example.com"
    source_db["users"][4] = {"id": 4, "name": "David", "email": "david@example.com"}

    cdc_events = [
        CDCEvent(
            event_id="e-101",
            relation="users",
            op="UPDATE",
            key=1,
            after={"id": 1, "name": "Alice", "email": "alice@example.com"},
            watermark=101,
        ),
        CDCEvent(
            event_id="e-102",
            relation="users",
            op="UPDATE",
            key=2,
            after={"id": 2, "name": "Bob", "email": "bob@example.com"},
            watermark=102,
        ),
        CDCEvent(
            event_id="e-103",
            relation="users",
            op="INSERT",
            key=4,
            after={"id": 4, "name": "David", "email": "david@example.com"},
            watermark=103,
        ),
    ]

    for ev in cdc_events:
        reconciler.buffer_event(ev)

    tracker.register_schema("users", ["id", "name", "email"], watermark=105)

    obs_list = tracker.get_pending_obligations("users")
    assert len(obs_list) == 1
    ob = obs_list[0]
    assert ob.columns == ["email"]

    reconciler.buffer_event(
        CDCEvent(
            event_id="e-106",
            relation="users",
            op="UPDATE",
            key=2,
            after={"id": 2, "name": "Bob", "email": "bob_new@example.com"},
            watermark=106,
        )
    )
    source_db["users"][2]["email"] = "bob_new@example.com"

    reconciler.save()

    restarted_reconciler = BackfillRaceReconciler.load(
        state_path=state_file,
        target_store=target_db,
    )

    restored_obs = restarted_reconciler.tracker.get_pending_obligations("users")
    assert len(restored_obs) == 1
    assert restored_obs[0].columns == ["email"]
    assert len(restarted_reconciler.get_buffered_events("users")) == 4

    def source_fetcher(relation, keys):
        return {k: dict(source_db[relation][k]) for k in keys if k in source_db[relation]}

    result = restarted_reconciler.reconcile_relation(
        relation="users",
        source_fetcher=source_fetcher,
        table_stats={"total_rows": 4, "affected_rows": 2},
    )

    assert result.success is True
    assert result.strategy_used == CoverageStrategy.SELECTIVE_BACKFILL
    assert result.rows_backfilled >= 2
    assert result.events_reconciled + result.duplicate_updates_prevented == 4
    assert result.duplicate_updates_prevented > 0

    assert target_db["users"][1]["email"] == "alice@example.com"
    assert target_db["users"][2]["email"] == "bob_new@example.com"
    assert target_db["users"][4]["email"] == "david@example.com"
    assert target_db["users"][3].get("email") is None

    assert target_db["inventory"] == {
        101: {"item_id": 101, "qty": 50},
        102: {"item_id": 102, "qty": 80},
    }
    assert restarted_reconciler.relation_reload_counts.get("inventory", 0) == 0

    assert len(restarted_reconciler.tracker.get_pending_obligations("users")) == 0
    assert len(restarted_reconciler.get_buffered_events("users")) == 0


def test_shadow_resnapshot_strategy_with_concurrent_cdc():
    source_db = {
        "products": {
            10: {"id": 10, "title": "Desk", "price": 100, "currency": "USD"},
            20: {"id": 20, "title": "Chair", "price": 50, "currency": "USD"},
            30: {"id": 30, "title": "Lamp", "price": 25, "currency": "USD"},
        },
        "orders": {
            1: {"id": 1, "product_id": 10, "amount": 1},
        },
    }

    target_db = {
        "products": {
            10: {"id": 10, "title": "Desk", "price": 100},
            20: {"id": 20, "title": "Chair", "price": 50},
            30: {"id": 30, "title": "Lamp", "price": 25},
        },
        "orders": {
            1: {"id": 1, "product_id": 10, "amount": 1},
        },
    }

    tracker = SchemaEpochTracker()
    tracker.register_schema("products", ["id", "title", "price"], watermark=50)
    tracker.register_schema("orders", ["id", "product_id", "amount"], watermark=50)

    reconciler = BackfillRaceReconciler(tracker=tracker, target_store=target_db)

    tracker.register_schema("products", ["id", "title", "price", "currency"], watermark=60)
    obs = tracker.get_pending_obligations("products")[0]
    obs.strategy = CoverageStrategy.SHADOW_RESNAPSHOT

    reconciler.buffer_event(
        CDCEvent(
            event_id="e-61",
            relation="products",
            op="UPDATE",
            key=20,
            after={"id": 20, "title": "Executive Chair", "price": 75, "currency": "EUR"},
            watermark=65,
        )
    )
    source_db["products"][20] = {"id": 20, "title": "Executive Chair", "price": 75, "currency": "EUR"}

    def shadow_snapshot_reader(relation):
        assert relation == "products"
        return {
            10: {"id": 10, "title": "Desk", "price": 100, "currency": "USD"},
            20: {"id": 20, "title": "Chair", "price": 50, "currency": "USD"},
            30: {"id": 30, "title": "Lamp", "price": 25, "currency": "USD"},
        }

    result = reconciler.reconcile_relation(
        relation="products",
        source_snapshot_reader=shadow_snapshot_reader,
        snapshot_watermark=60,
    )

    assert result.success is True
    assert result.strategy_used == CoverageStrategy.SHADOW_RESNAPSHOT
    assert target_db["products"][10]["currency"] == "USD"
    assert target_db["products"][20]["title"] == "Executive Chair"
    assert target_db["products"][20]["currency"] == "EUR"
    assert target_db["products"][30]["currency"] == "USD"

    assert target_db["orders"][1]["amount"] == 1
    assert reconciler.relation_reload_counts.get("orders", 0) == 0
    assert len(tracker.get_pending_obligations("products")) == 0


def test_idempotent_event_reconciliation():
    target_db = {
        "accounts": {
            1: {"id": 1, "balance": 100, "tier": "gold"},
        }
    }
    tracker = SchemaEpochTracker()
    tracker.register_schema("accounts", ["id", "balance", "tier"], watermark=10)
    reconciler = BackfillRaceReconciler(tracker=tracker, target_store=target_db)

    ev = CDCEvent(
        event_id="ev-dup",
        relation="accounts",
        op="UPDATE",
        key=1,
        after={"id": 1, "balance": 100, "tier": "gold"},
        watermark=15,
    )

    applied_1 = reconciler.apply_event(ev)
    assert applied_1 is False

    ev_change = CDCEvent(
        event_id="ev-change",
        relation="accounts",
        op="UPDATE",
        key=1,
        after={"id": 1, "balance": 150, "tier": "platinum"},
        watermark=20,
    )
    applied_2 = reconciler.apply_event(ev_change)
    assert applied_2 is True
    assert target_db["accounts"][1]["tier"] == "platinum"

    applied_3 = reconciler.apply_event(ev_change)
    assert applied_3 is False
    assert target_db["accounts"][1]["tier"] == "platinum"


def test_multiple_column_obligations_and_independent_resolution():
    tracker = SchemaEpochTracker()
    tracker.register_schema("customers", ["id", "name"], watermark=10)
    tracker.register_schema("customers", ["id", "name", "phone"], watermark=20)
    tracker.register_schema("customers", ["id", "name", "phone", "address"], watermark=30)

    obs = tracker.get_pending_obligations("customers")
    assert len(obs) == 2
    assert obs[0].columns == ["phone"]
    assert obs[0].epoch == 2
    assert obs[1].columns == ["address"]
    assert obs[1].epoch == 3

    obs[0].mark_satisfied(rows_covered=10)
    assert obs[0].is_satisfied() is True

    pending = tracker.get_pending_obligations("customers")
    assert len(pending) == 1
    assert pending[0].columns == ["address"]


def test_delete_events_during_buffered_reconciliation():
    target_db = {
        "users": {
            1: {"id": 1, "name": "Alice"},
            2: {"id": 2, "name": "Bob"},
        }
    }
    source_db = {
        "users": {
            1: {"id": 1, "name": "Alice", "role": "admin"},
        }
    }
    tracker = SchemaEpochTracker()
    tracker.register_schema("users", ["id", "name"], watermark=50)
    tracker.register_schema("users", ["id", "name", "role"], watermark=60)

    reconciler = BackfillRaceReconciler(tracker=tracker, target_store=target_db)

    del_event = CDCEvent(
        event_id="del-2",
        relation="users",
        op="DELETE",
        key=2,
        before={"id": 2, "name": "Bob"},
        watermark=65,
    )
    reconciler.buffer_event(del_event)

    def source_fetcher(relation, keys):
        return {k: dict(source_db[relation][k]) for k in keys if k in source_db[relation]}

    result = reconciler.reconcile_relation(
        relation="users",
        source_fetcher=source_fetcher,
    )
    assert result.success is True
    assert 2 not in target_db["users"]
    assert target_db["users"][1]["role"] == "admin"


def test_schema_epoch_serialization_roundtrip(tmp_path):
    path = tmp_path / "tracker.json"
    tracker = SchemaEpochTracker()
    tracker.register_schema("tbl", ["c1", "c2"], watermark=100)
    tracker.register_schema("tbl", ["c1", "c2", "c3"], watermark=200)

    tracker.save(path)
    loaded = SchemaEpochTracker.load(path)

    assert len(loaded.get_epoch_history("tbl")) == 2
    assert len(loaded.get_gaps("tbl")) == 1
    assert len(loaded.get_pending_obligations("tbl")) == 1
    assert loaded.get_pending_obligations("tbl")[0].columns == ["c3"]


def test_watermark_comparison_and_normalization():
    assert normalize_watermark("0/16B3748") == (0 << 32) | int("16B3748", 16)
    assert normalize_watermark(1234) == 1234
    assert normalize_watermark("5678") == 5678
    assert normalize_watermark(None) == 0

    assert compare_watermarks(100, 200) == -1
    assert compare_watermarks(200, 100) == 1
    assert compare_watermarks(100, 100) == 0
    assert compare_watermarks("0/100", "0/200") == -1


def test_backfill_coverage_rungs_under_watched():
    with watched() as seen:
        # 1. selective_key_range_backfill holds
        f1 = {
            "narrow_gap": True,
            "keys_available": True,
            "low_lock_pressure": True,
            "broad_or_dense_backfill": True,
            "shadow_storage_available": True,
            "epoch_discrepancy_resolvable": True,
            "schema_reprobe_allowed": True,
        }
        c1 = decide_backfill_coverage(f1)
        assert c1.rung.name == "selective_key_range_backfill"

        # 2. selective_key_range_backfill refuses, shadow_table_resnapshot holds
        f2 = {
            "narrow_gap": False,
            "keys_available": True,
            "low_lock_pressure": True,
            "broad_or_dense_backfill": True,
            "shadow_storage_available": True,
            "epoch_discrepancy_resolvable": True,
            "schema_reprobe_allowed": True,
        }
        c2 = decide_backfill_coverage(f2)
        assert c2.rung.name == "shadow_table_resnapshot"

        # 3. shadow_table_resnapshot refuses, dual_read_reconciliation holds
        f3 = {
            "narrow_gap": False,
            "keys_available": False,
            "low_lock_pressure": False,
            "broad_or_dense_backfill": False,
            "shadow_storage_available": False,
            "epoch_discrepancy_resolvable": True,
            "schema_reprobe_allowed": True,
        }
        c3 = decide_backfill_coverage(f3)
        assert c3.rung.name == "dual_read_reconciliation"

        # 4. dual_read_reconciliation refuses, drain_and_reprobe_schema holds
        f4 = {
            "narrow_gap": False,
            "keys_available": False,
            "low_lock_pressure": False,
            "broad_or_dense_backfill": False,
            "shadow_storage_available": False,
            "epoch_discrepancy_resolvable": False,
            "schema_reprobe_allowed": True,
        }
        c4 = decide_backfill_coverage(f4)
        assert c4.rung.name == "drain_and_reprobe_schema"

        # 5. drain_and_reprobe_schema refuses, refuse_backfill_race holds
        f5 = {
            "narrow_gap": False,
            "keys_available": False,
            "low_lock_pressure": False,
            "broad_or_dense_backfill": False,
            "shadow_storage_available": False,
            "epoch_discrepancy_resolvable": False,
            "schema_reprobe_allowed": False,
        }
        c5 = decide_backfill_coverage(f5)
        assert c5.rung.name == "refuse_backfill_race"

