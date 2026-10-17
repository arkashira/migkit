import pathlib
import pytest

from migkit.shard_ddl import (
    BarrierStatus,
    CareBadge,
    ColumnSpec,
    DDLConvergenceBarrier,
    DDLProgressMode,
    DuplicateKeyPlacementError,
    RoutingPlacementError,
    ShardDDLChange,
    ShardDDLCoordinator,
    ShardDDLError,
    ShardDeadlockError,
    ShardedRowEvent,
    ShardRoutingRule,
    ShardSchemaVersion,
    ShardSplitError,
    UnrepresentableShardDDLError,
    validate_keyspace_and_routing,
    decide_shard_ddl_compatibility,
)
from migkit.topology import KeySpace
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("shard_ddl", "shard_ddl_compatibility_rungs", "continue_async", "holds"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "continue_async", "refuses"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "barrier_sync", "holds"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "barrier_sync", "refuses"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "drain_and_lock", "holds"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "drain_and_lock", "refuses"),
    ("shard_ddl", "shard_ddl_compatibility_rungs", "refuse_incompatible_shard_ddl", "holds"),
]



def _base_columns():
    return [
        ColumnSpec(name="id", data_type="INT", nullable=False, is_primary_key=True),
        ColumnSpec(name="tenant", data_type="VARCHAR(32)", nullable=False, default="acme"),
        ColumnSpec(name="amount", data_type="INT", nullable=True, default=0),
    ]


def test_optimistic_add_column_and_type_widening_with_concurrent_writes():
    rule = ShardRoutingRule(by="hash", targets=("target_0", "target_1"))
    coord = ShardDDLCoordinator(routing_rule=rule, target_dialect="postgres")

    coord.register_shard_table("shard_1", "orders", _base_columns(), primary_key=("id",), keyspace={"range": [1, 500]})
    coord.register_shard_table(
        "shard_2", "orders", _base_columns(), primary_key=("id",), keyspace={"range": [501, 1000]}
    )

    res_add = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ddl_1",
            shard_id="shard_1",
            table="orders",
            op_type="ADD_COLUMN",
            column="discount",
            data_type="INT",
            nullable=True,
            default=0,
            position=10,
        )
    )
    assert res_add.is_compatible is True
    assert res_add.progress_mode == DDLProgressMode.OPTIMISTIC
    assert res_add.care_badge == CareBadge.BENIGN
    assert "discount" in res_add.divergent_columns
    assert not coord.is_shard_held("shard_1", "orders")
    assert not coord.is_shard_held("shard_2", "orders")

    t1 = coord.apply_row_event(
        ShardedRowEvent(
            shard_id="shard_1",
            table="orders",
            op="insert",
            row={"id": 10, "tenant": "acme", "amount": 100, "discount": 5},
            position=11,
        )
    )
    t2 = coord.apply_row_event(
        ShardedRowEvent(
            shard_id="shard_2",
            table="orders",
            op="insert",
            row={"id": 600, "tenant": "globex", "amount": 250},
            position=12,
        )
    )
    assert t1 in ("target_0", "target_1")
    assert t2 in ("target_0", "target_1")
    assert coord.target_store[t2]["orders"][(600,)]["discount"] == 0

    res_wide = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ddl_2",
            shard_id="shard_1",
            table="orders",
            op_type="MODIFY_COLUMN",
            column="amount",
            data_type="BIGINT",
            position=15,
        )
    )
    assert res_wide.progress_mode == DDLProgressMode.OPTIMISTIC
    assert coord.target_schemas["orders"].columns["amount"].normalized_type() == "BIGINT"

    res_s2_add = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ddl_3",
            shard_id="shard_2",
            table="orders",
            op_type="ADD_COLUMN",
            column="discount",
            data_type="INT",
            nullable=True,
            default=0,
            position=20,
        )
    )
    res_s2_wide = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ddl_4",
            shard_id="shard_2",
            table="orders",
            op_type="MODIFY_COLUMN",
            column="amount",
            data_type="BIGINT",
            position=21,
        )
    )
    assert res_s2_wide.progress_mode == DDLProgressMode.OPTIMISTIC
    assert res_s2_wide.care_badge == CareBadge.PASS
    assert res_s2_wide.divergent_columns == []

    report = coord.verify_placement_and_schema("orders")
    assert report.ok is True
    assert report.care_badge == CareBadge.PASS
    assert report.total_rows_verified == 2


def test_different_shard_ddl_order_preserves_all_columns_without_deadlock():
    rule = ShardRoutingRule(by="range", targets=("target_low", "target_high"), bounds=(500,))
    coord = ShardDDLCoordinator(routing_rule=rule, target_dialect="postgres")

    for sid, lo, hi in (("shard_1", 1, 300), ("shard_2", 301, 600), ("shard_3", 601, 900)):
        coord.register_shard_table("shard_" + sid.split("_")[1], "orders", _base_columns(), ("id",), {"range": [lo, hi]})

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="s1_c1",
            shard_id="shard_1",
            table="orders",
            op_type="ADD_COLUMN",
            column="region",
            data_type="VARCHAR(16)",
            nullable=True,
            default="us",
            position=10,
        )
    )
    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="s2_c2",
            shard_id="shard_2",
            table="orders",
            op_type="ADD_COLUMN",
            column="currency",
            data_type="CHAR(3)",
            nullable=True,
            default="USD",
            position=10,
        )
    )
    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="s3_c3",
            shard_id="shard_3",
            table="orders",
            op_type="ADD_COLUMN",
            column="priority",
            data_type="SMALLINT",
            nullable=True,
            default=1,
            position=10,
        )
    )

    target_cols = set(coord.target_schemas["orders"].columns.keys())
    assert {"id", "tenant", "amount", "region", "currency", "priority"} <= target_cols

    coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 100, "tenant": "t1", "amount": 10, "region": "eu"}, position=11)
    )
    coord.apply_row_event(
        ShardedRowEvent("shard_2", "orders", "insert", {"id": 400, "tenant": "t2", "amount": 20, "currency": "EUR"}, position=11)
    )
    coord.apply_row_event(
        ShardedRowEvent("shard_3", "orders", "insert", {"id": 700, "tenant": "t3", "amount": 30, "priority": 5}, position=11)
    )

    for sid, missing_cols in (
        ("shard_1", [("currency", "CHAR(3)", "USD"), ("priority", "SMALLINT", 1)]),
        ("shard_2", [("priority", "SMALLINT", 1), ("region", "VARCHAR(16)", "us")]),
        ("shard_3", [("region", "VARCHAR(16)", "us"), ("currency", "CHAR(3)", "USD")]),
    ):
        for col_name, dtype, dflt in missing_cols:
            coord.submit_shard_ddl(
                ShardDDLChange(
                    op_id=f"{sid}_{col_name}",
                    shard_id=sid,
                    table="orders",
                    op_type="ADD_COLUMN",
                    column=col_name,
                    data_type=dtype,
                    nullable=True,
                    default=dflt,
                    position=20,
                )
            )

    digests = {coord.shard_schemas[s]["orders"].schema_digest() for s in ("shard_1", "shard_2", "shard_3")}
    assert len(digests) == 1
    assert coord.target_schemas["orders"].schema_digest() in digests

    ver = coord.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.lost_columns == []
    assert ver.total_rows_verified == 3
    assert (100,) in coord.target_store["target_low"]["orders"]
    assert (400,) in coord.target_store["target_low"]["orders"]
    assert (700,) in coord.target_store["target_high"]["orders"]


def test_out_of_order_drop_columns_across_shards_converge_without_deadlock():
    rule = ShardRoutingRule(by="hash", targets=("target_0", "target_1"))
    coord = ShardDDLCoordinator(routing_rule=rule)

    cols = _base_columns() + [
        ColumnSpec(name="legacy_a", data_type="TEXT", nullable=True),
        ColumnSpec(name="legacy_b", data_type="TEXT", nullable=True),
    ]
    coord.register_shard_table("shard_1", "orders", cols, ("id",), {"range": [1, 100]})
    coord.register_shard_table("shard_2", "orders", cols, ("id",), {"range": [101, 200]})

    r1 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="d1", shard_id="shard_1", table="orders", op_type="DROP_COLUMN", column="legacy_a", position=10)
    )
    assert r1.progress_mode == DDLProgressMode.SERIALIZED_AFFECTED
    assert r1.care_badge == CareBadge.REVIEW
    assert "legacy_a" in coord.target_schemas["orders"].columns

    r2 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="d2", shard_id="shard_2", table="orders", op_type="DROP_COLUMN", column="legacy_b", position=10)
    )
    assert r2.progress_mode == DDLProgressMode.SERIALIZED_AFFECTED
    assert "legacy_b" in coord.target_schemas["orders"].columns

    r3 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="d3", shard_id="shard_1", table="orders", op_type="DROP_COLUMN", column="legacy_b", position=15)
    )
    assert "legacy_b" not in coord.target_schemas["orders"].columns

    r4 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="d4", shard_id="shard_2", table="orders", op_type="DROP_COLUMN", column="legacy_a", position=15)
    )
    assert r4.care_badge == CareBadge.PASS
    assert "legacy_a" not in coord.target_schemas["orders"].columns
    assert set(coord.target_schemas["orders"].columns.keys()) == {"id", "tenant", "amount"}
    assert len(coord.active_barriers) == 0


def test_barrier_serializes_only_affected_table_and_drains_held_events():
    rule = ShardRoutingRule(by="hash", targets=("target_0", "target_1"))
    coord = ShardDDLCoordinator(routing_rule=rule)

    for sid in ("shard_1", "shard_2"):
        coord.register_shard_table(sid, "orders", _base_columns(), ("id",))
        coord.register_shard_table(
            sid,
            "audit_log",
            [
                ColumnSpec(name="id", data_type="INT", nullable=False, is_primary_key=True),
                ColumnSpec(name="msg", data_type="TEXT", nullable=True),
            ],
            ("id",),
        )

    coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 1, "tenant": "acme", "amount": 50}, position=1)
    )

    res = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ren_1",
            shard_id="shard_1",
            table="orders",
            op_type="RENAME_COLUMN",
            column="amount",
            new_column_name="total_amount",
            position=5,
        )
    )
    assert res.progress_mode == DDLProgressMode.SERIALIZED_AFFECTED
    assert res.care_badge == CareBadge.REVIEW
    assert coord.is_shard_held("shard_1", "orders") is True
    assert coord.is_shard_held("shard_2", "orders") is False
    assert coord.is_shard_held("shard_1", "audit_log") is False

    audit_target = coord.apply_row_event(
        ShardedRowEvent("shard_1", "audit_log", "insert", {"id": 99, "msg": "unaffected table write"}, position=6)
    )
    assert audit_target in ("target_0", "target_1")

    held_status = coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 2, "tenant": "acme", "total_amount": 75}, position=7)
    )
    assert held_status == "HELD_AT_BARRIER"

    s2_pre_target = coord.apply_row_event(
        ShardedRowEvent("shard_2", "orders", "insert", {"id": 102, "tenant": "beta", "amount": 200}, position=7)
    )
    assert s2_pre_target in ("target_0", "target_1")

    res2 = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ren_2",
            shard_id="shard_2",
            table="orders",
            op_type="RENAME_COLUMN",
            column="amount",
            new_column_name="total_amount",
            position=8,
        )
    )
    assert res2.care_badge == CareBadge.PASS
    assert coord.is_shard_held("shard_1", "orders") is False
    assert "total_amount" in coord.target_schemas["orders"].columns
    assert "amount" not in coord.target_schemas["orders"].columns

    ver = coord.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.total_rows_verified == 3
    for tgt_tables in coord.target_store.values():
        for row in tgt_tables.get("orders", {}).values():
            assert "total_amount" in row
            assert "amount" not in row


def test_unrepresentable_ddl_and_conflicting_barrier_deadlock_detection():
    rule = ShardRoutingRule(by="hash", targets=("target_0",))
    coord = ShardDDLCoordinator(routing_rule=rule, barrier_timeout_seconds=10.0)

    coord.register_shard_table("shard_1", "orders", _base_columns(), ("id",))
    coord.register_shard_table("shard_2", "orders", _base_columns(), ("id",))

    with pytest.raises(UnrepresentableShardDDLError) as exc_info:
        coord.submit_shard_ddl(
            ShardDDLChange(
                op_id="bad_not_null",
                shard_id="shard_1",
                table="orders",
                op_type="ADD_COLUMN",
                column="mandatory_code",
                data_type="VARCHAR(10)",
                nullable=False,
                default=None,
                position=2,
            )
        )
    assert exc_info.value.care_badge == CareBadge.ACTION

    with pytest.raises(UnrepresentableShardDDLError):
        coord.submit_shard_ddl(
            ShardDDLChange(
                op_id="bad_narrow",
                shard_id="shard_1",
                table="orders",
                op_type="MODIFY_COLUMN",
                column="amount",
                data_type="SMALLINT",
                position=3,
            )
        )

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="conflict_ren_1",
            shard_id="shard_1",
            table="orders",
            op_type="RENAME_COLUMN",
            column="amount",
            new_column_name="amount_cents",
            position=10,
        )
    )
    with pytest.raises(ShardDeadlockError) as dl_exc:
        coord.submit_shard_ddl(
            ShardDDLChange(
                op_id="conflict_ren_2",
                shard_id="shard_2",
                table="orders",
                op_type="RENAME_COLUMN",
                column="amount",
                new_column_name="amount_usd",
                position=10,
            )
        )
    assert dl_exc.value.care_badge == CareBadge.ACTION


def test_keyspace_validation_and_duplicate_key_handling():
    bad_range_rule = ShardRoutingRule(by="range", targets=("t0", "t1"), bounds=(500, 200))
    rep_bad = validate_keyspace_and_routing(
        {
            "shard_1": {"orders": {"range": [1, 300]}},
            "shard_2": {"orders": {"range": [250, 600]}},
        },
        bad_range_rule,
        tables=["orders"],
        allow_keyspace_overlap=False,
    )
    assert rep_bad.is_valid is False
    assert rep_bad.care_badge == CareBadge.ACTION
    assert len(rep_bad.overlaps_found) == 1

    good_rule = ShardRoutingRule(by="range", targets=("t0", "t1"), bounds=(500,))
    rep_good = validate_keyspace_and_routing(
        {
            "shard_1": {"orders": {"range": [1, 499]}},
            "shard_2": {"orders": {"range": [500, 1000]}},
        },
        good_rule,
        tables=["orders"],
    )
    assert rep_good.is_valid is True
    assert rep_good.care_badge == CareBadge.PASS

    coord_refuse = ShardDDLCoordinator(routing_rule=good_rule, conflict_policy="refuse")
    coord_refuse.register_shard_table("shard_1", "orders", _base_columns(), ("id",))
    coord_refuse.register_shard_table("shard_2", "orders", _base_columns(), ("id",))
    coord_refuse.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 42, "tenant": "a", "amount": 10}, position=1)
    )
    with pytest.raises(DuplicateKeyPlacementError) as dup_exc:
        coord_refuse.apply_row_event(
            ShardedRowEvent("shard_2", "orders", "insert", {"id": 42, "tenant": "b", "amount": 99}, position=2)
        )
    assert dup_exc.value.care_badge == CareBadge.ACTION

    coord_prio = ShardDDLCoordinator(
        routing_rule=ShardRoutingRule(by="range", targets=("t0", "t1"), key_columns={"orders": ("amount",)}, bounds=(100,)),
        conflict_policy="source_priority",
        shard_priority=["shard_1", "shard_2"],
    )
    coord_prio.register_shard_table("shard_1", "orders", _base_columns(), ("id",))
    coord_prio.register_shard_table("shard_2", "orders", _base_columns(), ("id",))

    p1 = coord_prio.apply_row_event(
        ShardedRowEvent("shard_2", "orders", "insert", {"id": 7, "tenant": "s2", "amount": 50}, position=1)
    )
    assert p1 == "t0"

    p2 = coord_prio.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 7, "tenant": "s1_wins", "amount": 250}, position=2)
    )
    assert p2 == "t1"
    assert (7,) not in coord_prio.target_store["t0"].get("orders", {})
    assert coord_prio.target_store["t1"]["orders"][(7,)]["tenant"] == "s1_wins"

    p3 = coord_prio.apply_row_event(
        ShardedRowEvent("shard_2", "orders", "update", {"id": 7, "tenant": "s2_ignored", "amount": 30}, position=3)
    )
    assert coord_prio.target_store["t1"]["orders"][(7,)]["tenant"] == "s1_wins"
    assert (7,) not in coord_prio.target_store["t0"].get("orders", {})

    ver = coord_prio.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.total_rows_verified == 1


def test_shard_split_inherits_schema_and_updates_active_convergence_barriers():
    rule = ShardRoutingRule(by="hash", targets=("target_0", "target_1", "target_2"))
    coord = ShardDDLCoordinator(routing_rule=rule)

    coord.register_shard_table("shard_1", "orders", _base_columns(), ("id",), keyspace={"range": [1, 100]})
    coord.register_shard_table("shard_2", "orders", _base_columns(), ("id",), keyspace={"range": [101, 200]})

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="add_note_1",
            shard_id="shard_1",
            table="orders",
            op_type="ADD_COLUMN",
            column="note",
            data_type="TEXT",
            nullable=True,
            default="",
            position=10,
        )
    )
    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="add_note_2",
            shard_id="shard_2",
            table="orders",
            op_type="ADD_COLUMN",
            column="note",
            data_type="TEXT",
            nullable=True,
            default="",
            position=10,
        )
    )

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="drop_note_s2",
            shard_id="shard_2",
            table="orders",
            op_type="DROP_COLUMN",
            column="note",
            position=20,
        )
    )
    assert len(coord.active_barriers) == 1

    with pytest.raises(ShardSplitError):
        coord.register_shard_split(
            parent_shard="shard_1",
            child_shards=("shard_1a", "shard_1b"),
            table="orders",
            child_keyspaces={
                "shard_1a": {"range": [1, 60]},
                "shard_1b": {"range": [50, 100]},
            },
            split_watermark=30,
        )

    split_rec = coord.register_shard_split(
        parent_shard="shard_1",
        child_shards=("shard_1a", "shard_1b"),
        table="orders",
        child_keyspaces={
            "shard_1a": {"range": [1, 50]},
            "shard_1b": {"range": [51, 100]},
        },
        split_watermark=30,
    )
    assert split_rec.inherited_schema_version == 2
    assert "note" in coord.shard_schemas["shard_1a"]["orders"].columns
    assert "note" in coord.shard_schemas["shard_1b"]["orders"].columns

    assert coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 10, "tenant": "a", "amount": 1}, position=25)
    ) is None
    with pytest.raises(ShardSplitError):
        coord.apply_row_event(
            ShardedRowEvent("shard_1", "orders", "insert", {"id": 11, "tenant": "a", "amount": 1}, position=31)
        )
    assert coord.apply_row_event(
        ShardedRowEvent("shard_1a", "orders", "insert", {"id": 12, "tenant": "a", "amount": 1}, position=29)
    ) is None

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="drop_note_s1a",
            shard_id="shard_1a",
            table="orders",
            op_type="DROP_COLUMN",
            column="note",
            position=35,
        )
    )
    assert len(coord.active_barriers) == 1
    assert "note" in coord.target_schemas["orders"].columns

    res_final = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="drop_note_s1b",
            shard_id="shard_1b",
            table="orders",
            op_type="DROP_COLUMN",
            column="note",
            position=36,
        )
    )
    assert res_final.care_badge == CareBadge.PASS
    assert len(coord.active_barriers) == 0
    assert "note" not in coord.target_schemas["orders"].columns

    for k in (15, 45):
        coord.apply_row_event(
            ShardedRowEvent("shard_1a", "orders", "insert", {"id": k, "tenant": "t", "amount": k * 10}, position=40)
        )
    for k in (55, 95):
        coord.apply_row_event(
            ShardedRowEvent("shard_1b", "orders", "insert", {"id": k, "tenant": "t", "amount": k * 10}, position=40)
        )
    for k in (115, 185):
        coord.apply_row_event(
            ShardedRowEvent("shard_2", "orders", "insert", {"id": k, "tenant": "t", "amount": k * 10}, position=40)
        )

    ver = coord.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.total_rows_verified == 6


def test_coordinator_restart_recovery_preserves_columns_and_completes_barrier(tmp_path: pathlib.Path):
    state_file = tmp_path / "coordinator_state.json"
    rule = ShardRoutingRule(
        by="list",
        targets=("target_eu", "target_us"),
        key_columns={"orders": ("tenant",)},
        values={"target_eu": ("eu", "uk"), "target_us": ("us",)},
        default_target="target_us",
    )
    coord = ShardDDLCoordinator(routing_rule=rule, state_path=state_file)
    coord.register_shard_table("shard_1", "orders", _base_columns(), ("id",))
    coord.register_shard_table("shard_2", "orders", _base_columns(), ("id",))

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="s1_add_tax",
            shard_id="shard_1",
            table="orders",
            op_type="ADD_COLUMN",
            column="tax",
            data_type="INT",
            nullable=True,
            default=0,
            position=10,
        )
    )
    coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 1, "tenant": "eu", "amount": 100, "tax": 20}, position=11)
    )

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="s1_drop_amount",
            shard_id="shard_1",
            table="orders",
            op_type="DROP_COLUMN",
            column="amount",
            position=12,
        )
    )
    held = coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 2, "tenant": "uk", "tax": 15}, position=13)
    )
    assert held == "HELD_AT_BARRIER"
    ddls_before_restart = list(coord.applied_target_ddls)

    recovered = ShardDDLCoordinator.load_state(state_file)
    assert "tax" in recovered.target_schemas["orders"].columns
    assert "amount" in recovered.target_schemas["orders"].columns
    assert recovered.is_shard_held("shard_1", "orders") is True
    assert recovered.applied_target_ddls == ddls_before_restart

    recovered.submit_shard_ddl(
        ShardDDLChange(
            op_id="s2_add_tax",
            shard_id="shard_2",
            table="orders",
            op_type="ADD_COLUMN",
            column="tax",
            data_type="INT",
            nullable=True,
            default=0,
            position=14,
        )
    )
    res_conv = recovered.submit_shard_ddl(
        ShardDDLChange(
            op_id="s2_drop_amount",
            shard_id="shard_2",
            table="orders",
            op_type="DROP_COLUMN",
            column="amount",
            position=15,
        )
    )
    assert res_conv.care_badge == CareBadge.PASS
    assert recovered.is_shard_held("shard_1", "orders") is False
    assert "amount" not in recovered.target_schemas["orders"].columns
    assert "tax" in recovered.target_schemas["orders"].columns

    recovered.apply_row_event(
        ShardedRowEvent("shard_2", "orders", "insert", {"id": 3, "tenant": "us", "tax": 8}, position=16)
    )

    ver = recovered.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.care_badge == CareBadge.PASS
    assert ver.total_rows_verified == 3
    assert set(recovered.target_store["target_eu"]["orders"].keys()) == {(1,), (2,)}
    assert set(recovered.target_store["target_us"]["orders"].keys()) == {(3,)}


def test_truncate_and_change_pk_barrier_convergence_and_timeout_detection():
    rule = ShardRoutingRule(by="lookup", targets=("t_alpha", "t_beta"), lookup={1: "t_alpha", 2: "t_beta"}, default_target="t_beta")
    coord = ShardDDLCoordinator(routing_rule=rule, barrier_timeout_seconds=5.0)

    coord.register_shard_table("shard_1", "orders", _base_columns(), ("id",))
    coord.register_shard_table("shard_2", "orders", _base_columns(), ("id",))

    coord.apply_row_event(ShardedRowEvent("shard_1", "orders", "insert", {"id": 1, "tenant": "a", "amount": 10}, position=1))
    coord.apply_row_event(ShardedRowEvent("shard_2", "orders", "insert", {"id": 2, "tenant": "b", "amount": 20}, position=1))

    r_trunc_1 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="tr_1", shard_id="shard_1", table="orders", op_type="TRUNCATE", position=5)
    )
    assert r_trunc_1.progress_mode == DDLProgressMode.SERIALIZED_AFFECTED
    assert r_trunc_1.care_badge == CareBadge.REVIEW
    assert len(coord.target_store["t_alpha"]["orders"]) == 1
    assert len(coord.target_store["t_beta"]["orders"]) == 1

    expired = coord.check_barrier_timeouts(now=r_trunc_1.combined_schema.position + 9999999999.0)
    assert len(expired) == 1
    assert expired[0].status == BarrierStatus.DEADLOCK_DETECTED
    assert expired[0].care_badge == CareBadge.ACTION
    expired[0].status = BarrierStatus.WAITING

    r_trunc_2 = coord.submit_shard_ddl(
        ShardDDLChange(op_id="tr_2", shard_id="shard_2", table="orders", op_type="TRUNCATE", position=6)
    )
    assert r_trunc_2.care_badge == CareBadge.PASS
    assert len(coord.target_store["t_alpha"]["orders"]) == 0
    assert len(coord.target_store["t_beta"]["orders"]) == 0
    assert "TRUNCATE TABLE orders" in coord.applied_target_ddls

    r_pk_1 = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="pk_1",
            shard_id="shard_1",
            table="orders",
            op_type="CHANGE_PK",
            primary_key=("id", "tenant"),
            position=10,
        )
    )
    assert r_pk_1.progress_mode == DDLProgressMode.PESSIMISTIC_BARRIER
    assert r_pk_1.care_badge == CareBadge.REVIEW

    r_pk_2 = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="pk_2",
            shard_id="shard_2",
            table="orders",
            op_type="CHANGE_PK",
            primary_key=("id", "tenant"),
            position=11,
        )
    )
    assert r_pk_2.care_badge == CareBadge.PASS
    assert coord.target_schemas["orders"].primary_key == ("id", "tenant")


def test_routing_key_migration_and_post_arrival_shard_split():
    rule = ShardRoutingRule(
        by="range",
        targets=("target_small", "target_large"),
        key_columns={"orders": ("amount",)},
        bounds=(100,),
    )
    coord = ShardDDLCoordinator(routing_rule=rule)
    coord.register_shard_table("shard_1", "orders", _base_columns(), ("id",), keyspace={"range": [1, 100]})
    coord.register_shard_table("shard_2", "orders", _base_columns(), ("id",), keyspace={"range": [101, 200]})

    t_init = coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 10, "tenant": "acme", "amount": 40}, position=1)
    )
    assert t_init == "target_small"
    assert (10,) in coord.target_store["target_small"]["orders"]

    t_moved = coord.apply_row_event(
        ShardedRowEvent(
            "shard_1",
            "orders",
            "update",
            {"id": 10, "tenant": "acme", "amount": 250},
            before={"id": 10, "tenant": "acme", "amount": 40},
            position=2,
        )
    )
    assert t_moved == "target_large"
    assert (10,) not in coord.target_store["target_small"]["orders"]
    assert (10,) in coord.target_store["target_large"]["orders"]

    coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ren_s1",
            shard_id="shard_1",
            table="orders",
            op_type="RENAME_COLUMN",
            column="tenant",
            new_column_name="org_id",
            position=10,
        )
    )
    coord.apply_row_event(
        ShardedRowEvent("shard_1", "orders", "insert", {"id": 75, "org_id": "split_org", "amount": 150}, position=11)
    )

    coord.register_shard_split(
        parent_shard="shard_1",
        child_shards=("shard_1a", "shard_1b"),
        table="orders",
        child_keyspaces={"shard_1a": {"range": [1, 50]}, "shard_1b": {"range": [51, 100]}},
        split_watermark=12,
    )
    assert coord.is_shard_held("shard_1a", "orders") is True
    assert coord.is_shard_held("shard_1b", "orders") is True

    res = coord.submit_shard_ddl(
        ShardDDLChange(
            op_id="ren_s2",
            shard_id="shard_2",
            table="orders",
            op_type="RENAME_COLUMN",
            column="tenant",
            new_column_name="org_id",
            position=15,
        )
    )
    assert res.care_badge == CareBadge.PASS
    assert coord.is_shard_held("shard_1a", "orders") is False
    assert coord.is_shard_held("shard_1b", "orders") is False

    ver = coord.verify_placement_and_schema("orders")
    assert ver.ok is True
    assert ver.total_rows_verified == 2


def test_shard_ddl_compatibility_rungs_reachability():
    """Prove all branches of shard_ddl_compatibility_rungs under watched()."""
    with watched():
        # 1. continue_async holds
        c1 = decide_shard_ddl_compatibility({
            "has_active_shards": True,
            "pks_compatible": True,
            "is_non_breaking_widening": True,
        })
        assert c1.rung.name == "continue_async"

        # 2. continue_async refuses, barrier_sync holds
        c2 = decide_shard_ddl_compatibility({
            "has_active_shards": True,
            "pks_compatible": True,
            "is_non_breaking_widening": False,
            "supremum_resolvable": True,
        })
        assert c2.rung.name == "barrier_sync"

        # 3. barrier_sync refuses, drain_and_lock holds
        c3 = decide_shard_ddl_compatibility({
            "has_active_shards": True,
            "pks_compatible": False,
            "is_non_breaking_widening": False,
            "is_table_level_operation": True,
        })
        assert c3.rung.name == "drain_and_lock"

        # 4. drain_and_lock refuses, refuse_incompatible_shard_ddl holds
        c4 = decide_shard_ddl_compatibility({
            "has_active_shards": False,
            "pks_compatible": False,
            "is_non_breaking_widening": False,
            "is_table_level_operation": False,
        })
        assert c4.rung.name == "refuse_incompatible_shard_ddl"

