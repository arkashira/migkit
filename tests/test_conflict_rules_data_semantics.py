import pytest

from migkit.conflict_semantics import (
    AddWinsSet,
    BeforeImage,
    CareBadge,
    ColumnMergePolicy,
    ConflictAuditLedger,
    ConflictAuditRecord,
    ConflictSemanticsEngine,
    ConflictType,
    HybridLogicalClock,
    MergeAlgebraKind,
    OriginIdentity,
    PNCounter,
    TableConflictPolicy,
    TombstoneHorizonLedger,
    VersionVector,
    reject_heuristic_server_precedence,
)


def test_origin_identity_anti_heuristic_precedence():
    heuristics = [
        ("master-db-01", "replica-10-0-0-2"),
        ("primary-node", "standby-node"),
        ("node-leader", "node-follower"),
        ("main-server", "backup-server"),
        ("active-host", "passive-host"),
    ]
    for origin_a, origin_b in heuristics:
        with pytest.raises(ValueError, match="Heuristic server name precedence rejected"):
            reject_heuristic_server_precedence(origin_a, origin_b)

    reject_heuristic_server_precedence("node-10-0-0-1", "node-10-0-0-2")
    reject_heuristic_server_precedence("replica-west", "replica-east")


def test_version_vector_causality_and_dominance():
    v1 = VersionVector({"node-1": 1, "node-2": 0})
    v2 = VersionVector({"node-1": 1, "node-2": 1})
    v3 = VersionVector({"node-1": 2, "node-2": 0})

    assert v2.dominates(v1)
    assert not v1.dominates(v2)
    assert v2.is_descendant_or_equal(v1)
    assert not v1.is_descendant_or_equal(v2)

    assert v2.is_concurrent(v3)
    assert v3.is_concurrent(v2)

    merged = v2.merge(v3)
    assert merged.clocks == {"node-1": 2, "node-2": 1}
    assert merged.dominates(v2)
    assert merged.dominates(v3)


def test_hybrid_logical_clock_and_clock_skew():
    hlc_a = HybridLogicalClock(physical_ts=100.0, logical_counter=0, origin_id="node-10-0-0-1", skew_bound_seconds=1.0)
    hlc_b = HybridLogicalClock(physical_ts=100.5, logical_counter=0, origin_id="node-10-0-0-2", skew_bound_seconds=1.0)
    hlc_skewed = HybridLogicalClock(physical_ts=105.0, logical_counter=0, origin_id="node-10-0-0-3", skew_bound_seconds=1.0)

    cmp_val, skew_exceeded = hlc_a.compare(hlc_b, max_skew_seconds=1.0)
    assert cmp_val == -1
    assert not skew_exceeded

    cmp_val, skew_exceeded = hlc_a.compare(hlc_skewed, max_skew_seconds=1.0)
    assert cmp_val == -1
    assert skew_exceeded


def test_tombstone_horizon_and_delete_reinsert():
    ledger = TombstoneHorizonLedger(default_horizon_seconds=60.0)
    key = {"id": 42}
    now = 1000.0

    tb = ledger.record(
        table="orders",
        key=key,
        deleted_at=now,
        generation=1,
        origin_id="node-10-0-0-1",
        horizon_seconds=60.0,
    )
    assert not tb.is_expired(now + 30.0)
    assert tb.is_expired(now + 65.0)

    assert not tb.is_valid_reinsert(new_generation=1, new_ts=now - 5.0)
    assert tb.is_valid_reinsert(new_generation=2, new_ts=now + 5.0)
    assert tb.is_valid_reinsert(new_generation=1, new_ts=now + 5.0)

    assert ledger.count() == 1
    purged = ledger.purge_expired(now=now + 70.0)
    assert purged == 1
    assert ledger.count() == 0


def test_pn_counter_algebra_concurrent_increments():
    c1 = PNCounter()
    c2 = PNCounter()

    c1.increment("node-10-0-0-1", 10.0)
    c1.decrement("node-10-0-0-1", 2.0)
    assert c1.value() == 8.0

    c2.increment("node-10-0-0-2", 15.0)
    c2.decrement("node-10-0-0-2", 5.0)
    assert c2.value() == 10.0

    c_merged = c1.merge(c2)
    assert c_merged.value() == 18.0


def test_add_wins_set_algebra():
    s1 = AddWinsSet()
    s2 = AddWinsSet()

    s1.add("item_a", tag="tag-1")
    s1.add("item_b", tag="tag-2")
    assert s1.elements() == {"item_a", "item_b"}

    s2.add("item_c", tag="tag-3")
    merged = s1.merge(s2)
    assert merged.elements() == {"item_a", "item_b", "item_c"}

    s1.remove("item_a")
    assert s1.elements() == {"item_b"}

    re_merged = s1.merge(s2)
    assert re_merged.elements() == {"item_b", "item_c"}


def test_engine_clean_serial_progression():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "accounts"
    key = {"id": 101}

    local_row = {"id": 101, "balance": 100, "status": "active"}
    incoming_row = {"id": 101, "balance": 150, "status": "active"}
    before = BeforeImage(columns={"id": 101, "balance": 100, "status": "active"})

    resolved_state, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.PASS.value
    assert audit.conflict_type == ConflictType.CLEAN_PROGRESSION.value
    assert audit.resolved is True
    assert resolved_state == incoming_row


def test_engine_cyclic_replication_echo_suppression():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "accounts"
    key = {"id": 101}
    local_row = {"id": 101, "balance": 100}
    incoming_row = {"id": 101, "balance": 100}

    resolved_state, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=BeforeImage(columns={"id": 101, "balance": 90}),
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-1"),
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.conflict_type == ConflictType.CYCLIC_ECHO.value
    assert audit.resolved is True
    assert resolved_state == local_row


def test_engine_out_of_order_delivery_discarded():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "accounts"
    key = {"id": 101}
    local_row = {"id": 101, "balance": 200}
    incoming_row = {"id": 101, "balance": 150}

    local_v = VersionVector({"node-10-0-0-1": 3, "node-10-0-0-2": 2})
    incoming_v = VersionVector({"node-10-0-0-1": 1, "node-10-0-0-2": 2})

    resolved_state, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=BeforeImage(columns={"id": 101, "balance": 100}),
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        local_vector=local_v,
        incoming_vector=incoming_v,
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.conflict_type == ConflictType.OUT_OF_ORDER_DELIVERY.value
    assert audit.resolved is True
    assert resolved_state == local_row


def test_engine_tombstone_stale_update_vs_valid_reinsert():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1", default_tombstone_horizon_seconds=100.0)
    table = "accounts"
    key = {"id": 101}
    now = 1000.0

    _, del_audit = engine.resolve_write(
        table=table,
        key=key,
        operation="DELETE",
        incoming_row=None,
        before_image=BeforeImage(columns={"id": 101, "balance": 200}),
        local_row={"id": 101, "balance": 200},
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        incoming_generation=1,
        now=now,
    )
    assert del_audit.care_badge == CareBadge.PASS.value

    _, stale_audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row={"id": 101, "balance": 210},
        before_image=BeforeImage(columns={"id": 101, "balance": 200}),
        local_row=None,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        incoming_generation=1,
        incoming_hlc=HybridLogicalClock(physical_ts=now - 10.0, origin_id="node-10-0-0-2"),
        now=now + 5.0,
    )
    assert stale_audit.care_badge == CareBadge.BENIGN.value
    assert stale_audit.conflict_type == ConflictType.STALE_UPDATE_ON_TOMBSTONE.value

    reinsert_row, reinsert_audit = engine.resolve_write(
        table=table,
        key=key,
        operation="INSERT",
        incoming_row={"id": 101, "balance": 50},
        before_image=None,
        local_row=None,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        incoming_generation=2,
        incoming_hlc=HybridLogicalClock(physical_ts=now + 10.0, origin_id="node-10-0-0-2"),
        now=now + 10.0,
    )
    assert reinsert_audit.care_badge == CareBadge.PASS.value
    assert reinsert_audit.conflict_type == ConflictType.DELETE_REINSERT.value
    assert reinsert_row == {"id": 101, "balance": 50}


def test_engine_tombstone_expired_flags_action():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1", default_tombstone_horizon_seconds=10.0)
    table = "accounts"
    key = {"id": 101}
    now = 1000.0

    engine.tombstones.record(
        table=table,
        key=key,
        deleted_at=now - 20.0,
        generation=1,
        origin_id="node-10-0-0-1",
        horizon_seconds=10.0,
    )

    _, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row={"id": 101, "balance": 150},
        before_image=BeforeImage(columns={"id": 101, "balance": 100}),
        local_row=None,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        incoming_generation=1,
        now=now,
    )
    assert audit.care_badge == CareBadge.ACTION.value
    assert audit.conflict_type == ConflictType.TOMBSTONE_EXPIRED.value
    assert audit.resolved is False


def test_engine_concurrent_pn_counter_commutative_merge():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "inventory"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "stock": ColumnMergePolicy(
                column_name="stock",
                algebra=MergeAlgebraKind.COMMUTATIVE_PN_COUNTER,
                data_type="int",
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"sku": "SKU-999"}
    before = BeforeImage(columns={"sku": "SKU-999", "stock": 100})
    local_row = {"sku": "SKU-999", "stock": 120}
    incoming_row = {"sku": "SKU-999", "stock": 90}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.conflict_type == ConflictType.COMMUTATIVE_MERGE_SUCCESS.value
    assert audit.resolved is True
    assert resolved["stock"] == 110


def test_engine_noncommutative_write_remains_visible_and_unresolved():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "orders"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "status": ColumnMergePolicy(
                column_name="status",
                algebra=MergeAlgebraKind.NON_COMMUTATIVE_STRICT,
                data_type="text",
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"id": 555}
    before = BeforeImage(columns={"id": 555, "status": "pending"})
    local_row = {"id": 555, "status": "cancelled"}
    incoming_row = {"id": 555, "status": "shipped"}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.ACTION.value
    assert audit.conflict_type == ConflictType.CONCURRENT_NONCOMMUTATIVE.value
    assert audit.resolved is False
    assert "status" in audit.unresolved_columns
    assert resolved == local_row

    unresolved = engine.audit_ledger.unresolved_conflicts(table=table)
    assert len(unresolved) == 1
    assert unresolved[0].audit_id == audit.audit_id


def test_engine_clock_skew_exceeded_flags_action():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "profiles"
    policy = TableConflictPolicy(
        table_name=table,
        clock_skew_tolerance_seconds=2.0,
        columns={
            "bio": ColumnMergePolicy(
                column_name="bio",
                algebra=MergeAlgebraKind.COMMUTATIVE_LWW_REGISTER,
                data_type="text",
                verifiable_clock=True,
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"user_id": 7}
    before = BeforeImage(columns={"user_id": 7, "bio": "initial"})
    local_row = {"user_id": 7, "bio": "local edit"}
    incoming_row = {"user_id": 7, "bio": "remote edit"}

    hlc_local = HybridLogicalClock(physical_ts=100.0, logical_counter=0, origin_id="node-10-0-0-1")
    hlc_incoming = HybridLogicalClock(physical_ts=110.0, logical_counter=0, origin_id="node-10-0-0-2")

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        local_origin=OriginIdentity(origin_id="node-10-0-0-1"),
        incoming_hlc=hlc_incoming,
        local_hlc=hlc_local,
    )

    assert audit.care_badge == CareBadge.ACTION.value
    assert audit.conflict_type == ConflictType.CLOCK_SKEW_ANOMALY.value
    assert audit.resolved is False
    assert "bio" in audit.unresolved_columns


def test_engine_verifiable_lww_within_skew_converges():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "profiles"
    policy = TableConflictPolicy(
        table_name=table,
        clock_skew_tolerance_seconds=5.0,
        columns={
            "bio": ColumnMergePolicy(
                column_name="bio",
                algebra=MergeAlgebraKind.COMMUTATIVE_LWW_REGISTER,
                data_type="text",
                verifiable_clock=True,
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"user_id": 7}
    before = BeforeImage(columns={"user_id": 7, "bio": "initial"})
    local_row = {"user_id": 7, "bio": "local edit"}
    incoming_row = {"user_id": 7, "bio": "remote edit"}

    hlc_local = HybridLogicalClock(physical_ts=100.0, logical_counter=0, origin_id="node-10-0-0-1")
    hlc_incoming = HybridLogicalClock(physical_ts=101.5, logical_counter=0, origin_id="node-10-0-0-2")

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        local_origin=OriginIdentity(origin_id="node-10-0-0-1"),
        incoming_hlc=hlc_incoming,
        local_hlc=hlc_local,
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.conflict_type == ConflictType.COMMUTATIVE_MERGE_SUCCESS.value
    assert audit.resolved is True
    assert resolved["bio"] == "remote edit"


def test_engine_origin_priority_rank_on_clock_tie():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "configs"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "setting": ColumnMergePolicy(
                column_name="setting",
                algebra=MergeAlgebraKind.COMMUTATIVE_LWW_REGISTER,
                data_type="text",
                verifiable_clock=True,
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"key": "timeout"}
    before = BeforeImage(columns={"key": "timeout", "setting": "30s"})
    local_row = {"key": "timeout", "setting": "45s"}
    incoming_row = {"key": "timeout", "setting": "60s"}

    hlc_local = HybridLogicalClock(physical_ts=100.0, logical_counter=0, origin_id="node-10-0-0-1")
    hlc_incoming = HybridLogicalClock(physical_ts=100.0, logical_counter=0, origin_id="node-10-0-0-2")

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2", priority_rank=10),
        local_origin=OriginIdentity(origin_id="node-10-0-0-1", priority_rank=5),
        incoming_hlc=hlc_incoming,
        local_hlc=hlc_local,
    )

    assert audit.resolved is True
    assert resolved["setting"] == "60s"


def test_engine_absent_before_image_flags_action():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "sensitive_data"
    policy = TableConflictPolicy(table_name=table, require_before_image=True)
    engine.register_policy(table, policy)

    key = {"id": 1}
    local_row = {"id": 1, "val": "abc"}
    incoming_row = {"id": 1, "val": "xyz"}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=BeforeImage(columns={}, is_absent=True),
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.ACTION.value
    assert audit.conflict_type == ConflictType.ABSENT_BEFORE_IMAGE.value
    assert audit.resolved is False


def test_engine_type_algebra_mismatch_flags_action():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "metrics"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "rate": ColumnMergePolicy(
                column_name="rate",
                algebra=MergeAlgebraKind.COMMUTATIVE_PN_COUNTER,
                data_type="int",
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"id": 10}
    before = BeforeImage(columns={"id": 10, "rate": "invalid_string_type"})
    local_row = {"id": 10, "rate": "local_str"}
    incoming_row = {"id": 10, "rate": "incoming_str"}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.ACTION.value
    assert audit.conflict_type == ConflictType.TYPE_ALGEBRA_MISMATCH.value
    assert audit.resolved is False


def test_engine_tx_digest_cyclic_echo_suppressed():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "accounts"
    key = {"id": 101}
    local_row = {"id": 101, "balance": 100}
    incoming_row = {"id": 101, "balance": 150}

    digest = "sha256-tx-abcdef123456"
    _, audit1 = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=BeforeImage(columns={"id": 101, "balance": 100}),
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        tx_digest=digest,
    )
    assert audit1.care_badge == CareBadge.PASS.value

    _, audit2 = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=BeforeImage(columns={"id": 101, "balance": 100}),
        local_row=incoming_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
        tx_digest=digest,
    )
    assert audit2.care_badge == CareBadge.BENIGN.value
    assert audit2.conflict_type == ConflictType.CYCLIC_ECHO.value


def test_engine_g_counter_and_min_max_lattice():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "stats"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "peak_traffic": ColumnMergePolicy(
                column_name="peak_traffic",
                algebra=MergeAlgebraKind.COMMUTATIVE_MAX,
                data_type="int",
            ),
            "latency_floor": ColumnMergePolicy(
                column_name="latency_floor",
                algebra=MergeAlgebraKind.COMMUTATIVE_MIN,
                data_type="float",
            ),
        },
    )
    engine.register_policy(table, policy)

    key = {"service": "gateway"}
    before = BeforeImage(columns={"service": "gateway", "peak_traffic": 500, "latency_floor": 20.0})
    local_row = {"service": "gateway", "peak_traffic": 750, "latency_floor": 15.0}
    incoming_row = {"service": "gateway", "peak_traffic": 600, "latency_floor": 12.0}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.resolved is True
    assert resolved["peak_traffic"] == 750
    assert resolved["latency_floor"] == 12.0


def test_engine_set_union_merge():
    engine = ConflictSemanticsEngine(node_id="node-10-0-0-1")
    table = "articles"
    policy = TableConflictPolicy(
        table_name=table,
        columns={
            "tags": ColumnMergePolicy(
                column_name="tags",
                algebra=MergeAlgebraKind.COMMUTATIVE_SET_UNION,
                data_type="array",
            )
        },
    )
    engine.register_policy(table, policy)

    key = {"slug": "release-v2"}
    before = BeforeImage(columns={"slug": "release-v2", "tags": ["db", "infra"]})
    local_row = {"slug": "release-v2", "tags": ["db", "infra", "pg"]}
    incoming_row = {"slug": "release-v2", "tags": ["db", "infra", "mysql"]}

    resolved, audit = engine.resolve_write(
        table=table,
        key=key,
        operation="UPDATE",
        incoming_row=incoming_row,
        before_image=before,
        local_row=local_row,
        incoming_origin=OriginIdentity(origin_id="node-10-0-0-2"),
    )

    assert audit.care_badge == CareBadge.BENIGN.value
    assert audit.resolved is True
    assert resolved["tags"] == ["db", "infra", "mysql", "pg"]


def test_audit_ledger_counts_by_badge():
    ledger = ConflictAuditLedger()
    ledger.record(ConflictAuditRecord(audit_id="1", table="t", key={}, conflict_type="c", care_badge=CareBadge.ACTION.value, resolved=False))
    ledger.record(ConflictAuditRecord(audit_id="2", table="t", key={}, conflict_type="c", care_badge=CareBadge.REVIEW.value, resolved=True))
    ledger.record(ConflictAuditRecord(audit_id="3", table="t", key={}, conflict_type="c", care_badge=CareBadge.BENIGN.value, resolved=True))
    ledger.record(ConflictAuditRecord(audit_id="4", table="t", key={}, conflict_type="c", care_badge=CareBadge.PASS.value, resolved=True))

    counts = ledger.counts_by_badge()
    assert counts[CareBadge.ACTION.value] == 1
    assert counts[CareBadge.REVIEW.value] == 1
    assert counts[CareBadge.BENIGN.value] == 1
    assert counts[CareBadge.PASS.value] == 1


def test_pure_ascii_and_unix_formatting():
    with open("migkit/conflict_semantics.py", "rb") as f:
        content = f.read()
        content.decode("ascii")

    with open("tests/test_conflict_rules_data_semantics.py", "rb") as f:
        content = f.read()
        content.decode("ascii")
