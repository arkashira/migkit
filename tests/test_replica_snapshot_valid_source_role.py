"""Tests for replica snapshot and position validity under source roles (Row 623: RCH15).

Proves that:
1. Primary and replica roles, lineage, portable positions (LSN/GTID), and slot failover are discovered.
2. Assessment, snapshot, CDC, and verification workloads read only where data loss is prevented.
3. Replica failover and lag are incorporated into proof freshness and cost, not treated as blanket source-load optimization.
4. Replica lag and primary failover between snapshot and tail cannot skip changes or compare incompatible timelines.
5. Dynamic role changes invalidate affected decisions immediately.
6. Pure UNIX reporting with care badges (PASS, BENIGN, REVIEW, ACTION).
7. Durable O(1) typed resume and active mutual exclusion lease.
"""

from __future__ import annotations

import copy
import os
import tempfile
import time
from pathlib import Path

import pytest

from migkit.source_role import (
    CareBadge,
    ChangeSkipError,
    CostModel,
    EngineDialect,
    FreshnessModel,
    InvalidatedDecisionError,
    NodeEndpoint,
    NodeRole,
    PortablePosition,
    ReplayState,
    SlotFailoverStatus,
    SourceRoleCheckpoint,
    SourceRoleCoordinator,
    SourceRoleLease,
    SourceRolePolicy,
    TimelineBranch,
    TimelineDivergenceError,
    TopologySnapshot,
    WorkloadRoutingDecision,
    WorkloadStage,
    format_lsn_str,
    gtid_total_tx_count,
    is_gtid_subset,
    parse_gtid_intervals,
    parse_lsn_int,
)

PROVES = [("replica-snapshot.valid-source-role", "postgres"), ("replica-snapshot.valid-source-role", "mysql")]


# -----------------------------------------------------------------------------
# Test Fixtures and Topology Helpers
# -----------------------------------------------------------------------------

@pytest.fixture
def base_policy() -> SourceRolePolicy:
    return SourceRolePolicy(
        max_assessment_lag_sec=60.0,
        max_snapshot_lag_sec=30.0,
        max_snapshot_lag_bytes=64 * 1024 * 1024,
        max_cdc_lag_sec=5.0,
        max_cdc_lag_bytes=10 * 1024 * 1024,
        max_verification_lag_sec=15.0,
        verification_freshness_sla_sec=20.0,
        require_slot_failover_for_replica_cdc=True,
        require_snapshot_export_for_replica_snapshot=True,
        require_watermark_fencing_for_verification=True,
    )


@pytest.fixture
def pg_cluster(base_policy: SourceRolePolicy) -> SourceRoleCoordinator:
    coord = SourceRoleCoordinator(cluster_id="pg-prod-cluster-01", policy=base_policy)
    # Primary node
    primary = NodeEndpoint(
        node_id="pg-node-01",
        host="127.0.0.1",
        port=5432,
        role=NodeRole.PRIMARY,
        engine=EngineDialect.POSTGRESQL,
        cluster_id="pg-prod-cluster-01",
        timeline_id=1,
        is_read_only=False,
        current_position=PortablePosition.from_postgres("0/2000000", timeline_id=1),
        replay_position=PortablePosition.from_postgres("0/2000000", timeline_id=1),
        replay_lag_bytes=0,
        replay_lag_seconds=0.0,
        slot_failover_status=SlotFailoverStatus.ENABLED,
        supports_replica_cdc=True,
        supports_snapshot_export=True,
    )
    # Synced replica with slot failover enabled
    replica_synced = NodeEndpoint(
        node_id="pg-node-02",
        host="10.0.0.21",
        port=5432,
        role=NodeRole.REPLICA,
        engine=EngineDialect.POSTGRESQL,
        cluster_id="pg-prod-cluster-01",
        timeline_id=1,
        upstream_node_id="pg-node-01",
        is_read_only=True,
        current_position=PortablePosition.from_postgres("0/2000000", timeline_id=1),
        replay_position=PortablePosition.from_postgres("0/1FFFF00", timeline_id=1),
        replay_lag_bytes=256,
        replay_lag_seconds=0.1,
        slot_failover_status=SlotFailoverStatus.ENABLED,
        supports_replica_cdc=True,
        supports_snapshot_export=True,
    )
    # Lagging replica with slot failover disabled
    replica_lagging = NodeEndpoint(
        node_id="pg-node-03",
        host="10.0.0.22",
        port=5432,
        role=NodeRole.REPLICA,
        engine=EngineDialect.POSTGRESQL,
        cluster_id="pg-prod-cluster-01",
        timeline_id=1,
        upstream_node_id="pg-node-01",
        is_read_only=True,
        current_position=PortablePosition.from_postgres("0/1A00000", timeline_id=1),
        replay_position=PortablePosition.from_postgres("0/1A00000", timeline_id=1),
        replay_lag_bytes=10 * 1024 * 1024,
        replay_lag_seconds=45.0,
        slot_failover_status=SlotFailoverStatus.DISABLED,
        supports_replica_cdc=False,
        supports_snapshot_export=True,
    )
    coord.register_node(primary)
    coord.register_node(replica_synced)
    coord.register_node(replica_lagging)
    return coord


@pytest.fixture
def mysql_cluster(base_policy: SourceRolePolicy) -> SourceRoleCoordinator:
    coord = SourceRoleCoordinator(cluster_id="mysql-cluster-01", policy=base_policy)
    primary = NodeEndpoint(
        node_id="mysql-node-01",
        host="127.0.0.1",
        port=3306,
        role=NodeRole.PRIMARY,
        engine=EngineDialect.MYSQL,
        cluster_id="mysql-cluster-01",
        timeline_id=1,
        is_read_only=False,
        current_position=PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-100"),
        replay_position=PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-100"),
        replay_lag_bytes=0,
        replay_lag_seconds=0.0,
        slot_failover_status=SlotFailoverStatus.UNSUPPORTED,
        supports_replica_cdc=False,
        supports_snapshot_export=True,
    )
    replica = NodeEndpoint(
        node_id="mysql-node-02",
        host="10.0.0.32",
        port=3306,
        role=NodeRole.REPLICA,
        engine=EngineDialect.MYSQL,
        cluster_id="mysql-cluster-01",
        timeline_id=1,
        upstream_node_id="mysql-node-01",
        is_read_only=True,
        current_position=PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-98"),
        replay_position=PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-98"),
        replay_lag_bytes=2,
        replay_lag_seconds=1.2,
        slot_failover_status=SlotFailoverStatus.UNSUPPORTED,
        supports_replica_cdc=False,
        supports_snapshot_export=True,
    )
    coord.register_node(primary)
    coord.register_node(replica)
    return coord


# -----------------------------------------------------------------------------
# Unit Tests: Portable Positions & Timeline
# -----------------------------------------------------------------------------

def test_portable_position_postgres_lsn():
    p1 = PortablePosition.from_postgres("0/16B3748", timeline_id=1)
    p2 = PortablePosition.from_postgres("0/16B3800", timeline_id=1)
    p3 = PortablePosition.from_postgres("0/16B3748", timeline_id=2)

    assert p1.byte_offset == parse_lsn_int("0/16B3748")
    assert p1 < p2
    assert p2 > p1
    assert p1 == PortablePosition.from_postgres("0/16B3748", timeline_id=1)
    assert p1.distance_to(p2) == 0x800 - 0x748
    assert p1.is_compatible_timeline(p2) is True
    assert p1.is_compatible_timeline(p3) is False

    with pytest.raises(TimelineDivergenceError):
        _ = p1.distance_to(p3)


def test_portable_position_mysql_gtid():
    gtid_str = "3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-10:15-20"
    parsed = parse_gtid_intervals(gtid_str)
    assert "3e11fa47-71ca-11e1-9e33-c80aa9e2953a" in parsed
    assert gtid_total_tx_count(parsed) == 16  # 1-10 (10) + 15-20 (6) = 16

    p1 = PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-10")
    p2 = PortablePosition.from_mysql(gtid_set="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-20")

    assert p1 < p2
    assert p1.distance_to(p2) == 10
    assert is_gtid_subset(p1.gtid_intervals, p2.gtid_intervals) is True
    assert is_gtid_subset(p2.gtid_intervals, p1.gtid_intervals) is False


# -----------------------------------------------------------------------------
# Workload Routing: Assessment
# -----------------------------------------------------------------------------

def test_assessment_routing_prefers_healthy_replica(pg_cluster: SourceRoleCoordinator):
    decision = pg_cluster.evaluate_assessment(preferred_node_id="pg-node-02")
    assert decision.stage == WorkloadStage.ASSESSMENT
    assert decision.target_node_id == "pg-node-02"
    assert decision.badge in (CareBadge.PASS, CareBadge.BENIGN)
    assert decision.allowed is True


def test_assessment_routing_falls_back_when_replica_too_lagged(pg_cluster: SourceRoleCoordinator):
    decision = pg_cluster.evaluate_assessment(preferred_node_id="pg-node-03")
    # pg-node-03 lag is 45s, but policy max is 60s -> still benign
    assert decision.allowed is True

    # Increase lag on pg-node-03 past 60s
    pg_cluster.topology.nodes["pg-node-03"].replay_lag_seconds = 75.0
    decision2 = pg_cluster.evaluate_assessment(preferred_node_id="pg-node-03")
    assert decision2.target_node_id == "pg-node-01"  # routed to primary
    assert decision2.badge == CareBadge.REVIEW
    assert "exceeds max" in decision2.reason


# -----------------------------------------------------------------------------
# Workload Routing: Snapshot
# -----------------------------------------------------------------------------

def test_snapshot_allowed_on_synced_replica(pg_cluster: SourceRoleCoordinator):
    decision = pg_cluster.evaluate_snapshot(preferred_node_id="pg-node-02")
    assert decision.stage == WorkloadStage.SNAPSHOT
    assert decision.target_node_id == "pg-node-02"
    assert decision.badge in (CareBadge.PASS, CareBadge.BENIGN)
    assert decision.allowed is True
    assert decision.snapshot_position is not None
    assert decision.cost_model is not None


def test_snapshot_rejected_on_lagging_replica(pg_cluster: SourceRoleCoordinator):
    # pg-node-03 lag is 45s, max snapshot lag is 30s
    decision = pg_cluster.evaluate_snapshot(preferred_node_id="pg-node-03")
    assert decision.target_node_id == "pg-node-01"  # fallback to primary
    assert decision.badge == CareBadge.REVIEW
    assert "exceeds safe snapshot threshold" in decision.reason


def test_snapshot_rejected_on_replica_with_divergent_timeline(pg_cluster: SourceRoleCoordinator):
    # Alter timeline of pg-node-02 to simulate divergence
    pg_cluster.topology.nodes["pg-node-02"].timeline_id = 2
    decision = pg_cluster.evaluate_snapshot(preferred_node_id="pg-node-02")
    assert decision.target_node_id == "pg-node-01"
    assert decision.badge == CareBadge.ACTION
    assert "diverged" in decision.reason


def test_snapshot_rejected_without_snapshot_export_capability(pg_cluster: SourceRoleCoordinator):
    pg_cluster.topology.nodes["pg-node-02"].supports_snapshot_export = False
    decision = pg_cluster.evaluate_snapshot(preferred_node_id="pg-node-02")
    assert decision.target_node_id == "pg-node-01"
    assert decision.badge == CareBadge.ACTION
    assert "lacks verified snapshot export capability" in decision.reason


# -----------------------------------------------------------------------------
# Workload Routing: CDC Streaming
# -----------------------------------------------------------------------------

def test_cdc_allowed_on_replica_with_slot_failover(pg_cluster: SourceRoleCoordinator):
    decision = pg_cluster.evaluate_cdc(preferred_node_id="pg-node-02")
    assert decision.stage == WorkloadStage.CDC
    assert decision.target_node_id == "pg-node-02"
    assert decision.badge == CareBadge.PASS
    assert decision.allowed is True
    assert "slot failover enabled" in decision.reason


def test_cdc_blocked_on_replica_without_slot_failover(pg_cluster: SourceRoleCoordinator):
    decision = pg_cluster.evaluate_cdc(preferred_node_id="pg-node-03")
    # pg-node-03 has supports_replica_cdc=False and slot_failover_status=DISABLED
    assert decision.target_node_id == "pg-node-01"
    assert decision.badge == CareBadge.ACTION
    assert "does not support replica CDC" in decision.reason or "slot failover" in decision.reason


def test_cdc_blocked_on_mysql_replica(mysql_cluster: SourceRoleCoordinator):
    decision = mysql_cluster.evaluate_cdc(preferred_node_id="mysql-node-02")
    # MySQL replica CDC without failover slot is disallowed
    assert decision.target_node_id == "mysql-node-01"
    assert decision.badge == CareBadge.ACTION


def test_cdc_blocked_when_replica_lag_exceeds_threshold(pg_cluster: SourceRoleCoordinator):
    # Set lag above max_cdc_lag_sec (5.0s)
    pg_cluster.topology.nodes["pg-node-02"].replay_lag_seconds = 7.5
    decision = pg_cluster.evaluate_cdc(preferred_node_id="pg-node-02")
    assert decision.target_node_id == "pg-node-01"
    assert decision.badge == CareBadge.REVIEW
    assert "exceeds safe threshold" in decision.reason


# -----------------------------------------------------------------------------
# Workload Routing: Verification & Proof Freshness/Cost
# -----------------------------------------------------------------------------

def test_verification_watermark_fencing_prevents_false_divergence(pg_cluster: SourceRoleCoordinator):
    # Target has applied up to LSN 0/1FFFF50
    target_wm = PortablePosition.from_postgres("0/1FFFF50", timeline_id=1)
    # Replica pg-node-02 replay position is only at 0/1FFFF00 (< target_wm)
    decision = pg_cluster.evaluate_verification(
        target_watermark=target_wm,
        preferred_node_id="pg-node-02",
    )
    assert decision.target_node_id == "pg-node-01"  # Must route to primary
    assert decision.badge == CareBadge.ACTION
    assert "has not replayed up to target watermark" in decision.reason


def test_verification_allowed_when_replica_caught_up_to_watermark(pg_cluster: SourceRoleCoordinator):
    # Replica has replayed beyond target watermark
    target_wm = PortablePosition.from_postgres("0/1FF0000", timeline_id=1)
    decision = pg_cluster.evaluate_verification(
        target_watermark=target_wm,
        preferred_node_id="pg-node-02",
    )
    assert decision.target_node_id == "pg-node-02"
    assert decision.allowed is True
    assert decision.badge in (CareBadge.PASS, CareBadge.BENIGN)
    assert decision.freshness_model is not None
    assert decision.freshness_model.is_fresh is True


def test_verification_freshness_sla_enforcement(pg_cluster: SourceRoleCoordinator):
    # Verification SLA is 20s. Set replica lag to 18s + 5s query = 23s (> 20s)
    pg_cluster.topology.nodes["pg-node-02"].replay_lag_seconds = 18.0
    decision = pg_cluster.evaluate_verification(
        preferred_node_id="pg-node-02",
        query_duration_estimate_sec=5.0,
    )
    assert decision.target_node_id == "pg-node-01"
    assert decision.badge == CareBadge.REVIEW
    assert "freshness age" in decision.reason


def test_cost_model_avoids_blanket_source_load_optimization():
    # Primary has low CPU (20%), replica has high lag (10s) and no slot failover
    cost = CostModel.evaluate(
        primary_cpu_pct=20.0,
        replica_lag_seconds=10.0,
        slot_failover_status=SlotFailoverStatus.DISABLED,
    )
    assert cost.is_replica_cost_effective is False
    assert cost.total_replica_cost > cost.total_primary_cost

    # Primary has high CPU (85%), replica has zero lag and slot failover enabled
    cost_hot = CostModel.evaluate(
        primary_cpu_pct=85.0,
        replica_lag_seconds=0.0,
        slot_failover_status=SlotFailoverStatus.ENABLED,
    )
    assert cost_hot.is_replica_cost_effective is True
    assert cost_hot.total_replica_cost < cost_hot.total_primary_cost


# -----------------------------------------------------------------------------
# Failover, Timeline Divergence & Change-Skip Verification
# -----------------------------------------------------------------------------

def test_dynamic_role_change_invalidates_in_flight_decisions(pg_cluster: SourceRoleCoordinator):
    dec = pg_cluster.evaluate_snapshot(preferred_node_id="pg-node-02")
    assert dec.allowed is True
    assert not dec.invalidated
    dec.check_valid()

    # Demote pg-node-02 to demoted replica
    invalidated = pg_cluster.notify_role_change("pg-node-02", NodeRole.DEMOTED_REPLICA)
    assert dec.decision_id in invalidated
    assert dec.invalidated is True
    assert dec.allowed is False
    assert dec.badge == CareBadge.ACTION

    with pytest.raises(InvalidatedDecisionError):
        dec.check_valid()


def test_cluster_failover_invalidates_all_active_decisions(pg_cluster: SourceRoleCoordinator):
    dec1 = pg_cluster.evaluate_assessment()
    dec2 = pg_cluster.evaluate_snapshot()
    dec3 = pg_cluster.evaluate_cdc()

    assert not dec1.invalidated and not dec2.invalidated and not dec3.invalidated

    # Failover: promote pg-node-02, new timeline 2
    switch_lsn = PortablePosition.from_postgres("0/2100000", timeline_id=1)
    invalidated = pg_cluster.notify_failover(
        new_primary_id="pg-node-02",
        old_primary_id="pg-node-01",
        new_timeline_id=2,
        switch_position=switch_lsn,
    )
    assert dec1.decision_id in invalidated
    assert dec2.decision_id in invalidated
    assert dec3.decision_id in invalidated
    assert pg_cluster.topology.primary_node_id == "pg-node-02"


def test_snapshot_to_tail_continuity_detects_change_skip(pg_cluster: SourceRoleCoordinator):
    snap_pos = PortablePosition.from_postgres("0/2000000", timeline_id=1)
    # Tail start is ahead of snapshot end (e.g. 0/2000100) -> change skip!
    tail_ahead = PortablePosition.from_postgres("0/2000100", timeline_id=1)

    is_cont, badge, msg = pg_cluster.check_snapshot_to_tail_continuity(
        snapshot_position=snap_pos,
        tail_start_position=tail_ahead,
    )
    assert is_cont is False
    assert badge == CareBadge.ACTION
    assert "change skip detected" in msg


def test_snapshot_to_tail_continuity_passes_with_safe_overlap(pg_cluster: SourceRoleCoordinator):
    snap_pos = PortablePosition.from_postgres("0/2000000", timeline_id=1)
    # Tail start is before or equal to snapshot end (e.g. 0/1FFFF00)
    tail_safe = PortablePosition.from_postgres("0/1FFFF00", timeline_id=1)

    is_cont, badge, msg = pg_cluster.check_snapshot_to_tail_continuity(
        snapshot_position=snap_pos,
        tail_start_position=tail_safe,
    )
    assert is_cont is True
    assert badge == CareBadge.PASS
    assert "safe overlap" in msg


def test_snapshot_to_tail_continuity_detects_incompatible_timeline(pg_cluster: SourceRoleCoordinator):
    snap_pos = PortablePosition.from_postgres("0/2000000", timeline_id=1)
    tail_diff_tl = PortablePosition.from_postgres("0/2000000", timeline_id=2)

    is_cont, badge, msg = pg_cluster.check_snapshot_to_tail_continuity(
        snapshot_position=snap_pos,
        tail_start_position=tail_diff_tl,
    )
    assert is_cont is False
    assert badge == CareBadge.ACTION
    assert "incompatible timeline" in msg


# -----------------------------------------------------------------------------
# UNIX Reporting & Format Quality
# -----------------------------------------------------------------------------

def test_unix_report_formatting(pg_cluster: SourceRoleCoordinator):
    pg_cluster.evaluate_assessment()
    pg_cluster.evaluate_snapshot()
    pg_cluster.evaluate_cdc()
    report = pg_cluster.format_unix_report()

    assert "CLUSTER REPLICATION TOPOLOGY" in report
    assert "pg-node-01" in report
    assert "pg-node-02" in report
    assert "[PASS]" in report or "[ACTION]" in report or "[BENIGN]" in report
    # Pure ASCII check, zero non-ASCII emojis
    assert report.isascii() is True


# -----------------------------------------------------------------------------
# Durable Checkpoint & Lease Lock
# -----------------------------------------------------------------------------

def test_durable_checkpoint_save_and_resume(pg_cluster: SourceRoleCoordinator, tmp_path: Path):
    pg_cluster.evaluate_assessment()
    pg_cluster.evaluate_snapshot()
    cp_file = tmp_path / "source_role_state.json"

    SourceRoleCheckpoint.save(pg_cluster, cp_file)
    assert cp_file.exists()

    resumed = SourceRoleCheckpoint.load(cp_file)
    assert resumed.cluster_id == pg_cluster.cluster_id
    assert resumed.topology.generation == pg_cluster.topology.generation
    assert len(resumed.topology.nodes) == len(pg_cluster.topology.nodes)
    assert len(resumed.active_decisions) == len(pg_cluster.active_decisions)

    # Check that position data is preserved intact
    p1 = resumed.topology.nodes["pg-node-01"].current_position
    assert p1 is not None
    assert p1.raw_position == "0/2000000"


def test_source_role_lease_lock(tmp_path: Path):
    lease1 = SourceRoleLease(lock_dir=tmp_path / "locks", term_seconds=10.0)
    lease2 = SourceRoleLease(lock_dir=tmp_path / "locks", term_seconds=10.0)

    assert lease1.acquire() is True
    assert lease1.is_held() is True
    # Second concurrent acquire must fail
    assert lease2.acquire() is False
    assert lease2.is_held() is False

    lease1.release()
    assert lease1.is_held() is False

    # Now second acquire succeeds
    assert lease2.acquire() is True
    lease2.release()
