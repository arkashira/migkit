"""Tests for source role discovery probes, decide rungs, and durable resume (Row 623: RCH15).

Proves that:
1. Real role discovery probes accurately detect PostgreSQL primary/standby roles (pg_is_in_recovery).
2. Real role discovery probes accurately detect MySQL replica/primary roles (SHOW REPLICA/SLAVE STATUS, @@read_only).
3. Failed role discovery probes raise unswallowed SourceRoleError.
4. Replica role admission climbs replica_role_rungs decide ladder across all branches.
5. Checkpointing leverages migkit.durable with atomic serialization and Torn exception handling.
"""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from migkit.durable import Torn
from migkit.source_role import (
    CareBadge,
    EngineDialect,
    NodeRole,
    ReplayState,
    SlotFailoverStatus,
    SourceRoleCheckpoint,
    SourceRoleCoordinator,
    SourceRoleError,
    SourceRolePolicy,
    WorkloadStage,
    decide_replica_admission,
    discover_node_endpoint,
    probe_mysql_role,
    probe_postgres_role,
    replica_role_rungs,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("source_role", "replica_role_rungs", "admit_synced_replica", "holds"),
    ("source_role", "replica_role_rungs", "admit_synced_replica", "refuses"),
    ("source_role", "replica_role_rungs", "admit_degraded_replica_review", "holds"),
    ("source_role", "replica_role_rungs", "admit_degraded_replica_review", "refuses"),
    ("source_role", "replica_role_rungs", "route_primary_authoritative", "holds"),
]


# -----------------------------------------------------------------------------
# Zero-Mock Fake Query Executors
# -----------------------------------------------------------------------------

def make_pg_executor(
    in_recovery: bool = False,
    lsn: str = "0/2000000",
    lag_seconds: float = 0.0,
    timeline_id: int = 1,
    paused: bool = False,
    fail_recovery: bool = False,
):
    def query(sql: str):
        if fail_recovery:
            raise RuntimeError("Database connection refused")
        q = " ".join(sql.strip().split()).lower()
        if "pg_is_in_recovery()" in q:
            return [(in_recovery,)]
        if "timeline_id from pg_control_checkpoint" in q:
            return [(timeline_id,)]
        if "pg_current_wal_lsn()" in q or "pg_current_xlog_location()" in q:
            return [(lsn,)]
        if "pg_last_wal_replay_lsn()" in q or "pg_last_xlog_replay_location()" in q:
            return [(lsn,)]
        if "pg_last_xact_replay_timestamp()" in q:
            return [(lag_seconds,)]
        if "pg_is_wal_replay_paused()" in q:
            return [(paused,)]
        raise ValueError(f"Unhandled fake PG query: {sql}")

    return query


def make_mysql_executor(
    is_replica: bool = True,
    use_modern_syntax: bool = True,
    sql_running: str = "Yes",
    io_running: str = "Yes",
    seconds_behind: int = 0,
    gtid: str = "3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-20",
    read_only: bool = False,
    fail_all: bool = False,
):
    def query(sql: str):
        if fail_all:
            raise RuntimeError("MySQL connection timed out")
        q = " ".join(sql.strip().split()).upper()
        if "SHOW REPLICA STATUS" in q:
            if not use_modern_syntax:
                raise RuntimeError("You have an error in your SQL syntax near 'REPLICA STATUS'")
            if not is_replica:
                return []
            return [{
                "Replica_SQL_Running": sql_running,
                "Replica_IO_Running": io_running,
                "Seconds_Behind_Source": str(seconds_behind),
                "Executed_Gtid_Set": gtid,
            }]
        if "SHOW SLAVE STATUS" in q:
            if not is_replica:
                return []
            return [{
                "Slave_SQL_Running": sql_running,
                "Slave_IO_Running": io_running,
                "Seconds_Behind_Master": str(seconds_behind),
                "Executed_Gtid_Set": gtid,
            }]
        if "SELECT @@READ_ONLY" in q:
            return [(1 if read_only else 0,)]
        if "SELECT @@GTID_EXECUTED" in q:
            return [(gtid,)]
        raise ValueError(f"Unhandled fake MySQL query: {sql}")

    return query


# -----------------------------------------------------------------------------
# Decide Ladder Branch Coverage
# -----------------------------------------------------------------------------

def test_replica_role_decide_rungs_coverage():
    with watched() as seen:
        # 1. Healthy synced replica -> admit_synced_replica holds
        c1 = decide_replica_admission({
            "is_replica": True,
            "timeline_aligned": True,
            "replay_lag_seconds": 0.0,
            "replay_lag_bytes": 0,
            "max_lag_seconds": 30.0,
            "max_lag_bytes": 64 * 1024 * 1024,
            "max_review_lag_seconds": 60.0,
            "capability_supported": True,
        })
        assert c1.path == "admit_synced_replica"

        # 2. Lag exceeds SLA but within review threshold -> admit_synced_replica refuses, admit_degraded_replica_review holds
        c2 = decide_replica_admission({
            "is_replica": True,
            "timeline_aligned": True,
            "replay_lag_seconds": 45.0,
            "replay_lag_bytes": 1024,
            "max_lag_seconds": 30.0,
            "max_lag_bytes": 64 * 1024 * 1024,
            "max_review_lag_seconds": 60.0,
            "capability_supported": True,
        })
        assert c2.path == "admit_degraded_replica_review"

        # 3. Lag exceeds review threshold -> both refuse, route_primary_authoritative holds
        c3 = decide_replica_admission({
            "is_replica": True,
            "timeline_aligned": True,
            "replay_lag_seconds": 120.0,
            "replay_lag_bytes": 1024,
            "max_lag_seconds": 30.0,
            "max_lag_bytes": 64 * 1024 * 1024,
            "max_review_lag_seconds": 60.0,
            "capability_supported": True,
        })
        assert c3.path == "route_primary_authoritative"

        # 4. Timeline diverged -> both refuse, route_primary_authoritative holds
        c4 = decide_replica_admission({
            "is_replica": True,
            "timeline_aligned": False,
            "replay_lag_seconds": 0.0,
            "replay_lag_bytes": 0,
            "max_lag_seconds": 30.0,
            "max_lag_bytes": 64 * 1024 * 1024,
            "max_review_lag_seconds": 60.0,
            "capability_supported": True,
        })
        assert c4.path == "route_primary_authoritative"

    assert set(branch(seen, "admit_synced_replica")) == {HOLDS, REFUSES}
    assert set(branch(seen, "admit_degraded_replica_review")) == {HOLDS, REFUSES}
    assert branch(seen, "route_primary_authoritative") == [HOLDS]


# -----------------------------------------------------------------------------
# PostgreSQL Real Role Discovery Probes
# -----------------------------------------------------------------------------

def test_probe_postgres_role_primary():
    fn = make_pg_executor(in_recovery=False, lsn="0/3000000", timeline_id=2)
    info = probe_postgres_role(fn)
    assert info["role"] == NodeRole.PRIMARY
    assert info["is_read_only"] is False
    assert info["timeline_id"] == 2
    assert info["current_position"] is not None
    assert info["current_position"].raw_position == "0/3000000"
    assert info["replay_position"] is None


def test_probe_postgres_role_replica_streaming():
    fn = make_pg_executor(in_recovery=True, lsn="0/2500000", lag_seconds=4.2, timeline_id=2, paused=False)
    info = probe_postgres_role(fn)
    assert info["role"] == NodeRole.REPLICA
    assert info["is_read_only"] is True
    assert info["timeline_id"] == 2
    assert info["current_position"] is not None
    assert info["current_position"].raw_position == "0/2500000"
    assert info["replay_position"] is not None
    assert info["replay_lag_seconds"] == 4.2
    assert info["replay_state"] == ReplayState.STREAMING


def test_probe_postgres_role_replica_paused():
    fn = make_pg_executor(in_recovery=True, lsn="0/2500000", lag_seconds=12.0, timeline_id=1, paused=True)
    info = probe_postgres_role(fn)
    assert info["role"] == NodeRole.REPLICA
    assert info["replay_state"] == ReplayState.PAUSED


def test_probe_postgres_role_unswallowed_error():
    fn = make_pg_executor(fail_recovery=True)
    with pytest.raises(SourceRoleError) as excinfo:
        probe_postgres_role(fn)
    assert "PostgreSQL role probe failed" in str(excinfo.value)


# -----------------------------------------------------------------------------
# MySQL Real Role Discovery Probes
# -----------------------------------------------------------------------------

def test_probe_mysql_role_replica_modern():
    fn = make_mysql_executor(is_replica=True, use_modern_syntax=True, seconds_behind=5)
    info = probe_mysql_role(fn)
    assert info["role"] == NodeRole.REPLICA
    assert info["is_read_only"] is True
    assert info["replay_lag_seconds"] == 5.0
    assert info["replay_state"] == ReplayState.STREAMING
    assert info["current_position"] is not None


def test_probe_mysql_role_replica_legacy_fallback():
    fn = make_mysql_executor(is_replica=True, use_modern_syntax=False, seconds_behind=10)
    info = probe_mysql_role(fn)
    assert info["role"] == NodeRole.REPLICA
    assert info["is_read_only"] is True
    assert info["replay_lag_seconds"] == 10.0


def test_probe_mysql_role_primary():
    fn = make_mysql_executor(is_replica=False, read_only=False, gtid="3e11fa47-71ca-11e1-9e33-c80aa9e2953a:1-50")
    info = probe_mysql_role(fn)
    assert info["role"] == NodeRole.PRIMARY
    assert info["is_read_only"] is False
    assert info["replay_lag_seconds"] == 0.0
    assert info["current_position"] is not None


def test_probe_mysql_role_standby_readonly():
    fn = make_mysql_executor(is_replica=False, read_only=True)
    info = probe_mysql_role(fn)
    assert info["role"] == NodeRole.STANDBY
    assert info["is_read_only"] is True


def test_probe_mysql_role_unswallowed_error():
    fn = make_mysql_executor(fail_all=True)
    with pytest.raises(SourceRoleError) as excinfo:
        probe_mysql_role(fn)
    assert "MySQL replica status probe failed" in str(excinfo.value)


# -----------------------------------------------------------------------------
# Endpoint Discovery & Coordinator Integration
# -----------------------------------------------------------------------------

def test_discover_node_endpoint_integration():
    fn = make_pg_executor(in_recovery=True, lsn="0/1000000", lag_seconds=2.0)
    endpoint = discover_node_endpoint(
        node_id="pg-rep-01",
        host="10.0.0.12",
        port=5432,
        engine=EngineDialect.POSTGRESQL,
        query_fn=fn,
        cluster_id="prod-cluster",
        supports_snapshot_export=True,
    )
    assert endpoint.node_id == "pg-rep-01"
    assert endpoint.role == NodeRole.REPLICA
    assert endpoint.is_replica() is True
    assert endpoint.replay_lag_seconds == 2.0


def test_coordinator_discover_and_register_node():
    coord = SourceRoleCoordinator(cluster_id="pg-cluster")
    fn_pri = make_pg_executor(in_recovery=False, lsn="0/2000000")
    fn_rep = make_pg_executor(in_recovery=True, lsn="0/1900000", lag_seconds=1.0)

    p_node = coord.discover_and_register_node("node-pri", "127.0.0.1", 5432, EngineDialect.POSTGRESQL, fn_pri)
    r_node = coord.discover_and_register_node("node-rep", "10.0.0.2", 5432, EngineDialect.POSTGRESQL, fn_rep)

    assert coord.topology.get_primary() == p_node
    assert coord.topology.get_replicas() == [r_node]

    # Admission via decide ladder
    rung_path, badge, reason = coord.admit_replica_role(r_node, WorkloadStage.SNAPSHOT)
    assert rung_path == "admit_synced_replica"
    assert badge in (CareBadge.PASS, CareBadge.BENIGN)


# -----------------------------------------------------------------------------
# Durable Checkpoint Reconciliation
# -----------------------------------------------------------------------------

def test_source_role_checkpoint_durable_reconciled(tmp_path: Path):
    coord = SourceRoleCoordinator(cluster_id="test-cluster")
    fn = make_pg_executor(in_recovery=False, lsn="0/1000000")
    coord.discover_and_register_node("p1", "127.0.0.1", 5432, EngineDialect.POSTGRESQL, fn)
    coord.evaluate_snapshot()

    cp_file = tmp_path / "checkpoint.json"
    SourceRoleCheckpoint.save(coord, cp_file)
    assert cp_file.exists()

    resumed = SourceRoleCheckpoint.load(cp_file)
    assert resumed.cluster_id == "test-cluster"
    assert len(resumed.topology.nodes) == 1
    assert "p1" in resumed.topology.nodes
    assert len(resumed.active_decisions) == 1


def test_source_role_checkpoint_missing_file_raises(tmp_path: Path):
    missing_file = tmp_path / "absent.json"
    with pytest.raises(FileNotFoundError):
        SourceRoleCheckpoint.load(missing_file)


def test_source_role_checkpoint_torn_file_raises_torn(tmp_path: Path):
    torn_file = tmp_path / "torn_state.json"
    torn_file.write_text('{"cluster_id": "test", "topology": {')
    with pytest.raises(Torn):
        SourceRoleCheckpoint.load(torn_file)
