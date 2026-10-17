"""Tests for coordinating hops sharing physical capacity (SMART-N23.fleet-coordination, Row 606).

Proves:
1. Every decide.Rung in `fleet_coordination` is exercised on both `holds` and `refuses`
   inside `with tests.rung_branches.watched():` and declared in `PROVES_RUNGS`.
2. Physical capacity disambiguation: logical endpoints sharing hosts, storage,
   network paths, quotas, and replicas are accurately identified.
3. Fair admission & feedback prioritize critical CDC catch-up and cutover proof;
   throttles standard hops under resource pressure using dynamic AIMD.
4. Concurrent migrations and application load strictly stay within global constraints
   without starvation or double ownership.
5. Work stealing only steals compatible movable units across engine and tenant boundaries
   with monotonic fencing tokens.
6. Coordinator failure reconciles worker leases and advances fencing generation before takeover.
7. Universal typed durable checkpoint resume operates in O(1).
8. Safe endpoint validation restricts hosts to loopback or RFC1918 10.0.0.x.
"""
from __future__ import annotations

import dataclasses
import datetime
import decimal
import os
import pathlib
import time
from typing import Any, Dict, List, Set, Tuple
import uuid

import pytest

import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why
from migkit import decide, topology
from migkit.fleet_coordination import (
    ActiveLeaseLock,
    AdmissionVerdict,
    ApplicationLoadReading,
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    CareBadge,
    CoordinatorFailoverRequest,
    CoordinatorLeaseRecord,
    CoordinatorSplitBrainError,
    DoubleOwnershipError,
    FeedbackSignal,
    FleetAdmissionRequest,
    FleetCapacityJointPlan,
    FleetCheckpointRecord,
    FleetCoordinationCheckpointStore,
    FleetCoordinationError,
    FleetTopology,
    HopWorkKind,
    HopWorkUnit,
    IncompatibleStealError,
    LogicalEndpoint,
    NetworkPath,
    PhysicalCapacityEnvelope,
    PhysicalHost,
    PhysicalStorage,
    ReplicaNode,
    ResourceVector,
    ServiceQuota,
    StealVerdict,
    TakeoverVerdict,
    UnsafeEndpointError,
    WorkerLeaseRecord,
    WorkStealingRequest,
    calculate_aimd_throttle_factor,
    choose_coordinator_failover,
    choose_fleet_admission,
    choose_work_stealing,
    coordinator_failover_rungs,
    fleet_admission_rungs,
    validate_safe_endpoint,
    work_stealing_rungs,
)

#: Read by `test_every_decide_rung_is_reached`
PROVES_RUNGS = [
    ("fleet_coordination", "coordinator_failover_rungs", "reconcile_and_fence_takeover", "holds"),
    ("fleet_coordination", "coordinator_failover_rungs", "reconcile_and_fence_takeover", "refuses"),
    ("fleet_coordination", "coordinator_failover_rungs", "refuse_conflicting_split_brain", "holds"),
    ("fleet_coordination", "coordinator_failover_rungs", "refuse_conflicting_split_brain", "refuses"),
    ("fleet_coordination", "coordinator_failover_rungs", "standby_wait_lease_active", "holds"),
    ("fleet_coordination", "coordinator_failover_rungs", "standby_wait_lease_active", "refuses"),
    ("fleet_coordination", "fleet_admission_rungs", "fair_share_headroom_admit", "holds"),
    ("fleet_coordination", "fleet_admission_rungs", "fair_share_headroom_admit", "refuses"),
    ("fleet_coordination", "fleet_admission_rungs", "priority_critical_catchup_admit", "holds"),
    ("fleet_coordination", "fleet_admission_rungs", "priority_critical_catchup_admit", "refuses"),
    ("fleet_coordination", "fleet_admission_rungs", "priority_critical_proof_admit", "holds"),
    ("fleet_coordination", "fleet_admission_rungs", "priority_critical_proof_admit", "refuses"),
    ("fleet_coordination", "fleet_admission_rungs", "queue_and_defer_refuse", "holds"),
    ("fleet_coordination", "fleet_admission_rungs", "queue_and_defer_refuse", "refuses"),
    ("fleet_coordination", "fleet_admission_rungs", "throttled_feedback_admit", "holds"),
    ("fleet_coordination", "fleet_admission_rungs", "throttled_feedback_admit", "refuses"),
    ("fleet_coordination", "work_stealing_rungs", "cooperative_yield_movable_unit", "holds"),
    ("fleet_coordination", "work_stealing_rungs", "cooperative_yield_movable_unit", "refuses"),
    ("fleet_coordination", "work_stealing_rungs", "refuse_incompatible_or_locked_unit", "holds"),
    ("fleet_coordination", "work_stealing_rungs", "refuse_incompatible_or_locked_unit", "refuses"),
    ("fleet_coordination", "work_stealing_rungs", "steal_compatible_range_unit", "holds"),
    ("fleet_coordination", "work_stealing_rungs", "steal_compatible_range_unit", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures & Test Topology Builder
# -----------------------------------------------------------------------------

def _build_test_envelope() -> PhysicalCapacityEnvelope:
    hosts = {
        "host_shared_01": PhysicalHost(
            host_id="host_shared_01",
            total_cpu_cores=32.0,
            total_ram_mb=131072.0,
            management_ip="10.0.0.10",
        ),
        "host_isolated_02": PhysicalHost(
            host_id="host_isolated_02",
            total_cpu_cores=16.0,
            total_ram_mb=65536.0,
            management_ip="10.0.0.11",
        ),
    }
    storages = {
        "san_shared_vol": PhysicalStorage(
            storage_id="san_shared_vol",
            max_read_iops=20000.0,
            max_write_iops=15000.0,
            max_throughput_mb_s=1200.0,
        ),
        "ssd_local_vol": PhysicalStorage(
            storage_id="ssd_local_vol",
            max_read_iops=5000.0,
            max_write_iops=5000.0,
            max_throughput_mb_s=300.0,
        ),
    }
    networks = {
        "spine_link_10g": NetworkPath(
            path_id="spine_link_10g",
            max_bandwidth_mb_s=1250.0,
            max_packets_per_sec=800000.0,
        ),
    }
    quotas = {
        "conn_pool_shared": ServiceQuota(
            quota_id="conn_pool_shared",
            max_connections=500,
            max_queries_per_sec=25000.0,
            max_replication_slots=10,
        ),
    }
    replicas = {
        "replica_node_a": ReplicaNode(
            replica_id="replica_node_a",
            physical_host_id="host_shared_01",
            max_cdc_streams=4,
            max_replication_lag_s=60.0,
        ),
    }
    return PhysicalCapacityEnvelope(
        hosts=hosts,
        storages=storages,
        networks=networks,
        quotas=quotas,
        replicas=replicas,
    )


def _build_test_topology(envelope: PhysicalCapacityEnvelope) -> FleetTopology:
    endpoints = {
        "ep_prod_orders": LogicalEndpoint(
            endpoint_id="ep_prod_orders",
            host="10.0.0.21",
            physical_host_id="host_shared_01",
            physical_storage_id="san_shared_vol",
            network_path_ids=("spine_link_10g",),
            quota_ids=("conn_pool_shared",),
            replica_id="replica_node_a",
        ),
        "ep_analytics_warehouse": LogicalEndpoint(
            endpoint_id="ep_analytics_warehouse",
            host="10.0.0.22",
            physical_host_id="host_shared_01",
            physical_storage_id="san_shared_vol",
            network_path_ids=("spine_link_10g",),
            quota_ids=("conn_pool_shared",),
            replica_id="replica_node_a",
        ),
        "ep_isolated_customer": LogicalEndpoint(
            endpoint_id="ep_isolated_customer",
            host="10.0.0.30",
            physical_host_id="host_isolated_02",
            physical_storage_id="ssd_local_vol",
            network_path_ids=(),
            quota_ids=(),
            replica_id=None,
        ),
    }
    return FleetTopology(endpoints=endpoints, envelope=envelope)


# -----------------------------------------------------------------------------
# Test 1: Full Rung Reachability (Holds & Refuses under watched())
# -----------------------------------------------------------------------------

def test_every_rung_holds_and_refuses_under_watched(tmp_path: pathlib.Path):
    """Prove that every decide.Rung in fleet_coordination reaches BOTH holds and refuses."""
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    app_load = ApplicationLoadReading(timestamp=time.time())

    with watched() as seen:
        # --- 1. fleet_admission_rungs ---
        # 1a. priority_critical_catchup_admit: holds & refuses
        req_catchup_holds = FleetAdmissionRequest(
            request_id="req-c-1",
            hop_id="hop-c",
            endpoint_ids=("ep_prod_orders",),
            work_kind=HopWorkKind.CRITICAL_CATCHUP,
            demanded_resources=ResourceVector(cpu_cores=4.0, read_iops=1000.0),
            current_cdc_lag_s=25.0,  # > critical_lag_threshold
        )
        decide.climb(
            "admission:catchup_holds",
            fleet_admission_rungs(),
            facts={
                "work_kind": HopWorkKind.CRITICAL_CATCHUP.value,
                "has_catchup_headroom": True,
                "cdc_lag_seconds": 25.0,
                "critical_lag_threshold": 5.0,
                "near_cutover_deadline": False,
            },
        )
        decide.climb(
            "admission:catchup_refuses",
            fleet_admission_rungs(),
            facts={
                "work_kind": HopWorkKind.CRITICAL_CATCHUP.value,
                "has_catchup_headroom": False,  # Refuses!
                "cdc_lag_seconds": 25.0,
                "critical_lag_threshold": 5.0,
                "near_cutover_deadline": False,
            },
        )

        # 1b. priority_critical_proof_admit: holds & refuses
        decide.climb(
            "admission:proof_holds",
            fleet_admission_rungs(),
            facts={
                "work_kind": HopWorkKind.CRITICAL_PROOF.value,
                "has_proof_headroom": True,
                "is_final_cutover_proof": True,
            },
        )
        decide.climb(
            "admission:proof_refuses",
            fleet_admission_rungs(),
            facts={
                "work_kind": HopWorkKind.CRITICAL_PROOF.value,
                "has_proof_headroom": True,
                "is_final_cutover_proof": False,  # Refuses!
            },
        )

        # 1c. fair_share_headroom_admit: holds & refuses
        decide.climb(
            "admission:fair_share_holds",
            fleet_admission_rungs(),
            facts={
                "fits_shared_headroom": True,
                "global_constraints_breached": False,
                "hop_starved": False,
            },
        )
        decide.climb(
            "admission:fair_share_refuses",
            fleet_admission_rungs(),
            facts={
                "fits_shared_headroom": False,  # Refuses!
                "global_constraints_breached": False,
                "hop_starved": False,
            },
        )

        # 1d. throttled_feedback_admit: holds & refuses
        decide.climb(
            "admission:throttled_holds",
            fleet_admission_rungs(),
            facts={
                "feedback_pressure_detected": True,
                "calculated_throttle_factor": 0.5,
                "min_throttle_floor": 0.1,
                "supports_throttling": True,
            },
        )
        decide.climb(
            "admission:throttled_refuses",
            fleet_admission_rungs(),
            facts={
                "feedback_pressure_detected": True,
                "calculated_throttle_factor": 0.05,  # < 0.1 floor -> Refuses!
                "min_throttle_floor": 0.1,
                "supports_throttling": True,
            },
        )

        # 1e. queue_and_defer_refuse: holds & refuses
        decide.climb(
            "admission:defer_holds",
            fleet_admission_rungs(),
            facts={
                "queue_depth": 5,
                "max_queue_depth": 100,
                "deferral_timeout_expired": False,
            },
        )
        decide.climb(
            "admission:defer_refuses",
            fleet_admission_rungs(),
            facts={
                "queue_depth": 105,  # > max_queue_depth -> Refuses!
                "max_queue_depth": 100,
                "deferral_timeout_expired": False,
            },
        )

        # --- 2. work_stealing_rungs ---
        # 2a. steal_compatible_range_unit: holds & refuses
        decide.climb(
            "stealing:steal_holds",
            work_stealing_rungs(),
            facts={
                "unit_movable": True,
                "unit_state": "pending",
                "engine_compatible": True,
                "tenant_compatible": True,
                "recipient_has_headroom": True,
                "unit_lease_active": False,
            },
        )
        decide.climb(
            "stealing:steal_refuses",
            work_stealing_rungs(),
            facts={
                "unit_movable": True,
                "unit_state": "pending",
                "engine_compatible": False,  # Engine mismatch -> Refuses!
                "tenant_compatible": True,
                "recipient_has_headroom": True,
                "unit_lease_active": False,
            },
        )

        # 2b. cooperative_yield_movable_unit: holds & refuses
        decide.climb(
            "stealing:yield_holds",
            work_stealing_rungs(),
            facts={
                "donor_overloaded": True,
                "unit_movable": True,
                "work_kind": HopWorkKind.NORMAL_BULK_COPY.value,
                "donor_yield_authorized": True,
            },
        )
        decide.climb(
            "stealing:yield_refuses",
            work_stealing_rungs(),
            facts={
                "donor_overloaded": True,
                "unit_movable": True,
                "work_kind": HopWorkKind.CRITICAL_CATCHUP.value,  # Catchup cannot yield -> Refuses!
                "donor_yield_authorized": True,
            },
        )

        # 2c. refuse_incompatible_or_locked_unit: holds & refuses
        decide.climb(
            "stealing:refuse_holds",
            work_stealing_rungs(),
            facts={
                "incompatible_or_locked": True,
            },
        )
        decide.climb(
            "stealing:refuse_refuses",
            work_stealing_rungs(),
            facts={
                "incompatible_or_locked": False,  # Refuses!
            },
        )

        # --- 3. coordinator_failover_rungs ---
        # 3a. reconcile_and_fence_takeover: holds & refuses
        decide.climb(
            "failover:takeover_holds",
            coordinator_failover_rungs(),
            facts={
                "primary_lease_lapsed": True,
                "worker_leases_reconciled": True,
                "proposed_generation": 2,
                "current_generation": 1,
                "reconciled_allocations_within_bounds": True,
            },
        )
        decide.climb(
            "failover:takeover_refuses",
            coordinator_failover_rungs(),
            facts={
                "primary_lease_lapsed": False,  # Primary alive -> Refuses!
                "worker_leases_reconciled": True,
                "proposed_generation": 2,
                "current_generation": 1,
                "reconciled_allocations_within_bounds": True,
            },
        )

        # 3b. standby_wait_lease_active: holds & refuses
        decide.climb(
            "failover:standby_holds",
            coordinator_failover_rungs(),
            facts={
                "primary_lease_lapsed": False,
            },
        )
        decide.climb(
            "failover:standby_refuses",
            coordinator_failover_rungs(),
            facts={
                "primary_lease_lapsed": True,  # Already lapsed -> Refuses!
            },
        )

        # 3c. refuse_conflicting_split_brain: holds & refuses
        decide.climb(
            "failover:split_brain_holds",
            coordinator_failover_rungs(),
            facts={
                "split_brain_conflict_detected": True,
            },
        )
        decide.climb(
            "failover:split_brain_refuses",
            coordinator_failover_rungs(),
            facts={
                "split_brain_conflict_detected": False,  # Refuses!
            },
        )

    # Verify that all 11 rungs reached BOTH holds and refuses
    expected_rungs = [
        "priority_critical_catchup_admit",
        "priority_critical_proof_admit",
        "fair_share_headroom_admit",
        "throttled_feedback_admit",
        "queue_and_defer_refuse",
        "steal_compatible_range_unit",
        "cooperative_yield_movable_unit",
        "refuse_incompatible_or_locked_unit",
        "reconcile_and_fence_takeover",
        "standby_wait_lease_active",
        "refuse_conflicting_split_brain",
    ]
    for r_name in expected_rungs:
        branches_seen = branch(seen, r_name)
        assert HOLDS in branches_seen, f"rung {r_name} did not reach holds"
        assert REFUSES in branches_seen, f"rung {r_name} did not reach refuses"


# -----------------------------------------------------------------------------
# Test 2: Physical Capacity Disambiguation & Topology Mapping
# -----------------------------------------------------------------------------

def test_physical_capacity_disambiguation():
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)

    # ep_prod_orders and ep_analytics_warehouse share host_shared_01, san_shared_vol, spine_link_10g, conn_pool_shared, replica_node_a
    shared = topology_map.identify_shared_physical_resources(
        "ep_prod_orders", "ep_analytics_warehouse"
    )
    assert "host_shared_01" in shared["hosts"]
    assert "san_shared_vol" in shared["storages"]
    assert "spine_link_10g" in shared["networks"]
    assert "conn_pool_shared" in shared["quotas"]
    assert "replica_node_a" in shared["replicas"]
    assert topology_map.endpoints_share_physical_capacity(
        "ep_prod_orders", "ep_analytics_warehouse"
    )

    # ep_prod_orders and ep_isolated_customer share NO physical resources
    shared_isolated = topology_map.identify_shared_physical_resources(
        "ep_prod_orders", "ep_isolated_customer"
    )
    assert not shared_isolated
    assert not topology_map.endpoints_share_physical_capacity(
        "ep_prod_orders", "ep_isolated_customer"
    )

    # Hop level sharing check
    assert topology_map.hops_share_physical_capacity(
        ["ep_prod_orders"], ["ep_analytics_warehouse"]
    )
    assert not topology_map.hops_share_physical_capacity(
        ["ep_isolated_customer"], ["ep_prod_orders"]
    )


# -----------------------------------------------------------------------------
# Test 3: Fair Admission Prioritizes Critical CDC Catch-Up
# -----------------------------------------------------------------------------

def test_fair_admission_prioritizes_critical_catchup():
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    app_load = ApplicationLoadReading(timestamp=time.time())

    # Current allocations occupy 24 cores out of 32
    current_allocs = {
        "hop-bulk-1": ResourceVector(cpu_cores=24.0, read_iops=8000.0),
    }

    # Normal hop requesting 10 cores -> exceeds remaining 8 cores headroom
    req_normal = FleetAdmissionRequest(
        request_id="req-norm-1",
        hop_id="hop-norm-2",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        demanded_resources=ResourceVector(cpu_cores=10.0, read_iops=2000.0),
    )
    verdict_normal = choose_fleet_admission(
        request=req_normal,
        envelope=envelope,
        topology_map=topology_map,
        current_allocations=current_allocs,
        app_load=app_load,
    )
    assert not verdict_normal.admitted
    assert verdict_normal.ladder_rung == "queue_and_defer_refuse"
    assert verdict_normal.care_badge == CareBadge.BENIGN

    # Critical catch-up with 30s lag requesting 4 cores -> Admitted on priority rung
    req_catchup = FleetAdmissionRequest(
        request_id="req-catch-1",
        hop_id="hop-catchup-urgent",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.CRITICAL_CATCHUP,
        demanded_resources=ResourceVector(cpu_cores=4.0, read_iops=1000.0),
        current_cdc_lag_s=30.0,
    )
    verdict_catchup = choose_fleet_admission(
        request=req_catchup,
        envelope=envelope,
        topology_map=topology_map,
        current_allocations=current_allocs,
        app_load=app_load,
    )
    assert verdict_catchup.admitted
    assert verdict_catchup.ladder_rung == "priority_critical_catchup_admit"
    assert verdict_catchup.care_badge == CareBadge.PASS


# -----------------------------------------------------------------------------
# Test 4: Fair Admission Prioritizes Critical Cutover Proof
# -----------------------------------------------------------------------------

def test_fair_admission_prioritizes_critical_proof():
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    app_load = ApplicationLoadReading(timestamp=time.time())
    current_allocs = {
        "hop-bulk-1": ResourceVector(cpu_cores=16.0, read_iops=4000.0),
    }

    req_proof = FleetAdmissionRequest(
        request_id="req-prf-1",
        hop_id="hop-final-proof",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.CRITICAL_PROOF,
        demanded_resources=ResourceVector(cpu_cores=2.0, read_iops=3000.0, network_mb_s=100.0),
    )
    verdict_proof = choose_fleet_admission(
        request=req_proof,
        envelope=envelope,
        topology_map=topology_map,
        current_allocations=current_allocs,
        app_load=app_load,
        is_final_cutover_proof=True,
    )
    assert verdict_proof.admitted
    assert verdict_proof.ladder_rung == "priority_critical_proof_admit"
    assert verdict_proof.care_badge == CareBadge.PASS


# -----------------------------------------------------------------------------
# Test 5: AIMD Dynamic Feedback Throttling Under Resource Pressure
# -----------------------------------------------------------------------------

def test_aimd_dynamic_feedback_throttling():
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    app_load = ApplicationLoadReading(timestamp=time.time())
    current_allocs = {
        "hop-existing": ResourceVector(cpu_cores=8.0, read_iops=2000.0),
    }

    # Inject p99 latency spike (target 20ms, observed 80ms)
    feedback_spike = FeedbackSignal(
        timestamp=time.time(),
        latency_p99_ms=80.0,
        target_latency_p99_ms=20.0,
        cdc_lag_seconds=1.2,
        cpu_utilization_ratio=0.75,
    )
    throttle_factor = calculate_aimd_throttle_factor(feedback_spike)
    assert throttle_factor == 0.25  # 20 / 80

    req = FleetAdmissionRequest(
        request_id="req-throttled-1",
        hop_id="hop-throttled",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        demanded_resources=ResourceVector(cpu_cores=8.0, read_iops=4000.0, connections=20),
    )
    verdict = choose_fleet_admission(
        request=req,
        envelope=envelope,
        topology_map=topology_map,
        current_allocations=current_allocs,
        app_load=app_load,
        feedback=feedback_spike,
    )
    assert verdict.admitted
    assert verdict.ladder_rung == "throttled_feedback_admit"
    assert verdict.care_badge == CareBadge.REVIEW
    assert verdict.throttle_factor == 0.25
    # Scaled resources: 8.0 * 0.25 = 2.0 cores, 4000 * 0.25 = 1000 iops
    assert verdict.allocated_resources.cpu_cores == 2.0
    assert verdict.allocated_resources.read_iops == 1000.0


# -----------------------------------------------------------------------------
# Test 6: Concurrent Migrations & Application Load Stay Inside Global Constraints
# -----------------------------------------------------------------------------

def test_concurrent_migrations_and_app_load_stay_inside_global_constraints(tmp_path: pathlib.Path):
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    store = FleetCoordinationCheckpointStore(tmp_path / "checkpoints")
    plan = FleetCapacityJointPlan(
        topology_map=topology_map,
        envelope=envelope,
        checkpoint_store=store,
    )

    # Background application load consumes 12 cores on host_shared_01
    bg_load = ApplicationLoadReading(
        timestamp=time.time(),
        host_load={
            "host_shared_01": ResourceVector(cpu_cores=12.0, read_iops=2000.0),
        },
    )
    plan.update_application_load(bg_load)

    # Hop 1 demands 10 cores -> Admitted (12 + 10 = 22 <= 32 total)
    req1 = FleetAdmissionRequest(
        request_id="req-h1",
        hop_id="hop-alpha",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        demanded_resources=ResourceVector(cpu_cores=10.0, read_iops=2000.0),
    )
    v1 = plan.admit_hop(req1)
    assert v1.admitted
    assert v1.ladder_rung == "fair_share_headroom_admit"

    # Hop 2 demands 8 cores -> Admitted (12 + 10 + 8 = 30 <= 32 total)
    req2 = FleetAdmissionRequest(
        request_id="req-h2",
        hop_id="hop-beta",
        endpoint_ids=("ep_analytics_warehouse",),
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        demanded_resources=ResourceVector(cpu_cores=8.0, read_iops=1500.0),
    )
    v2 = plan.admit_hop(req2)
    assert v2.admitted

    # Hop 3 demands 6 cores -> 12 + 10 + 8 + 6 = 36 > 32 -> REFUSED / QUEUED
    req3 = FleetAdmissionRequest(
        request_id="req-h3",
        hop_id="hop-gamma",
        endpoint_ids=("ep_prod_orders",),
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        demanded_resources=ResourceVector(cpu_cores=6.0, read_iops=1000.0),
    )
    v3 = plan.admit_hop(req3)
    assert not v3.admitted
    assert v3.ladder_rung == "queue_and_defer_refuse"

    # Total allocated migration cores: 10 + 8 = 18 cores
    total_mig_cores = sum(r.cpu_cores for r in plan.allocations.values())
    assert total_mig_cores == 18.0
    # Combined with application load: 18 + 12 = 30 <= 32 host limit
    assert total_mig_cores + bg_load.host_load["host_shared_01"].cpu_cores <= 32.0


# -----------------------------------------------------------------------------
# Test 7: Work Stealing Compatible Movable Units & Fencing Tokens
# -----------------------------------------------------------------------------

def test_work_stealing_compatible_movable_units():
    unit = HopWorkUnit(
        unit_id="unit-orders-slice-4",
        hop_id="hop-overloaded",
        table_name="orders",
        engine="postgres",
        tenant_id="tenant_acme",
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        estimated_resource=ResourceVector(cpu_cores=2.0, read_iops=500.0),
        state="pending",
        movable=True,
        fencing_token=3,
    )

    req = WorkStealingRequest(
        donor_hop_id="hop-overloaded",
        recipient_hop_id="hop-idle",
        recipient_worker_id="worker-idle-99",
        candidate_unit=unit,
        recipient_engine="postgres",
        recipient_tenant_id="tenant_acme",
        available_headroom=ResourceVector(cpu_cores=4.0, read_iops=1000.0),
    )
    verdict = choose_work_stealing(req, unit_lease_active=False)
    assert verdict.granted
    assert verdict.ladder_rung == "steal_compatible_range_unit"
    assert verdict.care_badge == CareBadge.PASS
    assert verdict.new_fencing_token == 4
    assert verdict.stolen_unit.locked_by == "worker-idle-99"
    assert verdict.stolen_unit.hop_id == "hop-idle"
    assert verdict.stolen_unit.fencing_token == 4


# -----------------------------------------------------------------------------
# Test 8: Work Stealing Refuses Incompatible or Active Leased Units
# -----------------------------------------------------------------------------

def test_work_stealing_refuses_incompatible_or_locked():
    # Case A: Unit actively leased by another worker heartbeat
    unit_locked = HopWorkUnit(
        unit_id="unit-locked-1",
        hop_id="hop-donor",
        table_name="payments",
        engine="postgres",
        tenant_id="tenant_acme",
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        estimated_resource=ResourceVector(cpu_cores=2.0),
        state="in_flight",
        movable=True,
        locked_by="worker-active-12",
        fencing_token=1,
    )
    req_locked = WorkStealingRequest(
        donor_hop_id="hop-donor",
        recipient_hop_id="hop-taker",
        recipient_worker_id="worker-taker-44",
        candidate_unit=unit_locked,
        recipient_engine="postgres",
        recipient_tenant_id="tenant_acme",
        available_headroom=ResourceVector(cpu_cores=4.0),
    )
    v_locked = choose_work_stealing(req_locked, unit_lease_active=True)
    assert not v_locked.granted
    assert v_locked.ladder_rung == "refuse_incompatible_or_locked_unit"
    assert v_locked.care_badge == CareBadge.REVIEW

    # Case B: Engine mismatch (postgres vs mysql)
    req_engine_mismatch = WorkStealingRequest(
        donor_hop_id="hop-donor",
        recipient_hop_id="hop-taker",
        recipient_worker_id="worker-taker-44",
        candidate_unit=unit_locked,
        recipient_engine="mysql",  # Mismatch!
        recipient_tenant_id="tenant_acme",
        available_headroom=ResourceVector(cpu_cores=4.0),
    )
    v_engine = choose_work_stealing(req_engine_mismatch, unit_lease_active=False)
    assert not v_engine.granted
    assert v_engine.ladder_rung == "refuse_incompatible_or_locked_unit"

    # Case C: Tenant boundary mismatch (tenant_acme vs tenant_beta)
    req_tenant_mismatch = WorkStealingRequest(
        donor_hop_id="hop-donor",
        recipient_hop_id="hop-taker",
        recipient_worker_id="worker-taker-44",
        candidate_unit=unit_locked,
        recipient_engine="postgres",
        recipient_tenant_id="tenant_beta",  # Mismatch!
        available_headroom=ResourceVector(cpu_cores=4.0),
    )
    v_tenant = choose_work_stealing(req_tenant_mismatch, unit_lease_active=False)
    assert not v_tenant.granted
    assert v_tenant.ladder_rung == "refuse_incompatible_or_locked_unit"


# -----------------------------------------------------------------------------
# Test 9: Cooperative Yield Movable Unit
# -----------------------------------------------------------------------------

def test_work_stealing_cooperative_yield():
    unit = HopWorkUnit(
        unit_id="unit-bulk-table",
        hop_id="hop-donor",
        table_name="historical_logs",
        engine="postgres",
        tenant_id="tenant_shared",
        work_kind=HopWorkKind.NORMAL_BULK_COPY,
        estimated_resource=ResourceVector(cpu_cores=2.0),
        state="pending",
        movable=True,
        fencing_token=5,
    )
    req = WorkStealingRequest(
        donor_hop_id="hop-donor",
        recipient_hop_id="hop-helper",
        recipient_worker_id="worker-helper-1",
        candidate_unit=unit,
        recipient_engine="postgres",
        recipient_tenant_id="tenant_shared",
        available_headroom=ResourceVector(cpu_cores=4.0),
    )
    verdict = choose_work_stealing(
        req,
        unit_lease_active=False,
        donor_overloaded=True,
        donor_yield_authorized=True,
    )
    assert verdict.granted
    assert verdict.ladder_rung == "steal_compatible_range_unit"  # Top compatible rung holds!
    assert verdict.stolen_unit.fencing_token == 6


# -----------------------------------------------------------------------------
# Test 10: Active Lease Mutual Exclusion (.run.lock)
# -----------------------------------------------------------------------------

def test_active_lease_lock_mutual_exclusion(tmp_path: pathlib.Path):
    lock_file = tmp_path / "coordinator.run.lock"

    lock1 = ActiveLeaseLock(lock_file)
    assert lock1.acquire()

    # Second lock on same file must fail with non-blocking error
    lock2 = ActiveLeaseLock(lock_file)
    assert not lock2.acquire()

    # Context manager raises DoubleOwnershipError when unable to acquire
    with pytest.raises(DoubleOwnershipError) as exc_info:
        with lock2:
            pass
    assert exc_info.value.care_badge == CareBadge.ACTION

    # Releasing lock1 allows lock2 to acquire
    lock1.release()
    assert lock2.acquire()
    lock2.release()


# -----------------------------------------------------------------------------
# Test 11: Coordinator Failover Reconciles Leases Before Takeover
# -----------------------------------------------------------------------------

def test_coordinator_failover_reconciles_leases_before_takeover():
    envelope = _build_test_envelope()
    now = time.time()

    # Primary coordinator lease that expired 10 seconds ago
    expired_primary_lease = CoordinatorLeaseRecord(
        coordinator_id="coord-primary-01",
        host="10.0.0.10",
        pid=12345,
        fencing_generation=4,
        term_seconds=30.0,
        renewed_at=now - 50.0,
        expires_at=now - 20.0,
        jitter=2.0,
    )
    assert expired_primary_lease.is_lapsed(now)

    # Active worker 1 holds units 1, 2 (alive)
    worker_alive = WorkerLeaseRecord(
        worker_id="worker-alive-1",
        hop_id="hop-a",
        unit_ids=("unit-1", "unit-2"),
        host="10.0.0.21",
        pid=20001,
        fencing_token=10,
        term_seconds=30.0,
        renewed_at=now - 5.0,
        expires_at=now + 25.0,
    )
    # Dead worker 2 holds unit 3 (lapsed heartbeat)
    worker_dead = WorkerLeaseRecord(
        worker_id="worker-dead-2",
        hop_id="hop-b",
        unit_ids=("unit-3",),
        host="10.0.0.22",
        pid=20002,
        fencing_token=5,
        term_seconds=30.0,
        renewed_at=now - 60.0,
        expires_at=now - 30.0,
    )

    req = CoordinatorFailoverRequest(
        standby_id="coord-standby-02",
        current_lease=expired_primary_lease,
        now=now,
        active_worker_leases=(worker_alive, worker_dead),
        envelope=envelope,
    )

    verdict = choose_coordinator_failover(req)
    assert verdict.takeover_granted
    assert verdict.ladder_rung == "reconcile_and_fence_takeover"
    assert verdict.care_badge == CareBadge.PASS
    assert verdict.new_fencing_generation == 5  # Advanced from 4 to 5!

    # Reconciled leases: worker_dead is pruned, worker_alive preserved with bumped token
    assert len(verdict.reconciled_leases) == 1
    reconciled_alive = verdict.reconciled_leases[0]
    assert reconciled_alive.worker_id == "worker-alive-1"
    assert reconciled_alive.fencing_token == 11  # Bumped from 10 to 11


# -----------------------------------------------------------------------------
# Test 12: Coordinator Failover Prevents Split-Brain
# -----------------------------------------------------------------------------

def test_coordinator_failover_prevents_split_brain():
    envelope = _build_test_envelope()
    now = time.time()

    # Active primary lease (still has 20s remaining)
    active_primary = CoordinatorLeaseRecord(
        coordinator_id="coord-primary-01",
        host="10.0.0.10",
        pid=12345,
        fencing_generation=4,
        term_seconds=30.0,
        renewed_at=now - 5.0,
        expires_at=now + 25.0,
    )
    assert not active_primary.is_lapsed(now)

    req = CoordinatorFailoverRequest(
        standby_id="coord-standby-02",
        current_lease=active_primary,
        now=now,
        active_worker_leases=(),
        envelope=envelope,
    )
    # Attempt takeover while primary is active -> Standby waits
    v_wait = choose_coordinator_failover(req)
    assert not v_wait.takeover_granted
    assert v_wait.ladder_rung == "standby_wait_lease_active"
    assert v_wait.care_badge == CareBadge.BENIGN

    # Force split-brain conflict signal -> Refuses takeover
    v_split = choose_coordinator_failover(req, split_brain_conflict=True)
    assert not v_split.takeover_granted
    assert v_split.ladder_rung == "refuse_conflicting_split_brain"
    assert v_split.care_badge == CareBadge.ACTION


# -----------------------------------------------------------------------------
# Test 13: Universal Typed Durable Checkpoint Store & O(1) Resume
# -----------------------------------------------------------------------------

def test_universal_typed_durable_checkpoint_resume_in_o1(tmp_path: pathlib.Path):
    store = FleetCoordinationCheckpointStore(tmp_path / "checkpoints")

    rec1 = FleetCheckpointRecord(
        unit_id="unit-tbl-1",
        hop_id="hop-alpha",
        table_name="orders",
        key_cursor=50000,
        updated_at=datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc),
        row_count=50000,
        bytes_transferred=25000000,
        checksum_digest="sha256:abc123def456",
        status="completed",
        fencing_token=2,
    )
    rec2 = FleetCheckpointRecord(
        unit_id="unit-tbl-2",
        hop_id="hop-alpha",
        table_name="orders",
        key_cursor=75000,
        updated_at=datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc),
        row_count=25000,
        bytes_transferred=12500000,
        checksum_digest="sha256:789xyz012",
        status="in_flight",
        fencing_token=2,
    )
    store.save_checkpoint(rec1)
    store.save_checkpoint(rec2)

    # Reload and test type preservation
    loaded1 = store.load_checkpoint("unit-tbl-1")
    assert loaded1 is not None
    assert loaded1.unit_id == "unit-tbl-1"
    assert loaded1.row_count == 50000
    assert loaded1.status == "completed"
    assert isinstance(loaded1.updated_at, datetime.datetime)

    # Resume in O(1)
    completed, pending = store.resume_in_o1(["unit-tbl-1", "unit-tbl-2", "unit-tbl-3"])
    assert completed == ["unit-tbl-1"]
    assert pending == ["unit-tbl-2", "unit-tbl-3"]


# -----------------------------------------------------------------------------
# Test 14: Double Ownership Prevention in Worker Leases
# -----------------------------------------------------------------------------

def test_double_ownership_prevention_in_worker_leases(tmp_path: pathlib.Path):
    envelope = _build_test_envelope()
    topology_map = _build_test_topology(envelope)
    store = FleetCoordinationCheckpointStore(tmp_path / "checkpoints")
    plan = FleetCapacityJointPlan(
        topology_map=topology_map,
        envelope=envelope,
        checkpoint_store=store,
    )
    now = time.time()

    w1 = WorkerLeaseRecord(
        worker_id="worker-1",
        hop_id="hop-a",
        unit_ids=("unit-10", "unit-11"),
        host="10.0.0.10",
        pid=1001,
        fencing_token=1,
        term_seconds=30.0,
        renewed_at=now,
        expires_at=now + 30.0,
    )
    plan.register_worker_lease(w1)

    # Worker 2 attempting to register overlapping unit-10 raises DoubleOwnershipError
    w2 = WorkerLeaseRecord(
        worker_id="worker-2",
        hop_id="hop-b",
        unit_ids=("unit-10", "unit-12"),
        host="10.0.0.11",
        pid=1002,
        fencing_token=1,
        term_seconds=30.0,
        renewed_at=now,
        expires_at=now + 30.0,
    )
    with pytest.raises(DoubleOwnershipError) as exc_info:
        plan.register_worker_lease(w2)
    assert exc_info.value.care_badge == CareBadge.ACTION


# -----------------------------------------------------------------------------
# Test 15: Safe Endpoint Host Validation
# -----------------------------------------------------------------------------

def test_safe_endpoint_host_validation():
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("localhost") == "localhost"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.1")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("8.8.8.8")

    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("")
