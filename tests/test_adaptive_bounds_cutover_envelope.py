import os
import tempfile
import pytest

from migkit.adaptive_bounds import (
    AdaptiveBoundsEnvelope,
    AdaptiveBoundsError,
    ClusterNode,
    ClusterTopology,
    CutoverIteration,
    CutoverStatus,
    InfeasibleBoundsError,
    PartitionProgress,
    PartitionRoute,
    PartitionRoutingJournal,
    ShardInfo,
    ShardSplitRecord,
)


def test_adaptive_bounds_envelope_valid():
    env = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=8)
    assert env.min_workers == 2
    assert env.start_workers == 4
    assert env.max_workers == 8
    assert env.is_within(4) is True
    assert env.is_within(1) is False
    assert env.is_within(9) is False
    assert env.clamp(1) == 2
    assert env.clamp(10) == 8
    assert env.clamp(5) == 5


def test_adaptive_bounds_envelope_fixed_bounds():
    env = AdaptiveBoundsEnvelope(min_workers=4, start_workers=4, max_workers=4)
    assert env.min_workers == 4
    assert env.start_workers == 4
    assert env.max_workers == 4
    assert env.is_within(4) is True
    assert env.is_within(3) is False
    assert env.is_within(5) is False
    assert env.clamp(1) == 4
    assert env.clamp(10) == 4


def test_adaptive_bounds_envelope_refuses_max_below_start():
    with pytest.raises(InfeasibleBoundsError) as exc_info:
        AdaptiveBoundsEnvelope(min_workers=2, start_workers=8, max_workers=4)
    assert "max_workers" in str(exc_info.value) or "infeasible" in str(exc_info.value).lower()


def test_adaptive_bounds_envelope_refuses_start_below_min():
    with pytest.raises(InfeasibleBoundsError) as exc_info:
        AdaptiveBoundsEnvelope(min_workers=5, start_workers=3, max_workers=10)
    assert "start_workers" in str(exc_info.value) or "infeasible" in str(exc_info.value).lower()


def test_adaptive_bounds_envelope_refuses_zero_or_negative_min():
    with pytest.raises(InfeasibleBoundsError):
        AdaptiveBoundsEnvelope(min_workers=0, start_workers=2, max_workers=4)
    with pytest.raises(InfeasibleBoundsError):
        AdaptiveBoundsEnvelope(min_workers=-1, start_workers=2, max_workers=4)


def test_adaptive_bounds_envelope_batch_size_validation():
    env = AdaptiveBoundsEnvelope(
        min_workers=1,
        start_workers=2,
        max_workers=4,
        min_batch_size=100,
        start_batch_size=500,
        max_batch_size=1000,
    )
    assert env.min_batch_size == 100
    assert env.start_batch_size == 500
    assert env.max_batch_size == 1000

    with pytest.raises(InfeasibleBoundsError):
        AdaptiveBoundsEnvelope(
            min_workers=1,
            start_workers=2,
            max_workers=4,
            min_batch_size=500,
            start_batch_size=200,
            max_batch_size=1000,
        )

    with pytest.raises(InfeasibleBoundsError):
        AdaptiveBoundsEnvelope(
            min_workers=1,
            start_workers=2,
            max_workers=4,
            min_batch_size=100,
            start_batch_size=500,
            max_batch_size=400,
        )

    with pytest.raises(InfeasibleBoundsError):
        AdaptiveBoundsEnvelope(
            min_workers=1,
            start_workers=2,
            max_workers=4,
            min_batch_size=0,
            start_batch_size=100,
            max_batch_size=200,
        )


def test_partition_routing_journal_persist_and_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        journal_file = os.path.join(tmpdir, "routing_journal.json")
        envelope = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=12)

        topo = ClusterTopology(
            cluster_id="cluster-primary",
            nodes={
                "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=20),
                "node-2": ClusterNode(node_id="node-2", host="10.0.0.2", port=5432, max_connections=20),
            },
        )

        journal = PartitionRoutingJournal(envelope=envelope, topology=topo, journal_path=journal_file)

        route_a = PartitionRoute(
            partition_id="shard-001",
            target_endpoint="10.0.0.2:5432",
            source_endpoint="10.0.0.1:5432",
            key_range=("0", "50000"),
            worker_budget=3,
            is_custom=True,
            metadata={"priority": "high", "cdc_routing_key": "custom_route_v1"},
        )
        route_b = PartitionRoute(
            partition_id="shard-002",
            target_endpoint="10.0.0.2:5432",
            source_endpoint="10.0.0.1:5432",
            key_range=("50000", "100000"),
            worker_budget=3,
            is_custom=True,
            metadata={"priority": "medium", "cdc_routing_key": "custom_route_v2"},
        )

        journal.set_route(route_a)
        journal.set_route(route_b)
        journal.record_progress(partition_id="shard-001", cursor="25000", processed_rows=25000, bytes_count=102400)
        journal.persist()

        assert os.path.exists(journal_file)

        restarted = PartitionRoutingJournal.load(journal_file)

        assert restarted.envelope.min_workers == 2
        assert restarted.envelope.start_workers == 4
        assert restarted.envelope.max_workers == 12

        loaded_a = restarted.get_route("shard-001")
        assert loaded_a is not None
        assert loaded_a.target_endpoint == "10.0.0.2:5432"
        assert loaded_a.key_range == ("0", "50000")
        assert loaded_a.is_custom is True
        assert loaded_a.worker_budget == 3
        assert loaded_a.metadata["cdc_routing_key"] == "custom_route_v1"

        loaded_b = restarted.get_route("shard-002")
        assert loaded_b is not None
        assert loaded_b.target_endpoint == "10.0.0.2:5432"
        assert loaded_b.worker_budget == 3
        assert loaded_b.is_custom is True
        assert loaded_b.metadata["cdc_routing_key"] == "custom_route_v2"

        prog = restarted.get_progress("shard-001")
        assert prog is not None
        assert prog.cursor == "25000"
        assert prog.processed_rows == 25000
        assert prog.bytes_count == 102400


def test_tablet_shard_split_preserves_custom_routing_and_rechecks_feasibility():
    with tempfile.TemporaryDirectory() as tmpdir:
        journal_file = os.path.join(tmpdir, "split_journal.json")
        envelope = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=10)

        topo = ClusterTopology(
            cluster_id="cluster-split",
            nodes={
                "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=30),
                "node-2": ClusterNode(node_id="node-2", host="10.0.0.2", port=5432, max_connections=30),
            },
        )

        journal = PartitionRoutingJournal(envelope=envelope, topology=topo, journal_path=journal_file)

        parent_route = PartitionRoute(
            partition_id="shard-001",
            target_endpoint="10.0.0.2:5432",
            source_endpoint="10.0.0.1:5432",
            key_range=("0", "100000"),
            worker_budget=4,
            is_custom=True,
            metadata={"retention_policy": "strict", "cdc_stream": "events_shard_1"},
        )
        journal.set_route(parent_route)
        journal.record_progress(partition_id="shard-001", cursor="45000", processed_rows=45000, bytes_count=204800)

        child_a = PartitionRoute(
            partition_id="shard-001-a",
            target_endpoint="10.0.0.2:5432",
            source_endpoint="10.0.0.1:5432",
            key_range=("0", "50000"),
            worker_budget=2,
            is_custom=True,
            metadata={"retention_policy": "strict", "cdc_stream": "events_shard_1"},
        )
        child_b = PartitionRoute(
            partition_id="shard-001-b",
            target_endpoint="10.0.0.2:5432",
            source_endpoint="10.0.0.1:5432",
            key_range=("50000", "100000"),
            worker_budget=2,
            is_custom=True,
            metadata={"retention_policy": "strict", "cdc_stream": "events_shard_1"},
        )

        split_record = journal.apply_shard_split(
            parent_partition_id="shard-001",
            child_routes=[child_a, child_b],
        )

        assert split_record.parent_id == "shard-001"
        assert len(split_record.child_ids) == 2

        res_a = journal.get_route("shard-001-a")
        res_b = journal.get_route("shard-001-b")
        assert res_a.target_endpoint == "10.0.0.2:5432"
        assert res_b.target_endpoint == "10.0.0.2:5432"
        assert res_a.is_custom is True
        assert res_b.is_custom is True
        assert res_a.metadata["retention_policy"] == "strict"
        assert res_b.metadata["retention_policy"] == "strict"

        assert journal.is_partition_active("shard-001") is False
        assert journal.is_partition_active("shard-001-a") is True
        assert journal.is_partition_active("shard-001-b") is True
        assert journal.recheck_feasibility() is True


def test_shard_split_refuses_when_exceeding_envelope_or_cluster_capacity():
    envelope = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=5)
    topo = ClusterTopology(
        cluster_id="cluster-tight",
        nodes={
            "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=6),
        },
        max_cluster_workers=5,
    )
    journal = PartitionRoutingJournal(envelope=envelope, topology=topo)

    child_shards = [
        PartitionRoute(partition_id=f"shard-split-{i}", target_endpoint="10.0.0.1:5432", worker_budget=2)
        for i in range(3)
    ]

    with pytest.raises(InfeasibleBoundsError) as exc_info:
        journal.apply_shard_split(
            parent_partition_id="shard-parent",
            child_routes=child_shards,
        )
    assert "capacity" in str(exc_info.value).lower() or "infeasible" in str(exc_info.value).lower()


def test_topology_change_recheck_refuses_infeasible_without_silent_defaulting():
    envelope = AdaptiveBoundsEnvelope(min_workers=3, start_workers=6, max_workers=12)
    topo = ClusterTopology(
        cluster_id="cluster-prod",
        nodes={
            "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=20),
            "node-2": ClusterNode(node_id="node-2", host="10.0.0.2", port=5432, max_connections=20),
        },
    )
    journal = PartitionRoutingJournal(envelope=envelope, topology=topo)
    journal.set_route(
        PartitionRoute(
            partition_id="shard-custom",
            target_endpoint="10.0.0.2:5432",
            worker_budget=6,
            is_custom=True,
            metadata={"strict_pin": True},
        )
    )

    shrunk_topo = ClusterTopology(
        cluster_id="cluster-prod",
        nodes={
            "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=20),
        },
    )

    with pytest.raises(InfeasibleBoundsError) as exc_info:
        journal.update_topology_and_recheck(shrunk_topo)

    assert "unreachable" in str(exc_info.value).lower() or "node-2" in str(exc_info.value) or "10.0.0.2" in str(exc_info.value)

    saved_route = journal.get_route("shard-custom")
    assert saved_route.target_endpoint == "10.0.0.2:5432"
    assert saved_route.is_custom is True


def test_topology_change_insufficient_connections_for_min_workers():
    envelope = AdaptiveBoundsEnvelope(min_workers=6, start_workers=8, max_workers=12)
    topo = ClusterTopology(
        cluster_id="cluster-small",
        nodes={
            "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=4),
        },
    )
    journal = PartitionRoutingJournal(envelope=envelope, topology=topo)
    with pytest.raises(InfeasibleBoundsError) as exc_info:
        journal.recheck_feasibility()
    assert "cannot support minimum envelope" in str(exc_info.value)


def test_repeated_cutover_iterations_preserve_routing_progress_and_budgets():
    with tempfile.TemporaryDirectory() as tmpdir:
        journal_file = os.path.join(tmpdir, "iterative_cutover.json")
        envelope = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=16)

        topo = ClusterTopology(
            cluster_id="cluster-iter",
            nodes={
                "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=50),
                "node-2": ClusterNode(node_id="node-2", host="10.0.0.2", port=5432, max_connections=50),
            },
        )

        journal = PartitionRoutingJournal(envelope=envelope, topology=topo, journal_path=journal_file)

        journal.set_route(
            PartitionRoute(
                partition_id="p-0",
                target_endpoint="10.0.0.1:5432",
                worker_budget=2,
                is_custom=True,
                metadata={"cdc_channel": "p0_custom"},
            )
        )
        journal.set_route(
            PartitionRoute(
                partition_id="p-1",
                target_endpoint="10.0.0.2:5432",
                worker_budget=2,
                is_custom=True,
                metadata={"cdc_channel": "p1_custom"},
            )
        )

        it1 = journal.begin_cutover(iteration_id="iter-001", target_stage="pilot")
        assert it1.iteration_id == "iter-001"
        assert it1.status == CutoverStatus.IN_PROGRESS

        journal.record_progress("p-0", cursor="1000", processed_rows=1000, bytes_count=50000)
        journal.record_progress("p-1", cursor="2000", processed_rows=2000, bytes_count=100000)

        journal.scale_worker_budgets({"p-0": 4, "p-1": 4})
        assert journal.get_worker_budget("p-0") == 4
        assert journal.get_worker_budget("p-1") == 4
        assert envelope.is_within(journal.get_total_worker_budget())

        journal.commit_cutover("iter-001")
        journal.persist()

        journal = PartitionRoutingJournal.load(journal_file)

        assert journal.get_progress("p-0").cursor == "1000"
        assert journal.get_progress("p-1").cursor == "2000"
        assert journal.get_route("p-0").metadata["cdc_channel"] == "p0_custom"
        assert journal.get_route("p-1").metadata["cdc_channel"] == "p1_custom"

        new_topo = ClusterTopology(
            cluster_id="cluster-iter",
            nodes={
                "node-1": ClusterNode(node_id="node-1", host="10.0.0.1", port=5432, max_connections=50),
                "node-2": ClusterNode(node_id="node-2", host="10.0.0.2", port=5432, max_connections=50),
                "node-3": ClusterNode(node_id="node-3", host="10.0.0.3", port=5432, max_connections=50),
            },
        )
        journal.update_topology_and_recheck(new_topo)

        it2 = journal.begin_cutover(iteration_id="iter-002", target_stage="secondary")
        assert it2.iteration_id == "iter-002"

        journal.record_progress("p-0", cursor="5000", processed_rows=4000, bytes_count=200000)
        journal.record_progress("p-1", cursor="8000", processed_rows=6000, bytes_count=300000)

        journal.scale_worker_budgets({"p-0": 6, "p-1": 8})
        assert journal.get_total_worker_budget() == 14
        assert envelope.is_within(14)

        journal.commit_cutover("iter-002")
        journal.persist()

        journal = PartitionRoutingJournal.load(journal_file)
        child_p1_a = PartitionRoute(
            partition_id="p-1-a",
            target_endpoint="10.0.0.2:5432",
            worker_budget=4,
            is_custom=True,
            metadata={"cdc_channel": "p1_custom"},
        )
        child_p1_b = PartitionRoute(
            partition_id="p-1-b",
            target_endpoint="10.0.0.3:5432",
            worker_budget=4,
            is_custom=True,
            metadata={"cdc_channel": "p1_custom"},
        )
        journal.apply_shard_split("p-1", [child_p1_a, child_p1_b])

        it3 = journal.begin_cutover(iteration_id="iter-003", target_stage="final")
        assert it3.iteration_id == "iter-003"

        assert journal.get_progress("p-0").cursor == "5000"
        assert journal.get_progress("p-0").processed_rows == 5000

        journal.scale_worker_budgets({"p-0": 4, "p-1-a": 4, "p-1-b": 4})
        assert journal.get_total_worker_budget() == 12
        assert envelope.is_within(journal.get_total_worker_budget())

        journal.commit_cutover("iter-003")
        journal.persist()

        final_loaded = PartitionRoutingJournal.load(journal_file)
        assert len(final_loaded.cutover_iterations) == 3
        assert [it.iteration_id for it in final_loaded.cutover_iterations] == ["iter-001", "iter-002", "iter-003"]
        assert all(it.status == CutoverStatus.COMMITTED for it in final_loaded.cutover_iterations)

        assert final_loaded.get_route("p-0").target_endpoint == "10.0.0.1:5432"
        assert final_loaded.get_route("p-1-a").target_endpoint == "10.0.0.2:5432"
        assert final_loaded.get_route("p-1-b").target_endpoint == "10.0.0.3:5432"


def test_scale_worker_budgets_refuses_exceeding_envelope_max():
    envelope = AdaptiveBoundsEnvelope(min_workers=2, start_workers=4, max_workers=6)
    topo = ClusterTopology(
        cluster_id="c1",
        nodes={"n1": ClusterNode(node_id="n1", host="10.0.0.1", port=5432, max_connections=50)},
    )
    journal = PartitionRoutingJournal(envelope=envelope, topology=topo)
    journal.set_route(PartitionRoute(partition_id="part-1", target_endpoint="10.0.0.1:5432", worker_budget=2))

    with pytest.raises(InfeasibleBoundsError):
        journal.scale_worker_budgets({"part-1": 8})


def test_scale_worker_budgets_refuses_below_envelope_min():
    envelope = AdaptiveBoundsEnvelope(min_workers=3, start_workers=4, max_workers=6)
    topo = ClusterTopology(
        cluster_id="c1",
        nodes={"n1": ClusterNode(node_id="n1", host="10.0.0.1", port=5432, max_connections=50)},
    )
    journal = PartitionRoutingJournal(envelope=envelope, topology=topo)
    journal.set_route(PartitionRoute(partition_id="part-1", target_endpoint="10.0.0.1:5432", worker_budget=3))

    with pytest.raises(InfeasibleBoundsError):
        journal.scale_worker_budgets({"part-1": 1})


def test_cutover_iteration_rollback():
    journal = PartitionRoutingJournal()
    it = journal.begin_cutover(iteration_id="cutover-abort")
    assert it.status == CutoverStatus.IN_PROGRESS
    journal.rollback_cutover("cutover-abort", reason="cluster network latency spike")
    assert it.status == CutoverStatus.ROLLED_BACK
    assert it.metadata["rollback_reason"] == "cluster network latency spike"

    with pytest.raises(AdaptiveBoundsError):
        journal.commit_cutover("nonexistent-iteration")

    with pytest.raises(AdaptiveBoundsError):
        journal.rollback_cutover("nonexistent-iteration")


def test_persist_without_path_raises_error():
    journal = PartitionRoutingJournal()
    with pytest.raises(AdaptiveBoundsError):
        journal.persist()


def test_load_nonexistent_file_raises_not_found():
    with pytest.raises(FileNotFoundError):
        PartitionRoutingJournal.load("/path/to/definitely/nonexistent/journal.json")
