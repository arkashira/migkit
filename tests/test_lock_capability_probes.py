import threading
import time
import pytest

from migkit.lock_capability import (
    CoordinationStrategy,
    FencedCoordinationLease,
    FencedOutError,
    InMemoryLeaseStore,
    LeaseAcquisitionError,
    LeaseLostError,
    LockCapabilities,
    LockCapabilityProber,
    MissingCoordinationPrimitiveError,
    choose_coordination_lease,
    lock_capability_rungs,
    decide_lock_capability,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("lock_capability", "lock_capability_rungs", "session_advisory_lock", "holds"),
    ("lock_capability", "lock_capability_rungs", "session_advisory_lock", "refuses"),
    ("lock_capability", "lock_capability_rungs", "transactional_fence_lock", "holds"),
    ("lock_capability", "lock_capability_rungs", "transactional_fence_lock", "refuses"),
    ("lock_capability", "lock_capability_rungs", "durable_lease_table", "holds"),
    ("lock_capability", "lock_capability_rungs", "durable_lease_table", "refuses"),
    ("lock_capability", "lock_capability_rungs", "external_fenced_lease", "holds"),
    ("lock_capability", "lock_capability_rungs", "external_fenced_lease", "refuses"),
    ("lock_capability", "lock_capability_rungs", "refuse_uncoordinated_concurrency", "holds"),
]


def test_standard_postgres_prober_discovers_all_primitives():
    queries = []

    def pg_executor(sql, params=None):
        queries.append(sql.strip())
        normalized = sql.lower()
        if "pg_try_advisory_lock" in normalized:
            return [(True,)]
        if "pg_advisory_unlock" in normalized:
            return [(True,)]
        if "lock table" in normalized:
            return []
        if "create temp table _migkit_tx_probe" in normalized:
            return []
        if "relname = '_migkit_tx_probe'" in normalized:
            return [(0,)]
        if "lock_timeout" in normalized or "statement_timeout" in normalized:
            return []
        if "listen" in normalized or "unlisten" in normalized:
            return []
        if "pg_locks" in normalized or "pg_stat_activity" in normalized:
            return [(0,)]
        if "pg_replication_slots" in normalized:
            return [(0,)]
        return []

    prober = LockCapabilityProber(executor=pg_executor, endpoint_id="10.0.0.12:5432")
    caps = prober.probe_all()

    assert caps.advisory_locks is True
    assert caps.table_locks is True
    assert caps.transactional_ddl is True
    assert caps.session_timeouts is True
    assert caps.notification_channels is True
    assert caps.catalog_inspection is True
    assert caps.replication_slots is True
    assert caps.coordination_strategy == CoordinationStrategy.ADVISORY_LOCK
    assert caps.can_guarantee_mutual_exclusion is True


def test_cockroachdb_lacking_advisory_locks_prober():
    def crdb_executor(sql, params=None):
        normalized = sql.lower()
        if "pg_try_advisory_lock" in normalized:
            raise RuntimeError("function pg_try_advisory_lock() does not exist in CockroachDB")
        if "lock table" in normalized:
            raise RuntimeError("LOCK TABLE is not supported")
        if "create temp table _migkit_tx_probe" in normalized:
            return []
        if "relname = '_migkit_tx_probe'" in normalized:
            return [(0,)]
        if "lock_timeout" in normalized:
            raise RuntimeError('unrecognized configuration parameter "lock_timeout"')
        if "listen" in normalized:
            raise RuntimeError("LISTEN is not supported")
        if "pg_locks" in normalized:
            raise RuntimeError("relation pg_locks does not exist")
        if "pg_stat_activity" in normalized:
            return [(0,)]
        if "pg_replication_slots" in normalized:
            raise RuntimeError("relation pg_replication_slots does not exist")
        return []

    prober = LockCapabilityProber(executor=crdb_executor, endpoint_id="10.0.0.25:26257")
    caps = prober.probe_all()

    assert caps.advisory_locks is False
    assert caps.table_locks is False
    assert caps.transactional_ddl is True
    assert caps.session_timeouts is False
    assert caps.notification_channels is False
    assert caps.catalog_inspection is False
    assert caps.replication_slots is False
    assert caps.coordination_strategy == CoordinationStrategy.FENCED_LEASE
    assert caps.can_guarantee_mutual_exclusion is True


def test_missing_coordination_primitives_fails_fast():
    def broken_executor(sql, params=None):
        raise RuntimeError("read-only transaction or unsupported primitive")

    prober = LockCapabilityProber(executor=broken_executor, endpoint_id="10.0.0.99:5432")
    caps = prober.probe_all()

    assert caps.advisory_locks is False
    assert caps.table_locks is False
    assert caps.transactional_ddl is False
    assert caps.can_guarantee_mutual_exclusion is False
    assert caps.coordination_strategy == CoordinationStrategy.NONE

    with pytest.raises(MissingCoordinationPrimitiveError) as exc_info:
        prober.choose_coordination_lease(resource_id="orders_hop")
    assert "mutual exclusion cannot be guaranteed" in str(exc_info.value)

    with pytest.raises(MissingCoordinationPrimitiveError):
        choose_coordination_lease(caps, resource_id="orders_hop")


def test_concurrent_migrators_mutual_exclusion_on_pgwire_without_advisory_locks():
    caps = LockCapabilities(
        advisory_locks=False,
        table_locks=False,
        transactional_ddl=True,
        session_timeouts=False,
        notification_channels=False,
        catalog_inspection=False,
        replication_slots=False,
    )
    store = InMemoryLeaseStore()
    now_ts = 1000.0

    lease1 = FencedCoordinationLease(
        resource_id="migration:users",
        holder_id="migrator-worker-1@10.0.0.10",
        lease_ttl=10.0,
        store=store,
        capabilities=caps,
        clock=lambda: now_ts,
    )
    lease2 = FencedCoordinationLease(
        resource_id="migration:users",
        holder_id="migrator-worker-2@10.0.0.11",
        lease_ttl=10.0,
        store=store,
        capabilities=caps,
        clock=lambda: now_ts,
    )

    assert lease1.acquire() is True
    assert lease1.fence_token == 1
    assert lease1.assert_held() == 1

    with pytest.raises(LeaseAcquisitionError) as exc:
        lease2.acquire()
    assert "actively held" in str(exc.value)

    assert lease1.renew() is True
    assert lease1.fence_token == 1


def test_stale_owner_takeover_is_fenced_and_deterministic():
    now_box = [1000.0]

    def current_time():
        return now_box[0]

    caps = LockCapabilities(
        advisory_locks=False,
        table_locks=True,
        transactional_ddl=True,
        session_timeouts=False,
        notification_channels=False,
    )
    store = InMemoryLeaseStore()

    migrator_a = FencedCoordinationLease(
        resource_id="hop:catalog_sync",
        holder_id="migrator-a@10.0.0.5",
        lease_ttl=5.0,
        store=store,
        capabilities=caps,
        clock=current_time,
    )
    migrator_b = FencedCoordinationLease(
        resource_id="hop:catalog_sync",
        holder_id="migrator-b@10.0.0.6",
        lease_ttl=5.0,
        store=store,
        capabilities=caps,
        clock=current_time,
    )

    assert migrator_a.acquire() is True
    assert migrator_a.fence_token == 1

    now_box[0] += 6.0

    assert migrator_b.acquire() is True
    assert migrator_b.fence_token == 2
    assert migrator_b.takeover_from == "migrator-a@10.0.0.5"

    with pytest.raises(FencedOutError) as exc:
        migrator_a.renew()
    assert "taken over" in str(exc.value)

    with pytest.raises(FencedOutError):
        migrator_a.assert_held()

    now_box[0] += 2.0
    assert migrator_b.renew() is True
    assert migrator_b.fence_token == 2
    assert migrator_b.assert_held() == 2


def test_concurrent_acquisition_race_deterministic_winner():
    caps = LockCapabilities(
        advisory_locks=False,
        table_locks=True,
        transactional_ddl=True,
        session_timeouts=False,
        notification_channels=False,
    )
    store = InMemoryLeaseStore()
    num_threads = 16
    barrier = threading.Barrier(num_threads)
    results = []
    lock = threading.Lock()

    def run_worker(worker_id):
        lease = FencedCoordinationLease(
            resource_id="hop:concurrent_race",
            holder_id=f"worker-{worker_id}@10.0.0.20",
            lease_ttl=30.0,
            store=store,
            capabilities=caps,
        )
        barrier.wait()
        try:
            acquired = lease.acquire(raise_on_failure=True)
            with lock:
                results.append((worker_id, acquired, lease.fence_token))
        except LeaseAcquisitionError:
            with lock:
                results.append((worker_id, False, None))

    threads = [threading.Thread(target=run_worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    winners = [r for r in results if r[1] is True]
    losers = [r for r in results if r[1] is False]

    assert len(winners) == 1
    assert len(losers) == num_threads - 1
    assert winners[0][2] == 1


def test_graceful_release_and_monotonic_fence_progression():
    store = InMemoryLeaseStore()
    caps = LockCapabilities(
        advisory_locks=False,
        table_locks=True,
        transactional_ddl=True,
        session_timeouts=True,
        notification_channels=True,
    )

    lease1 = FencedCoordinationLease(
        resource_id="res:sequence",
        holder_id="holder1@10.0.0.30",
        lease_ttl=10.0,
        store=store,
        capabilities=caps,
    )
    lease2 = FencedCoordinationLease(
        resource_id="res:sequence",
        holder_id="holder2@10.0.0.31",
        lease_ttl=10.0,
        store=store,
        capabilities=caps,
    )

    with lease1:
        assert lease1.fence_token == 1
        assert lease1.assert_held() == 1
        with pytest.raises(LeaseAcquisitionError):
            lease2.acquire()

    with lease2:
        assert lease2.fence_token == 2
        assert lease2.assert_held() == 2

    assert lease1.fence_token == 1
    with pytest.raises(LeaseLostError):
        lease1.assert_held()


def test_lock_capability_rungs_under_watched():
    with watched():
        # 1. session_advisory_lock
        c1 = decide_lock_capability({
            "supports_advisory_locks": True,
            "probe_successful": True,
        })
        assert c1.rung.name == "session_advisory_lock"

        # 2. transactional_fence_lock
        c2 = decide_lock_capability({
            "supports_advisory_locks": False,
            "supports_transactional_fences": True,
        })
        assert c2.rung.name == "transactional_fence_lock"

        # 3. durable_lease_table
        c3 = decide_lock_capability({
            "supports_advisory_locks": False,
            "supports_transactional_fences": False,
            "supports_table_leases": True,
        })
        assert c3.rung.name == "durable_lease_table"

        # 4. external_fenced_lease
        c4 = decide_lock_capability({
            "supports_advisory_locks": False,
            "supports_transactional_fences": False,
            "supports_table_leases": False,
            "external_coordinator_available": True,
        })
        assert c4.rung.name == "external_fenced_lease"

        # 5. refuse_uncoordinated_concurrency
        c5 = decide_lock_capability({
            "supports_advisory_locks": False,
            "supports_transactional_fences": False,
            "supports_table_leases": False,
            "external_coordinator_available": False,
        })
        assert c5.rung.name == "refuse_uncoordinated_concurrency"
