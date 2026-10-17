import shutil
import tempfile
from pathlib import Path
import pytest

from migkit import decide
from migkit.traffic_state import (
    ActiveLeaseLock,
    CareBadge,
    ClientSession,
    CounterAdjustmentPlanner,
    CutoverIncompleteError,
    CutoverPhase,
    DatabaseEndpoint,
    DrainTimeoutError,
    InvalidPhaseTransitionError,
    LeaseLockError,
    ProofMismatchError,
    ReadRoutingState,
    SessionObservationReport,
    SessionObserver,
    SourceFencedWriteError,
    SplitBrainDetectedError,
    TrafficStateMachine,
    TrafficStateStore,
    UnsafeTestHostError,
    WriteRoutingState,
    counter_headroom_rungs,
    determine_safety_headroom,
    is_safe_test_host,
    observe_endpoint_active_sessions,
    resolve_dns_target,
    validate_test_host,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("traffic_state", "counter_headroom_rungs", "headroom_by_configured_margin", "holds"),
    ("traffic_state", "counter_headroom_rungs", "headroom_by_configured_margin", "refuses"),
    ("traffic_state", "counter_headroom_rungs", "headroom_by_write_rate", "holds"),
    ("traffic_state", "counter_headroom_rungs", "headroom_by_write_rate", "refuses"),
    ("traffic_state", "counter_headroom_rungs", "headroom_by_default_fallback", "holds"),
    ("traffic_state", "counter_headroom_rungs", "headroom_by_default_fallback", "refuses"),
]


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="migkit_traffic_state_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def endpoints():
    source = DatabaseEndpoint(
        name="source",
        host="127.0.0.1",
        port=5432,
        database="production",
        role="source",
        read_only=False,
        current_lsn=5000,
        counters={"orders_seq": 10000, "users_seq": 500},
        write_epoch=0,
    )
    target = DatabaseEndpoint(
        name="target",
        host="10.0.0.42",
        port=5432,
        database="production",
        role="target",
        read_only=True,
        current_lsn=5000,
        counters={"orders_seq": 9000, "users_seq": 450},
        write_epoch=0,
    )
    return source, target


def test_safe_test_hosts_validation():
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("localhost") is True
    assert is_safe_test_host("10.0.0.1") is True
    assert is_safe_test_host("10.0.0.254") is True
    assert is_safe_test_host("0.0.0.0") is True
    assert is_safe_test_host("::1") is True
    assert is_safe_test_host("192.168.1.1") is True
    assert is_safe_test_host("172.16.0.1") is True
    assert is_safe_test_host("8.8.8.8") is True
    assert is_safe_test_host("prod.db.internal") is True
    assert is_safe_test_host("") is False
    assert is_safe_test_host("host with space") is False
    assert is_safe_test_host("host;drop") is False

    assert validate_test_host("192.168.1.50") == "192.168.1.50"
    assert validate_test_host("prod.db.internal") == "prod.db.internal"

    with pytest.raises(UnsafeTestHostError):
        validate_test_host("")
    with pytest.raises(UnsafeTestHostError):
        validate_test_host("  ")


def test_state_machine_full_happy_path(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    c1 = ClientSession(
        client_id="app-1",
        app_name="order-service",
        host="127.0.0.1",
        connected_endpoint="source",
        cached_dns_target="source",
        dns_ttl_remaining_sec=30.0,
        pool_size=10,
        active_connections=5,
    )
    observer.register_client(c1)

    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
        epoch=1,
        safety_headroom=1000,
    )

    assert sm.current_phase == CutoverPhase.INIT
    assert sm.active_writer == "source"

    # Step 1: PAUSE
    badge = sm.step_pause()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.PAUSE

    # Step 2: DRAIN
    badge = sm.step_drain(timeout_sec=2.0)
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.DRAIN

    # Step 3: FENCE
    badge = sm.step_fence()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.FENCE
    assert source.read_only is True
    assert sm.write_routing == WriteRoutingState.FENCED
    assert sm.active_writer == "none"

    # Step 4: PROOF
    badge = sm.step_proof()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.PROOF

    # Step 5: COUNTER ADJUSTMENT
    badge = sm.step_counter_adjustment()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.COUNTER_ADJUSTMENT
    assert target.get_counter("orders_seq") == 11000  # max(10000, 9000) + 1000
    assert target.get_counter("users_seq") == 1500  # max(500, 450) + 1000

    # Step 6: ROUTING
    badge = sm.step_routing()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.ROUTING
    assert target.read_only is False
    assert target.write_epoch == 1
    assert sm.write_routing == WriteRoutingState.TARGET
    assert sm.active_writer == "target"

    # Step 7: RESUME
    badge = sm.step_resume()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.RESUME

    # Step 8: CLIENT ACK
    # Before client acknowledges, completion is rejected
    badge = sm.step_client_ack()
    assert badge == CareBadge.ACTION

    with pytest.raises(CutoverIncompleteError):
        sm.complete_cutover()

    # Client now drains source pool and acknowledges target
    observer.acknowledge_client("app-1", epoch=1, connected_endpoint="target")
    observer.update_heartbeat("app-1", active_connections=5, pool_size=10, dns_ttl_remaining_sec=0.0)

    # Now completion succeeds!
    badge = sm.complete_cutover()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.COMPLETED


def test_durable_resume_o1(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    sm1 = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
        epoch=2,
        safety_headroom=500,
    )

    sm1.step_pause()
    sm1.step_drain()
    sm1.step_fence()
    sm1.step_proof()
    sm1.step_counter_adjustment()
    sm1.step_routing()

    assert sm1.current_phase == CutoverPhase.ROUTING
    assert sm1.active_writer == "target"
    assert sm1.target_promoted is True

    # Simulate crash and resume with fresh instance loading from same directory
    source_restored = DatabaseEndpoint(
        name="source",
        host="127.0.0.1",
        port=5432,
        database="production",
        role="source",
        read_only=True,
    )
    target_restored = DatabaseEndpoint(
        name="target",
        host="10.0.0.42",
        port=5432,
        database="production",
        role="target",
        read_only=False,
    )

    sm2 = TrafficStateMachine(
        state_dir=temp_dir,
        source=source_restored,
        target=target_restored,
        session_observer=observer,
    )

    assert sm2.current_phase == CutoverPhase.ROUTING
    assert sm2.epoch == 2
    assert sm2.active_writer == "target"
    assert sm2.target_promoted is True
    assert sm2.source_fenced is True
    assert sm2.counter_adjustments["orders_seq"] == 10500


def test_split_write_prevention_cached_connections(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    c1 = ClientSession(
        client_id="app-stale-writer",
        app_name="checkout",
        host="10.0.0.10",
        connected_endpoint="source",
        cached_dns_target="source",
        pool_size=5,
        active_connections=3,
    )
    observer.register_client(c1)

    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
        epoch=1,
    )

    # Initial write to source succeeds before cutover
    res = source.execute_write("INSERT INTO orders VALUES (101)", epoch=0)
    assert res["status"] == "committed"

    # Execute cutover steps up to routing (with replication catchup)
    sm.step_pause()
    sm.step_drain(catchup_fn=lambda: setattr(target, "current_lsn", source.current_lsn))
    sm.step_fence()
    sm.step_proof()
    sm.step_counter_adjustment()
    sm.step_routing()

    # Network failure simulation: Client C1 holds cached connection to source
    # and attempts write without knowing routing changed to target
    with pytest.raises(SourceFencedWriteError) as exc_info:
        source.execute_write("INSERT INTO orders VALUES (102)", epoch=0)
    assert "database is read-only (fenced)" in str(exc_info.value)

    # Target accepts writes
    res_tgt = target.execute_write("INSERT INTO orders VALUES (102)", epoch=1)
    assert res_tgt["status"] == "committed"
    assert res_tgt["endpoint"] == "target"

    # Verify no split writes occurred: source orders still only has the 1 initial write
    assert source.applied_writes == ["INSERT INTO orders VALUES (101)"]
    assert target.applied_writes == ["INSERT INTO orders VALUES (102)"]


def test_restart_reconciliation_active_writer(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
        epoch=1,
    )

    sm.step_pause()
    sm.step_drain()
    sm.step_fence()
    sm.step_proof()
    sm.step_counter_adjustment()
    sm.step_routing()

    # Coordinator crashes immediately after routing
    # On restart, instantiate fresh state machine
    sm_recovered = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
    )

    active_writer, badge = sm_recovered.reconcile_active_writer()
    assert active_writer == "target"
    assert badge == CareBadge.PASS
    assert sm_recovered.write_routing == WriteRoutingState.TARGET


def test_restart_reconciliation_detects_split_brain(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
    )

    # Abnormal split brain scenario: both source and target are writable
    source.set_read_only(False)
    target.set_read_only(False)

    with pytest.raises(SplitBrainDetectedError) as exc:
        sm.reconcile_active_writer()
    assert "split-brain detected" in str(exc.value)

    # Both endpoints must be immediately fenced
    assert source.read_only is True
    assert target.read_only is True
    assert sm.active_writer == "none"
    assert sm.write_routing == WriteRoutingState.FENCED
    assert sm.last_care_badge == CareBadge.ACTION


def test_api_success_or_promotion_alone_not_completion(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    c1 = ClientSession(
        client_id="client-payment",
        app_name="payment",
        host="127.0.0.1",
        connected_endpoint="source",
        cached_dns_target="source",
        pool_size=10,
        active_connections=4,
    )
    observer.register_client(c1)

    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
        epoch=5,
    )

    sm.step_pause()
    sm.step_drain()
    sm.step_fence()
    sm.step_proof()
    sm.step_counter_adjustment()
    sm.step_routing()
    sm.step_resume()
    sm.step_client_ack()

    # DB is promoted, but client has NOT acknowledged and has connections to source
    assert target.read_only is False
    assert sm.target_promoted is True

    with pytest.raises(CutoverIncompleteError) as exc:
        sm.complete_cutover()
    assert "API success or database promotion alone is not application cutover completion" in str(exc.value)
    assert sm.last_care_badge == CareBadge.ACTION

    # Acknowledge client
    observer.acknowledge_client("client-payment", epoch=5, connected_endpoint="target")
    observer.update_heartbeat("client-payment", active_connections=4, pool_size=10)

    # Now cutover completes
    badge = sm.complete_cutover()
    assert badge == CareBadge.PASS
    assert sm.current_phase == CutoverPhase.COMPLETED


def test_vitess_read_write_routing_separation(temp_dir, endpoints):
    source, target = endpoints
    observer = SessionObserver()
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        session_observer=observer,
    )

    assert sm.read_routing == ReadRoutingState.SOURCE
    assert sm.write_routing == WriteRoutingState.SOURCE

    # Vitess MoveTables allows switching reads first
    sm.shift_read_routing(ReadRoutingState.REPLICA)
    assert sm.read_routing == ReadRoutingState.REPLICA
    assert sm.write_routing == WriteRoutingState.SOURCE

    sm.shift_read_routing(ReadRoutingState.SPLIT)
    assert sm.read_routing == ReadRoutingState.SPLIT

    sm.shift_read_routing(ReadRoutingState.TARGET)
    assert sm.read_routing == ReadRoutingState.TARGET
    assert sm.write_routing == WriteRoutingState.SOURCE  # writes still on source!


def test_session_observer_stale_dns_and_pools():
    observer = SessionObserver()
    c1 = ClientSession(
        client_id="app-stale-dns",
        app_name="web",
        host="127.0.0.1",
        connected_endpoint="target",
        cached_dns_target="source",
        dns_ttl_remaining_sec=45.0,
        pool_size=5,
        active_connections=0,
        acked_epoch=1,
    )
    observer.register_client(c1)

    # When writes are fenced and client has stale DNS, report care badge REVIEW
    rep = observer.observe(
        target_epoch=1,
        read_routing=ReadRoutingState.TARGET,
        write_routing=WriteRoutingState.FENCED,
    )
    assert rep.care_badge == CareBadge.REVIEW
    assert "stale_dns=1" in rep.format_unix_report()

    # When client retains open pool to source during TARGET write routing, care badge is ACTION
    c2 = ClientSession(
        client_id="app-stale-pool",
        app_name="worker",
        host="10.0.0.15",
        connected_endpoint="source",
        cached_dns_target="target",
        dns_ttl_remaining_sec=0.0,
        pool_size=10,
        active_connections=3,
        acked_epoch=1,
    )
    observer.register_client(c2)

    rep2 = observer.observe(
        target_epoch=1,
        read_routing=ReadRoutingState.TARGET,
        write_routing=WriteRoutingState.TARGET,
    )
    assert rep2.care_badge == CareBadge.ACTION
    assert "app-stale-pool" in rep2.stale_pool_clients


def test_drain_timeout(temp_dir, endpoints):
    source, target = endpoints
    # Source LSN ahead of target
    source.current_lsn = 8000
    target.current_lsn = 7000

    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
    )

    sm.step_pause()
    with pytest.raises(DrainTimeoutError):
        sm.step_drain(timeout_sec=0.05)
    assert sm.current_phase == CutoverPhase.FAILED
    assert sm.last_care_badge == CareBadge.ACTION


def test_proof_failure_when_target_lags(temp_dir, endpoints):
    source, target = endpoints
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
    )

    sm.step_pause()
    sm.step_drain()
    sm.step_fence()

    # Simulate target lagging at proof time
    target.current_lsn = source.current_lsn - 10
    with pytest.raises(ProofMismatchError):
        sm.step_proof()
    assert sm.current_phase == CutoverPhase.FAILED


def test_active_lease_lock_mutual_exclusion(temp_dir):
    lock_file = temp_dir / ".run.lock"
    lease1 = ActiveLeaseLock(lock_file, term_seconds=10.0)
    assert lease1.acquire() is True

    # Second acquisition fails
    lease2 = ActiveLeaseLock(lock_file, term_seconds=10.0)
    with pytest.raises(LeaseLockError):
        lease2.acquire()

    lease1.heartbeat()
    lease1.release()

    # Now lease2 can acquire
    assert lease2.acquire() is True
    lease2.release()


def test_rollback_reverts_state_safely(temp_dir, endpoints):
    source, target = endpoints
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
        epoch=1,
    )

    sm.step_pause()
    sm.step_drain()
    sm.step_fence()
    sm.step_proof()
    sm.step_counter_adjustment()
    sm.step_routing()

    assert sm.active_writer == "target"
    assert target.read_only is False

    # Abort cutover: rollback
    badge = sm.rollback(reason="application latency spike on target")
    assert badge == CareBadge.REVIEW
    assert sm.current_phase == CutoverPhase.ROLLED_BACK
    assert sm.active_writer == "source"
    assert source.read_only is False
    assert target.read_only is True
    assert sm.write_routing == WriteRoutingState.SOURCE


def test_pure_unix_ascii_formatting():
    rep = SessionObservationReport(
        total_clients=3,
        outstanding_source_sessions=["app-1"],
        stale_dns_clients=["app-2"],
        stale_pool_clients=["app-1"],
        unacknowledged_clients=["app-3"],
        read_routing=ReadRoutingState.TARGET,
        write_routing=WriteRoutingState.TARGET,
        care_badge=CareBadge.ACTION,
        summary="action required: clients retain connections/pools to source",
    )
    out = rep.format_unix_report()
    # Must be 100% pure ASCII
    out.encode("ascii")
    assert "[ACTION] SESSION-REPORT:" in out
    assert "outstanding_source=1: app-1" in out
    assert "stale_dns=1: app-2" in out
    assert "stale_pools=1: app-1" in out
    assert "unacknowledged=1: app-3" in out
    # Ensure zero emojis or decorative glyphs
    assert "\u2705" not in out
    assert "\u274c" not in out
    assert "\u26a0" not in out


def test_invalid_phase_transitions(temp_dir, endpoints):
    source, target = endpoints
    sm = TrafficStateMachine(
        state_dir=temp_dir,
        source=source,
        target=target,
    )
    assert sm.current_phase == CutoverPhase.INIT

    # Cannot skip directly to FENCE without PAUSE and DRAIN
    with pytest.raises(InvalidPhaseTransitionError):
        sm.step_fence()

    # Cannot skip directly to ROUTING
    with pytest.raises(InvalidPhaseTransitionError):
        sm.step_routing()

    # Cannot skip directly to PROOF
    with pytest.raises(InvalidPhaseTransitionError):
        sm.step_proof()


def test_counter_adjustment_planner_standalone(endpoints):
    _, target = endpoints
    src_counters = {"t1": 100, "t2": 500}
    tgt_counters = {"t1": 150, "t2": 400}

    adj = CounterAdjustmentPlanner.plan_adjustments(src_counters, tgt_counters, safety_headroom=200)
    assert adj["t1"] == 350  # max(100, 150) + 200
    assert adj["t2"] == 700  # max(500, 400) + 200

    CounterAdjustmentPlanner.apply_adjustments(target, adj)
    assert target.get_counter("t1") == 350
    assert target.get_counter("t2") == 700


def test_traffic_state_store_direct(temp_dir):
    store = TrafficStateStore(temp_dir)
    assert store.read_state() is None

    test_data = {"phase": "DRAIN", "epoch": 3}
    store.write_state(test_data)
    read_back = store.read_state()
    assert read_back == test_data

    store.append_journal({"event": "TEST_EVENT", "status": "OK"})
    with open(store.journal_file, "r") as f:
        lines = f.readlines()
    assert len(lines) == 1
    assert "TEST_EVENT" in lines[0]


def test_every_decide_rung_is_reached_on_both_branches():
    """Exercise both holds and refuses for counter_headroom_rungs under watched()."""
    with watched() as seen:
        rungs = counter_headroom_rungs()

        # 1. headroom_by_configured_margin
        # holds: explicit positive headroom
        f_cfg_hold = {"safety_headroom": 500, "allow_default_fallback": True}
        c1 = decide.climb("t1", rungs, f_cfg_hold)
        assert c1.rung.name == "headroom_by_configured_margin"

        # refuses: no explicit headroom
        f_cfg_ref = {"safety_headroom": 0, "write_rate": 50.0, "restart_gap_sec": 2.0}
        c2 = decide.climb("t2", rungs, f_cfg_ref)
        assert c2.rung.name == "headroom_by_write_rate"

        # 2. headroom_by_write_rate
        # holds: positive write_rate and restart_gap
        f_rate_hold = {"safety_headroom": None, "write_rate": 100.0, "restart_gap_sec": 3.0}
        c3 = decide.climb("t3", rungs, f_rate_hold)
        assert c3.rung.name == "headroom_by_write_rate"

        # refuses: write_rate = 0
        f_rate_ref = {"safety_headroom": None, "write_rate": 0.0, "restart_gap_sec": 0.0, "allow_default_fallback": True}
        c4 = decide.climb("t4", rungs, f_rate_ref)
        assert c4.rung.name == "headroom_by_default_fallback"

        # 3. headroom_by_default_fallback
        # holds: allow_default_fallback is True
        f_def_hold = {"safety_headroom": None, "write_rate": 0.0, "allow_default_fallback": True}
        c5 = decide.climb("t5", rungs, f_def_hold)
        assert c5.rung.name == "headroom_by_default_fallback"

        # refuses: allow_default_fallback is False
        f_def_ref = {"safety_headroom": None, "write_rate": 0.0, "allow_default_fallback": False}
        c6 = decide.climb("t6", rungs, f_def_ref)
        assert c6.rung is None

    assert set(branch(seen, "headroom_by_configured_margin")) == {HOLDS, REFUSES}
    assert set(branch(seen, "headroom_by_write_rate")) == {HOLDS, REFUSES}
    assert set(branch(seen, "headroom_by_default_fallback")) == {HOLDS, REFUSES}


def test_real_dns_observation_probe():
    """Verify real DNS observation returns target, source, and measures TTL."""
    dns_map = {
        "db.company.com": ["10.0.0.42"],
        "stale.company.com": ["127.0.0.1"],
    }
    resolver = lambda host: dns_map.get(host, [])

    res_tgt, ttl_tgt = resolve_dns_target("db.company.com", target_host="10.0.0.42", source_host="127.0.0.1", resolver_fn=resolver)
    assert res_tgt == "target"
    assert ttl_tgt == 0.0

    res_src, ttl_src = resolve_dns_target("stale.company.com", target_host="10.0.0.42", source_host="127.0.0.1", resolver_fn=resolver)
    assert res_src == "source"
    assert ttl_src > 0.0


def test_real_database_session_observation():
    """Verify querying active database sessions through SQL probe function."""
    ep = DatabaseEndpoint(name="pg_src", host="127.0.0.1", port=5432, database="prod")
    fake_query = lambda sql: [{"count": 8}] if "pg_stat_activity" in sql else []
    cnt = observe_endpoint_active_sessions(ep, query_fn=fake_query)
    assert cnt == 8
    assert ep.active_sessions == 8


def test_dynamic_headroom_derivation():
    """Verify CounterAdjustmentPlanner derives headroom dynamically."""
    src = {"seq": 1000}
    tgt = {"seq": 900}
    # 1. Configured margin
    adj1 = CounterAdjustmentPlanner.plan_adjustments(src, tgt, safety_headroom=250)
    assert adj1["seq"] == 1250

    # 2. Rate-based headroom (rate=100/s, restart_gap=5s -> 500 * 1.1 = 550)
    adj2 = CounterAdjustmentPlanner.plan_adjustments(src, tgt, write_rate=100.0, restart_gap_sec=5.0)
    assert adj2["seq"] == 1550


def test_state_machine_transition_to_macro(temp_dir, endpoints):
    """Verify sm.transition_to macro calls for autopilot/cutover integration."""
    source, target = endpoints
    sm = TrafficStateMachine(state_dir=temp_dir, source=source, target=target, epoch=3)
    sm.transition_to(CutoverPhase.DRAIN)
    assert sm.current_phase == CutoverPhase.DRAIN
    sm.transition_to(CutoverPhase.ROUTING)
    assert sm.current_phase == CutoverPhase.ROUTING
    assert target.read_only is False
    assert sm.active_writer == "target"
