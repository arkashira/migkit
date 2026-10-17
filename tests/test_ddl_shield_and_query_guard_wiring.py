import threading
import time

import pytest

from migkit import decide, tailctl
from migkit.autopilot import autopilot_cutover_gate_rungs
from migkit.ddl_shield import (
    CDCReplicationApplier,
    DDLBarrierError,
    OnlineDDLShield,
    TransactionalFence,
)
from migkit.merkle import VerificationSliceManager
from migkit.query_guard import ExecutionPlan, QueryOptimizerParityGuard, QueryPattern


def test_slice_manager_lives_in_merkle():
    from migkit import ddl_shield
    assert ddl_shield.VerificationSliceManager is VerificationSliceManager


def test_postgres_fence_bounds_lock_and_statement_time():
    sent = []
    with TransactionalFence(target_executor=sent.append, dialect="postgres") as f:
        f.execute("ALTER TABLE t ADD COLUMN c INT")
    assert sent[0] == "BEGIN"
    assert "SET LOCAL lock_timeout = '2s'" in sent
    assert "SET LOCAL statement_timeout = '5s'" in sent
    assert sent[-1] == "COMMIT"


def test_mysql_fence_bounds_lock_wait():
    sent = []
    with TransactionalFence(target_executor=sent.append, dialect="mysql"):
        pass
    assert "SET SESSION lock_wait_timeout = 2" in sent


def test_pause_waits_for_in_flight_dml_to_drain():
    release = threading.Event()
    applier = CDCReplicationApplier(downstream=lambda e: release.wait(5))
    t = threading.Thread(target=applier.apply_dml, args=("e1",))
    t.start()
    while applier.in_flight == 0:
        time.sleep(0.001)
    paused = []
    p = threading.Thread(target=lambda: (applier.pause(), paused.append(1)))
    p.start()
    time.sleep(0.05)
    assert not paused
    release.set()
    t.join()
    p.join()
    assert paused and applier.is_paused
    applier.resume()


def test_pause_refuses_when_dml_never_drains():
    release = threading.Event()
    applier = CDCReplicationApplier(downstream=lambda e: release.wait(5), drain_timeout_s=0.05)
    t = threading.Thread(target=applier.apply_dml, args=("e1",))
    t.start()
    while applier.in_flight == 0:
        time.sleep(0.001)
    with pytest.raises(DDLBarrierError):
        applier.pause()
    assert not applier.is_paused
    release.set()
    t.join()


def test_apply_unit_routes_ddl_through_the_shield():
    ran = []
    shield = OnlineDDLShield(source_dialect="mysql", target_dialect="postgres",
                             target_executor=ran.append)

    class Eng:
        ddl_shield = shield

    seen = []
    changes = [
        {"op": "insert", "table": "users", "key": {"id": 1}, "values": {"id": 1}},
        {"type": "DDL", "sql": "ALTER TABLE users ADD COLUMN phone VARCHAR(20)"},
    ]
    tailctl.apply_unit(Eng(), "db", {"pos": 1}, changes, lambda c: seen.extend(c) or len(c))
    assert any('ADD COLUMN "phone"' in s for s in ran)
    assert len(seen) == 1 and seen[0]["op"] == "insert"


def test_query_guard_thresholds_come_from_a_rung():
    assert decide.query_guard_thresholds().latency_threshold == 2.0
    strict = decide.query_guard_thresholds(options={"profile": "strict"})
    assert (strict.latency_threshold, strict.sample_size) == (1.25, 100)
    g = QueryOptimizerParityGuard(options={"profile": "permissive"})
    assert g.latency_threshold == 3.5 and g.sample_size == 20


def test_explain_never_analyzes_dml_and_never_needs_rollback():
    calls = []

    class Eng:
        dialect = "postgres"

        def explain(self, db, sql, rollback=False):
            calls.append((sql, rollback))
            return None

    g = QueryOptimizerParityGuard(target_eng=Eng(), db="d")
    g.replay_on_target(QueryPattern("q", "DELETE FROM t WHERE id = 1", "x"), analyze=True, read_only=False)
    assert calls == [("EXPLAIN DELETE FROM t WHERE id = 1", False)]


def test_explain_timeout_is_capped_at_two_seconds():
    got = {}

    class Eng:
        dialect = "postgres"

        def explain(self, db, sql, rollback=False, timeout_ms=None, read_only=False):
            got.update(timeout_ms=timeout_ms, read_only=read_only)

    g = QueryOptimizerParityGuard(target_eng=Eng(), db="d", options={"explain_timeout_ms": 60000})
    g.replay_on_target(QueryPattern("q", "SELECT 1", "x"))
    assert got == {"timeout_ms": 2000, "read_only": True}


def test_cutover_gate_refuses_on_query_regression():
    facts = {"zero_drift": True, "auto_approve": True, "query_parity_ok": False}
    assert decide.climb("t", autopilot_cutover_gate_rungs(), facts).rung is None or \
        decide.climb("t", autopilot_cutover_gate_rungs(), facts).rung.name != "certify_zero_drift_cutover_admit"
    facts["query_parity_ok"] = True
    assert decide.climb("t", autopilot_cutover_gate_rungs(), facts).rung.name == "certify_zero_drift_cutover_admit"


def test_autopilot_audits_the_workload_before_the_gate():
    from migkit.autopilot import AutopilotOrchestrator
    bad = ExecutionPlan("q", execution_time_ms=100.0,
                        scans=[{"table": "t", "scan_type": "Seq Scan"}])
    pat = QueryPattern("q", "SELECT * FROM t WHERE a = 1", "x", mean_time_ms=1.0,
                       tables=["t"], filter_columns=["a"], source_plan=None)
    g = QueryOptimizerParityGuard(options={"workload": [pat]})
    g.replay_on_target = lambda p, **k: bad
    holder = type("H", (), {})()
    holder.query_guard = g
    ok = AutopilotOrchestrator._query_parity_ok(holder)
    assert g.last_report is not None
    assert ok is g.last_report.can_cutover
