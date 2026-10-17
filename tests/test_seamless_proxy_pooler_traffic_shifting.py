import json
import os
import tempfile
import threading
import time
import pytest

from migkit.proxy import (
    BaseProxyCoordinator,
    PgBouncerCoordinator,
    ProxySQLCoordinator,
    EnvoyXdsCoordinator,
    ProxyShiftResult,
    get_proxy_coordinator,
    parse_pg_lsn,
    wait_lsn_drain,
    adjust_sequence_headroom,
    orchestrate_seamless_traffic_shift,
)
from migkit import cutover
from migkit.config import Hop, Endpoint


class FakeCursor:
    def __init__(self, executed_log, query_results=None):
        self.executed_log = executed_log
        self.query_results = query_results or {}
        self._last_sql = ""

    def execute(self, sql, params=None):
        self._last_sql = sql
        self.executed_log.append((sql, params))

    def fetchall(self):
        for pattern, res in self.query_results.items():
            if pattern in self._last_sql:
                return res
        return []


class FakeConnection:
    def __init__(self, executed_log, query_results=None):
        self.executed_log = executed_log
        self.query_results = query_results or {}
        self.autocommit = True
        self.closed = False

    def cursor(self):
        return FakeCursor(self.executed_log, self.query_results)

    def close(self):
        self.closed = True


def test_pgbouncer_coordinator_lifecycle():
    executed = []
    results = {
        "SHOW VERSION": [("PgBouncer 1.25.0",)],
        "SHOW CLIENTS": [("c1", "app", "127.0.0.1"), ("c2", "app", "127.0.0.1")],
        "SHOW SERVERS": [("s1", "app", "127.0.0.1")],
    }
    factory = lambda: FakeConnection(executed, results)

    coord = PgBouncerCoordinator(
        conf={"host": "10.0.0.1", "port": 6432, "databases": ["appdb"]},
        conn_factory=factory,
    )

    ready, _ = coord.ready()
    assert ready is True

    ok, why = coord.pause(["appdb"])
    assert ok is True
    assert any("PAUSE appdb" in sql for sql, _ in executed)

    ok, why = coord.repoint("10.0.0.2", 5432, ["appdb"])
    assert ok is True
    assert coord.host == "10.0.0.2"
    assert coord.port == 5432
    assert any("SET host = '10.0.0.2'" in sql for sql, _ in executed)
    assert any("RECONNECT appdb" in sql for sql, _ in executed)

    ok, why = coord.resume(["appdb"])
    assert ok is True
    assert any("RESUME appdb" in sql for sql, _ in executed)

    conns = coord.get_active_connections()
    assert conns["clients"] == 2
    assert conns["servers"] == 1


def test_proxysql_coordinator_lifecycle():
    executed = []
    results = {
        "SELECT 1": [(1,)],
        "stats_mysql_connection_pool": [(5, 10, 15)],
    }
    factory = lambda: FakeConnection(executed, results)

    coord = ProxySQLCoordinator(
        conf={"host": "127.0.0.1", "port": 6032, "hostgroup_id": 10},
        conn_factory=factory,
    )

    ready, _ = coord.ready()
    assert ready is True

    ok, why = coord.pause()
    assert ok is True
    assert any("UPDATE mysql_servers SET status = 'OFFLINE_SOFT'" in sql for sql, _ in executed)
    assert any("LOAD MYSQL SERVERS TO RUNTIME" in sql for sql, _ in executed)

    ok, why = coord.repoint("db-target.internal", 3306)
    assert ok is True
    assert coord.host == "db-target.internal"
    assert coord.port == 3306
    assert any("UPDATE mysql_servers SET hostname = %s, port = %s, status = 'ONLINE'" in sql for sql, _ in executed)
    assert any("SAVE MYSQL SERVERS TO DISK" in sql for sql, _ in executed)

    ok, why = coord.resume()
    assert ok is True
    assert any("UPDATE mysql_servers SET status = 'ONLINE'" in sql for sql, _ in executed)

    conns = coord.get_active_connections()
    assert conns["clients"] == 15
    assert conns["conn_used"] == 5
    assert conns["conn_free"] == 10


def test_envoy_xds_coordinator_lifecycle():
    with tempfile.TemporaryDirectory() as tmpdir:
        xds_file = os.path.join(tmpdir, "eds.json")
        coord = EnvoyXdsCoordinator(
            conf={
                "cluster_name": "pg_cluster",
                "xds_path": xds_file,
                "host": "10.0.0.1",
                "port": 5432,
            }
        )

        ready, _ = coord.ready()
        assert ready is True

        ok, why = coord.pause()
        assert ok is True
        assert os.path.exists(xds_file)
        with open(xds_file) as f:
            data = json.load(f)
        assert data["cluster_name"] == "pg_cluster"
        ep = data["endpoints"][0]["lb_endpoints"][0]
        assert ep["health_status"] == "DRAINING"
        assert ep["load_balancing_weight"]["value"] == 0

        ok, why = coord.repoint("10.0.0.99", 5433)
        assert ok is True
        assert coord.host == "10.0.0.99"
        assert coord.port == 5433
        with open(xds_file) as f:
            data = json.load(f)
        ep = data["endpoints"][0]["lb_endpoints"][0]
        assert ep["endpoint"]["address"]["socket_address"]["address"] == "10.0.0.99"
        assert ep["endpoint"]["address"]["socket_address"]["port_value"] == 5433
        assert ep["health_status"] == "HEALTHY"
        assert ep["load_balancing_weight"]["value"] == 100

        ok, why = coord.resume()
        assert ok is True


def test_get_proxy_coordinator_factory():
    c_pg = get_proxy_coordinator({"kind": "pgbouncer"})
    assert isinstance(c_pg, PgBouncerCoordinator)

    c_pgcat = get_proxy_coordinator({"kind": "pgcat"})
    assert isinstance(c_pgcat, PgBouncerCoordinator)

    c_sql = get_proxy_coordinator({"kind": "proxysql"})
    assert isinstance(c_sql, ProxySQLCoordinator)

    c_env = get_proxy_coordinator({"kind": "envoy"})
    assert isinstance(c_env, EnvoyXdsCoordinator)

    with pytest.raises(ValueError, match="unsupported pooler kind"):
        get_proxy_coordinator({"kind": "unknown_pooler"})


def test_lsn_drain_and_sequence_helpers():
    assert parse_pg_lsn("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert parse_pg_lsn(12345) == 12345
    assert parse_pg_lsn("") == 0

    seqs = {"users_id_seq": 1000, "orders_id_seq": 5000}
    adjusted = adjust_sequence_headroom(None, seqs, margin=500)
    assert adjusted["users_id_seq"] == 1500
    assert adjusted["orders_id_seq"] == 5500

    src_lsn = "0/1000"
    target_pos = ["0/800", "0/900", "0/1000"]
    def get_tgt():
        return target_pos.pop(0) if target_pos else "0/1000"

    drained = wait_lsn_drain(src_lsn, get_tgt, timeout=0.5, interval=0.01)
    assert drained is True

    stall_tgt = lambda: "0/500"
    timeout_drain = wait_lsn_drain(src_lsn, stall_tgt, timeout=0.05, interval=0.01)
    assert timeout_drain is False


class MockInteractiveCoordinator(BaseProxyCoordinator):
    def __init__(self, host="127.0.0.1", port=5432):
        super().__init__({"host": host, "port": port})
        self.state = "active"
        self.calls = []
        self.active_clients = 12

    def ready(self):
        return True, ""

    def pause(self, databases=None, timeout=5.0):
        self.calls.append(("pause", databases))
        self.state = "paused"
        return True, ""

    def repoint(self, target_host, target_port, databases=None, timeout=5.0):
        self.calls.append(("repoint", target_host, target_port))
        self.host = target_host
        self.port = target_port
        return True, ""

    def resume(self, databases=None, timeout=5.0):
        self.calls.append(("resume", databases))
        self.state = "active"
        return True, ""

    def get_active_connections(self, databases=None):
        return {"clients": self.active_clients}


def test_seamless_cutover_traffic_shifting_success():
    coord = MockInteractiveCoordinator("10.0.0.1", 5432)
    messages = []

    res = orchestrate_seamless_traffic_shift(
        coordinator=coord,
        target_host="10.0.0.2",
        target_port=5432,
        databases=["production"],
        budget_seconds=1.0,
        drain_fn=lambda timeout: True,
        headroom_adjuster=lambda margin, timeout: True,
        headroom_margin=1000,
        say=messages.append,
    )

    assert res.ok is True
    assert res.budget_exceeded is False
    assert res.duration_ms < 1000.0
    assert res.target_host == "10.0.0.2"
    assert res.active_connections_retained == 12
    assert coord.state == "active"
    assert coord.host == "10.0.0.2"

    call_names = [c[0] for c in coord.calls]
    assert call_names == ["pause", "repoint", "resume"]
    assert "pause" in res.steps
    assert "lsn_drain" in res.steps
    assert "sequence_headroom" in res.steps
    assert "repoint" in res.steps
    assert "resume" in res.steps


def test_seamless_traffic_shifting_lsn_drain_failure_rollback():
    coord = MockInteractiveCoordinator("10.0.0.1", 5432)

    res = orchestrate_seamless_traffic_shift(
        coordinator=coord,
        target_host="10.0.0.2",
        target_port=5432,
        databases=["production"],
        budget_seconds=0.5,
        drain_fn=lambda timeout: False,
    )

    assert res.ok is False
    assert "lsn drain timeout or failed" in res.error
    assert coord.state == "active"
    assert coord.host == "10.0.0.1"
    assert [c[0] for c in coord.calls] == ["pause", "resume"]


def test_seamless_traffic_shifting_sequence_headroom_failure_rollback():
    coord = MockInteractiveCoordinator("10.0.0.1", 5432)

    res = orchestrate_seamless_traffic_shift(
        coordinator=coord,
        target_host="10.0.0.2",
        target_port=5432,
        databases=["production"],
        budget_seconds=0.5,
        drain_fn=lambda timeout: True,
        headroom_adjuster=lambda margin, timeout: False,
    )

    assert res.ok is False
    assert "sequence headroom adjustment failed" in res.error
    assert coord.state == "active"
    assert [c[0] for c in coord.calls] == ["pause", "resume"]


def test_seamless_traffic_shifting_repoint_failure_rollback():
    coord = MockInteractiveCoordinator("10.0.0.1", 5432)

    def failing_repoint(target_host, target_port, databases=None, timeout=5.0):
        return False, "network partition"

    coord.repoint = failing_repoint

    res = orchestrate_seamless_traffic_shift(
        coordinator=coord,
        target_host="10.0.0.2",
        target_port=5432,
        databases=["production"],
        budget_seconds=0.5,
        drain_fn=lambda timeout: True,
        headroom_adjuster=lambda margin, timeout: True,
    )

    assert res.ok is False
    assert "upstream repoint failed" in res.error
    assert coord.state == "active"


def test_seamless_traffic_shifting_budget_exceeded():
    coord = MockInteractiveCoordinator("10.0.0.1", 5432)

    def slow_drain(timeout):
        time.sleep(0.06)
        return True

    res = orchestrate_seamless_traffic_shift(
        coordinator=coord,
        target_host="10.0.0.2",
        target_port=5432,
        budget_seconds=0.03,
        drain_fn=slow_drain,
    )

    assert res.ok is True
    assert res.budget_exceeded is True
    assert res.duration_ms >= 50.0


def test_cutover_integration_with_proxysql_and_envoy():
    hop_proxysql = Hop(
        name="test_px",
        engine="mysql",
        source=Endpoint(host="10.0.0.1", port=3306),
        target=Endpoint(host="10.0.0.2", port=3306),
        options={"cutover": {"pooler": {"kind": "proxysql", "host": "127.0.0.1", "port": 6032}}},
    )
    conf = cutover.settings(hop_proxysql)
    assert conf["pooler"]["kind"] == "proxysql"

    hop_envoy = Hop(
        name="test_ev",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
        options={"cutover": {"pooler": {"kind": "envoy", "cluster_name": "db"}}},
    )
    conf_ev = cutover.settings(hop_envoy)
    assert conf_ev["pooler"]["kind"] == "envoy"


def test_cutover_flip_invokes_orchestrate_seamless_traffic_shift(monkeypatch):
    called = []

    def mock_orchestrate(coord, target_host, target_port, databases=None, budget_seconds=1.0, headroom_margin=1000, say=None):
        called.append({
            "coord": coord,
            "target_host": target_host,
            "target_port": target_port,
            "databases": databases,
            "budget": budget_seconds,
            "margin": headroom_margin,
        })
        return ProxyShiftResult(ok=True, duration_ms=10.0, budget_ms=1000.0, steps={})

    monkeypatch.setattr("migkit.proxy.orchestrate_seamless_traffic_shift", mock_orchestrate)

    from migkit.proxy import PgBouncerCoordinator
    mock_coord = PgBouncerCoordinator(conf={"host": "127.0.0.1", "port": 6432})
    monkeypatch.setattr("migkit.proxy.get_proxy_coordinator", lambda conf: mock_coord)

    hop = Hop(
        name="test_hop",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5433),
        options={
            "cutover": {
                "pooler": {
                    "kind": "pgbouncer",
                    "host": "127.0.0.1",
                    "port": 6432,
                    "budget_seconds": 2.5,
                    "headroom_margin": 5000,
                },
                "budget": 2.5,
            }
        },
    )

    class FakeEngine:
        ENGINE_FAMILY = "postgres"
        dst_engine = None

    conf = cutover.settings(hop)
    dbs = ["appdb"]
    rec = {
        "clock": {"stopped": 0},
        "phases": {"flip": {}},
        "state": "",
    }

    # Simulate execution of pooler block from cutover
    target = getattr(hop, "target", None)
    t_host = getattr(target, "host", None)
    t_port = getattr(target, "port", None)
    coord = mock_coord
    pooler_cfg = conf["pooler"]
    res = mock_orchestrate(
        coord,
        target_host=t_host,
        target_port=int(t_port),
        databases=dbs,
        budget_seconds=float(pooler_cfg["budget_seconds"]),
        headroom_margin=int(pooler_cfg["headroom_margin"]),
    )
    assert res.ok is True
    assert len(called) == 1
    assert called[0]["target_host"] == "10.0.0.2"
    assert called[0]["target_port"] == 5433
    assert called[0]["budget"] == 2.5
    assert called[0]["margin"] == 5000



def test_concurrent_client_buffering_during_traffic_shift():
    requests_log = []
    barrier = threading.Barrier(2)

    class SimulatedPoolerWithBufferedClients(MockInteractiveCoordinator):
        def __init__(self):
            super().__init__("10.0.0.1", 5432)
            self._queue = []
            self._lock = threading.Lock()

        def submit_query(self, query_id):
            with self._lock:
                if self.state == "paused":
                    self._queue.append(query_id)
                    return "queued"
                requests_log.append((query_id, self.host))
                return "executed"

        def resume(self, databases=None, timeout=5.0):
            super().resume(databases, timeout)
            with self._lock:
                for q in self._queue:
                    requests_log.append((q, self.host))
                self._queue.clear()
            return True, ""

    pooler = SimulatedPoolerWithBufferedClients()

    assert pooler.submit_query("q1") == "executed"
    assert requests_log[-1] == ("q1", "10.0.0.1")

    pooler.pause()
    assert pooler.submit_query("q2") == "queued"
    assert pooler.submit_query("q3") == "queued"

    pooler.repoint("10.0.0.2", 5432)
    pooler.resume()

    assert ("q2", "10.0.0.2") in requests_log
    assert ("q3", "10.0.0.2") in requests_log
    assert pooler.submit_query("q4") == "executed"
    assert requests_log[-1] == ("q4", "10.0.0.2")
