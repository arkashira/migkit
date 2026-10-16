import time
import pytest
from click.testing import CliRunner

from migkit import cli, cutover
from migkit.config import Endpoint, Hop
from migkit.proxy import BaseProxyCoordinator
from migkit.reverse import (
    CDCEvent,
    FailbackResult,
    OriginMarker,
    ReverseFailbackController,
    ReverseFailbackLoop,
    clear_reverse_loops,
    format_lsn,
    get_reverse_loop,
    parse_lsn,
    pre_warm_reverse_loop,
    register_reverse_loop,
    trigger_failback,
    ORIGIN_FORWARD,
    ORIGIN_REVERSE,
)


class MockTestCoordinator(BaseProxyCoordinator):
    def __init__(self, host="127.0.0.1", port=5432):
        super().__init__({"host": host, "port": port})
        self.state = "active"
        self.calls = []
        self.active_clients = 10
        self.fail_on = None

    def ready(self):
        return True, ""

    def pause(self, databases=None, timeout=5.0):
        self.calls.append(("pause", databases))
        if self.fail_on == "pause":
            return False, "injected pause failure"
        self.state = "paused"
        return True, ""

    def repoint(self, target_host, target_port, databases=None, timeout=5.0):
        self.calls.append(("repoint", target_host, target_port))
        if self.fail_on == "repoint":
            return False, "injected repoint failure"
        self.host = target_host
        self.port = target_port
        return True, ""

    def resume(self, databases=None, timeout=5.0):
        self.calls.append(("resume", databases))
        if self.fail_on == "resume":
            return False, "injected resume failure"
        self.state = "active"
        return True, ""

    def get_active_connections(self, databases=None):
        return {"clients": self.active_clients}


class FakeDBConn:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def cursor(self):
        return self


def test_parse_and_format_lsn():
    assert parse_lsn("16/B374D848") == (0x16 << 32) + 0xB374D848
    assert parse_lsn("0/1000") == 0x1000
    assert parse_lsn(5000) == 5000
    assert parse_lsn(5000.0) == 5000
    assert parse_lsn("") == 0
    assert parse_lsn(None) == 0
    assert parse_lsn("mysql-bin.000001:107") == 107
    assert parse_lsn("invalid") == 0

    val = (0x16 << 32) + 0xB374D848
    assert format_lsn(val) == "16/B374D848"


def test_origin_marker_session_tag_and_app_name():
    pg_tag = OriginMarker.format_session_tag(ORIGIN_FORWARD, engine="postgres")
    assert pg_tag == "SET LOCAL migkit.origin = 'forward';"

    my_tag = OriginMarker.format_session_tag(ORIGIN_FORWARD, engine="mysql")
    assert my_tag == "SET @migkit_origin = 'forward';"

    assert OriginMarker.format_app_name(ORIGIN_FORWARD) == "migkit_forward"
    assert OriginMarker.format_app_name(ORIGIN_REVERSE) == "migkit_reverse"

    stmt = OriginMarker.tag_statement("INSERT INTO users VALUES (1);", ORIGIN_FORWARD)
    assert stmt == "/* migkit:origin=forward */ INSERT INTO users VALUES (1);"


def test_origin_marker_tag_session_execution():
    conn = FakeDBConn()
    OriginMarker.tag_session(conn, origin=ORIGIN_FORWARD, engine="postgres")
    assert any("SET LOCAL migkit.origin = 'forward';" in sql for sql, _ in conn.executed)
    assert any("SET application_name = 'migkit_forward';" in sql for sql, _ in conn.executed)


def test_is_echo_transaction_suppression():
    assert OriginMarker.is_echo_transaction({"origin": "forward"}) is True
    assert OriginMarker.is_echo_transaction({"session_origin": "forward"}) is True
    assert OriginMarker.is_echo_transaction({"session_vars": {"migkit.origin": "forward"}}) is True
    assert OriginMarker.is_echo_transaction({"session_vars": {"@migkit_origin": "forward"}}) is True
    assert OriginMarker.is_echo_transaction({"app_name": "migkit_forward"}) is True
    assert OriginMarker.is_echo_transaction({"sql": "/* migkit:origin=forward */ UPDATE orders SET status='paid'"}) is True
    assert OriginMarker.is_echo_transaction("/* migkit:origin=forward */ SELECT 1") is True

    event_obj = CDCEvent(lsn="0/100", origin="forward")
    assert OriginMarker.is_echo_transaction(event_obj) is True

    user_event = {"table": "orders", "sql": "UPDATE orders SET status='delivered'", "origin": None}
    assert OriginMarker.is_echo_transaction(user_event) is False

    events = [
        {"id": 1, "origin": "forward"},
        {"id": 2, "origin": None, "table": "users"},
        {"id": 3, "sql": "/* migkit:origin=forward */ DELETE FROM cart"},
        {"id": 4, "table": "products"},
    ]
    filtered = OriginMarker.filter_events(events)
    assert len(filtered) == 2
    assert [e["id"] for e in filtered] == [2, 4]


def test_reverse_failback_loop_pre_warm_and_lifecycle():
    hop = Hop(
        name="hop_test",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
        databases=["appdb"],
    )
    now = time.time()
    loop = ReverseFailbackLoop(hop, cutover_time=now, max_retention_hours=48.0)
    assert loop.state == "idle"
    assert loop.pre_warmed is False
    assert loop.running is False

    loop.pre_warm("0/2000")
    assert loop.pre_warmed is True
    assert loop.state == "pre_warmed"
    assert loop.target_start_lsn == "0/2000"
    assert loop.current_target_lsn == "0/2000"
    assert loop.applied_source_lsn == "0/2000"

    loop.start()
    assert loop.running is True
    assert loop.state == "running"

    st = loop.status()
    assert st["state"] == "running"
    assert st["within_retention_window"] is True
    assert st["target_start_lsn"] == "0/2000"

    loop.stop()
    assert loop.running is False
    assert loop.state == "stopped"


def test_reverse_failback_loop_event_ingestion_and_echo_filtering():
    hop = Hop(
        name="hop_echo",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
        databases=["appdb"],
    )
    loop = ReverseFailbackLoop(hop)
    loop.pre_warm("0/1000")
    loop.start()

    echo_event = {
        "lsn": "0/1100",
        "table": "users",
        "origin": "forward",
        "sql": "/* migkit:origin=forward */ INSERT INTO users VALUES (1);",
    }
    accepted = loop.ingest_event(echo_event)
    assert accepted is False
    assert loop.filtered_echo_count == 1
    assert len(loop.captured_events) == 0

    app_event = {
        "lsn": "0/1200",
        "table": "users",
        "op": "insert",
        "data": {"id": 2, "name": "alice"},
        "origin": None,
    }
    accepted = loop.ingest_event(app_event)
    assert accepted is True
    assert len(loop.captured_events) == 1
    assert len(loop.applied_events) == 1
    assert loop.applied_events[0].origin == ORIGIN_REVERSE
    assert loop.current_target_lsn == "0/1200"
    assert loop.applied_source_lsn == "0/1200"


def test_reverse_failback_loop_retention_window():
    hop = Hop(
        name="hop_window",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    now = time.time()

    loop_fresh = ReverseFailbackLoop(hop, cutover_time=now, max_retention_hours=48.0)
    assert loop_fresh.is_within_retention_window() is True
    assert 47.9 <= loop_fresh.remaining_retention_hours() <= 48.0

    loop_24h = ReverseFailbackLoop(hop, cutover_time=now - (24 * 3600), max_retention_hours=48.0)
    assert loop_24h.is_within_retention_window() is True
    assert 23.9 <= loop_24h.remaining_retention_hours() <= 24.1

    loop_expired = ReverseFailbackLoop(hop, cutover_time=now - (49 * 3600), max_retention_hours=48.0)
    assert loop_expired.is_within_retention_window() is False
    assert loop_expired.remaining_retention_hours() == 0.0


def test_reverse_failback_loop_lsn_sync():
    hop = Hop(
        name="hop_sync",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    loop = ReverseFailbackLoop(hop)
    loop.pre_warm("0/1000")
    loop.start()

    assert loop.sync_to_lsn("0/1000", timeout=0.1) is True

    loop.ingest_event({"lsn": "0/1500", "table": "orders", "origin": None})
    assert loop.sync_to_lsn("0/1500", timeout=0.1) is True

    assert loop.sync_to_lsn("0/9999", timeout=0.05) is False


def test_reverse_failback_controller_success():
    clear_reverse_loops()
    hop = Hop(
        name="hop_fb_success",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
        databases=["production"],
        options={"cutover": {"pooler": {"kind": "pgbouncer", "host": "127.0.0.1", "port": 6432}}},
    )

    loop = pre_warm_reverse_loop(hop, baseline_lsn="0/1000")
    loop.ingest_event({"lsn": "0/1100", "table": "t1", "origin": "forward"})
    loop.ingest_event({"lsn": "0/1200", "table": "t1", "origin": None})
    loop.ingest_event({"lsn": "0/1300", "table": "t1", "origin": None})

    coord = MockTestCoordinator(host="10.0.0.2", port=5432)
    inflight = [2, 1, 0]
    drain_func = lambda: inflight.pop(0) if inflight else 0

    res = ReverseFailbackController.trigger_failback(
        hop=hop,
        pooler_coord=coord,
        get_target_inflight=drain_func,
        get_target_lsn=lambda: "0/1300",
        timeout=5.0,
    )

    assert res.ok is True
    assert res.zero_data_loss_verified is True
    assert res.target_final_lsn == "0/1300"
    assert res.source_synced_lsn == "0/1300"
    assert res.transactions_replicated == 2
    assert coord.host == "10.0.0.1"
    assert coord.port == 5432
    assert coord.state == "active"
    assert ("pause", ["production"]) in coord.calls
    assert ("repoint", "10.0.0.1", 5432) in coord.calls
    assert ("resume", ["production"]) in coord.calls
    assert res.steps["lsn_sync"]["lag_bytes"] == 0


def test_reverse_failback_controller_window_expiration():
    clear_reverse_loops()
    hop = Hop(
        name="hop_expired",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    cutover_past = time.time() - (50 * 3600)
    loop = ReverseFailbackLoop(hop, cutover_time=cutover_past, max_retention_hours=48.0)
    loop.pre_warm("0/1000")
    register_reverse_loop(hop.name, loop)

    res = trigger_failback(hop, force=False)
    assert res.ok is False
    assert "retention window expired" in res.error.lower()

    coord = MockTestCoordinator(host="10.0.0.2", port=5432)
    res_forced = trigger_failback(hop, force=True, pooler_coord=coord)
    assert res_forced.ok is True
    assert res_forced.zero_data_loss_verified is True


def test_reverse_failback_controller_pooler_pause_failure():
    clear_reverse_loops()
    hop = Hop(
        name="hop_pause_fail",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    coord = MockTestCoordinator(host="10.0.0.2", port=5432)
    coord.fail_on = "pause"

    res = ReverseFailbackController.trigger_failback(hop, pooler_coord=coord)
    assert res.ok is False
    assert "failed to pause pooler" in res.error.lower()


def test_reverse_failback_controller_lsn_sync_failure():
    clear_reverse_loops()
    hop = Hop(
        name="hop_sync_fail",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    coord = MockTestCoordinator(host="10.0.0.2", port=5432)
    loop = pre_warm_reverse_loop(hop, baseline_lsn="0/1000")

    res = ReverseFailbackController.trigger_failback(
        hop=hop,
        pooler_coord=coord,
        get_target_lsn=lambda: "0/9999",
        timeout=0.05,
    )
    assert res.ok is False
    assert res.zero_data_loss_verified is False
    assert "lsn sync failed" in res.error.lower()
    assert coord.state == "active"


def test_reverse_failback_controller_pooler_repoint_failure():
    clear_reverse_loops()
    hop = Hop(
        name="hop_repoint_fail",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    coord = MockTestCoordinator(host="10.0.0.2", port=5432)
    coord.fail_on = "repoint"

    res = ReverseFailbackController.trigger_failback(hop, pooler_coord=coord)
    assert res.ok is False
    assert "failed to repoint pooler" in res.error.lower()
    assert coord.state == "active"


def test_cutover_settings_allows_reverse_failback():
    hop = Hop(
        name="hop_cutover_conf",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
        options={"cutover": {"reverse_failback": True, "budget": 20}},
    )
    conf = cutover.settings(hop)
    assert conf["reverse_failback"] is True
    assert conf["budget"] == 20.0


def test_cli_failback_command_success(monkeypatch):
    clear_reverse_loops()
    test_hop = Hop(
        name="demo_hop",
        engine="postgres",
        source=Endpoint(host="10.0.0.1", port=5432),
        target=Endpoint(host="10.0.0.2", port=5432),
    )
    monkeypatch.setattr(cli, "get_hop", lambda name: test_hop)
    pre_warm_reverse_loop(test_hop, baseline_lsn="0/1000")

    runner = CliRunner()
    result = runner.invoke(cli.main, ["failback", "demo_hop"])
    assert result.exit_code == 0
    assert "failback completed" in result.output
    assert "zero data loss verified: True" in result.output
