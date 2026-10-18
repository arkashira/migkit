import os
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from migkit import keepalive
from migkit.config import Endpoint, Hop
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine, keepalives as pg_keepalives


def test_tcp_keepalive_constants():
    assert keepalive.DEFAULT_KEEPIDLE == 15
    assert keepalive.DEFAULT_KEEPINTVL == 5
    assert keepalive.DEFAULT_KEEPCNT == 3
    assert keepalive.DEFAULT_HEARTBEAT_INTERVAL == 20.0
    assert keepalive.HEARTBEAT_QUERY == "SELECT 1"


def test_apply_tcp_keepalive_on_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        res = keepalive.apply_tcp_keepalive(s)
        assert res == {}
        assert s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0

        info = keepalive.read_tcp_keepalive(s)
        assert info["keepalive"] is True
        if hasattr(socket, "TCP_KEEPIDLE") or hasattr(socket, "TCP_KEEPALIVE"):
            assert info["idle"] == 15
        if hasattr(socket, "TCP_KEEPINTVL"):
            assert info["interval"] == 5
        if hasattr(socket, "TCP_KEEPCNT"):
            assert info["count"] == 3
    finally:
        s.close()


def test_apply_tcp_keepalive_custom_values():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        res = keepalive.apply_tcp_keepalive(s, idle=25, interval=7, count=4)
        assert res == {}
        info = keepalive.read_tcp_keepalive(s)
        if hasattr(socket, "TCP_KEEPIDLE") or hasattr(socket, "TCP_KEEPALIVE"):
            assert info["idle"] == 25
        if hasattr(socket, "TCP_KEEPINTVL"):
            assert info["interval"] == 7
        if hasattr(socket, "TCP_KEEPCNT"):
            assert info["count"] == 4
    finally:
        s.close()


def test_apply_tcp_keepalive_handles_none_and_oserror():
    assert keepalive.apply_tcp_keepalive(None) == {"error": "no socket to set"}

    class FailingSocket:
        def setsockopt(self, *a, **k):
            raise OSError("permission denied")

    res = keepalive.apply_tcp_keepalive(FailingSocket())
    assert "error" in res
    assert "keepalive not set" in res["error"]


def test_endpoint_keepalive_configuration():
    ep = Endpoint(host="10.0.0.1", port=3306)
    cfg = ep.tcp_keepalive()
    assert cfg["idle"] == 15
    assert cfg["interval"] == 5
    assert cfg["count"] == 3
    assert ep.heartbeat_interval() == 20.0

    lpq = ep.libpq_keepalive()
    assert lpq["keepalives"] == 1
    assert lpq["keepalives_idle"] == 15
    assert lpq["keepalives_interval"] == 5
    assert lpq["keepalives_count"] == 3

    custom_ep = Endpoint(host="10.0.0.1", port=3306, options={
        "tcp_keepidle": 30,
        "tcp_keepintvl": 8,
        "tcp_keepcnt": 4,
        "heartbeat_interval": 10.0,
    })
    custom_cfg = custom_ep.tcp_keepalive()
    assert custom_cfg["idle"] == 30
    assert custom_cfg["interval"] == 8
    assert custom_cfg["count"] == 4
    assert custom_ep.heartbeat_interval() == 10.0


def test_postgres_keepalives_defaults_and_env(monkeypatch):
    monkeypatch.delenv("MIGKIT_KEEPALIVE_IDLE", raising=False)
    kw = pg_keepalives()
    assert kw["keepalives"] == 1
    assert kw["keepalives_idle"] == 15
    assert kw["keepalives_interval"] == 5
    assert kw["keepalives_count"] == 3

    monkeypatch.setenv("MIGKIT_KEEPALIVE_IDLE", "42")
    kw2 = pg_keepalives()
    assert kw2["keepalives_idle"] == 42
    assert kw2["keepalives_interval"] == 5
    assert kw2["keepalives_count"] == 3


def test_background_heartbeat_runner():
    pings = []

    def ping(sql):
        pings.append((time.monotonic(), sql))

    hb = keepalive.BackgroundHeartbeat(ping, interval=0.05, sql="SELECT 1")
    with hb:
        time.sleep(0.18)

    assert len(pings) >= 2
    assert all(sql == "SELECT 1" for _, sql in pings)
    assert not hb._thread.is_alive() if hb._thread else True


def test_background_heartbeat_resilient_to_callback_failure():
    attempts = []

    def fail_ping(sql):
        attempts.append(sql)
        raise RuntimeError("network blip")

    hb = keepalive.BackgroundHeartbeat(fail_ping, interval=0.05)
    with hb:
        time.sleep(0.12)

    assert len(attempts) >= 1
    assert len(hb.errors) >= 1


def test_mysql_engine_heartbeat_and_pushdown_integration(monkeypatch):
    hop = Hop(name="test", engine="mysql", source=Endpoint("src"), target=Endpoint("dst"))
    eng = MySQLEngine(hop)

    q_calls = []

    def fake_q(side, sql, *a, **k):
        q_calls.append((side, sql))
        if sql == "SELECT 1":
            return [(1,)]
        time.sleep(0.12)
        return [(100, 0, 0, 0)]

    monkeypatch.setattr(eng, "_q", fake_q)
    monkeypatch.setattr(eng, "_text_columns", lambda db: [])
    monkeypatch.setattr(eng, "_row_expr", lambda db, tbl: "1")

    with patch.object(keepalive, "DEFAULT_HEARTBEAT_INTERVAL", 0.05):
        res = eng.fused_pushdown_verification("appdb", "orders", sides=("src", "dst"))

    assert res.status == "ok"
    heartbeats = [c for c in q_calls if c[1] == "SELECT 1"]
    assert len(heartbeats) >= 1


def test_postgres_engine_heartbeat_and_pushdown_integration(monkeypatch):
    hop = Hop(name="test", engine="postgres", source=Endpoint("src"), target=Endpoint("dst"), databases=["appdb"])
    eng = PostgresEngine(hop)

    psql_calls = []

    def fake_psql(side, db, sql, *a, **k):
        psql_calls.append((side, sql))
        if "SELECT 1" == sql:
            return "1"
        time.sleep(0.12)
        return "100|0|0|0"

    monkeypatch.setattr(eng, "_psql", fake_psql)
    monkeypatch.setattr(eng, "_text_columns", lambda side, db: {})
    monkeypatch.setattr(eng, "_row_hash_expr", lambda side, db, tbl: "1")

    with patch.object(keepalive, "DEFAULT_HEARTBEAT_INTERVAL", 0.05):
        res = eng.fused_pushdown_verification("appdb", "orders", sides=("src", "dst"))

    assert res.status == "ok"
    heartbeats = [c for c in psql_calls if c[1] == "SELECT 1"]
    assert len(heartbeats) >= 1


def test_mysql_client_socket_has_keepalive_applied(monkeypatch):
    hop = Hop(name="test", engine="mysql", source=Endpoint("src"), target=Endpoint("dst"))
    eng = MySQLEngine(hop)

    created_socks = []

    class DummyConn:
        def __init__(self):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock = s
            created_socks.append(s)

        def thread_id(self):
            return 1

    monkeypatch.setattr("pymysql.connect", lambda **kw: DummyConn())
    try:
        conn = eng._conn("src")
        s = getattr(conn, "_sock")
        info = keepalive.read_tcp_keepalive(s)
        assert info["keepalive"] is True
        if hasattr(socket, "TCP_KEEPIDLE") or hasattr(socket, "TCP_KEEPALIVE"):
            assert info["idle"] == 15
        if hasattr(socket, "TCP_KEEPINTVL"):
            assert info["interval"] == 5
        if hasattr(socket, "TCP_KEEPCNT"):
            assert info["count"] == 3
    finally:
        for s in created_socks:
            s.close()


def test_mysql_adaptive_merkle_bucket_diff_heartbeat(monkeypatch):
    hop = Hop(name="test", engine="mysql", source=Endpoint("src"), target=Endpoint("dst"))
    eng = MySQLEngine(hop)

    q_calls = []

    def fake_q(side, sql, *a, **k):
        q_calls.append((side, sql))
        if sql == "SELECT 1":
            return [(1,)]
        if "MIN(" in sql and "MAX(" in sql:
            return [(1, 100, 100)]
        time.sleep(0.12)
        return [(i, 10, i * 11) for i in range(4)]

    monkeypatch.setattr(eng, "_q", fake_q)

    with patch.object(keepalive, "DEFAULT_HEARTBEAT_INTERVAL", 0.05):
        res = eng.adaptive_merkle_bucket_diff("appdb", "orders", "id", num_buckets=4, sides=("src", "dst"))

    assert res.status == "ok"
    heartbeats = [c for c in q_calls if c[1] == "SELECT 1"]
    assert len(heartbeats) >= 1


def test_postgres_adaptive_merkle_bucket_diff_heartbeat(monkeypatch):
    hop = Hop(name="test", engine="postgres", source=Endpoint("src"), target=Endpoint("dst"), databases=["appdb"])
    eng = PostgresEngine(hop)

    psql_calls = []

    def fake_psql(side, db, sql, *a, **k):
        psql_calls.append((side, sql))
        if sql == "SELECT 1":
            return "1"
        if "MIN(" in sql and "MAX(" in sql:
            return "1|100|100"
        time.sleep(0.12)
        return "\n".join(f"{i}|10|{i * 11}" for i in range(4))

    monkeypatch.setattr(eng, "_psql", fake_psql)

    with patch.object(keepalive, "DEFAULT_HEARTBEAT_INTERVAL", 0.05):
        res = eng.adaptive_merkle_bucket_diff("appdb", "orders", "id", num_buckets=4, sides=("src", "dst"))

    assert res.status == "ok"
    heartbeats = [c for c in psql_calls if c[1] == "SELECT 1"]
    assert len(heartbeats) >= 1


def test_postgres_connect_socket_keepalives(monkeypatch):
    from migkit.engines import postgres
    ep = Endpoint("10.0.0.1", 5432)

    created_socks = []

    class DummyPgConn:
        def __init__(self):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock = s
            created_socks.append(s)

        def fileno(self):
            return self._sock.fileno()

    monkeypatch.setattr("psycopg2.connect", lambda **kw: DummyPgConn())
    try:
        conn = postgres.connect(ep, "appdb", retry=False, session=False)
        s = getattr(conn, "_sock")
        info = keepalive.read_tcp_keepalive(s)
        assert info["keepalive"] is True
        if hasattr(socket, "TCP_KEEPIDLE") or hasattr(socket, "TCP_KEEPALIVE"):
            assert info["idle"] == 15
        if hasattr(socket, "TCP_KEEPINTVL"):
            assert info["interval"] == 5
        if hasattr(socket, "TCP_KEEPCNT"):
            assert info["count"] == 3
    finally:
        for s in created_socks:
            s.close()


PROVES_RUNGS = [
    ("keepalive", "keepalive_armor_rungs", "tcp_socket_keepalive_applied", "holds"),
    ("keepalive", "keepalive_armor_rungs", "tcp_socket_keepalive_applied", "refuses"),
    ("keepalive", "keepalive_armor_rungs", "background_heartbeat_session_armor", "holds"),
    ("keepalive", "keepalive_armor_rungs", "background_heartbeat_session_armor", "refuses"),
    ("keepalive", "keepalive_armor_rungs", "keepalive_custom_parameters_envelope", "holds"),
    ("keepalive", "keepalive_armor_rungs", "keepalive_custom_parameters_envelope", "refuses"),
    ("keepalive", "keepalive_armor_rungs", "keepalive_connection_resilience", "holds"),
    ("keepalive", "keepalive_armor_rungs", "keepalive_connection_resilience", "refuses"),
    ("keepalive", "keepalive_armor_rungs", "refuse_unarmored_proxy_connection", "holds"),
    ("keepalive", "keepalive_armor_rungs", "refuse_unarmored_proxy_connection", "refuses"),
]

from tests.rung_branches import branch, watched, HOLDS, REFUSES


def test_keepalive_armor_decide_ladder_all_branches():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with watched() as seen:
            c = keepalive.decide_keepalive_armor({
                "sock": s,
                "keepalive_applied": True,
            })
        assert branch(seen, "tcp_socket_keepalive_applied") == [HOLDS]
        assert c.rung.name == "tcp_socket_keepalive_applied"

        with watched() as seen:
            c = keepalive.decide_keepalive_armor({
                "disabled": True,
                "heartbeat_active": True,
            })
        assert branch(seen, "tcp_socket_keepalive_applied") == [REFUSES]
        assert branch(seen, "background_heartbeat_session_armor") == [HOLDS]
        assert c.rung.name == "background_heartbeat_session_armor"

        with watched() as seen:
            c = keepalive.decide_keepalive_armor({
                "disabled": True,
                "heartbeat_failed": True,
                "idle": 30, "interval": 10, "count": 5,
            })
        assert branch(seen, "background_heartbeat_session_armor") == [REFUSES]
        assert branch(seen, "keepalive_custom_parameters_envelope") == [HOLDS]
        assert c.rung.name == "keepalive_custom_parameters_envelope"

        with watched() as seen:
            c = keepalive.decide_keepalive_armor({
                "disabled": True,
                "heartbeat_failed": True,
                "envelope_violation": True,
                "long_query_in_progress": True,
            })
        assert branch(seen, "keepalive_custom_parameters_envelope") == [REFUSES]
        assert branch(seen, "keepalive_connection_resilience") == [HOLDS]
        assert c.rung.name == "keepalive_connection_resilience"

        with watched() as seen:
            c = keepalive.decide_keepalive_armor({
                "disabled": True,
                "heartbeat_failed": True,
                "envelope_violation": True,
                "resilience_compromised": True,
                "explicit_unarmored_bypass": True,
            })
        assert branch(seen, "keepalive_connection_resilience") == [REFUSES]
        assert branch(seen, "refuse_unarmored_proxy_connection") == [HOLDS]
        assert c.rung.name == "refuse_unarmored_proxy_connection"

        with watched() as seen:
            try:
                keepalive.decide_keepalive_armor({
                    "disabled": True,
                    "heartbeat_failed": True,
                    "envelope_violation": True,
                    "resilience_compromised": True,
                    "unarmored_refused": True,
                })
            except Exception:
                pass
        assert branch(seen, "refuse_unarmored_proxy_connection") == [REFUSES]
    finally:
        s.close()


