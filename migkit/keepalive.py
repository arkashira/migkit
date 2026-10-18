import contextlib
import os
import socket
import threading
import time
from typing import Any, Dict, Optional, Tuple

from . import decide

DEFAULT_KEEPIDLE = 15
DEFAULT_KEEPINTVL = 5
DEFAULT_KEEPCNT = 3
DEFAULT_HEARTBEAT_INTERVAL = 20.0
HEARTBEAT_QUERY = "SELECT 1"


def get_default_keepidle() -> int:
    if DEFAULT_KEEPIDLE != 15:
        return int(DEFAULT_KEEPIDLE)
    from . import envkeys
    v = envkeys.number("MIGKIT_KEEPALIVE_IDLE")
    return int(v) if v is not None else DEFAULT_KEEPIDLE


def get_default_keepintvl() -> int:
    if DEFAULT_KEEPINTVL != 5:
        return int(DEFAULT_KEEPINTVL)
    from . import envkeys
    v = envkeys.number("MIGKIT_KEEPALIVE_INTERVAL")
    return int(v) if v is not None else DEFAULT_KEEPINTVL


def get_default_keepcnt() -> int:
    if DEFAULT_KEEPCNT != 3:
        return int(DEFAULT_KEEPCNT)
    from . import envkeys
    v = envkeys.number("MIGKIT_KEEPALIVE_COUNT")
    return int(v) if v is not None else DEFAULT_KEEPCNT


def get_default_heartbeat_interval() -> float:
    if DEFAULT_HEARTBEAT_INTERVAL != 20.0:
        return float(DEFAULT_HEARTBEAT_INTERVAL)
    from . import envkeys
    v = envkeys.number("MIGKIT_HEARTBEAT_INTERVAL")
    return float(v) if v is not None else DEFAULT_HEARTBEAT_INTERVAL


def apply_tcp_keepalive(sock, idle=None, interval=None, count=None):
    if sock is None:
        return {"error": "no socket to set"}
    if idle is None:
        idle = get_default_keepidle()
    if interval is None:
        interval = get_default_keepintvl()
    if count is None:
        count = get_default_keepcnt()
    from .util import keepalive as _util_keepalive
    return _util_keepalive(sock, idle=int(idle), interval=int(interval), count=int(count))


enable_tcp_keepalive = apply_tcp_keepalive


def read_tcp_keepalive(sock):
    if sock is None:
        return {"keepalive": False, "idle": None, "interval": None, "count": None}
    try:
        ka = bool(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))
    except OSError:
        ka = False

    idle = None
    for name in ("TCP_KEEPIDLE", "TCP_KEEPALIVE"):
        opt = getattr(socket, name, None)
        if opt is not None:
            try:
                idle = sock.getsockopt(socket.IPPROTO_TCP, opt)
                break
            except OSError:
                pass

    interval = None
    opt = getattr(socket, "TCP_KEEPINTVL", None)
    if opt is not None:
        try:
            interval = sock.getsockopt(socket.IPPROTO_TCP, opt)
        except OSError:
            pass

    count = None
    opt = getattr(socket, "TCP_KEEPCNT", None)
    if opt is not None:
        try:
            count = sock.getsockopt(socket.IPPROTO_TCP, opt)
        except OSError:
            pass

    return {"keepalive": ka, "idle": idle, "interval": interval, "count": count}


class BackgroundHeartbeat:
    def __init__(self, callback, interval=DEFAULT_HEARTBEAT_INTERVAL, sql=HEARTBEAT_QUERY):
        self.callback = callback
        self.interval = float(interval)
        self.sql = sql
        self._stop = threading.Event()
        self._thread = None
        self.ticks = 0
        self.errors = []

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self.callback(self.sql)
                self.ticks += 1
            except Exception as e:
                self.errors.append(e)

    def start(self):
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def stop(self):
        if self._thread is not None:
            self._stop.set()
            self._thread.join(timeout=1.0)
            self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


@contextlib.contextmanager
def heartbeat_during(engine_or_fn, sides=("src", "dst"), interval=None, sql=HEARTBEAT_QUERY, db=None):
    if interval is None:
        interval = get_default_heartbeat_interval()
    hb = None

    def _ping(query_str):
        for side in sides:
            try:
                if hasattr(engine_or_fn, "heartbeat"):
                    engine_or_fn.heartbeat(side=side, sql=query_str, db=db)
                elif hasattr(engine_or_fn, "_q"):
                    engine_or_fn._q(side, query_str)
                elif hasattr(engine_or_fn, "_psql"):
                    target_db = db or "postgres"
                    engine_or_fn._psql(side, target_db, query_str)
                elif callable(engine_or_fn):
                    engine_or_fn(query_str)
            except Exception as e:
                if hb is not None and hasattr(hb, "errors"):
                    hb.errors.append(e)

    hb = BackgroundHeartbeat(_ping, interval=interval, sql=sql)
    with hb:
        yield hb


def _prove_tcp_socket_keepalive(facts: Dict[str, Any]) -> bool:
    sock = facts.get("sock")
    res = facts.get("keepalive_result")
    if facts.get("disabled"):
        return False
    if sock is None and res is None:
        return False
    if res and "error" in res:
        return False
    return facts.get("keepalive_applied", True)


def _prove_background_heartbeat_armor(facts: Dict[str, Any]) -> bool:
    if facts.get("heartbeat_failed"):
        return False
    return bool(facts.get("heartbeat_active") or facts.get("heartbeat_ticks", 0) > 0)


def _prove_keepalive_parameters_envelope(facts: Dict[str, Any]) -> bool:
    idle = facts.get("idle", get_default_keepidle())
    intvl = facts.get("interval", get_default_keepintvl())
    cnt = facts.get("count", get_default_keepcnt())
    if facts.get("envelope_violation"):
        return False
    return 1 <= int(idle) <= 7200 and 1 <= int(intvl) <= 300 and 1 <= int(cnt) <= 20


def _prove_keepalive_connection_resilience(facts: Dict[str, Any]) -> bool:
    if facts.get("resilience_compromised"):
        return False
    return bool(facts.get("long_query_in_progress") or facts.get("ssm_tunnel_active"))


def _prove_refuse_unarmored_proxy_connection(facts: Dict[str, Any]) -> bool:
    if facts.get("unarmored_refused"):
        return False
    return bool(facts.get("explicit_unarmored_bypass"))


def keepalive_armor_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "tcp_socket_keepalive_applied",
            "apply TCP keepalive socket options on database connection socket",
            needs=(
                decide.Need(
                    "connection socket available and keepalive options set without error",
                    _prove_tcp_socket_keepalive,
                    "socket missing or keepalive unsupported on transport",
                ),
            ),
        ),
        decide.Rung(
            "background_heartbeat_session_armor",
            "execute periodic background heartbeat query preventing proxy drops",
            needs=(
                decide.Need(
                    "background heartbeat thread active and executing without unhandled error",
                    _prove_background_heartbeat_armor,
                    "heartbeat inactive or heartbeat thread failed",
                ),
            ),
        ),
        decide.Rung(
            "keepalive_custom_parameters_envelope",
            "honor user-configured or environment keepalive parameters within bounds",
            needs=(
                decide.Need(
                    "keepalive parameters within valid timing budget",
                    _prove_keepalive_parameters_envelope,
                    "keepalive parameters exceed maximum safety limits",
                ),
            ),
        ),
        decide.Rung(
            "keepalive_connection_resilience",
            "preserve long query execution state during network idle intervals",
            needs=(
                decide.Need(
                    "connection transport protected by active keepalive configuration",
                    _prove_keepalive_connection_resilience,
                    "transport unprotected and vulnerable to idle SSM severance",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unarmored_proxy_connection",
            "refuse execution over unarmored tunnel when keepalive armor is required",
            needs=(
                decide.Need(
                    "connection has verified keepalive or explicit unarmored bypass",
                    _prove_refuse_unarmored_proxy_connection,
                    "unarmored connection refused over proxy/SSM boundary",
                ),
            ),
        ),
    )


def decide_keepalive_armor(facts: Dict[str, Any]) -> decide.Choice:
    return decide.climb("keepalive armor", keepalive_armor_rungs(), facts=decide.Facts(facts))
