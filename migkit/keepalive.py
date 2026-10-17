import contextlib
import os
import socket
import threading
import time

DEFAULT_KEEPIDLE = 15
DEFAULT_KEEPINTVL = 5
DEFAULT_KEEPCNT = 3
DEFAULT_HEARTBEAT_INTERVAL = 20.0
HEARTBEAT_QUERY = "SELECT 1"


def apply_tcp_keepalive(sock, idle=DEFAULT_KEEPIDLE, interval=DEFAULT_KEEPINTVL, count=DEFAULT_KEEPCNT):
    if sock is None:
        return {"error": "no socket to set"}
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, val in (("TCP_KEEPIDLE", idle), ("TCP_KEEPALIVE", idle),
                          ("TCP_KEEPINTVL", interval), ("TCP_KEEPCNT", count)):
            opt = getattr(socket, name, None)
            if opt is not None:
                sock.setsockopt(socket.IPPROTO_TCP, opt, int(val))
    except OSError as e:
        return {"error": f"keepalive not set: {e.strerror or e}"}
    return {}


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
        interval = DEFAULT_HEARTBEAT_INTERVAL

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
            except Exception:
                pass

    hb = BackgroundHeartbeat(_ping, interval=interval, sql=sql)
    with hb:
        yield hb
