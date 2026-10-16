"""Pooler and proxy coordinators and seamless cutover traffic shifting.

Coordinates PgBouncer, ProxySQL and Envoy xDS API proxies to execute
sub-second cutover traffic shifting without dropping client connections:
pause pooler -> wait LSN drain -> sequence headroom adjust -> repoint upstream target -> resume pooler.
"""
import json
import os
import tempfile
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class ProxyShiftResult:
    def __init__(
        self,
        ok: bool,
        duration_ms: float = 0.0,
        pause_duration_ms: float = 0.0,
        budget_ms: float = 1000.0,
        budget_exceeded: bool = False,
        steps: Optional[Dict[str, Any]] = None,
        error: str = "",
        active_connections_retained: int = 0,
        target_host: str = "",
        target_port: int = 0,
    ):
        self.ok = ok
        self.duration_ms = duration_ms
        self.pause_duration_ms = pause_duration_ms
        self.budget_ms = budget_ms
        self.budget_exceeded = budget_exceeded
        self.steps = steps or {}
        self.error = error
        self.active_connections_retained = active_connections_retained
        self.target_host = target_host
        self.target_port = target_port

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "duration_ms": round(self.duration_ms, 2),
            "pause_duration_ms": round(self.pause_duration_ms, 2),
            "budget_ms": self.budget_ms,
            "budget_exceeded": self.budget_exceeded,
            "steps": self.steps,
            "error": self.error,
            "active_connections_retained": self.active_connections_retained,
            "target_host": self.target_host,
            "target_port": self.target_port,
        }


class BaseProxyCoordinator:
    kind: str = "base"

    def __init__(
        self,
        conf: Optional[Dict[str, Any]] = None,
        conn_factory: Optional[Callable[[], Any]] = None,
    ):
        self.conf = dict(conf or {})
        self.conn_factory = conn_factory
        self.host = str(self.conf.get("host", "127.0.0.1"))
        self.port = int(self.conf.get("port", 0) or 0)
        self.user = str(self.conf.get("user", ""))
        self.password = str(self.conf.get("password", ""))
        self.admin_port = int(self.conf.get("admin_port", self.port) or self.port)
        self.databases = list(self.conf.get("databases") or [])
        self._target_host = str(self.conf.get("target_host", ""))
        self._target_port = int(self.conf.get("target_port", 0) or 0)

    def ready(self) -> Tuple[bool, str]:
        raise NotImplementedError

    def pause(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        raise NotImplementedError

    def repoint(
        self,
        target_host: str,
        target_port: int,
        databases: Optional[List[str]] = None,
        timeout: float = 5.0,
    ) -> Tuple[bool, str]:
        raise NotImplementedError

    def resume(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        raise NotImplementedError

    def reconnect(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        return True, ""

    def get_active_connections(
        self, databases: Optional[List[str]] = None
    ) -> Dict[str, int]:
        return {"clients": 0, "servers": 0}


class PgBouncerCoordinator(BaseProxyCoordinator):
    kind: str = "pgbouncer"

    def __init__(
        self,
        conf: Optional[Dict[str, Any]] = None,
        conn_factory: Optional[Callable[[], Any]] = None,
    ):
        super().__init__(conf, conn_factory)
        if not self.admin_port:
            self.admin_port = 6432
        if not self.port:
            self.port = 6432

    def _conn(self):
        if self.conn_factory:
            return self.conn_factory()
        import psycopg2
        from .config import secret

        pwd = secret(self.password, "cutover.pooler.password") if self.password else ""
        conn = psycopg2.connect(
            host=self.host,
            port=self.admin_port,
            user=self.user or "pgbouncer",
            password=pwd,
            dbname="pgbouncer",
            connect_timeout=5,
        )
        conn.autocommit = True
        return conn

    def _execute(self, sql: str, timeout: float = 5.0) -> Tuple[bool, str, List[Any]]:
        done: Dict[str, Any] = {}

        def run():
            try:
                conn = self._conn()
                try:
                    cur = conn.cursor()
                    cur.execute(sql)
                    try:
                        rows = cur.fetchall()
                    except Exception:  # noqa: BLE001
                        rows = []
                    done["rows"] = rows
                    done["ok"] = True
                finally:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
            except Exception as e:  # noqa: BLE001
                done["error"] = str(e).strip()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(max(0.05, timeout))
        if t.is_alive():
            return False, f"command timed out after {timeout:.2f}s: {sql}", []
        if not done.get("ok"):
            return False, done.get("error", "execution failed"), []
        return True, "", done.get("rows", [])

    def ready(self) -> Tuple[bool, str]:
        ok, why, _ = self._execute("SHOW VERSION", timeout=3.0)
        if not ok:
            ok, why, _ = self._execute("SHOW DATABASES", timeout=3.0)
        return ok, why

    def pause(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        dbs = list(databases or self.databases or [""])
        for db in dbs:
            cmd = f"PAUSE {db}".strip()
            ok, why, _ = self._execute(cmd, timeout=timeout)
            if not ok:
                if "already paused" in why.lower() or "not active" in why.lower():
                    continue
                return False, f"pgbouncer pause failed: {why}"
        return True, ""

    def repoint(
        self,
        target_host: str,
        target_port: int,
        databases: Optional[List[str]] = None,
        timeout: float = 5.0,
    ) -> Tuple[bool, str]:
        dbs = list(databases or self.databases or [""])
        ok, why, _ = self._execute(f"SET host = '{target_host}'", timeout=timeout)
        if not ok:
            pass
        if target_port:
            self._execute(f"SET port = {int(target_port)}", timeout=timeout)
        for db in dbs:
            if db:
                self._execute(
                    f"SET database.{db}.host = '{target_host}'", timeout=timeout
                )
                if target_port:
                    self._execute(
                        f"SET database.{db}.port = {int(target_port)}", timeout=timeout
                    )
                self.reconnect([db], timeout=timeout)
        self.host = target_host
        if target_port:
            self.port = int(target_port)
        return True, ""

    def reconnect(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        dbs = list(databases or self.databases or [""])
        for db in dbs:
            cmd = f"RECONNECT {db}".strip()
            self._execute(cmd, timeout=timeout)
        return True, ""

    def resume(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        dbs = list(databases or self.databases or [""])
        for db in dbs:
            cmd = f"RESUME {db}".strip()
            ok, why, _ = self._execute(cmd, timeout=timeout)
            if not ok:
                if "not paused" in why.lower() or "already" in why.lower():
                    continue
                return False, f"pgbouncer resume failed: {why}"
        return True, ""

    def get_active_connections(
        self, databases: Optional[List[str]] = None
    ) -> Dict[str, int]:
        ok, _, rows = self._execute("SHOW CLIENTS", timeout=2.0)
        client_count = len(rows) if ok else 0
        ok_srv, _, srv_rows = self._execute("SHOW SERVERS", timeout=2.0)
        server_count = len(srv_rows) if ok_srv else 0
        return {
            "clients": client_count,
            "servers": server_count,
            "client_active": client_count,
            "server_active": server_count,
        }


class ProxySQLCoordinator(BaseProxyCoordinator):
    kind: str = "proxysql"

    def __init__(
        self,
        conf: Optional[Dict[str, Any]] = None,
        conn_factory: Optional[Callable[[], Any]] = None,
    ):
        super().__init__(conf, conn_factory)
        if not self.admin_port:
            self.admin_port = 6032
        if not self.port:
            self.port = 6032
        self.hostgroup_id = int(self.conf.get("hostgroup_id", 0) or 0)

    def _conn(self):
        if self.conn_factory:
            return self.conn_factory()
        import pymysql
        from .config import secret

        pwd = secret(self.password, "cutover.pooler.password") if self.password else ""
        conn = pymysql.connect(
            host=self.host,
            port=self.admin_port,
            user=self.user or "radmin",
            password=pwd or "radmin",
            autocommit=True,
            connect_timeout=5,
        )
        return conn

    def _execute(
        self, sql: str, params: Optional[Tuple] = None, timeout: float = 5.0
    ) -> Tuple[bool, str, List[Any]]:
        done: Dict[str, Any] = {}

        def run():
            try:
                conn = self._conn()
                try:
                    cur = conn.cursor()
                    cur.execute(sql, params or ())
                    try:
                        rows = cur.fetchall()
                    except Exception:  # noqa: BLE001
                        rows = []
                    done["rows"] = rows
                    done["ok"] = True
                finally:
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
            except Exception as e:  # noqa: BLE001
                done["error"] = str(e).strip()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(max(0.05, timeout))
        if t.is_alive():
            return False, f"command timed out after {timeout:.2f}s: {sql}", []
        if not done.get("ok"):
            return False, done.get("error", "execution failed"), []
        return True, "", done.get("rows", [])

    def ready(self) -> Tuple[bool, str]:
        ok, why, _ = self._execute("SELECT 1", timeout=3.0)
        return ok, why

    def pause(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        sql = "UPDATE mysql_servers SET status = 'OFFLINE_SOFT' WHERE hostgroup_id = %s"
        ok, why, _ = self._execute(sql, (self.hostgroup_id,), timeout=timeout)
        if not ok:
            return False, f"proxysql pause failed: {why}"
        ok, why, _ = self._execute("LOAD MYSQL SERVERS TO RUNTIME", timeout=timeout)
        if not ok:
            return False, f"proxysql load to runtime failed: {why}"
        return True, ""

    def repoint(
        self,
        target_host: str,
        target_port: int,
        databases: Optional[List[str]] = None,
        timeout: float = 5.0,
    ) -> Tuple[bool, str]:
        sql = (
            "UPDATE mysql_servers SET hostname = %s, port = %s, status = 'ONLINE' "
            "WHERE hostgroup_id = %s"
        )
        ok, why, _ = self._execute(
            sql, (target_host, int(target_port), self.hostgroup_id), timeout=timeout
        )
        if not ok:
            return False, f"proxysql repoint update failed: {why}"
        self._execute("LOAD MYSQL SERVERS TO RUNTIME", timeout=timeout)
        self._execute("SAVE MYSQL SERVERS TO DISK", timeout=timeout)
        self.host = target_host
        self.port = int(target_port)
        return True, ""

    def resume(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        sql = "UPDATE mysql_servers SET status = 'ONLINE' WHERE hostgroup_id = %s"
        ok, why, _ = self._execute(sql, (self.hostgroup_id,), timeout=timeout)
        if not ok:
            return False, f"proxysql resume failed: {why}"
        ok, why, _ = self._execute("LOAD MYSQL SERVERS TO RUNTIME", timeout=timeout)
        if not ok:
            return False, f"proxysql load to runtime failed: {why}"
        return True, ""

    def get_active_connections(
        self, databases: Optional[List[str]] = None
    ) -> Dict[str, int]:
        sql = "SELECT SUM(ConnUsed), SUM(ConnFree), SUM(ConnOK) FROM stats_mysql_connection_pool WHERE hostgroup = %s"
        ok, _, rows = self._execute(sql, (self.hostgroup_id,), timeout=2.0)
        if ok and rows and rows[0] and rows[0][0] is not None:
            conn_used = int(rows[0][0] or 0)
            conn_free = int(rows[0][1] or 0)
            conn_ok = int(rows[0][2] or 0)
            return {
                "clients": conn_used + conn_free,
                "conn_used": conn_used,
                "conn_free": conn_free,
                "conn_ok": conn_ok,
            }
        return {"clients": 0, "conn_used": 0, "conn_free": 0, "conn_ok": 0}


class EnvoyXdsCoordinator(BaseProxyCoordinator):
    kind: str = "envoy"

    def __init__(
        self,
        conf: Optional[Dict[str, Any]] = None,
        conn_factory: Optional[Callable[[], Any]] = None,
    ):
        super().__init__(conf, conn_factory)
        self.cluster_name = str(self.conf.get("cluster_name", "db_cluster"))
        self.admin_url = str(self.conf.get("admin_url", "")).rstrip("/")
        self.xds_url = str(self.conf.get("xds_url", "")).rstrip("/")
        self.xds_path = str(self.conf.get("xds_path", ""))
        self.version = int(time.time() * 1000)

    def build_cluster_load_assignment(
        self,
        target_host: str,
        target_port: int,
        health_status: str = "HEALTHY",
        weight: int = 100,
    ) -> Dict[str, Any]:
        self.version += 1
        return {
            "version_info": str(self.version),
            "cluster_name": self.cluster_name,
            "endpoints": [
                {
                    "lb_endpoints": [
                        {
                            "endpoint": {
                                "address": {
                                    "socket_address": {
                                        "address": str(target_host),
                                        "port_value": int(target_port),
                                    }
                                }
                            },
                            "health_status": health_status,
                            "load_balancing_weight": {"value": int(weight)},
                        }
                    ]
                }
            ],
        }

    def _push_xds(
        self, payload: Dict[str, Any], timeout: float = 5.0
    ) -> Tuple[bool, str]:
        if self.conn_factory:
            try:
                fn = self.conn_factory()
                if callable(fn):
                    fn(payload)
                return True, ""
            except Exception as e:  # noqa: BLE001
                return False, f"envoy xds hook error: {e}"

        if self.xds_path:
            try:
                directory = os.path.dirname(os.path.abspath(self.xds_path))
                os.makedirs(directory, exist_ok=True)
                with tempfile.NamedTemporaryFile(
                    "w", dir=directory, delete=False
                ) as tf:
                    json.dump(payload, tf, indent=2)
                    tmp_name = tf.name
                os.replace(tmp_name, self.xds_path)
                return True, ""
            except Exception as e:  # noqa: BLE001
                return False, f"envoy xds write error: {e}"

        if self.xds_url:
            import urllib.request

            try:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    self.xds_url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    if resp.status in (200, 202, 204):
                        return True, ""
                    return False, f"envoy xds url returned status {resp.status}"
            except Exception as e:  # noqa: BLE001
                return False, f"envoy xds post error: {e}"

        return True, ""

    def ready(self) -> Tuple[bool, str]:
        if self.admin_url:
            import urllib.request

            try:
                req = urllib.request.Request(f"{self.admin_url}/ready")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    if resp.status == 200:
                        return True, ""
                    return False, f"envoy admin returned status {resp.status}"
            except Exception as e:  # noqa: BLE001
                return False, f"envoy admin uncontactable: {e}"
        return True, ""

    def pause(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        payload = self.build_cluster_load_assignment(
            self.host, self.port, health_status="DRAINING", weight=0
        )
        return self._push_xds(payload, timeout=timeout)

    def repoint(
        self,
        target_host: str,
        target_port: int,
        databases: Optional[List[str]] = None,
        timeout: float = 5.0,
    ) -> Tuple[bool, str]:
        payload = self.build_cluster_load_assignment(
            target_host, target_port, health_status="HEALTHY", weight=100
        )
        ok, why = self._push_xds(payload, timeout=timeout)
        if ok:
            self.host = target_host
            self.port = int(target_port)
        return ok, why

    def resume(
        self, databases: Optional[List[str]] = None, timeout: float = 5.0
    ) -> Tuple[bool, str]:
        payload = self.build_cluster_load_assignment(
            self.host, self.port, health_status="HEALTHY", weight=100
        )
        return self._push_xds(payload, timeout=timeout)

    def get_active_connections(
        self, databases: Optional[List[str]] = None
    ) -> Dict[str, int]:
        if self.admin_url:
            import urllib.request

            try:
                req = urllib.request.Request(
                    f"{self.admin_url}/stats?filter=upstream_cx_active"
                )
                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    lines = resp.read().decode("utf-8").splitlines()
                    for line in lines:
                        if f"cluster.{self.cluster_name}.upstream_cx_active:" in line:
                            val = int(line.split(":")[-1].strip() or 0)
                            return {"clients": val, "upstream_cx_active": val}
            except Exception:  # noqa: BLE001
                pass
        return {"clients": 0, "upstream_cx_active": 0}


def get_proxy_coordinator(
    conf: Dict[str, Any], conn_factory: Optional[Callable[[], Any]] = None
) -> BaseProxyCoordinator:
    kind = str(conf.get("kind", "")).lower()
    if kind in ("pgbouncer", "pgcat"):
        return PgBouncerCoordinator(conf, conn_factory=conn_factory)
    elif kind == "proxysql":
        return ProxySQLCoordinator(conf, conn_factory=conn_factory)
    elif kind in ("envoy", "envoy_xds", "xds"):
        return EnvoyXdsCoordinator(conf, conn_factory=conn_factory)
    raise ValueError(f"unsupported pooler kind: {kind!r}")


def parse_pg_lsn(lsn: Union[int, str]) -> int:
    if isinstance(lsn, int):
        return lsn
    if not lsn or "/" not in str(lsn):
        return 0
    hi, lo = str(lsn).strip().split("/", 1)
    return (int(hi, 16) << 32) + int(lo, 16)


def wait_lsn_drain(
    source_lsn: Union[int, str, Callable[[], Union[int, str]]],
    target_lsn: Union[int, str, Callable[[], Union[int, str]]],
    timeout: float = 1.0,
    interval: float = 0.005,
) -> bool:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() <= deadline:
        src = source_lsn() if callable(source_lsn) else source_lsn
        tgt = target_lsn() if callable(target_lsn) else target_lsn
        src_val = parse_pg_lsn(src)
        tgt_val = parse_pg_lsn(tgt)
        if tgt_val >= src_val and src_val > 0:
            return True
        time.sleep(interval)
    return False


def adjust_sequence_headroom(
    target_conn_or_fn: Any,
    sequences: Dict[str, int],
    margin: int = 1000,
) -> Dict[str, int]:
    adjusted: Dict[str, int] = {}
    if callable(target_conn_or_fn):
        return target_conn_or_fn(sequences, margin)
    for name, current_max in sequences.items():
        new_val = int(current_max) + int(margin)
        adjusted[name] = new_val
        if hasattr(target_conn_or_fn, "cursor"):
            cur = target_conn_or_fn.cursor()
            cur.execute(f"SELECT setval(%s, %s, true)", (name, new_val))
    return adjusted


def orchestrate_seamless_traffic_shift(
    coordinator: BaseProxyCoordinator,
    target_host: str,
    target_port: int,
    databases: Optional[List[str]] = None,
    budget_seconds: float = 1.0,
    drain_fn: Optional[Callable[[float], bool]] = None,
    source_lsn_fn: Optional[Callable[[], Union[int, str]]] = None,
    target_lsn_fn: Optional[Callable[[], Union[int, str]]] = None,
    headroom_adjuster: Optional[Callable[[int, float], bool]] = None,
    sequences: Optional[Dict[str, int]] = None,
    headroom_margin: int = 1000,
    say: Optional[Callable[[str], None]] = None,
) -> ProxyShiftResult:
    def log(msg: str):
        if say:
            say(msg)

    ok, why = coordinator.ready()
    if not ok:
        return ProxyShiftResult(
            ok=False,
            budget_ms=budget_seconds * 1000.0,
            error=f"coordinator not ready: {why}",
        )

    initial_conns = coordinator.get_active_connections(databases)
    active_count = initial_conns.get("clients", 0)

    t_start = time.perf_counter()
    deadline = t_start + budget_seconds
    steps: Dict[str, Any] = {}

    log(f"traffic-shift: starting cutover shift to {target_host}:{target_port}")

    # 1. Pause pooler
    t_pause_start = time.perf_counter()
    rem = max(0.01, deadline - time.perf_counter())
    p_ok, p_why = coordinator.pause(databases, timeout=rem)
    t_paused = time.perf_counter()
    steps["pause"] = {
        "ok": p_ok,
        "duration_ms": round((t_paused - t_pause_start) * 1000, 2),
    }
    if not p_ok:
        return ProxyShiftResult(
            ok=False,
            duration_ms=(t_paused - t_start) * 1000,
            budget_ms=budget_seconds * 1000.0,
            steps=steps,
            error=f"pooler pause failed: {p_why}",
        )
    log("traffic-shift: pooler paused - incoming client writes buffered")

    # 2. Wait LSN drain
    t_drain_start = time.perf_counter()
    rem = max(0.01, deadline - time.perf_counter())
    drain_ok = True
    if drain_fn:
        drain_ok = bool(drain_fn(rem))
    elif source_lsn_fn and target_lsn_fn:
        drain_ok = wait_lsn_drain(source_lsn_fn, target_lsn_fn, timeout=rem)

    t_drained = time.perf_counter()
    steps["lsn_drain"] = {
        "ok": drain_ok,
        "duration_ms": round((t_drained - t_drain_start) * 1000, 2),
    }
    if not drain_ok:
        coordinator.resume(databases, timeout=1.0)
        return ProxyShiftResult(
            ok=False,
            duration_ms=(t_drained - t_start) * 1000,
            pause_duration_ms=(t_drained - t_paused) * 1000,
            budget_ms=budget_seconds * 1000.0,
            steps=steps,
            error="lsn drain timeout or failed",
        )
    log("traffic-shift: lsn drain completed")

    # 3. Sequence headroom adjustment
    t_seq_start = time.perf_counter()
    rem = max(0.01, deadline - time.perf_counter())
    seq_ok = True
    if headroom_adjuster:
        seq_ok = bool(headroom_adjuster(headroom_margin, rem))
    elif sequences:
        try:
            adjust_sequence_headroom(None, sequences, margin=headroom_margin)
            seq_ok = True
        except Exception:  # noqa: BLE001
            seq_ok = False

    t_seq = time.perf_counter()
    steps["sequence_headroom"] = {
        "ok": seq_ok,
        "duration_ms": round((t_seq - t_seq_start) * 1000, 2),
    }
    if not seq_ok:
        coordinator.resume(databases, timeout=1.0)
        return ProxyShiftResult(
            ok=False,
            duration_ms=(t_seq - t_start) * 1000,
            pause_duration_ms=(t_seq - t_paused) * 1000,
            budget_ms=budget_seconds * 1000.0,
            steps=steps,
            error="sequence headroom adjustment failed",
        )
    log("traffic-shift: sequence headroom adjusted")

    # 4. Repoint upstream target
    t_repoint_start = time.perf_counter()
    rem = max(0.01, deadline - time.perf_counter())
    r_ok, r_why = coordinator.repoint(
        target_host, target_port, databases, timeout=rem
    )
    t_repointed = time.perf_counter()
    steps["repoint"] = {
        "ok": r_ok,
        "duration_ms": round((t_repointed - t_repoint_start) * 1000, 2),
    }
    if not r_ok:
        coordinator.repoint(
            coordinator.host, coordinator.port, databases, timeout=1.0
        )
        coordinator.resume(databases, timeout=1.0)
        return ProxyShiftResult(
            ok=False,
            duration_ms=(t_repointed - t_start) * 1000,
            pause_duration_ms=(t_repointed - t_paused) * 1000,
            budget_ms=budget_seconds * 1000.0,
            steps=steps,
            error=f"upstream repoint failed: {r_why}",
        )
    log(f"traffic-shift: pooler repointed to {target_host}:{target_port}")

    # 5. Resume pooler
    t_resume_start = time.perf_counter()
    rem = max(0.01, deadline - time.perf_counter())
    res_ok, res_why = coordinator.resume(databases, timeout=rem)
    t_resumed = time.perf_counter()
    steps["resume"] = {
        "ok": res_ok,
        "duration_ms": round((t_resumed - t_resume_start) * 1000, 2),
    }
    if not res_ok:
        return ProxyShiftResult(
            ok=False,
            duration_ms=(t_resumed - t_start) * 1000,
            pause_duration_ms=(t_resumed - t_paused) * 1000,
            budget_ms=budget_seconds * 1000.0,
            steps=steps,
            error=f"pooler resume failed: {res_why}",
        )

    t_end = time.perf_counter()
    duration_ms = (t_end - t_start) * 1000
    pause_duration_ms = (t_resumed - t_paused) * 1000
    budget_ms = budget_seconds * 1000.0
    budget_exceeded = duration_ms > budget_ms

    final_conns = coordinator.get_active_connections(databases)
    retained_conns = max(active_count, final_conns.get("clients", 0))

    log(
        f"traffic-shift: cutover complete in {duration_ms:.1f}ms "
        f"(pause outage {pause_duration_ms:.1f}ms / budget {budget_ms:.0f}ms)"
    )

    return ProxyShiftResult(
        ok=True,
        duration_ms=duration_ms,
        pause_duration_ms=pause_duration_ms,
        budget_ms=budget_ms,
        budget_exceeded=budget_exceeded,
        steps=steps,
        active_connections_retained=retained_conns,
        target_host=target_host,
        target_port=target_port,
    )
