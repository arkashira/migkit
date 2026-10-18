import enum
import logging
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from . import decide


def lock_capability_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for endpoint lock and coordination capability (Row 665: NEED17)."""
    return (
        decide.Rung(
            "session_advisory_lock",
            "use native session-level advisory locks for lightweight coordination",
            needs=(
                decide.Need(
                    "advisory locks supported",
                    lambda f: bool(f.get("supports_advisory_locks", False)),
                    "endpoint does not support advisory locks",
                ),
                decide.Need(
                    "probe successful",
                    lambda f: bool(f.get("probe_successful", True)),
                    "advisory lock probe failed or was refused",
                ),
            ),
            footprint=0,
            leaves="use native session advisory lock for mutual exclusion",
        ),
        decide.Rung(
            "transactional_fence_lock",
            "use transactional table or row locking with explicit timeout fence",
            needs=(
                decide.Need(
                    "transactional fence supported",
                    lambda f: bool(f.get("supports_transactional_fences", False)),
                    "endpoint does not support transactional fences",
                ),
            ),
            footprint=1,
            leaves="use transactional fence locking with statement timeouts",
        ),
        decide.Rung(
            "durable_lease_table",
            "use durable database lease table with monotonic fence counter",
            needs=(
                decide.Need(
                    "table lease supported",
                    lambda f: bool(f.get("supports_table_leases", False)),
                    "endpoint does not support durable lease table creation",
                ),
            ),
            footprint=2,
            leaves="use durable database lease table for distributed coordination",
        ),
        decide.Rung(
            "external_fenced_lease",
            "use external storage coordinator with heartbeat lease",
            needs=(
                decide.Need(
                    "external coordinator available",
                    lambda f: bool(f.get("external_coordinator_available", False)),
                    "no external storage coordinator configured",
                ),
            ),
            footprint=3,
            leaves="use external storage coordinator with active heartbeat lease",
        ),
        decide.Rung(
            "refuse_uncoordinated_concurrency",
            "refuse concurrent operations without verified coordination primitives",
            footprint=4,
            leaves="refuse concurrent operations without verified coordination primitives",
        ),
    )


def decide_lock_capability(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the lock_capability_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("endpoint lock coordination capability", lock_capability_rungs(), fact_obj)


logger = logging.getLogger(__name__)


class LockCapabilityError(Exception):
    pass


class MissingCoordinationPrimitiveError(LockCapabilityError):
    pass


class LeaseAcquisitionError(LockCapabilityError):
    pass


class LeaseLostError(LockCapabilityError):
    pass


class FencedOutError(LeaseLostError):
    pass


class CoordinationStrategy(str, enum.Enum):
    ADVISORY_LOCK = "advisory_lock"
    FENCED_LEASE = "fenced_lease"
    NONE = "none"


@dataclass(frozen=True)
class CapabilityProbeResult:
    name: str
    supported: bool
    latency_ms: float = 0.0
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LockCapabilities:
    advisory_locks: bool
    table_locks: bool
    transactional_ddl: bool
    session_timeouts: bool
    notification_channels: bool
    catalog_inspection: bool = True
    replication_slots: bool = False
    probes: Dict[str, CapabilityProbeResult] = field(default_factory=dict)
    endpoint_info: Dict[str, Any] = field(default_factory=dict)

    @property
    def supports_advisory_locks(self) -> bool:
        return self.advisory_locks

    @property
    def supports_table_locks(self) -> bool:
        return self.table_locks

    @property
    def supports_transactional_ddl(self) -> bool:
        return self.transactional_ddl

    @property
    def supports_session_timeouts(self) -> bool:
        return self.session_timeouts

    @property
    def supports_notification_channels(self) -> bool:
        return self.notification_channels

    @property
    def supports_fenced_lease(self) -> bool:
        return self.table_locks or self.transactional_ddl

    @property
    def can_guarantee_mutual_exclusion(self) -> bool:
        return self.advisory_locks or self.supports_fenced_lease

    @property
    def coordination_strategy(self) -> CoordinationStrategy:
        if self.advisory_locks:
            return CoordinationStrategy.ADVISORY_LOCK
        if self.supports_fenced_lease:
            return CoordinationStrategy.FENCED_LEASE
        return CoordinationStrategy.NONE


class QueryExecutorAdapter:
    def __init__(self, target: Any):
        self.target = target

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Tuple[Any, ...]]:
        if callable(self.target):
            res = self.target(sql, params)
            if res is None:
                return []
            if isinstance(res, list):
                return [(r if isinstance(r, tuple) else (r,)) for r in res]
            return [(res,)]

        if hasattr(self.target, "cursor"):
            cur = self.target.cursor()
            try:
                cur.execute(sql, params or ())
                if hasattr(cur, "fetchall"):
                    try:
                        rows = cur.fetchall()
                        return [tuple(r) for r in rows]
                    except Exception as exc:
                        if "no results" in str(exc).lower() or type(exc).__name__ in ("ProgrammingError", "OperationalError"):
                            return []
                        raise LockCapabilityError(f"cursor fetchall failed: {exc}") from exc
                return []
            finally:
                if hasattr(cur, "close"):
                    cur.close()

        if hasattr(self.target, "execute"):
            res = self.target.execute(sql, params or ())
            if hasattr(res, "fetchall"):
                try:
                    rows = res.fetchall()
                    return [tuple(r) for r in rows]
                except Exception as exc:
                    if "no results" in str(exc).lower() or type(exc).__name__ in ("ProgrammingError", "OperationalError"):
                        return []
                    raise LockCapabilityError(f"execute fetchall failed: {exc}") from exc
            if isinstance(res, list):
                return [(r if isinstance(r, tuple) else (r,)) for r in res]
            return []

        raise TypeError(f"Unsupported query executor type: {type(self.target)}")


class LockCapabilityProber:
    PROBE_KEY = 718281828459045235

    def __init__(
        self,
        executor: Optional[Any] = None,
        endpoint_id: Optional[str] = None,
    ):
        self.executor = QueryExecutorAdapter(executor) if executor is not None else None
        self.endpoint_id = endpoint_id or "unspecified_endpoint"
        self._capabilities: Optional[LockCapabilities] = None

    def _exec(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Tuple[Any, ...]]:
        if self.executor is None:
            raise RuntimeError("No executor configured for LockCapabilityProber")
        return self.executor.execute(sql, params)

    def probe_advisory_locks(self, engine: str = "postgres") -> CapabilityProbeResult:
        t0 = time.monotonic()
        eng = str(engine).lower()
        try:
            if eng in ("mysql", "mariadb"):
                rows = self._exec("SELECT GET_LOCK('__migkit_probe__', 0);")
                val = rows[0][0] if rows and len(rows[0]) > 0 else 0
                if val == 1:
                    self._exec("SELECT RELEASE_LOCK('__migkit_probe__');")
                    return CapabilityProbeResult(
                        name="advisory_locks",
                        supported=True,
                        latency_ms=(time.monotonic() - t0) * 1000.0,
                    )
                return CapabilityProbeResult(
                    name="advisory_locks",
                    supported=False,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                    error="GET_LOCK failed",
                )
            rows = self._exec(f"SELECT pg_try_advisory_lock({self.PROBE_KEY});")
            val = rows[0][0] if rows and len(rows[0]) > 0 else False
            if bool(val):
                self._exec(f"SELECT pg_advisory_unlock({self.PROBE_KEY});")
                return CapabilityProbeResult(
                    name="advisory_locks",
                    supported=True,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                )
            return CapabilityProbeResult(
                name="advisory_locks",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error="pg_try_advisory_lock returned false/null",
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="advisory_locks",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_table_locks(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("LOCK TABLE pg_catalog.pg_database IN ACCESS SHARE MODE NOWAIT;")
            return CapabilityProbeResult(
                name="table_locks",
                supported=True,
                latency_ms=(time.monotonic() - t0) * 1000.0,
            )
        except Exception as e1:
            try:
                self._exec("BEGIN; CREATE TEMP TABLE _migkit_lk_probe (x int); LOCK TABLE _migkit_lk_probe IN EXCLUSIVE MODE NOWAIT; ROLLBACK;")
                return CapabilityProbeResult(
                    name="table_locks",
                    supported=True,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                )
            except Exception as e2:
                return CapabilityProbeResult(
                    name="table_locks",
                    supported=False,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                    error=f"{e1}; fallback: {e2}",
                )

    def probe_transactional_ddl(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("BEGIN; CREATE TEMP TABLE _migkit_tx_probe (id int); ROLLBACK;")
            rows = self._exec("SELECT count(*) FROM pg_catalog.pg_class WHERE relname = '_migkit_tx_probe';")
            exists_count = int(rows[0][0]) if rows and len(rows[0]) > 0 else 0
            if exists_count == 0:
                return CapabilityProbeResult(
                    name="transactional_ddl",
                    supported=True,
                    latency_ms=(time.monotonic() - t0) * 1000.0,
                )
            return CapabilityProbeResult(
                name="transactional_ddl",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error="DDL committed despite ROLLBACK",
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="transactional_ddl",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_session_timeouts(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("SET LOCAL lock_timeout = '250ms';")
            self._exec("SET LOCAL statement_timeout = '500ms';")
            return CapabilityProbeResult(
                name="session_timeouts",
                supported=True,
                latency_ms=(time.monotonic() - t0) * 1000.0,
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="session_timeouts",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_notification_channels(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("LISTEN _migkit_probe_chan;")
            self._exec("UNLISTEN _migkit_probe_chan;")
            return CapabilityProbeResult(
                name="notification_channels",
                supported=True,
                latency_ms=(time.monotonic() - t0) * 1000.0,
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="notification_channels",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_catalog_inspection(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("SELECT count(*) FROM pg_catalog.pg_locks WHERE 1 = 0;")
            self._exec("SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE 1 = 0;")
            return CapabilityProbeResult(
                name="catalog_inspection",
                supported=True,
                latency_ms=(time.monotonic() - t0) * 1000.0,
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="catalog_inspection",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_replication_slots(self) -> CapabilityProbeResult:
        t0 = time.monotonic()
        try:
            self._exec("SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE 1 = 0;")
            return CapabilityProbeResult(
                name="replication_slots",
                supported=True,
                latency_ms=(time.monotonic() - t0) * 1000.0,
            )
        except Exception as e:
            return CapabilityProbeResult(
                name="replication_slots",
                supported=False,
                latency_ms=(time.monotonic() - t0) * 1000.0,
                error=str(e),
            )

    def probe_all(self, force: bool = False) -> LockCapabilities:
        if self._capabilities is not None and not force:
            return self._capabilities

        probes = {
            "advisory_locks": self.probe_advisory_locks(),
            "table_locks": self.probe_table_locks(),
            "transactional_ddl": self.probe_transactional_ddl(),
            "session_timeouts": self.probe_session_timeouts(),
            "notification_channels": self.probe_notification_channels(),
            "catalog_inspection": self.probe_catalog_inspection(),
            "replication_slots": self.probe_replication_slots(),
        }

        self._capabilities = LockCapabilities(
            advisory_locks=probes["advisory_locks"].supported,
            table_locks=probes["table_locks"].supported,
            transactional_ddl=probes["transactional_ddl"].supported,
            session_timeouts=probes["session_timeouts"].supported,
            notification_channels=probes["notification_channels"].supported,
            catalog_inspection=probes["catalog_inspection"].supported,
            replication_slots=probes["replication_slots"].supported,
            probes=probes,
            endpoint_info={"endpoint_id": self.endpoint_id},
        )
        return self._capabilities

    def choose_coordination_lease(
        self,
        resource_id: str,
        holder_id: Optional[str] = None,
        lease_ttl: float = 10.0,
        store: Optional["LeaseStore"] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> "FencedCoordinationLease":
        caps = self.probe_all()
        return choose_coordination_lease(
            prober_or_caps=caps,
            resource_id=resource_id,
            holder_id=holder_id,
            lease_ttl=lease_ttl,
            store=store,
            clock=clock,
        )


class LeaseStore:
    def acquire(
        self,
        resource_id: str,
        holder_id: str,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, int, Dict[str, Any]]:
        raise NotImplementedError

    def renew(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, Dict[str, Any]]:
        raise NotImplementedError

    def release(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
    ) -> bool:
        raise NotImplementedError

    def get(self, resource_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class InMemoryLeaseStore(LeaseStore):
    def __init__(self):
        self._lock = threading.Lock()
        self._leases: Dict[str, Dict[str, Any]] = {}

    def acquire(
        self,
        resource_id: str,
        holder_id: str,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, int, Dict[str, Any]]:
        with self._lock:
            cur = self._leases.get(resource_id)
            if not cur or cur.get("expires_at", 0.0) <= now or cur.get("holder") is None:
                prev_fence = (cur or {}).get("fence_token", 0)
                fence = prev_fence + 1
                rec = {
                    "resource_id": resource_id,
                    "holder": holder_id,
                    "fence_token": fence,
                    "renewed_at": now,
                    "expires_at": now + lease_ttl,
                    "takeover_from": (cur or {}).get("holder"),
                }
                self._leases[resource_id] = rec
                return True, fence, dict(rec)

            if cur.get("holder") == holder_id:
                cur["expires_at"] = now + lease_ttl
                cur["renewed_at"] = now
                return True, cur["fence_token"], dict(cur)

            return False, cur.get("fence_token", 0), dict(cur)

    def renew(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, Dict[str, Any]]:
        with self._lock:
            cur = self._leases.get(resource_id)
            if not cur:
                return False, {}
            if cur.get("fence_token") != fence_token:
                return False, dict(cur)
            if cur.get("holder") != holder_id:
                return False, dict(cur)
            if cur.get("expires_at", 0.0) <= now:
                return False, dict(cur)

            cur["renewed_at"] = now
            cur["expires_at"] = now + lease_ttl
            return True, dict(cur)

    def release(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
    ) -> bool:
        with self._lock:
            cur = self._leases.get(resource_id)
            if cur and cur.get("fence_token") == fence_token and cur.get("holder") == holder_id:
                cur["holder"] = None
                cur["expires_at"] = 0.0
                return True
            return False

    def get(self, resource_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cur = self._leases.get(resource_id)
            return dict(cur) if cur else None


class DatabaseLeaseStore(LeaseStore):
    TABLE_NAME = "_migkit_coordination_lease"

    def __init__(self, executor: Any):
        self.executor = QueryExecutorAdapter(executor)
        self._initialized = False
        self._lock = threading.Lock()

    def _ensure_table(self):
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ("
                f"resource_id VARCHAR(255) PRIMARY KEY, "
                f"holder VARCHAR(255), "
                f"fence_token BIGINT NOT NULL DEFAULT 0, "
                f"renewed_at DOUBLE PRECISION NOT NULL, "
                f"expires_at DOUBLE PRECISION NOT NULL"
                f");"
            )
            self.executor.execute(ddl)
            self._initialized = True

    def acquire(
        self,
        resource_id: str,
        holder_id: str,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, int, Dict[str, Any]]:
        self._ensure_table()
        rows = self.executor.execute(
            f"SELECT holder, fence_token, expires_at FROM {self.TABLE_NAME} WHERE resource_id = %s;",
            (resource_id,),
        )
        if not rows:
            try:
                self.executor.execute(
                    f"INSERT INTO {self.TABLE_NAME} (resource_id, holder, fence_token, renewed_at, expires_at) "
                    f"VALUES (%s, %s, 1, %s, %s);",
                    (resource_id, holder_id, now, now + lease_ttl),
                )
                rec = {
                    "resource_id": resource_id,
                    "holder": holder_id,
                    "fence_token": 1,
                    "renewed_at": now,
                    "expires_at": now + lease_ttl,
                    "takeover_from": None,
                }
                return True, 1, rec
            except Exception:
                rows = self.executor.execute(
                    f"SELECT holder, fence_token, expires_at FROM {self.TABLE_NAME} WHERE resource_id = %s;",
                    (resource_id,),
                )

        if rows:
            cur_holder, cur_fence, cur_expires = rows[0][0], int(rows[0][1]), float(rows[0][2])
            if cur_expires <= now or cur_holder is None or cur_holder == holder_id:
                new_fence = cur_fence + 1 if cur_holder != holder_id else cur_fence
                self.executor.execute(
                    f"UPDATE {self.TABLE_NAME} SET holder = %s, fence_token = %s, renewed_at = %s, expires_at = %s "
                    f"WHERE resource_id = %s AND (expires_at <= %s OR holder IS NULL OR holder = %s);",
                    (holder_id, new_fence, now, now + lease_ttl, resource_id, now, holder_id),
                )
                rec = {
                    "resource_id": resource_id,
                    "holder": holder_id,
                    "fence_token": new_fence,
                    "renewed_at": now,
                    "expires_at": now + lease_ttl,
                    "takeover_from": cur_holder if cur_holder != holder_id else None,
                }
                return True, new_fence, rec

            rec = {
                "resource_id": resource_id,
                "holder": cur_holder,
                "fence_token": cur_fence,
                "expires_at": cur_expires,
            }
            return False, cur_fence, rec

        return False, 0, {}

    def renew(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
        lease_ttl: float,
        now: float,
    ) -> Tuple[bool, Dict[str, Any]]:
        self._ensure_table()
        cur = self.get(resource_id)
        if not cur:
            return False, {}
        if cur.get("fence_token") != fence_token or cur.get("holder") != holder_id or cur.get("expires_at", 0.0) <= now:
            return False, cur

        self.executor.execute(
            f"UPDATE {self.TABLE_NAME} SET renewed_at = %s, expires_at = %s "
            f"WHERE resource_id = %s AND holder = %s AND fence_token = %s AND expires_at > %s;",
            (now, now + lease_ttl, resource_id, holder_id, fence_token, now),
        )
        cur["renewed_at"] = now
        cur["expires_at"] = now + lease_ttl
        return True, cur

    def release(
        self,
        resource_id: str,
        holder_id: str,
        fence_token: int,
    ) -> bool:
        self._ensure_table()
        cur = self.get(resource_id)
        if cur and cur.get("fence_token") == fence_token and cur.get("holder") == holder_id:
            self.executor.execute(
                f"UPDATE {self.TABLE_NAME} SET holder = NULL, expires_at = 0.0 "
                f"WHERE resource_id = %s AND holder = %s AND fence_token = %s;",
                (resource_id, holder_id, fence_token),
            )
            return True
        return False

    def get(self, resource_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_table()
        rows = self.executor.execute(
            f"SELECT holder, fence_token, renewed_at, expires_at FROM {self.TABLE_NAME} WHERE resource_id = %s;",
            (resource_id,),
        )
        if not rows:
            return None
        return {
            "resource_id": resource_id,
            "holder": rows[0][0],
            "fence_token": int(rows[0][1]),
            "renewed_at": float(rows[0][2]),
            "expires_at": float(rows[0][3]),
        }


class FencedCoordinationLease:
    def __init__(
        self,
        resource_id: str,
        holder_id: Optional[str] = None,
        lease_ttl: float = 10.0,
        store: Optional[LeaseStore] = None,
        capabilities: Optional[LockCapabilities] = None,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.resource_id = resource_id
        self.holder_id = holder_id or f"{socket.gethostname()}:{time.time_ns()}"
        self.lease_ttl = float(lease_ttl)
        self.store = store or InMemoryLeaseStore()
        self.capabilities = capabilities
        self._clock = clock or time.time
        self.fence_token: Optional[int] = None
        self.expires_at: Optional[float] = None
        self.renewed_at: Optional[float] = None
        self.takeover_from: Optional[str] = None
        self._held = False

        if capabilities is not None and not capabilities.can_guarantee_mutual_exclusion:
            raise MissingCoordinationPrimitiveError(
                f"Cannot guarantee mutual exclusion on endpoint for resource '{resource_id}' "
                f"(missing advisory locks and fenced table primitives)"
            )

    @property
    def is_held(self) -> bool:
        if not self._held or self.expires_at is None:
            return False
        return self._clock() < self.expires_at

    def acquire(self, raise_on_failure: bool = True) -> bool:
        now = self._clock()
        ok, fence, rec = self.store.acquire(
            resource_id=self.resource_id,
            holder_id=self.holder_id,
            lease_ttl=self.lease_ttl,
            now=now,
        )
        if not ok:
            if raise_on_failure:
                active_holder = rec.get("holder")
                exp = rec.get("expires_at", 0.0)
                raise LeaseAcquisitionError(
                    f"Resource '{self.resource_id}' is actively held by '{active_holder}' until {exp}"
                )
            return False

        self.fence_token = fence
        self.expires_at = rec.get("expires_at")
        self.renewed_at = rec.get("renewed_at")
        self.takeover_from = rec.get("takeover_from")
        self._held = True
        return True

    def renew(self) -> bool:
        if getattr(self, "_fenced_out", False):
            raise FencedOutError(f"Holder permanently fenced out for '{self.resource_id}'")

        if self.fence_token is None:
            raise LeaseLostError(f"Lease for '{self.resource_id}' is not held")

        now = self._clock()
        ok, rec = self.store.renew(
            resource_id=self.resource_id,
            holder_id=self.holder_id,
            fence_token=self.fence_token,
            lease_ttl=self.lease_ttl,
            now=now,
        )
        if not ok:
            self._held = False
            cur_fence = rec.get("fence_token") if rec else None
            if cur_fence is not None and cur_fence > self.fence_token:
                self._fenced_out = True
                raise FencedOutError(
                    f"Lease for '{self.resource_id}' was taken over by '{rec.get('holder')}'; "
                    f"fence {self.fence_token} is obsolete (current fence: {cur_fence})"
                )
            raise LeaseLostError(f"Lease renewal failed for '{self.resource_id}'")

        self.expires_at = rec.get("expires_at")
        self.renewed_at = rec.get("renewed_at")
        return True

    def assert_held(self) -> int:
        if getattr(self, "_fenced_out", False):
            raise FencedOutError(f"Holder permanently fenced out for '{self.resource_id}'")

        if self.fence_token is None:
            raise LeaseLostError(f"Lease for '{self.resource_id}' was never acquired")

        cur = self.store.get(self.resource_id)
        current_fence = (cur or {}).get("fence_token")
        if current_fence is not None and current_fence > self.fence_token:
            self._held = False
            self._fenced_out = True
            raise FencedOutError(
                f"Holder fenced out for '{self.resource_id}': lease was taken over by "
                f"'{cur.get('holder')}' with fence {current_fence} (holder fence: {self.fence_token})"
            )

        if not self._held:
            raise LeaseLostError(f"Lease for '{self.resource_id}' is not held")

        now = self._clock()
        if self.expires_at is not None and now >= self.expires_at:
            self._held = False
            raise LeaseLostError(
                f"Lease for '{self.resource_id}' has expired (expired at {self.expires_at}, now {now})"
            )

        if not cur or cur.get("holder") != self.holder_id:
            self._held = False
            raise LeaseLostError(f"Lease ownership lost for '{self.resource_id}'")

        return self.fence_token

    def release(self) -> bool:
        if not self._held or self.fence_token is None:
            return False

        ok = self.store.release(
            resource_id=self.resource_id,
            holder_id=self.holder_id,
            fence_token=self.fence_token,
        )
        self._held = False
        return ok

    def __enter__(self) -> "FencedCoordinationLease":
        self.acquire(raise_on_failure=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def choose_coordination_lease(
    prober_or_caps: Union[LockCapabilityProber, LockCapabilities],
    resource_id: str,
    holder_id: Optional[str] = None,
    lease_ttl: float = 10.0,
    store: Optional[LeaseStore] = None,
    clock: Optional[Callable[[], float]] = None,
) -> FencedCoordinationLease:
    if isinstance(prober_or_caps, LockCapabilityProber):
        caps = prober_or_caps.probe_all()
    else:
        caps = prober_or_caps

    if not caps.can_guarantee_mutual_exclusion and store is None:
        raise MissingCoordinationPrimitiveError(
            f"PostgreSQL-compatible endpoint lacks required coordination primitives "
            f"(advisory_locks={caps.advisory_locks}, table_locks={caps.table_locks}, "
            f"transactional_ddl={caps.transactional_ddl}); mutual exclusion cannot be guaranteed"
        )

    return FencedCoordinationLease(
        resource_id=resource_id,
        holder_id=holder_id,
        lease_ttl=lease_ttl,
        store=store or InMemoryLeaseStore(),
        capabilities=caps,
        clock=clock,
    )
