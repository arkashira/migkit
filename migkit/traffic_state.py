import enum
import json
import os
import socket
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

from . import decide, lease

SAFE_TEST_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}


class TrafficStateError(Exception):
    pass


class UnsafeTestHostError(TrafficStateError, ValueError):
    pass


class InvalidPhaseTransitionError(TrafficStateError):
    pass


class SourceFencedWriteError(TrafficStateError):
    pass


class SplitBrainDetectedError(TrafficStateError):
    pass


class CutoverIncompleteError(TrafficStateError):
    pass


class LeaseLockError(TrafficStateError, lease.Held):
    pass


class DrainTimeoutError(TrafficStateError):
    pass


class ProofMismatchError(TrafficStateError):
    pass


class CounterAdjustmentError(TrafficStateError):
    pass


def is_safe_test_host(host: str) -> bool:
    """Check if host is non-empty and contains valid host syntax without control characters."""
    h = str(host).strip().lower()
    if not h:
        return False
    return not any(c in h for c in " \t\r\n;\"'\\")


def validate_test_host(host: str) -> str:
    """Validate host string, accepting any valid network host (FQDN, IPv4, IPv6, localhost)."""
    h = str(host).strip()
    if not h or not is_safe_test_host(h):
        raise UnsafeTestHostError(f"unsafe host rejected: {host!r} (must be non-empty valid host/IP)")
    return h


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class CutoverPhase(str, enum.Enum):
    INIT = "INIT"
    PAUSE = "PAUSE"
    DRAIN = "DRAIN"
    FENCE = "FENCE"
    PROOF = "PROOF"
    COUNTER_ADJUSTMENT = "COUNTER_ADJUSTMENT"
    ROUTING = "ROUTING"
    RESUME = "RESUME"
    CLIENT_ACK = "CLIENT_ACK"
    COMPLETED = "COMPLETED"
    ROLLED_BACK = "ROLLED_BACK"
    FAILED = "FAILED"


class ReadRoutingState(str, enum.Enum):
    SOURCE = "SOURCE"
    REPLICA = "REPLICA"
    SPLIT = "SPLIT"
    TARGET = "TARGET"


class WriteRoutingState(str, enum.Enum):
    SOURCE = "SOURCE"
    FENCED = "FENCED"
    TARGET = "TARGET"


class DatabaseEndpoint:
    name: str = "endpoint"
    host: str = "127.0.0.1"
    port: int = 5432
    database: str = "production"
    role: str = "source"
    read_only: bool = False
    current_lsn: int = 1000
    counters: Dict[str, int]
    write_epoch: int = 0
    active_sessions: int = 0
    last_committed_write: Optional[str] = None
    applied_writes: List[str]

    def __init__(
        self,
        name: str = "endpoint",
        host: str = "127.0.0.1",
        port: int = 5432,
        database: Optional[str] = None,
        role: str = "source",
        read_only: bool = False,
        current_lsn: int = 1000,
        counters: Optional[Dict[str, int]] = None,
        write_epoch: int = 0,
        active_sessions: int = 0,
        last_committed_write: Optional[str] = None,
        applied_writes: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        self.name = str(name)
        self.host = validate_test_host(str(host))
        self.port = int(port)
        db = database or kwargs.get("db") or "production"
        self.database = str(db)
        self.role = str(role)
        self.read_only = bool(read_only)
        self.current_lsn = int(current_lsn)
        self.counters = dict(counters or {})
        self.write_epoch = int(write_epoch)
        self.active_sessions = int(active_sessions)
        self.last_committed_write = last_committed_write
        self.applied_writes = list(applied_writes or [])

    def set_read_only(self, ro: bool) -> None:
        self.read_only = bool(ro)

    def execute_read(self, query: str) -> List[Dict[str, Any]]:
        return [{"query": query, "endpoint": self.name, "lsn": self.current_lsn, "read_only": self.read_only}]

    def execute_write(self, query: str, epoch: Optional[int] = None) -> Dict[str, Any]:
        if self.read_only:
            raise SourceFencedWriteError(
                f"write rejected on endpoint '{self.name}': database is read-only (fenced)"
            )
        if epoch is not None and epoch != self.write_epoch:
            raise SourceFencedWriteError(
                f"write rejected on endpoint '{self.name}': epoch mismatch (expected {self.write_epoch}, got {epoch})"
            )
        self.current_lsn += 1
        self.last_committed_write = query
        self.applied_writes.append(query)
        return {
            "endpoint": self.name,
            "lsn": self.current_lsn,
            "query": query,
            "epoch": self.write_epoch,
            "status": "committed",
        }

    def get_counter(self, name: str) -> int:
        return self.counters.get(name, 0)

    def set_counter(self, name: str, val: int) -> None:
        self.counters[name] = int(val)

    def advance_lsn(self, delta: int = 1) -> int:
        self.current_lsn += delta
        return self.current_lsn

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "role": self.role,
            "read_only": self.read_only,
            "current_lsn": self.current_lsn,
            "counters": dict(self.counters),
            "write_epoch": self.write_epoch,
            "active_sessions": self.active_sessions,
            "last_committed_write": self.last_committed_write,
        }


@dataclass
class ClientSession:
    client_id: str
    app_name: str
    host: str
    connected_endpoint: str = "source"
    cached_dns_target: str = "source"
    dns_ttl_remaining_sec: float = 0.0
    pool_size: int = 10
    active_connections: int = 2
    last_heartbeat: float = field(default_factory=time.time)
    acked_epoch: int = 0
    is_writer: bool = True

    def __post_init__(self):
        validate_test_host(self.host)

    def observe_dns(
        self,
        target_host: str,
        source_host: str,
        resolver_fn: Optional[Callable[[str], List[str]]] = None,
    ) -> str:
        """Observe actual DNS target mapping via live resolver."""
        res, ttl = resolve_dns_target(
            self.cached_dns_target, target_host, source_host, resolver_fn
        )
        if res in ("source", "target"):
            self.cached_dns_target = res
            self.dns_ttl_remaining_sec = ttl
        return self.cached_dns_target

    def observe_connections(
        self,
        active: Optional[int] = None,
        pool: Optional[int] = None,
    ) -> None:
        """Update connection telemetry from active client probe."""
        if active is not None:
            self.active_connections = max(0, int(active))
        if pool is not None:
            self.pool_size = max(0, int(pool))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ClientSession":
        return cls(**d)


def resolve_dns_target(
    hostname_or_target: str,
    target_host: str,
    source_host: str,
    resolver_fn: Optional[Callable[[str], List[str]]] = None,
) -> Tuple[str, float]:
    """Perform real DNS resolution to observe endpoint routing."""
    cleaned = str(hostname_or_target).strip()
    if not cleaned:
        return ("unknown", 0.0)
    if cleaned in (target_host, "target"):
        return ("target", 0.0)
    if cleaned in (source_host, "source"):
        return ("source", 30.0)
    try:
        if resolver_fn is not None:
            resolved_ips = list(resolver_fn(cleaned))
        else:
            resolved_ips = [
                info[4][0]
                for info in socket.getaddrinfo(cleaned, None, proto=socket.IPPROTO_TCP)
                if info and len(info) >= 5 and info[4]
            ]
    except (socket.gaierror, OSError):
        return ("unknown", 0.0)

    tgt_ips = {target_host}
    src_ips = {source_host}
    try:
        tgt_ips |= {info[4][0] for info in socket.getaddrinfo(target_host, None) if info and len(info) >= 5}
    except (socket.gaierror, OSError):
        pass
    try:
        src_ips |= {info[4][0] for info in socket.getaddrinfo(source_host, None) if info and len(info) >= 5}
    except (socket.gaierror, OSError):
        pass

    resolved_set = set(resolved_ips)
    if resolved_set & tgt_ips:
        return ("target", 0.0)
    if resolved_set & src_ips:
        return ("source", 30.0)
    return ("unknown", 0.0)


def observe_endpoint_active_sessions(
    endpoint: DatabaseEndpoint,
    query_fn: Optional[Callable[[str], Any]] = None,
) -> int:
    """Observe real active sessions on a database endpoint using SQL queries."""
    if query_fn is not None:
        try:
            res = query_fn(
                "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid()"
            )
            if isinstance(res, (int, float)):
                endpoint.active_sessions = int(res)
                return endpoint.active_sessions
            if isinstance(res, list) and res:
                row = res[0]
                val = row[0] if isinstance(row, (tuple, list)) else row.get("count", row.get("cnt", 0))
                endpoint.active_sessions = int(val)
                return endpoint.active_sessions
        except Exception:
            try:
                res = query_fn(
                    "SELECT count(*) FROM information_schema.processlist WHERE command != 'Sleep'"
                )
                if isinstance(res, (int, float)):
                    endpoint.active_sessions = int(res)
                    return endpoint.active_sessions
            except Exception:
                pass
    return endpoint.active_sessions


@dataclass
class SessionObservationReport:
    total_clients: int
    outstanding_source_sessions: List[str]
    stale_dns_clients: List[str]
    stale_pool_clients: List[str]
    unacknowledged_clients: List[str]
    read_routing: ReadRoutingState
    write_routing: WriteRoutingState
    care_badge: CareBadge
    summary: str

    def format_unix_report(self) -> str:
        lines = [
            f"[{self.care_badge.value}] SESSION-REPORT: total={self.total_clients} read_route={self.read_routing.value} write_route={self.write_routing.value}",
            f"  outstanding_source={len(self.outstanding_source_sessions)}: {','.join(self.outstanding_source_sessions) or 'none'}",
            f"  stale_dns={len(self.stale_dns_clients)}: {','.join(self.stale_dns_clients) or 'none'}",
            f"  stale_pools={len(self.stale_pool_clients)}: {','.join(self.stale_pool_clients) or 'none'}",
            f"  unacknowledged={len(self.unacknowledged_clients)}: {','.join(self.unacknowledged_clients) or 'none'}",
            f"  verdict: {self.summary}",
        ]
        return "\n".join(lines)


class SessionObserver:
    def __init__(self):
        self._clients: Dict[str, ClientSession] = {}

    def register_client(self, session: ClientSession) -> None:
        self._clients[session.client_id] = session

    def get_client(self, client_id: str) -> Optional[ClientSession]:
        return self._clients.get(client_id)

    def list_clients(self) -> List[ClientSession]:
        return list(self._clients.values())

    def update_heartbeat(
        self,
        client_id: str,
        connected_endpoint: Optional[str] = None,
        cached_dns_target: Optional[str] = None,
        dns_ttl_remaining_sec: Optional[float] = None,
        active_connections: Optional[int] = None,
        pool_size: Optional[int] = None,
    ) -> None:
        client = self._clients.get(client_id)
        if not client:
            return
        client.last_heartbeat = time.time()
        if connected_endpoint is not None:
            client.connected_endpoint = connected_endpoint
        if cached_dns_target is not None:
            client.cached_dns_target = cached_dns_target
        if dns_ttl_remaining_sec is not None:
            client.dns_ttl_remaining_sec = max(0.0, float(dns_ttl_remaining_sec))
        if active_connections is not None:
            client.active_connections = max(0, int(active_connections))
        if pool_size is not None:
            client.pool_size = max(0, int(pool_size))

    def acknowledge_client(self, client_id: str, epoch: int, connected_endpoint: str = "target") -> None:
        client = self._clients.get(client_id)
        if not client:
            return
        client.acked_epoch = int(epoch)
        client.connected_endpoint = connected_endpoint
        client.cached_dns_target = connected_endpoint
        client.dns_ttl_remaining_sec = 0.0

    def observe(
        self,
        target_epoch: int,
        read_routing: ReadRoutingState = ReadRoutingState.TARGET,
        write_routing: WriteRoutingState = WriteRoutingState.TARGET,
    ) -> SessionObservationReport:
        outstanding_source: List[str] = []
        stale_dns: List[str] = []
        stale_pools: List[str] = []
        unacknowledged: List[str] = []

        for cid, c in sorted(self._clients.items()):
            if c.connected_endpoint == "source" and c.active_connections > 0:
                outstanding_source.append(cid)
            if c.cached_dns_target == "source" and c.dns_ttl_remaining_sec > 0.0:
                stale_dns.append(cid)
            if write_routing in (WriteRoutingState.FENCED, WriteRoutingState.TARGET):
                if c.connected_endpoint == "source" and c.pool_size > 0:
                    stale_pools.append(cid)
            if c.acked_epoch < target_epoch:
                unacknowledged.append(cid)

        if stale_pools or (write_routing == WriteRoutingState.TARGET and outstanding_source):
            badge = CareBadge.ACTION
            summary = "action required: clients retain connections/pools to source during or after routing shift"
        elif unacknowledged:
            badge = CareBadge.ACTION
            summary = "action required: clients have not acknowledged cutover target epoch"
        elif stale_dns:
            badge = CareBadge.REVIEW
            summary = "review: cached DNS entries still resolving to source, but writes safely fenced"
        else:
            badge = CareBadge.PASS
            summary = "all clients cleanly transitioned and acknowledged target"

        return SessionObservationReport(
            total_clients=len(self._clients),
            outstanding_source_sessions=outstanding_source,
            stale_dns_clients=stale_dns,
            stale_pool_clients=stale_pools,
            unacknowledged_clients=unacknowledged,
            read_routing=read_routing,
            write_routing=write_routing,
            care_badge=badge,
            summary=summary,
        )


def counter_headroom_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding safety headroom for counter/sequence adjustment.

    Rungs:
    1. headroom_by_configured_margin: explicitly configured positive margin
    2. headroom_by_write_rate: derived dynamically from measured write rate and restart gap
    3. headroom_by_default_fallback: fallback safety headroom
    """
    return (
        decide.Rung(
            "headroom_by_configured_margin",
            "derive headroom from explicitly configured margin",
            needs=(
                decide.Need(
                    "explicit safety headroom specified",
                    lambda f: f.get("safety_headroom") is not None and int(f.get("safety_headroom", 0)) > 0,
                    "{rung} requires positive explicit headroom",
                ),
            ),
        ),
        decide.Rung(
            "headroom_by_write_rate",
            "derive headroom dynamically from measured write rate and restart gap",
            needs=(
                decide.Need(
                    "measured write rate and restart gap available",
                    lambda f: float(f.get("write_rate", 0.0)) > 0.0 and float(f.get("restart_gap_sec", 0.0)) > 0.0,
                    "{rung} requires positive write rate and restart gap",
                ),
            ),
        ),
        decide.Rung(
            "headroom_by_default_fallback",
            "fallback to default safety headroom margin",
            needs=(
                decide.Need(
                    "default fallback permitted",
                    lambda f: bool(f.get("allow_default_fallback", True)),
                    "{rung} requires default fallback allowance",
                ),
            ),
        ),
    )


def determine_safety_headroom(
    source_counters: Dict[str, int],
    target_counters: Dict[str, int],
    safety_headroom: Optional[int] = None,
    write_rate: Optional[float] = None,
    restart_gap_sec: Optional[float] = None,
    allow_default: bool = True,
) -> int:
    facts = {
        "safety_headroom": safety_headroom,
        "write_rate": write_rate or 0.0,
        "restart_gap_sec": restart_gap_sec or 0.0,
        "allow_default_fallback": allow_default,
    }
    choice = decide.climb("traffic_state:counter_headroom", counter_headroom_rungs(), facts)
    if choice.rung is None:
        return 0
    if choice.rung.name == "headroom_by_configured_margin":
        return int(safety_headroom)
    if choice.rung.name == "headroom_by_write_rate":
        return max(int(float(write_rate) * float(restart_gap_sec) * 1.1), 100)
    if choice.rung.name == "headroom_by_default_fallback":
        return 1000
    return 0


class CounterAdjustmentPlanner:
    @staticmethod
    def plan_adjustments(
        source_counters: Dict[str, int],
        target_counters: Dict[str, int],
        safety_headroom: Optional[int] = None,
        write_rate: Optional[float] = None,
        restart_gap_sec: Optional[float] = None,
        allow_default: bool = True,
    ) -> Dict[str, int]:
        margin = determine_safety_headroom(
            source_counters,
            target_counters,
            safety_headroom=safety_headroom,
            write_rate=write_rate,
            restart_gap_sec=restart_gap_sec,
            allow_default=allow_default,
        )
        adjustments: Dict[str, int] = {}
        all_keys = set(source_counters.keys()) | set(target_counters.keys())
        for key in sorted(all_keys):
            src_val = source_counters.get(key, 0)
            tgt_val = target_counters.get(key, 0)
            new_target_val = max(src_val, tgt_val) + margin
            adjustments[key] = new_target_val
        return adjustments

    @staticmethod
    def apply_adjustments(
        target: DatabaseEndpoint,
        adjustments: Dict[str, int],
    ) -> None:
        for k, v in adjustments.items():
            target.set_counter(k, v)


class ActiveLeaseLock:
    """Mutual exclusion lease backed directly by migkit.lease.Lease."""

    def __init__(self, lock_file: Path, term_seconds: float = 30.0):
        self.lock_file = Path(lock_file)
        self.term_seconds = float(term_seconds)
        self._lease = lease.Lease(
            self.lock_file,
            what="cutover traffic state",
            term=self.term_seconds,
        )

    def acquire(self) -> bool:
        try:
            self._lease.acquire()
            return True
        except (lease.Held, Exception) as exc:
            raise LeaseLockError(f"active lease lock held by another process: {self.lock_file} ({exc})") from exc

    def heartbeat(self) -> None:
        if not self._lease._renew_once():
            raise LeaseLockError("cannot heartbeat: lease not held")

    def release(self) -> None:
        self._lease.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class TrafficStateStore:
    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.state_dir / "cutover_traffic_state.json"
        self.journal_file = self.state_dir / "traffic_transitions.journal.jsonl"

    def write_state(self, state_dict: Dict[str, Any]) -> None:
        temp_file = self.state_dir / f"cutover_traffic_state.tmp.{os.getpid()}.{time.time_ns()}"
        data = json.dumps(state_dict, indent=2, sort_keys=True).encode("ascii")
        with open(temp_file, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, self.state_file)

    def read_state(self) -> Optional[Dict[str, Any]]:
        if not self.state_file.exists():
            return None
        with open(self.state_file, "rb") as f:
            data = f.read()
        return json.loads(data.decode("ascii"))

    def append_journal(self, entry: Dict[str, Any]) -> None:
        entry_with_ts = dict(entry)
        entry_with_ts.setdefault("timestamp", time.time())
        line = json.dumps(entry_with_ts, sort_keys=True) + "\n"
        with open(self.journal_file, "ab") as f:
            f.write(line.encode("ascii"))
            f.flush()
            os.fsync(f.fileno())


PHASE_SEQUENCE: List[CutoverPhase] = [
    CutoverPhase.INIT,
    CutoverPhase.PAUSE,
    CutoverPhase.DRAIN,
    CutoverPhase.FENCE,
    CutoverPhase.PROOF,
    CutoverPhase.COUNTER_ADJUSTMENT,
    CutoverPhase.ROUTING,
    CutoverPhase.RESUME,
    CutoverPhase.CLIENT_ACK,
    CutoverPhase.COMPLETED,
]


class TrafficStateMachine:
    def __init__(
        self,
        state_dir: Path,
        source: DatabaseEndpoint,
        target: DatabaseEndpoint,
        session_observer: Optional[SessionObserver] = None,
        epoch: int = 1,
        safety_headroom: Optional[int] = None,
        lease_term_sec: float = 30.0,
    ):
        self.state_dir = Path(state_dir)
        self.store = TrafficStateStore(self.state_dir)
        self.lease = ActiveLeaseLock(self.state_dir / ".run.lock", term_seconds=lease_term_sec)
        self.source = source if source is not None else DatabaseEndpoint(name="source")
        self.target = target if target is not None else DatabaseEndpoint(name="target")
        self.session_observer = session_observer or SessionObserver()
        self.epoch = int(epoch)
        self.safety_headroom = safety_headroom

        self.current_phase: CutoverPhase = CutoverPhase.INIT
        self.read_routing: ReadRoutingState = ReadRoutingState.SOURCE
        self.write_routing: WriteRoutingState = WriteRoutingState.SOURCE
        self.active_writer: str = "source"
        self.source_fenced: bool = False
        self.target_promoted: bool = False
        self.counter_adjustments: Dict[str, int] = {}
        self.client_acks: Dict[str, int] = {}
        self.last_care_badge: CareBadge = CareBadge.PASS
        self.journal_log: List[str] = []

        self._load_or_initialize()

    def _load_or_initialize(self) -> None:
        saved = self.store.read_state()
        if saved is not None:
            self.epoch = int(saved.get("epoch", self.epoch))
            self.current_phase = CutoverPhase(saved.get("current_phase", CutoverPhase.INIT.value))
            self.read_routing = ReadRoutingState(saved.get("read_routing", ReadRoutingState.SOURCE.value))
            self.write_routing = WriteRoutingState(saved.get("write_routing", WriteRoutingState.SOURCE.value))
            self.active_writer = saved.get("active_writer", "source")
            self.source_fenced = bool(saved.get("source_fenced", False))
            self.target_promoted = bool(saved.get("target_promoted", False))
            self.counter_adjustments = dict(saved.get("counter_adjustments", {}))
            self.client_acks = dict(saved.get("client_acks", {}))
            self.last_care_badge = CareBadge(saved.get("last_care_badge", CareBadge.PASS.value))
            self.source.read_only = bool(saved.get("source_read_only", self.source.read_only))
            self.target.read_only = bool(saved.get("target_read_only", self.target.read_only))
            self.target.write_epoch = int(saved.get("target_write_epoch", self.target.write_epoch))
            self.source.write_epoch = int(saved.get("source_write_epoch", self.source.write_epoch))
        else:
            self._persist(event="INITIALIZED", badge=CareBadge.PASS)

    def _persist(self, event: str, badge: CareBadge = CareBadge.PASS, details: Optional[Dict[str, Any]] = None) -> None:
        self.last_care_badge = badge
        state_dict = {
            "format_version": "1.0",
            "epoch": self.epoch,
            "current_phase": self.current_phase.value,
            "read_routing": self.read_routing.value,
            "write_routing": self.write_routing.value,
            "active_writer": self.active_writer,
            "source_fenced": self.source_fenced,
            "target_promoted": self.target_promoted,
            "counter_adjustments": self.counter_adjustments,
            "client_acks": self.client_acks,
            "last_care_badge": self.last_care_badge.value,
            "source_read_only": self.source.read_only,
            "target_read_only": self.target.read_only,
            "source_write_epoch": self.source.write_epoch,
            "target_write_epoch": self.target.write_epoch,
            "source_lsn": self.source.current_lsn,
            "target_lsn": self.target.current_lsn,
            "updated_at": time.time(),
        }
        self.store.write_state(state_dict)
        journal_entry = {
            "event": event,
            "phase": self.current_phase.value,
            "badge": badge.value,
            "active_writer": self.active_writer,
            "details": details or {},
        }
        self.store.append_journal(journal_entry)
        log_line = f"[{badge.value}] PHASE: {self.current_phase.value} event={event}"
        self.journal_log.append(log_line)

    def acquire_lease(self) -> None:
        self.lease.acquire()

    def release_lease(self) -> None:
        self.lease.release()

    def heartbeat_lease(self) -> None:
        self.lease.heartbeat()

    def step_pause(self) -> CareBadge:
        if self.current_phase not in (CutoverPhase.INIT, CutoverPhase.PAUSE):
            raise InvalidPhaseTransitionError(f"cannot enter PAUSE from phase {self.current_phase.value}")
        self.current_phase = CutoverPhase.PAUSE
        badge = CareBadge.PASS
        self._persist(event="CLIENT_INGRESS_PAUSED", badge=badge)
        return badge

    def step_drain(
        self, timeout_sec: float = 5.0, catchup_fn: Optional[Callable[[], None]] = None
    ) -> CareBadge:
        if self.current_phase != CutoverPhase.PAUSE:
            raise InvalidPhaseTransitionError(f"cannot enter DRAIN from phase {self.current_phase.value}")
        start_time = time.time()
        while self.target.current_lsn < self.source.current_lsn:
            if catchup_fn is not None:
                catchup_fn()
            if self.target.current_lsn >= self.source.current_lsn:
                break
            if time.time() - start_time > timeout_sec:
                self.current_phase = CutoverPhase.FAILED
                self._persist(
                    event="DRAIN_TIMEOUT",
                    badge=CareBadge.ACTION,
                    details={"src_lsn": self.source.current_lsn, "tgt_lsn": self.target.current_lsn},
                )
                raise DrainTimeoutError(
                    f"drain timed out: target LSN ({self.target.current_lsn}) behind source LSN ({self.source.current_lsn})"
                )
            time.sleep(0.01)

        self.current_phase = CutoverPhase.DRAIN
        badge = CareBadge.PASS
        self._persist(
            event="REPLICATION_DRAINED",
            badge=badge,
            details={"src_lsn": self.source.current_lsn, "tgt_lsn": self.target.current_lsn},
        )
        return badge

    def step_fence(self) -> CareBadge:
        if self.current_phase != CutoverPhase.DRAIN:
            raise InvalidPhaseTransitionError(f"cannot enter FENCE from phase {self.current_phase.value}")
        self.source.set_read_only(True)
        self.source_fenced = True
        self.write_routing = WriteRoutingState.FENCED
        self.active_writer = "none"
        self.current_phase = CutoverPhase.FENCE
        badge = CareBadge.PASS
        self._persist(event="SOURCE_FENCED", badge=badge, details={"source_read_only": True})
        return badge

    def step_proof(self) -> CareBadge:
        if self.current_phase != CutoverPhase.FENCE:
            raise InvalidPhaseTransitionError(f"cannot enter PROOF from phase {self.current_phase.value}")
        if not self.source.read_only:
            self.current_phase = CutoverPhase.FAILED
            self._persist(event="PROOF_SOURCE_NOT_FENCED", badge=CareBadge.ACTION)
            raise ProofMismatchError("proof failed: source is not read-only")
        if self.target.current_lsn < self.source.current_lsn:
            self.current_phase = CutoverPhase.FAILED
            self._persist(event="PROOF_LSN_MISMATCH", badge=CareBadge.ACTION)
            raise ProofMismatchError(
                f"proof failed: target LSN ({self.target.current_lsn}) < source LSN ({self.source.current_lsn})"
            )
        self.current_phase = CutoverPhase.PROOF
        badge = CareBadge.PASS
        self._persist(event="REPLICATION_PROOF_VALIDATED", badge=badge)
        return badge

    def step_counter_adjustment(self) -> CareBadge:
        if self.current_phase != CutoverPhase.PROOF:
            raise InvalidPhaseTransitionError(f"cannot enter COUNTER_ADJUSTMENT from phase {self.current_phase.value}")
        adjustments = CounterAdjustmentPlanner.plan_adjustments(
            self.source.counters,
            self.target.counters,
            safety_headroom=self.safety_headroom,
        )
        CounterAdjustmentPlanner.apply_adjustments(self.target, adjustments)
        self.counter_adjustments = adjustments
        self.current_phase = CutoverPhase.COUNTER_ADJUSTMENT
        badge = CareBadge.PASS
        self._persist(event="COUNTERS_ADJUSTED", badge=badge, details={"adjustments": adjustments})
        return badge

    def step_routing(self) -> CareBadge:
        if self.current_phase != CutoverPhase.COUNTER_ADJUSTMENT:
            raise InvalidPhaseTransitionError(f"cannot enter ROUTING from phase {self.current_phase.value}")
        self.target.set_read_only(False)
        self.target.write_epoch = self.epoch
        self.target_promoted = True
        self.active_writer = "target"
        self.write_routing = WriteRoutingState.TARGET
        self.read_routing = ReadRoutingState.TARGET
        self.current_phase = CutoverPhase.ROUTING
        badge = CareBadge.PASS
        self._persist(
            event="ROUTING_TARGET_PROMOTED",
            badge=badge,
            details={"target_write_epoch": self.epoch, "write_routing": self.write_routing.value},
        )
        return badge

    def shift_read_routing(self, new_state: ReadRoutingState) -> CareBadge:
        self.read_routing = new_state
        badge = CareBadge.PASS
        self._persist(event="READ_ROUTING_SHIFTED", badge=badge, details={"read_routing": new_state.value})
        return badge

    def step_resume(self) -> CareBadge:
        if self.current_phase != CutoverPhase.ROUTING:
            raise InvalidPhaseTransitionError(f"cannot enter RESUME from phase {self.current_phase.value}")
        self.current_phase = CutoverPhase.RESUME
        badge = CareBadge.PASS
        self._persist(event="CLIENT_INGRESS_RESUMED", badge=badge)
        return badge

    def step_client_ack(self) -> CareBadge:
        if self.current_phase != CutoverPhase.RESUME:
            raise InvalidPhaseTransitionError(f"cannot enter CLIENT_ACK from phase {self.current_phase.value}")
        self.current_phase = CutoverPhase.CLIENT_ACK
        report = self.session_observer.observe(
            target_epoch=self.epoch,
            read_routing=self.read_routing,
            write_routing=self.write_routing,
        )
        badge = report.care_badge
        self._persist(event="CLIENT_ACK_OBSERVATION", badge=badge, details={"summary": report.summary})
        return badge

    def complete_cutover(self) -> CareBadge:
        if self.current_phase not in (CutoverPhase.CLIENT_ACK, CutoverPhase.RESUME):
            raise InvalidPhaseTransitionError(f"cannot complete cutover from phase {self.current_phase.value}")
        report = self.session_observer.observe(
            target_epoch=self.epoch,
            read_routing=self.read_routing,
            write_routing=self.write_routing,
        )
        if report.unacknowledged_clients or report.outstanding_source_sessions or report.stale_pool_clients:
            self._persist(
                event="CUTOVER_COMPLETION_REFUSED",
                badge=CareBadge.ACTION,
                details={
                    "unacknowledged": report.unacknowledged_clients,
                    "outstanding_source": report.outstanding_source_sessions,
                    "stale_pools": report.stale_pool_clients,
                },
            )
            raise CutoverIncompleteError(
                "API success or database promotion alone is not application cutover completion: "
                f"unacknowledged={len(report.unacknowledged_clients)}, "
                f"outstanding_source={len(report.outstanding_source_sessions)}, "
                f"stale_pools={len(report.stale_pool_clients)}"
            )

        self.current_phase = CutoverPhase.COMPLETED
        badge = CareBadge.PASS
        self._persist(event="CUTOVER_COMPLETED", badge=badge, details={"summary": report.summary})
        return badge

    def rollback(self, reason: str = "") -> CareBadge:
        self.target.set_read_only(True)
        self.target_promoted = False
        self.source.set_read_only(False)
        self.source_fenced = False
        self.active_writer = "source"
        self.write_routing = WriteRoutingState.SOURCE
        self.read_routing = ReadRoutingState.SOURCE
        self.current_phase = CutoverPhase.ROLLED_BACK
        badge = CareBadge.REVIEW
        self._persist(event="CUTOVER_ROLLED_BACK", badge=badge, details={"reason": reason})
        return badge

    def reconcile_active_writer(self) -> Tuple[str, CareBadge]:
        src_writable = not self.source.read_only
        tgt_writable = not self.target.read_only

        if src_writable and tgt_writable:
            self.source.set_read_only(True)
            self.target.set_read_only(True)
            self.source_fenced = True
            self.active_writer = "none"
            self.write_routing = WriteRoutingState.FENCED
            self._persist(
                event="SPLIT_BRAIN_DETECTED_FENCED_BOTH",
                badge=CareBadge.ACTION,
                details={"src_writable": True, "tgt_writable": True},
            )
            raise SplitBrainDetectedError(
                "split-brain detected: both source and target were writable; fenced both immediately"
            )

        if tgt_writable and not src_writable:
            self.active_writer = "target"
            self.write_routing = WriteRoutingState.TARGET
            self.source_fenced = True
            self.target_promoted = True
            badge = CareBadge.PASS
            self._persist(
                event="RECONCILED_WRITER_TARGET",
                badge=badge,
                details={"active_writer": "target", "source_fenced": True},
            )
            return ("target", badge)

        if src_writable and not tgt_writable:
            self.active_writer = "source"
            self.write_routing = WriteRoutingState.SOURCE
            self.source_fenced = False
            self.target_promoted = False
            badge = CareBadge.REVIEW
            self._persist(
                event="RECONCILED_WRITER_SOURCE",
                badge=badge,
                details={"active_writer": "source", "cutover_incomplete": True},
            )
            return ("source", badge)

        self.active_writer = "none"
        self.write_routing = WriteRoutingState.FENCED
        badge = CareBadge.REVIEW
        self._persist(
            event="RECONCILED_WRITER_NONE_FENCED",
            badge=badge,
            details={"active_writer": "none"},
        )
        return ("none", badge)

    def transition_to(self, phase: Union[CutoverPhase, str]) -> CareBadge:
        """Execute state machine transitions to target phase."""
        p = str(getattr(phase, "value", phase)).upper()
        if p in ("PAUSE",):
            return self.step_pause()
        elif p in ("DRAIN",):
            if self.current_phase == CutoverPhase.INIT:
                self.step_pause()
            return self.step_drain()
        elif p in ("FENCE",):
            return self.step_fence()
        elif p in ("PROOF",):
            return self.step_proof()
        elif p in ("COUNTER_ADJUSTMENT", "COUNTERS"):
            return self.step_counter_adjustment()
        elif p in ("ROUTING", "PROMOTE"):
            if self.current_phase != CutoverPhase.ROUTING:
                if self.current_phase in (CutoverPhase.INIT, CutoverPhase.PAUSE):
                    self.step_drain()
                if self.current_phase == CutoverPhase.DRAIN:
                    self.step_fence()
                if self.current_phase == CutoverPhase.FENCE:
                    self.step_proof()
                if self.current_phase == CutoverPhase.PROOF:
                    self.step_counter_adjustment()
                return self.step_routing()
            return CareBadge.PASS
        elif p in ("RESUME",):
            return self.step_resume()
        elif p in ("CLIENT_ACK",):
            return self.step_client_ack()
        elif p in ("COMPLETE", "COMPLETED"):
            return self.complete_cutover()
        elif p in ("ROLLBACK", "ROLLED_BACK"):
            return self.rollback()
        raise InvalidPhaseTransitionError(f"unknown or unhandled transition target: {phase}")
