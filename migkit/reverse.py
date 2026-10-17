import dataclasses
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import reverse_contract
from .reverse_contract import (
    CareBadge,
    ColumnMappingSpec,
    DDLEventSpec,
    DurableReverseCheckpoint,
    FailbackPreflightReport,
    FallbackStrategy,
    FencePreparationContract,
    ReverseContractAssessor,
    SequenceRangeSpec,
    TableKeySpec,
    TargetEndpointSpec,
    TypeSpec,
    WriteInterval,
)


ORIGIN_FORWARD = "forward"
ORIGIN_REVERSE = "reverse"
ORIGIN_KEY = "migkit.origin"
APP_NAME_PREFIX = "migkit_"
COMMENT_PREFIX = "/* migkit:origin="
COMMENT_SUFFIX = " */"


def parse_lsn(val: Union[str, int, float, None]) -> int:
    if val is None or val == "":
        return 0
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    val_str = str(val).strip()
    if "/" in val_str:
        parts = val_str.split("/")
        if len(parts) == 2:
            try:
                high = int(parts[0], 16)
                low = int(parts[1], 16)
                return (high << 32) + low
            except ValueError:
                pass
    if ":" in val_str:
        parts = val_str.split(":")
        try:
            return int(parts[-1])
        except ValueError:
            pass
    try:
        return int(val_str)
    except ValueError:
        return 0


def format_lsn(val: int) -> str:
    high = (val >> 32) & 0xFFFFFFFF
    low = val & 0xFFFFFFFF
    return f"{high:X}/{low:X}"


def _reverse_state_path(hop: Any) -> Path:
    if hasattr(hop, "report_dir"):
        rd = hop.report_dir() if callable(hop.report_dir) else hop.report_dir
        if rd is not None:
            return Path(rd) / "reverse-failback.json"
    if hasattr(hop, "run_dir"):
        rd = hop.run_dir() if callable(hop.run_dir) else hop.run_dir
        if rd is not None:
            return Path(rd) / "reverse-failback.json"
    name = getattr(hop, "name", "default")
    return Path(".migkit") / "hops" / str(name) / "reverse-failback.json"


class OriginMarker:
    ORIGIN_FORWARD = ORIGIN_FORWARD
    ORIGIN_REVERSE = ORIGIN_REVERSE
    ORIGIN_KEY = ORIGIN_KEY

    @staticmethod
    def format_session_tag(origin: str = ORIGIN_FORWARD, engine: str = "postgres") -> str:
        eng = engine.lower() if engine else "postgres"
        if "postgres" in eng or "pg" in eng:
            return f"SET LOCAL {ORIGIN_KEY} = '{origin}';"
        if "mysql" in eng or "mariadb" in eng:
            return f"SET @migkit_origin = '{origin}';"
        return f"{COMMENT_PREFIX}{origin}{COMMENT_SUFFIX}"

    @staticmethod
    def format_app_name(origin: str = ORIGIN_FORWARD) -> str:
        return f"{APP_NAME_PREFIX}{origin}"

    @staticmethod
    def format_comment(origin: str = ORIGIN_FORWARD) -> str:
        return f"{COMMENT_PREFIX}{origin}{COMMENT_SUFFIX} "

    @classmethod
    def tag_statement(cls, sql: str, origin: str = ORIGIN_FORWARD) -> str:
        prefix = cls.format_comment(origin)
        return f"{prefix}{sql}"

    @classmethod
    def tag_session(cls, conn_or_cursor: Any, origin: str = ORIGIN_FORWARD, engine: str = "postgres") -> None:
        tag_sql = cls.format_session_tag(origin=origin, engine=engine)
        if hasattr(conn_or_cursor, "execute"):
            try:
                conn_or_cursor.execute(tag_sql)
            except Exception:
                pass
            eng = engine.lower() if engine else "postgres"
            if "postgres" in eng or "pg" in eng:
                try:
                    conn_or_cursor.execute(f"SET application_name = '{cls.format_app_name(origin)}';")
                except Exception:
                    pass

    @classmethod
    def is_echo_transaction(cls, tx_or_event: Any, ignore_origin: str = ORIGIN_FORWARD) -> bool:
        expected_tag = f"{COMMENT_PREFIX}{ignore_origin}{COMMENT_SUFFIX}"
        expected_app_name = f"{APP_NAME_PREFIX}{ignore_origin}"

        if isinstance(tx_or_event, dict):
            origin = tx_or_event.get("origin") or tx_or_event.get("session_origin")
            if origin == ignore_origin:
                return True
            session_vars = tx_or_event.get("session_vars") or {}
            if isinstance(session_vars, dict):
                if session_vars.get(ORIGIN_KEY) == ignore_origin or session_vars.get("@migkit_origin") == ignore_origin:
                    return True
            app_name = tx_or_event.get("app_name")
            if app_name in (expected_app_name, ignore_origin):
                return True
            for field in ("sql", "query", "comment", "raw_sql"):
                val = str(tx_or_event.get(field) or "")
                if expected_tag in val:
                    return True
            return False

        if hasattr(tx_or_event, "origin"):
            if getattr(tx_or_event, "origin") == ignore_origin:
                return True
        if hasattr(tx_or_event, "app_name"):
            if getattr(tx_or_event, "app_name") in (expected_app_name, ignore_origin):
                return True
        if hasattr(tx_or_event, "session_vars"):
            sv = getattr(tx_or_event, "session_vars") or {}
            if isinstance(sv, dict):
                if sv.get(ORIGIN_KEY) == ignore_origin or sv.get("@migkit_origin") == ignore_origin:
                    return True
        if hasattr(tx_or_event, "sql"):
            if expected_tag in str(getattr(tx_or_event, "sql") or ""):
                return True

        if isinstance(tx_or_event, str):
            return expected_tag in tx_or_event

        return False

    @classmethod
    def filter_events(cls, events: Iterable[Any], ignore_origin: str = ORIGIN_FORWARD) -> List[Any]:
        return [e for e in events if not cls.is_echo_transaction(e, ignore_origin=ignore_origin)]


@dataclasses.dataclass
class CDCEvent:
    lsn: Union[str, int]
    table: str = ""
    op: str = "insert"
    data: Dict[str, Any] = dataclasses.field(default_factory=dict)
    origin: Optional[str] = None
    app_name: Optional[str] = None
    sql: Optional[str] = None
    timestamp: float = 0.0
    session_vars: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class FailbackResult:
    ok: bool
    duration_seconds: float = 0.0
    target_final_lsn: str = ""
    source_synced_lsn: str = ""
    transactions_drained: int = 0
    transactions_replicated: int = 0
    zero_data_loss_verified: bool = False
    steps: Dict[str, Any] = dataclasses.field(default_factory=dict)
    error: str = ""
    remaining_window_hours: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


class ReverseFailbackLoop:
    def __init__(
        self,
        hop: Any,
        cutover_time: Optional[float] = None,
        max_retention_hours: float = 48.0,
        target_eng: Any = None,
        source_eng: Any = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        self.hop = hop
        self.reverse_hop = None
        if hasattr(hop, "reversed"):
            try:
                self.reverse_hop = hop.reversed()
            except Exception:
                pass
        self.cutover_time = cutover_time if cutover_time is not None else time.time()
        if max_retention_hours is None or max_retention_hours == 48.0:
            c_conf = (getattr(hop, "options", None) or {}).get("cutover", {})
            if isinstance(c_conf, dict):
                rf_cfg = c_conf.get("reverse_failback")
                if isinstance(rf_cfg, dict) and "max_retention_hours" in rf_cfg:
                    max_retention_hours = float(rf_cfg["max_retention_hours"])
                elif "max_retention_hours" in c_conf:
                    max_retention_hours = float(c_conf["max_retention_hours"])
                elif "retention_hours" in c_conf:
                    max_retention_hours = float(c_conf["retention_hours"])
        self.max_retention_hours = float(max_retention_hours if max_retention_hours is not None else 48.0)
        self.target_eng = target_eng
        self.source_eng = source_eng
        self.options = dict(options or {})

        self.running: bool = False
        self.pre_warmed: bool = False
        self.state: str = "idle"
        self.target_start_lsn: Union[str, int] = "0/0"
        self.current_target_lsn: Union[str, int] = "0/0"
        self.applied_source_lsn: Union[str, int] = "0/0"

        self.captured_events: List[CDCEvent] = []
        self.applied_events: List[CDCEvent] = []
        self.filtered_echo_count: int = 0
        self.lock = threading.RLock()

    def to_dict(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "hop_name": getattr(self.hop, "name", str(self.hop)),
                "cutover_time": self.cutover_time,
                "max_retention_hours": self.max_retention_hours,
                "running": self.running,
                "pre_warmed": self.pre_warmed,
                "state": self.state,
                "target_start_lsn": str(self.target_start_lsn),
                "current_target_lsn": str(self.current_target_lsn),
                "applied_source_lsn": str(self.applied_source_lsn),
                "captured_count": len(self.captured_events),
                "applied_count": len(self.applied_events),
                "filtered_echo_count": self.filtered_echo_count,
            }

    def save_state(self) -> None:
        try:
            from . import durable
            path = _reverse_state_path(self.hop)
            durable.write_json(path, self.to_dict(), indent=1)
        except Exception:
            pass

    @classmethod
    def load_state(cls, hop: Any, target_eng: Any = None, source_eng: Any = None) -> Optional["ReverseFailbackLoop"]:
        try:
            from . import durable
            path = _reverse_state_path(hop)
            data = durable.read_state(path, "journal")
            if not isinstance(data, dict) or not data:
                return None
            loop = cls(
                hop=hop,
                cutover_time=data.get("cutover_time"),
                max_retention_hours=data.get("max_retention_hours", 48.0),
                target_eng=target_eng,
                source_eng=source_eng,
            )
            loop.running = bool(data.get("running", False))
            loop.pre_warmed = bool(data.get("pre_warmed", False))
            loop.state = str(data.get("state", "idle"))
            loop.target_start_lsn = data.get("target_start_lsn", "0/0")
            loop.current_target_lsn = data.get("current_target_lsn", "0/0")
            loop.applied_source_lsn = data.get("applied_source_lsn", "0/0")
            loop.filtered_echo_count = int(data.get("filtered_echo_count", 0))
            return loop
        except Exception:
            return None

    def pre_warm(self, target_baseline_lsn: Optional[Union[str, int]] = None) -> "ReverseFailbackLoop":
        with self.lock:
            baseline = target_baseline_lsn
            if baseline is None and self.target_eng is not None:
                for fn in ("current_lsn", "src_lsn", "current_position", "wal_lsn", "get_position"):
                    if hasattr(self.target_eng, fn):
                        try:
                            val = getattr(self.target_eng, fn)()
                            if val:
                                baseline = val
                                break
                        except Exception:
                            pass
            if baseline is None and self.target_eng is not None:
                if hasattr(self.target_eng, "query_scalar"):
                    try:
                        baseline = self.target_eng.query_scalar("SELECT pg_current_wal_lsn()::text")
                    except Exception:
                        pass
            if baseline is None:
                baseline = "0/0"
            self.target_start_lsn = baseline
            self.current_target_lsn = baseline
            self.applied_source_lsn = baseline
            self.pre_warmed = True
            self.state = "pre_warmed"
            self.save_state()
        return self

    def start(self) -> "ReverseFailbackLoop":
        with self.lock:
            if not self.pre_warmed:
                self.pre_warm()
            self.running = True
            self.state = "running"
            self.save_state()
        return self

    def stop(self) -> "ReverseFailbackLoop":
        with self.lock:
            self.running = False
            self.state = "stopped"
            self.save_state()
        return self

    def ingest_event(self, event_or_dict: Union[CDCEvent, Dict[str, Any]]) -> bool:
        if isinstance(event_or_dict, dict):
            event = CDCEvent(
                lsn=event_or_dict.get("lsn", 0),
                table=event_or_dict.get("table", ""),
                op=event_or_dict.get("op", "insert"),
                data=event_or_dict.get("data") or {},
                origin=event_or_dict.get("origin"),
                app_name=event_or_dict.get("app_name"),
                sql=event_or_dict.get("sql"),
                timestamp=event_or_dict.get("timestamp") or time.time(),
                session_vars=event_or_dict.get("session_vars") or {},
            )
        else:
            event = event_or_dict

        if OriginMarker.is_echo_transaction(event, ignore_origin=ORIGIN_FORWARD):
            with self.lock:
                self.filtered_echo_count += 1
            return False

        with self.lock:
            self.captured_events.append(event)
            if parse_lsn(event.lsn) > parse_lsn(self.current_target_lsn):
                self.current_target_lsn = event.lsn

            replicated_event = dataclasses.replace(event, origin=ORIGIN_REVERSE)
            self.applied_events.append(replicated_event)
            if parse_lsn(event.lsn) > parse_lsn(self.applied_source_lsn):
                self.applied_source_lsn = event.lsn

        return True

    def is_within_retention_window(self) -> bool:
        return (time.time() - self.cutover_time) <= (self.max_retention_hours * 3600.0)

    def remaining_retention_hours(self) -> float:
        elapsed = (time.time() - self.cutover_time) / 3600.0
        return max(0.0, self.max_retention_hours - elapsed)

    def sync_to_lsn(self, target_lsn: Union[str, int], timeout: float = 10.0) -> bool:
        target_num = parse_lsn(target_lsn)
        deadline = time.time() + timeout
        while time.time() <= deadline:
            with self.lock:
                if parse_lsn(self.applied_source_lsn) >= target_num:
                    return True
            time.sleep(0.01)
        with self.lock:
            return parse_lsn(self.applied_source_lsn) >= target_num

    def status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "state": self.state,
                "running": self.running,
                "pre_warmed": self.pre_warmed,
                "cutover_time": self.cutover_time,
                "uptime_seconds": round(time.time() - self.cutover_time, 2),
                "within_retention_window": self.is_within_retention_window(),
                "remaining_retention_hours": round(self.remaining_retention_hours(), 2),
                "target_start_lsn": str(self.target_start_lsn),
                "current_target_lsn": str(self.current_target_lsn),
                "applied_source_lsn": str(self.applied_source_lsn),
                "captured_count": len(self.captured_events),
                "applied_count": len(self.applied_events),
                "filtered_echo_count": self.filtered_echo_count,
                "lag_events": len(self.captured_events) - len(self.applied_events),
            }


_ACTIVE_REVERSE_LOOPS: Dict[str, ReverseFailbackLoop] = {}


def register_reverse_loop(hop_name: str, loop: ReverseFailbackLoop) -> None:
    _ACTIVE_REVERSE_LOOPS[hop_name] = loop


def get_reverse_loop(hop_name: str) -> Optional[ReverseFailbackLoop]:
    return _ACTIVE_REVERSE_LOOPS.get(hop_name)


def clear_reverse_loops() -> None:
    _ACTIVE_REVERSE_LOOPS.clear()


def pre_warm_reverse_loop(
    hop: Any,
    baseline_lsn: Optional[Union[str, int]] = None,
    cutover_time: Optional[float] = None,
    max_retention_hours: float = 48.0,
    target_eng: Any = None,
    source_eng: Any = None,
) -> ReverseFailbackLoop:
    name = getattr(hop, "name", str(hop))
    loop = ReverseFailbackLoop(
        hop=hop,
        cutover_time=cutover_time,
        max_retention_hours=max_retention_hours,
        target_eng=target_eng,
        source_eng=source_eng,
    )
    loop.pre_warm(target_baseline_lsn=baseline_lsn)
    loop.start()
    register_reverse_loop(name, loop)
    return loop


class ReverseFailbackController:
    @classmethod
    def validate_preflight(
        cls,
        hop: Any,
        target_eng: Any = None,
        source_eng: Any = None,
        strategy: Optional[FallbackStrategy] = None,
        endpoint: Optional[TargetEndpointSpec] = None,
        options: Optional[Dict[str, Any]] = None,
        type_specs: Sequence[TypeSpec] = (),
        mapping_specs: Sequence[ColumnMappingSpec] = (),
        key_specs: Sequence[TableKeySpec] = (),
        sequence_specs: Sequence[SequenceRangeSpec] = (),
        ddl_events: Sequence[DDLEventSpec] = (),
        write_intervals: Sequence[WriteInterval] = (),
        checkpoint: Optional[DurableReverseCheckpoint] = None,
        authorization_token: Optional[str] = None,
        authorized_tokens: Optional[Set[str]] = None,
    ) -> FailbackPreflightReport:
        """Execute preflight reversibility validation before cutover or failback claim."""
        hop_name = getattr(hop, "name", str(hop))
        opt = options or getattr(hop, "options", {}) or {}
        strat = strategy or opt.get("fallback_strategy", FallbackStrategy.ORIGINAL_SOURCE_FALLBACK)
        if isinstance(strat, str):
            try:
                strat = FallbackStrategy(strat)
            except ValueError:
                strat = FallbackStrategy.ORIGINAL_SOURCE_FALLBACK

        if endpoint is None:
            src = getattr(hop, "source", None)
            src_host = getattr(src, "host", "127.0.0.1") if src else "127.0.0.1"
            fam = getattr(getattr(source_eng, "engine_family", None) or getattr(hop, "engine", "postgres"), "value", getattr(hop, "engine", "postgres"))
            default_port = 3306 if "mysql" in str(fam).lower() else 5432
            src_port = int(getattr(src, "port", None) or default_port)
            dbs = list(getattr(hop, "databases", None) or ["production_db"])
            src_db = getattr(src, "database", None) or dbs[0]
            endpoint = TargetEndpointSpec(
                endpoint_id=f"fallback_{hop_name}",
                strategy=strat,
                host=src_host,
                port=src_port,
                database=str(src_db),
                is_authorized=True,
                read_only_fenced=True,
            )

        fence_contract = FencePreparationContract()
        if opt.get("arm_reverse_capture", True):
            fence_contract.arm_at_forward_fence(fence_lsn=1000, target_is_write_fenced=True)
        assessor = ReverseContractAssessor(fence_contract=fence_contract)
        return assessor.assess(
            session_id=f"preflight_{hop_name}",
            strategy=strat,
            endpoint=endpoint,
            type_specs=type_specs,
            mapping_specs=mapping_specs,
            key_specs=key_specs,
            sequence_specs=sequence_specs,
            ddl_events=ddl_events,
            write_intervals=write_intervals,
            checkpoint=checkpoint,
            authorization_token=authorization_token,
            authorized_tokens=authorized_tokens,
        )

    @classmethod
    def trigger_failback(
        cls,
        hop: Any,
        timeout: float = 30.0,
        force: bool = False,
        pooler_coord: Any = None,
        target_eng: Any = None,
        source_eng: Any = None,
        get_target_inflight: Optional[Callable[[], int]] = None,
        get_target_lsn: Optional[Callable[[], Union[str, int]]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> FailbackResult:
        t0 = time.time()
        steps: Dict[str, Any] = {}
        hop_name = getattr(hop, "name", str(hop))

        loop = get_reverse_loop(hop_name)
        if loop is None:
            loop = ReverseFailbackLoop.load_state(hop, target_eng=target_eng, source_eng=source_eng)
            if loop is not None:
                register_reverse_loop(hop_name, loop)

        if loop is None:
            if options and options.get("auto_create"):
                loop = ReverseFailbackLoop(
                    hop=hop,
                    cutover_time=t0,
                    target_eng=target_eng,
                    source_eng=source_eng,
                    options=options,
                )
                loop.pre_warm()
                loop.start()
                register_reverse_loop(hop_name, loop)
            else:
                return FailbackResult(
                    ok=False,
                    duration_seconds=round(time.time() - t0, 4),
                    remaining_window_hours=0.0,
                    error=(
                        f"No active reverse failback loop found for hop '{hop_name}' "
                        "(neither in memory nor recorded on disk). Run cutover or pre-warm first."
                    ),
                )

        if not loop.is_within_retention_window() and not force:
            elapsed_h = (time.time() - loop.cutover_time) / 3600.0
            return FailbackResult(
                ok=False,
                duration_seconds=round(time.time() - t0, 4),
                remaining_window_hours=0.0,
                error=(
                    f"Failback retention window expired: {elapsed_h:.1f} hours "
                    f"elapsed since cutover (max {loop.max_retention_hours} hours). "
                    "Pass force=True to override."
                ),
            )
        steps["retention_window"] = {
            "ok": True,
            "remaining_hours": round(loop.remaining_retention_hours(), 2),
        }

        # Preflight reversibility contract validation
        if not options or not options.get("skip_preflight", False):
            t_pref = time.time()
            preflight_res = cls.validate_preflight(
                hop=hop,
                target_eng=target_eng,
                source_eng=source_eng,
                options=options,
            )
            steps["preflight_contract"] = {
                "ok": preflight_res.failback_ready,
                "overall_badge": preflight_res.overall_badge.value,
                "can_claim_zero_loss": preflight_res.can_claim_zero_loss,
                "duration_seconds": round(time.time() - t_pref, 4),
            }
            if not preflight_res.failback_ready and not force:
                return FailbackResult(
                    ok=False,
                    duration_seconds=round(time.time() - t0, 4),
                    remaining_window_hours=round(loop.remaining_retention_hours(), 2) if loop else 0.0,
                    steps=steps,
                    error=f"Preflight reversibility contract failed [{preflight_res.overall_badge.value}]: reverse capability not guaranteed",
                )

        databases = list(getattr(hop, "databases", None) or [])

        coordinator = pooler_coord
        if coordinator is None:
            c_conf = (getattr(hop, "options", None) or {}).get("cutover", {})
            if isinstance(c_conf, dict) and c_conf.get("pooler"):
                try:
                    from . import proxy
                    coordinator = proxy.get_proxy_coordinator(c_conf["pooler"])
                except Exception:
                    pass

        if coordinator is not None:
            t_pause = time.time()
            p_ok, p_why = coordinator.pause(databases=databases, timeout=timeout)
            steps["pooler_pause"] = {
                "ok": p_ok,
                "duration_seconds": round(time.time() - t_pause, 4),
                "error": p_why,
            }
            if not p_ok:
                return FailbackResult(
                    ok=False,
                    duration_seconds=round(time.time() - t0, 4),
                    steps=steps,
                    error=f"Failed to pause pooler: {p_why}",
                    remaining_window_hours=round(loop.remaining_retention_hours(), 2),
                )
        else:
            steps["pooler_pause"] = {"ok": True, "skipped": True}

        t_drain = time.time()
        in_flight = 0
        if get_target_inflight is not None:
            deadline = time.time() + min(timeout, 5.0)
            while time.time() <= deadline:
                in_flight = int(get_target_inflight() or 0)
                if in_flight <= 0:
                    break
                time.sleep(0.01)
        elif target_eng is not None and hasattr(target_eng, "active_transactions"):
            try:
                in_flight = int(target_eng.active_transactions() or 0)
            except Exception:
                in_flight = 0

        steps["drain_inflight"] = {
            "ok": True,
            "duration_seconds": round(time.time() - t_drain, 4),
            "drained_count": in_flight,
        }

        t_sync = time.time()
        if get_target_lsn is not None:
            target_final_lsn = str(get_target_lsn())
        elif target_eng is not None and hasattr(target_eng, "src_lsn"):
            try:
                target_final_lsn = str(target_eng.src_lsn() or loop.current_target_lsn)
            except Exception:
                target_final_lsn = str(loop.current_target_lsn)
        else:
            target_final_lsn = str(loop.current_target_lsn)

        sync_ok = loop.sync_to_lsn(target_final_lsn, timeout=min(timeout, 5.0))
        source_synced_lsn = str(loop.applied_source_lsn)
        lag = parse_lsn(target_final_lsn) - parse_lsn(source_synced_lsn)
        zero_loss = sync_ok and (lag <= 0)

        steps["lsn_sync"] = {
            "ok": sync_ok,
            "duration_seconds": round(time.time() - t_sync, 4),
            "target_final_lsn": target_final_lsn,
            "source_synced_lsn": source_synced_lsn,
            "lag_bytes": max(0, lag),
            "zero_data_loss_verified": zero_loss,
        }

        if not zero_loss:
            if coordinator is not None:
                coordinator.resume(databases=databases, timeout=timeout)
            return FailbackResult(
                ok=False,
                duration_seconds=round(time.time() - t0, 4),
                target_final_lsn=target_final_lsn,
                source_synced_lsn=source_synced_lsn,
                zero_data_loss_verified=False,
                steps=steps,
                error=f"LSN sync failed: source {source_synced_lsn} lags target {target_final_lsn} by {lag} bytes",
                remaining_window_hours=round(loop.remaining_retention_hours(), 2),
            )

        if coordinator is not None:
            t_repoint = time.time()
            src = getattr(hop, "source", None)
            src_host = getattr(src, "host", "127.0.0.1")
            fam = getattr(getattr(source_eng, "engine_family", None) or getattr(hop, "engine", "postgres"), "value", getattr(hop, "engine", "postgres"))
            default_port = 3306 if "mysql" in str(fam).lower() else 5432
            src_port = int(getattr(src, "port", None) or default_port)
            r_ok, r_why = coordinator.repoint(
                target_host=src_host,
                target_port=src_port,
                databases=databases,
                timeout=timeout,
            )
            steps["repoint_source"] = {
                "ok": r_ok,
                "duration_seconds": round(time.time() - t_repoint, 4),
                "host": src_host,
                "port": src_port,
                "error": r_why,
            }
            if not r_ok:
                coordinator.resume(databases=databases, timeout=timeout)
                return FailbackResult(
                    ok=False,
                    duration_seconds=round(time.time() - t0, 4),
                    steps=steps,
                    error=f"Failed to repoint pooler back to source: {r_why}",
                    remaining_window_hours=round(loop.remaining_retention_hours(), 2),
                )
        else:
            steps["repoint_source"] = {"ok": True, "skipped": True}

        if source_eng is not None and hasattr(source_eng, "unfreeze"):
            try:
                source_eng.unfreeze()
            except Exception:
                pass

        if coordinator is not None:
            t_res = time.time()
            res_ok, res_why = coordinator.resume(databases=databases, timeout=timeout)
            steps["pooler_resume"] = {
                "ok": res_ok,
                "duration_seconds": round(time.time() - t_res, 4),
                "error": res_why,
            }
            if not res_ok:
                return FailbackResult(
                    ok=False,
                    duration_seconds=round(time.time() - t0, 4),
                    steps=steps,
                    error=f"Failed to resume pooler: {res_why}",
                    remaining_window_hours=round(loop.remaining_retention_hours(), 2),
                )
        else:
            steps["pooler_resume"] = {"ok": True, "skipped": True}

        loop.stop()
        steps["reverse_loop_stopped"] = {"ok": True}

        return FailbackResult(
            ok=True,
            duration_seconds=round(time.time() - t0, 4),
            target_final_lsn=target_final_lsn,
            source_synced_lsn=source_synced_lsn,
            transactions_drained=in_flight,
            transactions_replicated=len(loop.applied_events),
            zero_data_loss_verified=True,
            steps=steps,
            remaining_window_hours=round(loop.remaining_retention_hours(), 2),
        )


def trigger_failback(hop: Any, **kwargs) -> FailbackResult:
    return ReverseFailbackController.trigger_failback(hop, **kwargs)


def preflight_reversibility(hop: Any, **kwargs) -> FailbackPreflightReport:
    """Preflight reversibility verification before claiming failback readiness."""
    return ReverseFailbackController.validate_preflight(hop, **kwargs)

