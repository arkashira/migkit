from dataclasses import dataclass, field
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
import uuid

from . import decide

DEFAULT_HEARTBEAT_INTERVAL: float = 10.0
DEFAULT_MAX_WAL_LAG_BYTES: int = 64 * 1024 * 1024


def quiet_source_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for quiet source WAL management and engine slot advancement (Row 653: NEED05)."""
    return (
        decide.Rung(
            "advance_slot_through_engine",
            "advance slot when engine authorizes advance, business transactions are covered, and lag or heartbeat triggered",
            needs=(
                decide.Need(
                    "target applied LSN covers confirmed business transactions",
                    lambda f: bool(f.get("business_transactions_applied", True)),
                    "target has not applied all confirmed business transactions up to watermark",
                ),
                decide.Need(
                    "WAL lag exceeds threshold or heartbeat interval elapsed",
                    lambda f: bool(f.get("lag_or_interval_triggered", True)),
                    "WAL lag is within bounds and heartbeat interval has not elapsed",
                ),
                decide.Need(
                    "slot advance is delegated to engine authority",
                    lambda f: bool(f.get("delegated_to_engine", True)),
                    "slot advance must be authorized and executed through engine authority",
                ),
            ),
            footprint=0,
            leaves="safe retention LSN advanced via engine without WAL bloating",
        ),
        decide.Rung(
            "issue_read_only_probe_heartbeat",
            "issue read-only probe to advance retention without mutating source when writes disallowed",
            needs=(
                decide.Need(
                    "source writes are disallowed",
                    lambda f: bool(f.get("source_writes_disallowed", True)),
                    "source writes are permitted",
                ),
                decide.Need(
                    "engine provides read-only LSN end probe",
                    lambda f: bool(f.get("engine_read_probe_available", True)),
                    "engine does not provide read-only LSN probe",
                ),
            ),
            footprint=1,
            leaves="non-mutating probe advancing retention mark",
        ),
        decide.Rung(
            "quiesce_and_retain_slot",
            "keep current slot position unchanged when lag is within bounds and no heartbeat is due",
            footprint=2,
            leaves="slot position retained at current safe position",
        ),
    )


def decide_quiet_source(facts: Dict[str, Any]) -> decide.Choice:
    """Climb the quiet_source_rungs ladder."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("quiet source wal advance", quiet_source_rungs(), fact_obj)


EVENT_BUSINESS = "business"
EVENT_FILTERED = "filtered"
EVENT_HEARTBEAT = "heartbeat"

METHOD_READ_ONLY_ACK = "read_only_ack"
METHOD_READ_ONLY_PROBE = "read_only_probe"
METHOD_HEARTBEAT_WRITE = "heartbeat_write"
METHOD_NONE = "none"


def parse_lsn(lsn: Union[str, int]) -> int:
    if isinstance(lsn, int):
        return lsn
    text = str(lsn).strip()
    if "/" in text:
        hi, _, lo = text.partition("/")
        return (int(hi, 16) << 32) | int(lo or "0", 16)
    return int(text)


def format_lsn(n: int) -> str:
    n = int(n)
    return f"{n >> 32:X}/{n & 0xFFFFFFFF:X}"


def lsn_diff(a: Union[str, int], b: Union[str, int]) -> int:
    return parse_lsn(a) - parse_lsn(b)


@dataclass(frozen=True)
class SourceEvent:
    lsn: str
    kind: str
    table: Optional[str] = None
    tx_id: Optional[str] = None
    commit_time: Optional[float] = None
    payload: Optional[Any] = None


@dataclass
class HeartbeatAuditRecord:
    audit_id: str
    timestamp: float
    engine: str
    db: str
    action: str
    sql: str
    lsn_before: str
    lsn_after: str
    authorized: bool
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QuietSourceProgress:
    safe_retention_lsn: str
    slot_confirmed_lsn: str
    target_applied_lsn: str
    last_business_lsn: Optional[str] = None
    wal_head_lsn: Optional[str] = None
    target_applied_business_writes: int = 0
    filtered_tx_count: int = 0
    heartbeat_count: int = 0
    wal_lag_bytes: int = 0
    safe_to_advance_bytes: int = 0


@dataclass
class AdvanceResult:
    advanced: bool
    method: str
    lsn_before: str
    lsn_after: str
    bytes_reclaimed: int = 0
    business_writes_claimed: int = 0
    audited: bool = False
    audit_record: Optional[HeartbeatAuditRecord] = None
    refused: bool = False
    refusal_reason: Optional[str] = None


class QuietSourceManager:
    def __init__(
        self,
        slot_name: str,
        in_scope_tables: Optional[Iterable[str]] = None,
        start_lsn: Union[str, int] = "0/0",
        engine: str = "postgres",
        db: str = "",
        heartbeat_interval_seconds: Optional[float] = None,
        max_wal_lag_bytes: Optional[int] = None,
        allow_source_writes: bool = False,
        heartbeat_table: str = "migkit_heartbeat",
        source_executor: Optional[Callable[[str], Any]] = None,
        slot_advance_fn: Optional[Callable[[str, str], Any]] = None,
        audit_callback: Optional[Callable[[HeartbeatAuditRecord], None]] = None,
    ):
        self.slot_name = slot_name
        self.engine = engine
        self.db = db
        self.heartbeat_interval_seconds = (
            float(heartbeat_interval_seconds)
            if heartbeat_interval_seconds is not None
            else DEFAULT_HEARTBEAT_INTERVAL
        )
        self.max_wal_lag_bytes = (
            int(max_wal_lag_bytes)
            if max_wal_lag_bytes is not None
            else DEFAULT_MAX_WAL_LAG_BYTES
        )
        self.allow_source_writes = bool(allow_source_writes)
        self.heartbeat_table = heartbeat_table
        self.source_executor = source_executor
        self.slot_advance_fn = slot_advance_fn
        self.audit_callback = audit_callback

        self._in_scope_tables: Set[str] = set(in_scope_tables) if in_scope_tables else set()

        start_int = parse_lsn(start_lsn)
        self._slot_confirmed_lsn_int = start_int
        self._safe_retention_lsn_int = start_int
        self._target_applied_lsn_int = start_int
        self._wal_head_lsn_int = start_int
        self._last_business_lsn_int: Optional[int] = None

        self._pending_business: List[Dict[str, int]] = []
        self._latest_stream_lsn = start_int
        self._open_transactions: Dict[str, Dict[str, Any]] = {}

        self._target_applied_business_writes = 0
        self._filtered_tx_count = 0
        self._heartbeat_count = 0

        self._last_activity_time: Optional[float] = None
        self._last_advance_time: Optional[float] = None
        self._last_heartbeat_time: Optional[float] = None
        self._audit_log: List[HeartbeatAuditRecord] = []

    def is_in_scope(self, table: Optional[str]) -> bool:
        if not table:
            return False
        if not self._in_scope_tables:
            return True
        return table in self._in_scope_tables

    def set_wal_head(self, lsn: Union[str, int]) -> None:
        self._wal_head_lsn_int = max(self._wal_head_lsn_int, parse_lsn(lsn))

    def begin_tx(self, tx_id: str, lsn: Union[str, int]) -> None:
        l_int = parse_lsn(lsn)
        self._open_transactions[str(tx_id)] = {
            "start_lsn": l_int,
            "has_business": False,
            "max_lsn": l_int,
            "preceding_safe_lsn": self._latest_stream_lsn,
        }

    def commit_tx(self, tx_id: str, commit_lsn: Union[str, int]) -> None:
        cid = str(tx_id)
        info = self._open_transactions.pop(cid, None)
        c_int = parse_lsn(commit_lsn)
        self.set_wal_head(c_int)

        if info and info.get("has_business"):
            self._last_business_lsn_int = max(self._last_business_lsn_int or 0, c_int)
            preceding = info.get("preceding_safe_lsn", self._latest_stream_lsn)
            self._pending_business.append({"lsn": c_int, "preceding_safe_lsn": preceding})
            self._pending_business.sort(key=lambda x: x["lsn"])
        else:
            self._filtered_tx_count += 1
            if not self._pending_business:
                self._safe_retention_lsn_int = max(self._safe_retention_lsn_int, c_int)
        self._latest_stream_lsn = max(self._latest_stream_lsn, c_int)

    def ingest_change(
        self,
        table: str,
        lsn: Union[str, int],
        tx_id: Optional[str] = None,
        commit_time: Optional[float] = None,
        payload: Optional[Any] = None,
    ) -> SourceEvent:
        now = time.monotonic()
        self._last_activity_time = now
        lsn_val = parse_lsn(lsn)
        self.set_wal_head(lsn_val)

        in_scope = self.is_in_scope(table)
        kind = EVENT_BUSINESS if in_scope else EVENT_FILTERED

        if tx_id is not None and str(tx_id) in self._open_transactions:
            tx_info = self._open_transactions[str(tx_id)]
            tx_info["max_lsn"] = max(tx_info["max_lsn"], lsn_val)
            if in_scope:
                tx_info["has_business"] = True
        else:
            if in_scope:
                self._last_business_lsn_int = max(self._last_business_lsn_int or 0, lsn_val)
                self._pending_business.append({"lsn": lsn_val, "preceding_safe_lsn": self._latest_stream_lsn})
                self._pending_business.sort(key=lambda x: x["lsn"])
            else:
                self._filtered_tx_count += 1
                if not self._pending_business:
                    self._safe_retention_lsn_int = max(self._safe_retention_lsn_int, lsn_val)
            self._latest_stream_lsn = max(self._latest_stream_lsn, lsn_val)

        return SourceEvent(
            lsn=format_lsn(lsn_val),
            kind=kind,
            table=table,
            tx_id=str(tx_id) if tx_id is not None else None,
            commit_time=commit_time,
            payload=payload,
        )

    def record_target_applied(self, lsn: Union[str, int], business_writes_count: int = 1) -> None:
        applied_int = parse_lsn(lsn)
        self._target_applied_lsn_int = max(self._target_applied_lsn_int, applied_int)
        self._target_applied_business_writes += int(business_writes_count)

        self._pending_business = [p for p in self._pending_business if p["lsn"] > applied_int]
        if self._pending_business:
            self._safe_retention_lsn_int = max(applied_int, self._pending_business[0]["preceding_safe_lsn"])
        else:
            self._safe_retention_lsn_int = max(applied_int, self._latest_stream_lsn, self._wal_head_lsn_int)

    def get_progress(self) -> QuietSourceProgress:
        wal_lag = max(0, self._wal_head_lsn_int - self._slot_confirmed_lsn_int)
        safe_to_advance = max(0, self._safe_retention_lsn_int - self._slot_confirmed_lsn_int)
        return QuietSourceProgress(
            safe_retention_lsn=format_lsn(self._safe_retention_lsn_int),
            slot_confirmed_lsn=format_lsn(self._slot_confirmed_lsn_int),
            target_applied_lsn=format_lsn(self._target_applied_lsn_int),
            last_business_lsn=format_lsn(self._last_business_lsn_int) if self._last_business_lsn_int else None,
            wal_head_lsn=format_lsn(self._wal_head_lsn_int),
            target_applied_business_writes=self._target_applied_business_writes,
            filtered_tx_count=self._filtered_tx_count,
            heartbeat_count=self._heartbeat_count,
            wal_lag_bytes=wal_lag,
            safe_to_advance_bytes=safe_to_advance,
        )

    def evaluate_advance_method(self, force: bool = False) -> Tuple[str, str, Optional[str]]:
        if self._safe_retention_lsn_int > self._slot_confirmed_lsn_int:
            return (
                METHOD_READ_ONLY_ACK,
                format_lsn(self._safe_retention_lsn_int),
                "safe_retention_ahead_read_only",
            )

        if not self._pending_business and self._wal_head_lsn_int > self._slot_confirmed_lsn_int:
            return (
                METHOD_READ_ONLY_PROBE,
                format_lsn(self._wal_head_lsn_int),
                "source_wal_ahead_read_only",
            )

        if not self.allow_source_writes:
            return (
                METHOD_NONE,
                format_lsn(self._slot_confirmed_lsn_int),
                "source_read_only_writes_disallowed",
            )

        return (
            METHOD_HEARTBEAT_WRITE,
            format_lsn(self._slot_confirmed_lsn_int),
            "explicit_heartbeat_required",
        )

    def advance_retention(self, force: bool = False, now: Optional[float] = None) -> AdvanceResult:
        t = time.monotonic() if now is None else float(now)
        method, target_lsn_str, reason = self.evaluate_advance_method(force=force)
        target_int = parse_lsn(target_lsn_str)
        cur_str = format_lsn(self._slot_confirmed_lsn_int)

        if method in (METHOD_READ_ONLY_ACK, METHOD_READ_ONLY_PROBE):
            facts = {
                "business_transactions_applied": not bool(self._pending_business),
                "lag_or_interval_triggered": True,
                "delegated_to_engine": True,
                "source_writes_disallowed": not self.allow_source_writes,
                "engine_read_probe_available": True,
            }
            decide_quiet_source(facts)
            diff = max(0, target_int - self._slot_confirmed_lsn_int)
            self._slot_confirmed_lsn_int = target_int
            self._safe_retention_lsn_int = max(self._safe_retention_lsn_int, target_int)
            if self.slot_advance_fn:
                self.slot_advance_fn(self.slot_name, target_lsn_str)
            self._last_advance_time = t
            return AdvanceResult(
                advanced=True,
                method=method,
                lsn_before=cur_str,
                lsn_after=target_lsn_str,
                bytes_reclaimed=diff,
                business_writes_claimed=0,
                audited=False,
            )

        if method == METHOD_HEARTBEAT_WRITE:
            return self._execute_heartbeat_write(cur_str, now=t)

        facts = {
            "business_transactions_applied": not bool(self._pending_business),
            "lag_or_interval_triggered": False,
            "delegated_to_engine": False,
            "source_writes_disallowed": not self.allow_source_writes,
            "engine_read_probe_available": False,
        }
        decide_quiet_source(facts)

        return AdvanceResult(
            advanced=False,
            method=method,
            lsn_before=cur_str,
            lsn_after=cur_str,
            bytes_reclaimed=0,
            business_writes_claimed=0,
            audited=False,
            refused=(reason == "source_read_only_writes_disallowed"),
            refusal_reason=reason,
        )

    def _execute_heartbeat_write(self, cur_str: str, now: Optional[float] = None) -> AdvanceResult:
        t = time.monotonic() if now is None else float(now)
        facts = {
            "business_transactions_applied": not bool(self._pending_business),
            "lag_or_interval_triggered": True,
            "delegated_to_engine": True,
            "source_writes_disallowed": not self.allow_source_writes,
            "engine_read_probe_available": True,
        }
        choice = decide_quiet_source(facts)
        hb_nonce = uuid.uuid4().hex[:12]
        if self.engine == "mysql":
            sql = (
                f"REPLACE INTO {self.heartbeat_table} (id, nonce, ts) "
                f"VALUES (1, '{hb_nonce}', CURRENT_TIMESTAMP)"
            )
        else:
            sql = (
                f"SELECT pg_logical_emit_message(true, 'migkit', "
                f"'heartbeat:{hb_nonce}:{int(time.time())}')"
            )
        new_lsn_str = cur_str
        if self.source_executor:
            res = self.source_executor(sql)
            if res:
                new_lsn_str = str(res)
        else:
            new_lsn_str = format_lsn(self._slot_confirmed_lsn_int + 0x1000)

        new_int = parse_lsn(new_lsn_str)
        diff = max(0, new_int - self._slot_confirmed_lsn_int)

        self.set_wal_head(new_int)
        self._safe_retention_lsn_int = max(self._safe_retention_lsn_int, new_int)
        self._slot_confirmed_lsn_int = new_int
        self._heartbeat_count += 1
        self._last_heartbeat_time = t
        self._last_advance_time = t

        if self.slot_advance_fn:
            self.slot_advance_fn(self.slot_name, new_lsn_str)

        audit_rec = HeartbeatAuditRecord(
            audit_id=str(uuid.uuid4()),
            timestamp=time.time(),
            engine=self.engine,
            db=self.db,
            action="heartbeat_write",
            sql=sql,
            lsn_before=cur_str,
            lsn_after=new_lsn_str,
            authorized=(choice.rung.name == "advance_slot_through_engine"),
            details={"nonce": hb_nonce, "bytes_reclaimed": diff, "rung": choice.rung.name},
        )
        self._audit_log.append(audit_rec)
        if self.audit_callback:
            self.audit_callback(audit_rec)

        return AdvanceResult(
            advanced=True,
            method=METHOD_HEARTBEAT_WRITE,
            lsn_before=cur_str,
            lsn_after=new_lsn_str,
            bytes_reclaimed=diff,
            business_writes_claimed=0,
            audited=True,
            audit_record=audit_rec,
        )

    def tick(self, now: Optional[float] = None) -> Optional[AdvanceResult]:
        t = time.monotonic() if now is None else float(now)
        if self._last_advance_time is None or t < self._last_advance_time:
            self._last_advance_time = t
            self._last_activity_time = t
            self._last_heartbeat_time = t
            if self._safe_retention_lsn_int > self._slot_confirmed_lsn_int:
                return self.advance_retention(now=t)
            return None

        lag_bytes = max(0, self._wal_head_lsn_int - self._slot_confirmed_lsn_int)
        time_since_advance = t - self._last_advance_time

        needs_advance = (
            (self._safe_retention_lsn_int > self._slot_confirmed_lsn_int)
            or (lag_bytes >= self.max_wal_lag_bytes)
            or (time_since_advance >= self.heartbeat_interval_seconds)
        )
        if not needs_advance:
            return None

        return self.advance_retention(now=t)

    def get_audit_records(self) -> List[HeartbeatAuditRecord]:
        return list(self._audit_log)
