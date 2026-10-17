"""Proactive retention, WAL and spool controller (backlog RCH16, row 624).

Forecasts retained log and spool growth from arrival rate minus
durable-consumption rate, free disk space, snapshot age, and recovery
windows.

Proactively selects between steady pace, speeding copy, throttling safe
stages, expanding authorized capacity, and stopping/reseeding from a valid
durable point before irreversible log loss occurs. Explicitly recognizes
that finite spool is not a guarantee against unlimited outage, and preserves
safety by refusing to acknowledge unretained source changes.
"""

from __future__ import annotations

import dataclasses
import decimal
import datetime
import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from . import decide, durable
from .ring_buffer import lsn_le, parse_lsn

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"

ACTION_STEADY = "steady"
ACTION_SPEED_COPY = "speed_copy"
ACTION_THROTTLE_SAFE_STAGES = "throttle_safe_stages"
ACTION_EXPAND_CAPACITY = "expand_capacity"
ACTION_STOP_AND_RESEED = "stop_and_reseed"


@dataclasses.dataclass
class RetentionMetrics:
    """Observed runtime metrics for WAL, spool and storage headroom."""
    arrival_rate_bytes_sec: float
    consumption_rate_bytes_sec: float
    current_retained_bytes: int
    free_disk_bytes: int
    spool_capacity_bytes: int
    snapshot_age_sec: float
    recovery_window_sec: float
    authorized_expansion_bytes: int = 0
    safe_disk_headroom_bytes: int = 512 * 1024 * 1024
    target_latency_sec: float = 0.0
    is_target_outage: bool = False
    outage_duration_sec: float = 0.0
    source_host: str = "127.0.0.1"
    target_host: str = "10.0.0.2"
    throttle_safe_active: bool = False
    can_speed_copy: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class RetentionForecast:
    """Dynamic growth and exhaustion forecast over recovery windows."""
    net_growth_rate_bytes_sec: float
    drain_rate_bytes_sec: float
    time_to_spool_exhaustion_sec: Optional[float]
    time_to_disk_exhaustion_sec: Optional[float]
    time_to_wal_expiry_sec: Optional[float]
    effective_time_to_breach_sec: Optional[float]
    projected_retained_bytes_1h: int
    projected_retained_bytes_4h: int
    projected_retained_bytes_24h: int
    finite_spool_outage_tolerance_sec: float
    unlimited_outage_risk: bool = True
    risk_assessment: str = "PASS_STEADY"
    care_badge: str = CARE_PASS
    care_reason: str = "consumption keeps pace with arrival rate"

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class RetentionAction:
    """Proactive intervention decided to prevent irreversible log loss."""
    action_type: str
    recommended_pace_multiplier: float
    authorized_bytes_to_request: int
    stop_truthfully: bool
    stop_reason: str
    safe_reseed_checkpoint: Optional[Dict[str, Any]]
    care_badge: str
    care_reason: str
    decision_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def _encode_typed_values(val: Any) -> Any:
    """Recursively encode native types to typed JSON objects preserving exact types."""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return {"__type__": "datetime", "val": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "val": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "val": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "val": [_encode_typed_values(x) for x in val]}
    if isinstance(val, set):
        return {"__type__": "set", "val": [_encode_typed_values(x) for x in val]}
    if isinstance(val, dict):
        return {k: _encode_typed_values(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_encode_typed_values(x) for x in val]
    return val


def _typed_json_hook(d: Dict[str, Any]) -> Any:
    """Native typed JSON decoder restoring typed cursors and identifiers."""
    if "__type__" in d and "val" in d:
        t = d["__type__"]
        v = d["val"]
        if t == "datetime":
            return datetime.datetime.fromisoformat(v)
        if t == "decimal":
            return decimal.Decimal(v)
        if t == "uuid":
            return uuid.UUID(v)
        if t == "tuple":
            return tuple(_typed_json_hook(x) if isinstance(x, dict) else x for x in v)
        if t == "set":
            return set(_typed_json_hook(x) if isinstance(x, dict) else x for x in v)
    return d


@dataclasses.dataclass
class DurableRetentionCheckpoint:
    """Typed durable checkpoint enabling O(1) resume without rescanning."""
    checkpoint_id: str
    timestamp: float
    last_durable_lsn: Any
    snapshot_lsn: Any
    retained_bytes: int
    spool_bytes: int
    source_retention_window_sec: float
    source_changes_acknowledged: bool
    unretained_source_changes_detected: bool
    requires_reseed: bool
    stop_truthfully: bool
    reseed_reason: str
    typed_cursors: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "timestamp": self.timestamp,
            "last_durable_lsn": self.last_durable_lsn,
            "snapshot_lsn": self.snapshot_lsn,
            "retained_bytes": self.retained_bytes,
            "spool_bytes": self.spool_bytes,
            "source_retention_window_sec": self.source_retention_window_sec,
            "source_changes_acknowledged": self.source_changes_acknowledged,
            "unretained_source_changes_detected": self.unretained_source_changes_detected,
            "requires_reseed": self.requires_reseed,
            "stop_truthfully": self.stop_truthfully,
            "reseed_reason": self.reseed_reason,
            "typed_cursors": self.typed_cursors,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DurableRetentionCheckpoint:
        return cls(
            checkpoint_id=str(data.get("checkpoint_id", "")),
            timestamp=float(data.get("timestamp", 0.0)),
            last_durable_lsn=data.get("last_durable_lsn"),
            snapshot_lsn=data.get("snapshot_lsn"),
            retained_bytes=int(data.get("retained_bytes", 0)),
            spool_bytes=int(data.get("spool_bytes", 0)),
            source_retention_window_sec=float(data.get("source_retention_window_sec", 0.0)),
            source_changes_acknowledged=bool(data.get("source_changes_acknowledged", False)),
            unretained_source_changes_detected=bool(data.get("unretained_source_changes_detected", False)),
            requires_reseed=bool(data.get("requires_reseed", False)),
            stop_truthfully=bool(data.get("stop_truthfully", False)),
            reseed_reason=str(data.get("reseed_reason", "")),
            typed_cursors=dict(data.get("typed_cursors") or {}),
        )


class RetentionForecaster:
    """Predictive log and spool accumulation forecaster."""

    @staticmethod
    def forecast(metrics: RetentionMetrics) -> RetentionForecast:
        arr = max(0.0, float(metrics.arrival_rate_bytes_sec))
        cons = max(0.0, float(metrics.consumption_rate_bytes_sec))

        net_growth = max(0.0, arr - cons)
        drain_rate = max(0.0, cons - arr)

        spool_avail = max(0, metrics.spool_capacity_bytes - metrics.current_retained_bytes)
        disk_headroom = max(0, metrics.free_disk_bytes - metrics.safe_disk_headroom_bytes)

        time_to_spool: Optional[float] = None
        if net_growth > 0:
            time_to_spool = spool_avail / net_growth
        elif drain_rate > 0:
            time_to_spool = None

        time_to_disk: Optional[float] = None
        if net_growth > 0:
            time_to_disk = disk_headroom / net_growth
        elif drain_rate > 0:
            time_to_disk = None

        time_to_wal: Optional[float] = None
        if metrics.recovery_window_sec > 0:
            wal_remaining = max(0.0, metrics.recovery_window_sec - metrics.snapshot_age_sec)
            time_to_wal = wal_remaining

        proj_1h = max(0, int(metrics.current_retained_bytes + (net_growth - drain_rate) * 3600.0))
        proj_4h = max(0, int(metrics.current_retained_bytes + (net_growth - drain_rate) * 14400.0))
        proj_24h = max(0, int(metrics.current_retained_bytes + (net_growth - drain_rate) * 86400.0))

        if arr > 0:
            finite_outage_tol = float(spool_avail) / arr
        else:
            finite_outage_tol = float("inf")

        candidates = [t for t in (time_to_spool, time_to_disk, time_to_wal) if t is not None]
        effective_time = min(candidates) if candidates else None

        care_badge = CARE_PASS
        care_reason = "consumption rate keeps pace with arrival rate"
        risk = "PASS_STEADY"

        if metrics.is_target_outage and metrics.outage_duration_sec > finite_outage_tol:
            care_badge = CARE_ACTION
            care_reason = (
                f"target outage ({metrics.outage_duration_sec:.0f}s) exceeded finite spool"
                f" tolerance ({finite_outage_tol:.0f}s); irreversible log loss imminent"
            )
            risk = "CRITICAL_BREACH"
        elif effective_time is not None and effective_time <= 1800.0:
            care_badge = CARE_ACTION
            care_reason = (
                f"log/spool capacity breach forecast within {effective_time:.0f}s"
                f" (spool={metrics.current_retained_bytes:,}B, disk_free={metrics.free_disk_bytes:,}B)"
            )
            risk = "CRITICAL_BREACH"
        elif metrics.free_disk_bytes <= metrics.safe_disk_headroom_bytes:
            care_badge = CARE_ACTION
            care_reason = (
                f"disk free ({metrics.free_disk_bytes:,}B) at or below safe headroom"
                f" ({metrics.safe_disk_headroom_bytes:,}B)"
            )
            risk = "CRITICAL_BREACH"
        elif effective_time is not None and effective_time <= 14400.0:
            care_badge = CARE_REVIEW
            care_reason = (
                f"spool growth outpaces drain rate; breach predicted in {effective_time:.0f}s"
            )
            risk = "WARNING_GROWTH"
        elif net_growth > 0:
            care_badge = CARE_REVIEW
            care_reason = (
                f"net spool growth {net_growth / (1024 * 1024):.2f} MB/s; monitoring headroom"
            )
            risk = "WARNING_GROWTH"
        elif drain_rate > 0 and metrics.current_retained_bytes > 0:
            care_badge = CARE_BENIGN
            care_reason = (
                f"spool actively draining at {drain_rate / (1024 * 1024):.2f} MB/s"
            )
            risk = "BENIGN_DRAINING"

        return RetentionForecast(
            net_growth_rate_bytes_sec=net_growth,
            drain_rate_bytes_sec=drain_rate,
            time_to_spool_exhaustion_sec=time_to_spool,
            time_to_disk_exhaustion_sec=time_to_disk,
            time_to_wal_expiry_sec=time_to_wal,
            effective_time_to_breach_sec=effective_time,
            projected_retained_bytes_1h=proj_1h,
            projected_retained_bytes_4h=proj_4h,
            projected_retained_bytes_24h=proj_24h,
            finite_spool_outage_tolerance_sec=finite_outage_tol,
            unlimited_outage_risk=True,
            risk_assessment=risk,
            care_badge=care_badge,
            care_reason=care_reason,
        )


class RetentionController:
    """Proactive WAL, spool and disk capacity controller."""

    def __init__(self, hop_name: str = "default", base_dir: Optional[Path] = None):
        self.hop_name = hop_name
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self._lease_lock_file: Optional[Path] = None
        self._lease_fd: Optional[int] = None
        self._last_heartbeat: float = 0.0

    def forecast(self, metrics: RetentionMetrics) -> RetentionForecast:
        return RetentionForecaster.forecast(metrics)

    def decide_action(
        self,
        metrics: RetentionMetrics,
        forecast: Optional[RetentionForecast] = None,
    ) -> RetentionAction:
        if forecast is None:
            forecast = self.forecast(metrics)

        spool_avail = max(0, metrics.spool_capacity_bytes - metrics.current_retained_bytes)
        disk_headroom = max(0, metrics.free_disk_bytes - metrics.safe_disk_headroom_bytes)
        time_to_breach = forecast.effective_time_to_breach_sec
        can_proact = (
            (metrics.can_speed_copy and not metrics.is_target_outage)
            or (not metrics.throttle_safe_active)
            or (metrics.authorized_expansion_bytes > 0)
        )

        unretained_risk = (
            metrics.snapshot_age_sec >= metrics.recovery_window_sec
            or (metrics.recovery_window_sec > 0 and (metrics.recovery_window_sec - metrics.snapshot_age_sec) <= 120.0)
        )

        # If expansion is available, calculate whether expansion averts breach
        time_with_exp: Optional[float] = None
        if metrics.authorized_expansion_bytes > 0 and forecast.net_growth_rate_bytes_sec > 0:
            time_with_exp = (spool_avail + metrics.authorized_expansion_bytes) / forecast.net_growth_rate_bytes_sec

        effective_breach_sec = time_with_exp if (time_with_exp is not None) else time_to_breach

        stop_required = (
            disk_headroom <= 0
            or (spool_avail <= 0 and metrics.authorized_expansion_bytes <= 0)
            or unretained_risk
            or (metrics.is_target_outage and metrics.outage_duration_sec >= forecast.finite_spool_outage_tolerance_sec and metrics.authorized_expansion_bytes <= 0)
            or (effective_breach_sec is not None and effective_breach_sec <= 60.0)
            or (not can_proact and time_to_breach is not None and time_to_breach <= 300.0)
        )

        drain_needed = forecast.net_growth_rate_bytes_sec > 0 or forecast.care_badge in (CARE_REVIEW, CARE_ACTION)

        facts = {
            "stop_required": stop_required,
            "expansion_available": metrics.authorized_expansion_bytes,
            "throttle_safe": not metrics.throttle_safe_active,
            "can_speed": metrics.can_speed_copy and not metrics.is_target_outage,
            "drain_needed": drain_needed,
            "free_disk": metrics.free_disk_bytes,
            "headroom": disk_headroom,
            "time_to_breach": time_to_breach,
        }

        action_name, choice = decide.retention_action(facts)

        if action_name == ACTION_STOP_AND_RESEED:
            stop_reason = (
                "imminent irreversible WAL/spool exhaustion or source log purge: "
                f"{forecast.care_reason}"
            )
            return RetentionAction(
                action_type=ACTION_STOP_AND_RESEED,
                recommended_pace_multiplier=0.0,
                authorized_bytes_to_request=0,
                stop_truthfully=True,
                stop_reason=stop_reason,
                safe_reseed_checkpoint=None,
                care_badge=CARE_ACTION,
                care_reason=forecast.care_reason,
                decision_reason=choice.reason if choice else stop_reason,
            )

        if action_name == ACTION_EXPAND_CAPACITY:
            exp_bytes = metrics.authorized_expansion_bytes
            return RetentionAction(
                action_type=ACTION_EXPAND_CAPACITY,
                recommended_pace_multiplier=1.0,
                authorized_bytes_to_request=exp_bytes,
                stop_truthfully=False,
                stop_reason="",
                safe_reseed_checkpoint=None,
                care_badge=CARE_REVIEW,
                care_reason=f"requesting authorized capacity expansion ({exp_bytes:,}B)",
                decision_reason=choice.reason if choice else "",
            )

        if action_name == ACTION_THROTTLE_SAFE_STAGES:
            return RetentionAction(
                action_type=ACTION_THROTTLE_SAFE_STAGES,
                recommended_pace_multiplier=0.5,
                authorized_bytes_to_request=0,
                stop_truthfully=False,
                stop_reason="",
                safe_reseed_checkpoint=None,
                care_badge=CARE_REVIEW,
                care_reason="throttling safe background stages to preserve spool headroom",
                decision_reason=choice.reason if choice else "",
            )

        if action_name == ACTION_SPEED_COPY:
            return RetentionAction(
                action_type=ACTION_SPEED_COPY,
                recommended_pace_multiplier=1.5,
                authorized_bytes_to_request=0,
                stop_truthfully=False,
                stop_reason="",
                safe_reseed_checkpoint=None,
                care_badge=CARE_REVIEW,
                care_reason="proactively increasing copy pace to drain spool backlog",
                decision_reason=choice.reason if choice else "",
            )

        return RetentionAction(
            action_type=ACTION_STEADY,
            recommended_pace_multiplier=1.0,
            authorized_bytes_to_request=0,
            stop_truthfully=False,
            stop_reason="",
            safe_reseed_checkpoint=None,
            care_badge=forecast.care_badge,
            care_reason=forecast.care_reason,
            decision_reason=choice.reason if choice else "",
        )

    def checkpoint(
        self,
        checkpoint_dir: Union[str, Path],
        last_durable_lsn: Any,
        snapshot_lsn: Any,
        unretained_source_changes: bool = False,
        typed_cursors: Optional[Dict[str, Any]] = None,
        reseed_reason: str = "",
        stop_truthfully: bool = False,
        retained_bytes: int = 0,
        spool_bytes: int = 0,
        source_retention_window_sec: float = 86400.0,
    ) -> DurableRetentionCheckpoint:
        """Atomically persist a native typed durable checkpoint down to cursor level."""
        cdir = Path(checkpoint_dir)
        cdir.mkdir(parents=True, exist_ok=True)

        chk_id = str(uuid.uuid4())
        now = time.time()

        source_acknowledged = not unretained_source_changes and not stop_truthfully
        requires_reseed = unretained_source_changes or bool(reseed_reason)

        chk = DurableRetentionCheckpoint(
            checkpoint_id=chk_id,
            timestamp=now,
            last_durable_lsn=last_durable_lsn,
            snapshot_lsn=snapshot_lsn,
            retained_bytes=retained_bytes,
            spool_bytes=spool_bytes,
            source_retention_window_sec=source_retention_window_sec,
            source_changes_acknowledged=source_acknowledged,
            unretained_source_changes_detected=unretained_source_changes,
            requires_reseed=requires_reseed,
            stop_truthfully=stop_truthfully,
            reseed_reason=reseed_reason,
            typed_cursors=dict(typed_cursors or {}),
        )

        target_file = cdir / ".retention_checkpoint.json"
        tmp_file = cdir / f".retention_checkpoint.{chk_id}.tmp"

        encoded = _encode_typed_values(chk.to_dict())
        payload = json.dumps(encoded, indent=2)
        tmp_file.write_text(payload, encoding="ascii")
        tmp_file.replace(target_file)

        return chk

    def load_checkpoint(self, checkpoint_path: Union[str, Path]) -> Optional[DurableRetentionCheckpoint]:
        """Load durable checkpoint preserving native types."""
        p = Path(checkpoint_path)
        if p.is_dir():
            p = p / ".retention_checkpoint.json"
        if not p.is_file():
            return None
        try:
            raw = p.read_text(encoding="ascii")
            data = json.loads(raw, object_hook=_typed_json_hook)
            return DurableRetentionCheckpoint.from_dict(data)
        except Exception:
            return None

    def evaluate_resume(
        self,
        checkpoint_path: Union[str, Path],
        current_source_min_lsn: Any,
        current_recovery_window_sec: float = 86400.0,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """O(1) durable resume verification against current source WAL boundaries.

        Returns (can_resume, status_code, details).
        Never acknowledges unretained source changes.
        """
        chk = self.load_checkpoint(checkpoint_path)
        if chk is None:
            return False, "no_checkpoint", {"reason": "durable checkpoint file not found"}

        if chk.unretained_source_changes_detected or chk.requires_reseed:
            return False, "reseed_required", {
                "reason": (
                    "unretained source changes detected; truthful stop recorded: "
                    + (chk.reseed_reason or "source changes purged")
                ),
                "checkpoint": chk.to_dict(),
            }

        if current_source_min_lsn is not None and chk.last_durable_lsn is not None:
            # Check if source purged past our last confirmed durable position
            if not lsn_le(current_source_min_lsn, chk.last_durable_lsn):
                reason = (
                    f"source purged logs past durable position: source min LSN {current_source_min_lsn} "
                    f"> last durable LSN {chk.last_durable_lsn}; irreversible log loss"
                )
                return False, "reseed_required", {
                    "reason": reason,
                    "checkpoint": chk.to_dict(),
                    "source_min_lsn": current_source_min_lsn,
                }

        age = time.time() - chk.timestamp
        if current_recovery_window_sec > 0 and age > current_recovery_window_sec:
            reason = (
                f"checkpoint elapsed age ({age:.0f}s) exceeded recovery window "
                f"({current_recovery_window_sec:.0f}s)"
            )
            return False, "reseed_required", {
                "reason": reason,
                "checkpoint": chk.to_dict(),
                "age_sec": age,
            }

        return True, "resume_valid", {
            "checkpoint": chk.to_dict(),
            "last_durable_lsn": chk.last_durable_lsn,
            "typed_cursors": chk.typed_cursors,
            "message": f"durable resume verified in O(1) from LSN {chk.last_durable_lsn}",
        }

    def format_status_report(
        self,
        metrics: RetentionMetrics,
        forecast: RetentionForecast,
        action: RetentionAction,
    ) -> str:
        """Format a pure UNIX terse status report with 4-tier care badges."""
        arr_mb = metrics.arrival_rate_bytes_sec / (1024 * 1024)
        cons_mb = metrics.consumption_rate_bytes_sec / (1024 * 1024)
        spool_mb = metrics.current_retained_bytes / (1024 * 1024)
        spool_cap_mb = metrics.spool_capacity_bytes / (1024 * 1024)
        pct_spool = (
            (float(metrics.current_retained_bytes) / float(metrics.spool_capacity_bytes) * 100.0)
            if metrics.spool_capacity_bytes > 0
            else 0.0
        )
        disk_gb = metrics.free_disk_bytes / (1024 * 1024 * 1024)

        badge = action.care_badge
        lines = [
            f"[{badge}] RETENTION: action={action.action_type.upper()} "
            f"arrival={arr_mb:.2f}MB/s cons={cons_mb:.2f}MB/s "
            f"spool={spool_mb:.1f}MB/{spool_cap_mb:.1f}MB({pct_spool:.0f}%) "
            f"disk_free={disk_gb:.1f}GB pace_mult={action.recommended_pace_multiplier:.1f}x",
            f"  care_reason: {action.care_reason}",
        ]

        if forecast.effective_time_to_breach_sec is not None:
            lines.append(f"  exhaustion_eta: {forecast.effective_time_to_breach_sec:.0f}s")
        else:
            lines.append("  exhaustion_eta: none (stable or draining)")

        if metrics.is_target_outage:
            lines.append(
                f"  target_outage: active ({metrics.outage_duration_sec:.0f}s) "
                f"finite_spool_tolerance={forecast.finite_spool_outage_tolerance_sec:.0f}s"
            )

        if action.stop_truthfully:
            lines.append(f"  stop_truthful: {action.stop_reason}")

        return "\n".join(lines)

    def acquire_lease(self, run_dir: Union[str, Path], timeout_sec: float = 5.0) -> bool:
        """Acquire active verification lease lock (.run.lock) for mutual exclusion."""
        rdir = Path(run_dir)
        rdir.mkdir(parents=True, exist_ok=True)
        lock_file = rdir / ".run.lock"

        import fcntl

        try:
            fd = os.open(lock_file, os.O_RDWR | os.O_CREAT, 0o644)
            start = time.time()
            acquired = False
            while time.time() - start < timeout_sec:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except (BlockingIOError, OSError):
                    time.sleep(0.05)
            if not acquired:
                os.close(fd)
                return False

            self._lease_lock_file = lock_file
            self._lease_fd = fd
            self.renew_lease()
            return True
        except Exception:
            return False

    def renew_lease(self) -> None:
        """Write active heartbeat to .run.lock file."""
        if self._lease_fd is None:
            return
        now = time.time()
        self._last_heartbeat = now
        payload = json.dumps({
            "pid": os.getpid(),
            "hop": self.hop_name,
            "heartbeat": now,
            "status": "active",
        }).encode("ascii")
        try:
            os.lseek(self._lease_fd, 0, os.SEEK_SET)
            os.write(self._lease_fd, payload)
            os.ftruncate(self._lease_fd, len(payload))
        except OSError:
            pass

    def release_lease(self) -> None:
        """Release active verification lease lock."""
        if self._lease_fd is not None:
            import fcntl
            try:
                fcntl.flock(self._lease_fd, fcntl.LOCK_UN)
                os.close(self._lease_fd)
            except OSError:
                pass
            self._lease_fd = None
        if self._lease_lock_file and self._lease_lock_file.is_file():
            try:
                self._lease_lock_file.unlink(missing_ok=True)
            except OSError:
                pass
            self._lease_lock_file = None
