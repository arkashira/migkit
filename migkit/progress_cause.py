import dataclasses
import re
import signal
import time
from typing import Any, Dict, List, Optional, Union


class CausalPhase:
    WAITING_BACKUP = "waiting_backup"
    LOCK_CONTENTION = "lock_contention"
    SNAPSHOT_QUEUE = "snapshot_queue"
    LOG_DRAIN_LAG = "log_drain_lag"
    NETWORK_ROUTE = "network_route"
    MISSING_PERMISSION = "missing_permission"
    CHILD_PROCESS_STALL = "child_process_stall"
    ACTIVE_PROGRESS = "active_progress"
    COMPLETED = "completed"
    IDLE = "idle"


class EtaStatus:
    COMPUTABLE = "computable"
    UNCERTAIN = "uncertain"
    BLOCKED_BY_USER_ACTION = "blocked_by_user_action"
    STALLED = "stalled"
    INFINITE = "infinite"
    NONE = "none"


def normalize_phase(val: str) -> str:
    if not val:
        return CausalPhase.IDLE
    s = str(val).strip().lower().replace("-", "_").replace(" ", "_")
    mapping = {
        "waiting_backup": CausalPhase.WAITING_BACKUP,
        "backup": CausalPhase.WAITING_BACKUP,
        "lock": CausalPhase.LOCK_CONTENTION,
        "lock_contention": CausalPhase.LOCK_CONTENTION,
        "locks": CausalPhase.LOCK_CONTENTION,
        "snapshot": CausalPhase.SNAPSHOT_QUEUE,
        "snapshot_queue": CausalPhase.SNAPSHOT_QUEUE,
        "log_drain": CausalPhase.LOG_DRAIN_LAG,
        "log_drain_lag": CausalPhase.LOG_DRAIN_LAG,
        "drain_lag": CausalPhase.LOG_DRAIN_LAG,
        "network": CausalPhase.NETWORK_ROUTE,
        "network_route": CausalPhase.NETWORK_ROUTE,
        "route": CausalPhase.NETWORK_ROUTE,
        "permission": CausalPhase.MISSING_PERMISSION,
        "missing_permission": CausalPhase.MISSING_PERMISSION,
        "child_process": CausalPhase.CHILD_PROCESS_STALL,
        "child_process_stall": CausalPhase.CHILD_PROCESS_STALL,
        "process_stall": CausalPhase.CHILD_PROCESS_STALL,
        "active": CausalPhase.ACTIVE_PROGRESS,
        "active_progress": CausalPhase.ACTIVE_PROGRESS,
        "completed": CausalPhase.COMPLETED,
        "idle": CausalPhase.IDLE,
    }
    return mapping.get(s, s)


@dataclasses.dataclass(frozen=True)
class Checkpoint:
    checkpoint_id: Union[str, int]
    position: Any
    units_completed: int
    units_total: Optional[int] = None
    timestamp: float = dataclasses.field(default_factory=time.time)
    detail: str = ""
    source: str = ""


@dataclasses.dataclass
class RequiredUserAction:
    action_type: str
    resource: str
    command_recommendation: str
    blocking_operation: str
    description: str = ""
    is_destructive: bool = False


@dataclasses.dataclass
class ProgressDiagnosis:
    phase: str
    is_stalled: bool
    is_blocked: bool
    liveness: bool
    progress_rate: float
    eta_status: str
    eta_seconds: Optional[float]
    fake_percentage_suppressed: bool
    percentage: Optional[float] = None
    blocking_operation: Optional[str] = None
    required_user_action: Optional[RequiredUserAction] = None
    next_step_recommendation: Optional[str] = None
    observed_checkpoints_count: int = 0
    last_checkpoint: Optional[Checkpoint] = None
    evidence: List[Dict[str, Any]] = dataclasses.field(default_factory=list)

    def format_terse(self) -> str:
        lines = [
            f"phase: {self.phase}",
            f"status: {'blocked' if self.is_blocked else ('stalled' if self.is_stalled else 'active')}",
            f"liveness: {str(self.liveness).lower()}",
        ]
        if self.blocking_operation:
            lines.append(f"blocking_operation: {self.blocking_operation}")
        if self.required_user_action:
            lines.append(f"required_action: {self.required_user_action.action_type} on {self.required_user_action.resource}")
        if self.next_step_recommendation:
            lines.append(f"action: {self.next_step_recommendation}")
        if self.eta_status == EtaStatus.COMPUTABLE and self.eta_seconds is not None:
            lines.append(f"eta: {self.eta_seconds:.1f}s")
        elif self.eta_status == EtaStatus.BLOCKED_BY_USER_ACTION:
            lines.append("eta: blocked by user action")
        elif self.eta_status in (EtaStatus.UNCERTAIN, EtaStatus.STALLED):
            lines.append(f"eta: {self.eta_status}")
        if self.percentage is not None and not self.fake_percentage_suppressed:
            lines.append(f"progress: {self.percentage:.1f}%")
        elif self.fake_percentage_suppressed and self.percentage is not None:
            lines.append(f"checkpoint_progress: {self.percentage:.1f}% (stalled, fake advance suppressed)")
        return "\n".join(lines)


@dataclasses.dataclass
class RestoreDiagnostic:
    tool: str
    pid: Optional[int]
    phase: str
    is_stalled: bool
    blocking_operation: str
    safe_recommendation: str
    detail: str = ""


@dataclasses.dataclass
class ChildProcessDiagnostic:
    pid: Optional[int]
    command: Optional[str]
    exit_code: Optional[int]
    phase: str
    is_failure: bool
    blocking_operation: str
    safe_recommendation: str
    required_user_action: Optional[RequiredUserAction] = None
    detail: str = ""


class ProgressCauseTracker:
    def __init__(self, stall_threshold_seconds: float = 30.0):
        self.stall_threshold_seconds = float(stall_threshold_seconds)
        self.checkpoints: List[Checkpoint] = []
        self._last_progress_timestamp: Optional[float] = None
        self._seq: int = 0
        self._active_causes: Dict[str, Dict[str, Any]] = {}
        self._child_processes: Dict[int, Dict[str, Any]] = {}

    @property
    def last_checkpoint_timestamp(self) -> Optional[float]:
        return self._last_progress_timestamp

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def record_checkpoint(
        self,
        checkpoint_id: Union[str, int],
        position: Any,
        units_completed: int,
        units_total: Optional[int] = None,
        timestamp: Optional[float] = None,
        detail: str = "",
        source: str = "",
    ) -> Checkpoint:
        ts = time.time() if timestamp is None else float(timestamp)
        cp = Checkpoint(
            checkpoint_id=checkpoint_id,
            position=position,
            units_completed=int(units_completed),
            units_total=int(units_total) if units_total is not None else None,
            timestamp=ts,
            detail=str(detail),
            source=str(source),
        )
        if not self.checkpoints or cp.units_completed > self.checkpoints[-1].units_completed:
            self._last_progress_timestamp = ts
        self.checkpoints.append(cp)
        return cp

    def record_required_user_action(self, action: RequiredUserAction) -> None:
        phase = CausalPhase.MISSING_PERMISSION
        if action.action_type == "release_lock":
            phase = CausalPhase.LOCK_CONTENTION
        elif action.action_type == "network_route":
            phase = CausalPhase.NETWORK_ROUTE
        self._active_causes[phase] = {
            "seq": self._next_seq(),
            "phase": phase,
            "req_action": action,
        }

    def clear_required_user_action(self) -> None:
        self._active_causes.pop(CausalPhase.MISSING_PERMISSION, None)
        self._active_causes.pop(CausalPhase.LOCK_CONTENTION, None)

    def record_lock_contention(
        self,
        relation: str,
        blocking_pid: Union[int, str],
        lock_type: str = "AccessExclusiveLock",
        query: str = "",
        duration_seconds: Optional[float] = None,
        engine: str = "postgres",
    ) -> None:
        rec_cmd = (
            f"SELECT pg_cancel_backend({blocking_pid});"
            if "postgres" in engine.lower()
            else f"KILL QUERY {blocking_pid};"
        )
        req = RequiredUserAction(
            action_type="release_lock",
            resource=relation,
            command_recommendation=rec_cmd,
            blocking_operation=f"lock contention on {relation} by pid {blocking_pid}",
            description=f"lock contention ({lock_type}) held by pid {blocking_pid}",
        )
        self._active_causes[CausalPhase.LOCK_CONTENTION] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.LOCK_CONTENTION,
            "relation": relation,
            "blocking_pid": blocking_pid,
            "lock_type": lock_type,
            "query": query,
            "duration_seconds": duration_seconds,
            "engine": engine.lower(),
            "req_action": req,
        }

    def clear_lock_contention(self) -> None:
        self._active_causes.pop(CausalPhase.LOCK_CONTENTION, None)

    def record_backup_wait(
        self,
        backup_id: str,
        status: str = "pending",
        expected_available_at: Optional[float] = None,
        detail: str = "",
    ) -> None:
        self._active_causes[CausalPhase.WAITING_BACKUP] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.WAITING_BACKUP,
            "backup_id": backup_id,
            "status": status,
            "expected_available_at": expected_available_at,
            "detail": detail,
            "blocking_operation": f"waiting on backup {backup_id} ({status})",
            "next_step": f"aws rds describe-export-tasks --export-task-identifier {backup_id}",
        }

    def clear_backup_wait(self) -> None:
        self._active_causes.pop(CausalPhase.WAITING_BACKUP, None)

    def record_snapshot_queue(
        self,
        queue_position: int,
        snapshot_id: str,
        queue_depth: Optional[int] = None,
        detail: str = "",
    ) -> None:
        qd = f"{queue_position}/{queue_depth}" if queue_depth else str(queue_position)
        self._active_causes[CausalPhase.SNAPSHOT_QUEUE] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.SNAPSHOT_QUEUE,
            "queue_position": queue_position,
            "snapshot_id": snapshot_id,
            "queue_depth": queue_depth,
            "detail": detail,
            "blocking_operation": f"snapshot queue position {qd} for {snapshot_id}",
            "next_step": "wait for preceding snapshot task slot or increase quota",
        }

    def clear_snapshot_queue(self) -> None:
        self._active_causes.pop(CausalPhase.SNAPSHOT_QUEUE, None)

    def record_log_drain_lag(
        self,
        current_lsn: Any,
        target_lsn: Any,
        lag_bytes: int,
        drain_rate: Optional[float] = None,
        detail: str = "",
    ) -> None:
        self._active_causes[CausalPhase.LOG_DRAIN_LAG] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.LOG_DRAIN_LAG,
            "current_lsn": current_lsn,
            "target_lsn": target_lsn,
            "lag_bytes": lag_bytes,
            "drain_rate": drain_rate,
            "detail": detail,
            "blocking_operation": f"log drain lag: {lag_bytes} bytes lag behind primary",
            "next_step": "tune CDC batch size or verify target IOPS capacity",
        }

    def clear_log_drain_lag(self) -> None:
        self._active_causes.pop(CausalPhase.LOG_DRAIN_LAG, None)

    def record_network_route(
        self,
        target_host: str,
        port: int,
        error_snippet: str = "",
        route_checked: bool = False,
    ) -> None:
        self._active_causes[CausalPhase.NETWORK_ROUTE] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.NETWORK_ROUTE,
            "target_host": target_host,
            "port": port,
            "error_snippet": error_snippet,
            "route_checked": route_checked,
            "blocking_operation": f"network route unreachable to {target_host}:{port}",
            "next_step": f"nc -zv {target_host} {port} && check security groups / routing table",
        }

    def clear_network_route(self) -> None:
        self._active_causes.pop(CausalPhase.NETWORK_ROUTE, None)

    def record_permission_gap(
        self,
        user: str,
        required_privilege: str,
        target_object: str,
        error_snippet: str = "",
        engine: str = "postgres",
    ) -> None:
        rec_cmd = (
            f"GRANT {required_privilege} ON {target_object} TO {user};"
            if "postgres" in engine.lower()
            else f"GRANT {required_privilege} ON {target_object} TO '{user}'@'%';"
        )
        req = RequiredUserAction(
            action_type="grant_permission",
            resource=f"{engine}:{target_object}",
            command_recommendation=rec_cmd,
            blocking_operation=f"missing {required_privilege} on {target_object}",
            description=f"database user {user} lacks {required_privilege} on {target_object}",
        )
        self._active_causes[CausalPhase.MISSING_PERMISSION] = {
            "seq": self._next_seq(),
            "phase": CausalPhase.MISSING_PERMISSION,
            "user": user,
            "privilege": required_privilege,
            "target_object": target_object,
            "error_snippet": error_snippet,
            "engine": engine,
            "req_action": req,
        }

    def clear_permission_gap(self) -> None:
        self._active_causes.pop(CausalPhase.MISSING_PERMISSION, None)

    def record_child_process(
        self,
        pid: int,
        command: str,
        returncode: Optional[int] = None,
        stdout: str = "",
        stderr: str = "",
        alive: bool = True,
        timestamp: Optional[float] = None,
    ) -> None:
        ts = time.time() if timestamp is None else float(timestamp)
        self._child_processes[pid] = {
            "pid": pid,
            "command": command,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
            "alive": alive,
            "timestamp": ts,
        }

    def is_live(self, now: Optional[float] = None) -> bool:
        current_time = time.time() if now is None else float(now)
        if not self.checkpoints:
            return True
        last_ts = self._last_progress_timestamp or self.checkpoints[-1].timestamp
        return (current_time - last_ts) <= self.stall_threshold_seconds

    def evaluate_progress(self, now: Optional[float] = None) -> ProgressDiagnosis:
        current_time = time.time() if now is None else float(now)
        liveness = self.is_live(now=current_time)
        is_stalled = not liveness

        last_cp = self.checkpoints[-1] if self.checkpoints else None
        obs_count = len(self.checkpoints)

        progress_rate = 0.0
        if obs_count >= 2:
            cp_first = self.checkpoints[0]
            cp_last = self.checkpoints[-1]
            dt = cp_last.timestamp - cp_first.timestamp
            d_units = cp_last.units_completed - cp_first.units_completed
            if dt > 0 and d_units > 0:
                progress_rate = d_units / dt
        elif obs_count == 1:
            progress_rate = 1.0

        if is_stalled:
            progress_rate = 0.0

        percentage = None
        if last_cp and last_cp.units_total and last_cp.units_total > 0:
            percentage = round((last_cp.units_completed / last_cp.units_total) * 100.0, 2)

        phase = CausalPhase.ACTIVE_PROGRESS
        is_blocked = False
        blocking_op = None
        next_step = None
        req_action = None

        top_cause = None
        if self._active_causes:
            top_cause = max(self._active_causes.values(), key=lambda c: c["seq"])

        if top_cause:
            phase = top_cause["phase"]
            is_blocked = True
            req_action = top_cause.get("req_action")
            blocking_op = top_cause.get("blocking_operation")
            next_step = top_cause.get("next_step")
            if req_action:
                if not blocking_op:
                    blocking_op = req_action.blocking_operation
                if not next_step:
                    next_step = req_action.command_recommendation
        elif is_stalled:
            phase = CausalPhase.CHILD_PROCESS_STALL
            blocking_op = "no checkpoint advance within stall threshold"
            next_step = "inspect worker process threads and target lock status"

        fake_percentage_suppressed = is_stalled or is_blocked

        if is_blocked and req_action:
            eta_status = EtaStatus.BLOCKED_BY_USER_ACTION
            eta_seconds = None
        elif is_blocked:
            eta_status = EtaStatus.UNCERTAIN
            eta_seconds = None
        elif is_stalled:
            eta_status = EtaStatus.STALLED
            eta_seconds = None
        elif progress_rate > 0 and last_cp and last_cp.units_total:
            remaining_units = max(0, last_cp.units_total - last_cp.units_completed)
            eta_seconds = remaining_units / progress_rate
            eta_status = EtaStatus.COMPUTABLE
        else:
            eta_status = EtaStatus.UNCERTAIN
            eta_seconds = None

        evidence: List[Dict[str, Any]] = [
            {"type": c["phase"], **c}
            for c in sorted(self._active_causes.values(), key=lambda c: c["seq"])
        ]

        return ProgressDiagnosis(
            phase=phase,
            is_stalled=is_stalled,
            is_blocked=is_blocked,
            liveness=liveness,
            progress_rate=progress_rate,
            eta_status=eta_status,
            eta_seconds=eta_seconds,
            fake_percentage_suppressed=fake_percentage_suppressed,
            percentage=percentage,
            blocking_operation=blocking_op,
            required_user_action=req_action,
            next_step_recommendation=next_step,
            observed_checkpoints_count=obs_count,
            last_checkpoint=last_cp,
            evidence=evidence,
        )

    def diagnose_restore_stall(
        self,
        restore_info: Dict[str, Any],
        now: Optional[float] = None,
    ) -> ProgressDiagnosis:
        tool = restore_info.get("tool", "restore")
        pid = restore_info.get("pid")
        tbl = restore_info.get("target_table", "relation")
        engine = restore_info.get("engine", "postgres").lower()

        if restore_info.get("lock_held_by"):
            blocker = restore_info["lock_held_by"]
            lock_type = restore_info.get("lock_type", "ExclusiveLock")
            rec = (
                f"SELECT pg_cancel_backend({blocker});"
                if "postgres" in engine
                else f"KILL QUERY {blocker};"
            )
            req = RequiredUserAction(
                action_type="release_lock",
                resource=str(tbl),
                command_recommendation=rec,
                blocking_operation=f"{tool} blocked on {tbl} held by pid {blocker}",
                description=f"lock contention ({lock_type}) on {tbl}",
            )
            return ProgressDiagnosis(
                phase=CausalPhase.LOCK_CONTENTION,
                is_stalled=True,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"restore blocked by pid {blocker} on {tbl}",
                required_user_action=req,
                next_step_recommendation=rec,
            )

        if restore_info.get("pipe_stall"):
            rec = f"kill -CONT {pid} || check stdin pipe buffer" if pid else "check pipe buffer"
            return ProgressDiagnosis(
                phase=CausalPhase.CHILD_PROCESS_STALL,
                is_stalled=True,
                is_blocked=False,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.STALLED,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"{tool} pipe stall on {tbl}",
                next_step_recommendation=rec,
            )

        if restore_info.get("backup_pending"):
            return ProgressDiagnosis(
                phase=CausalPhase.WAITING_BACKUP,
                is_stalled=True,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.UNCERTAIN,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"restore waiting on backup stream for {tbl}",
                next_step_recommendation="verify backup stream producer status",
            )

        return ProgressDiagnosis(
            phase=CausalPhase.CHILD_PROCESS_STALL,
            is_stalled=True,
            is_blocked=False,
            liveness=False,
            progress_rate=0.0,
            eta_status=EtaStatus.STALLED,
            eta_seconds=None,
            fake_percentage_suppressed=True,
            blocking_operation=f"{tool} stalled without checkpoint advance",
            next_step_recommendation=f"inspect restore pid {pid} with gdb / pstack or kill hung worker",
        )

    def diagnose_child_process_failure(
        self,
        pid: Optional[int],
        returncode: int,
        stderr: str = "",
        stdout: str = "",
        command: Optional[str] = None,
    ) -> ProgressDiagnosis:
        err = (stderr or "") + "\n" + (stdout or "")

        # Check permission patterns
        perm_match = re.search(
            r"permission denied for (?:table|relation)?\s*([a-zA-Z0-9_\.\"]+)"
            r"|command denied to user '[^']+'@'[^']+' for table '([^']+)'"
            r"|Access denied for user '[^']+'@'[^']+' to database '([^']+)'"
            r"|42501|NOPERM|must be (?:superuser|owner of)",
            err,
            re.IGNORECASE,
        )
        if perm_match:
            obj = next((g for g in perm_match.groups() if g), "target_table")
            rec_sql = f"GRANT SELECT ON TABLE {obj} TO migkit_worker;"
            req = RequiredUserAction(
                action_type="grant_permission",
                resource=str(obj),
                command_recommendation=rec_sql,
                blocking_operation=f"child process failed: permission denied for {obj}",
                description="insufficient privileges to execute extraction or copy",
            )
            return ProgressDiagnosis(
                phase=CausalPhase.MISSING_PERMISSION,
                is_stalled=False,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process pid {pid} failed: missing permission for {obj}",
                required_user_action=req,
                next_step_recommendation=rec_sql,
            )

        # Check network unreachable patterns
        net_match = re.search(
            r"(?:connection to server at|connect to|host)\s+([a-zA-Z0-9\.\-]+)\s+(?:port\s+([0-9]+))?"
            r"|No route to host|Connection refused|Network is unreachable|ETIMEDOUT",
            err,
            re.IGNORECASE,
        )
        if net_match or "route to host" in err.lower() or "connection refused" in err.lower():
            host = "target"
            port = "5432"
            if net_match and net_match.group(1):
                host = net_match.group(1)
            if net_match and len(net_match.groups()) >= 2 and net_match.group(2):
                port = net_match.group(2)
            else:
                hp = re.search(r"(\d+\.\d+\.\d+\.\d+)(?:\s+port\s+|\:)(\d+)", err)
                if hp:
                    host, port = hp.group(1), hp.group(2)
            rec = f"nc -zv {host} {port} && check security group egress/ingress"
            return ProgressDiagnosis(
                phase=CausalPhase.NETWORK_ROUTE,
                is_stalled=False,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process network unreachable to {host}:{port}",
                next_step_recommendation=rec,
            )

        # Check lock wait timeout / deadlock patterns
        lock_match = re.search(
            r"40P01|deadlock detected|lock wait timeout exceeded|1205",
            err,
            re.IGNORECASE,
        )
        if lock_match:
            rec = "retry with randomized backoff or reduce concurrent worker lock scope"
            return ProgressDiagnosis(
                phase=CausalPhase.LOCK_CONTENTION,
                is_stalled=False,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.UNCERTAIN,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process pid {pid} failed: deadlock or lock wait timeout",
                next_step_recommendation=rec,
            )

        # Check disk space full
        disk_match = re.search(
            r"53100|could not extend file|no space left on device|disk full",
            err,
            re.IGNORECASE,
        )
        if disk_match:
            rec = "expand target volume storage or clean unneeded temporary tables"
            req = RequiredUserAction(
                action_type="expand_storage",
                resource="filesystem/tablespace",
                command_recommendation=rec,
                blocking_operation=f"child process pid {pid} failed: out of disk space",
                description="insufficient disk space on database or host volume",
            )
            return ProgressDiagnosis(
                phase=CausalPhase.CHILD_PROCESS_STALL,
                is_stalled=True,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process pid {pid} failed: disk full",
                required_user_action=req,
                next_step_recommendation=rec,
            )

        # Check killed by signal / OOM
        if returncode in (-9, 137) or "out of memory" in err.lower() or "killed" in err.lower():
            sig_name = "SIGKILL (OOM killer)"
            rec = "increase container/machine memory limit or decrease batch/worker concurrency"
            return ProgressDiagnosis(
                phase=CausalPhase.CHILD_PROCESS_STALL,
                is_stalled=True,
                is_blocked=False,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.STALLED,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process pid {pid} terminated by {sig_name}",
                next_step_recommendation=rec,
            )

        if isinstance(returncode, int) and returncode < 0:
            try:
                sig_name = signal.Signals(-returncode).name
            except (ValueError, AttributeError):
                sig_name = f"signal {-returncode}"
            rec = f"investigate external signal {sig_name} and process crash logs"
            return ProgressDiagnosis(
                phase=CausalPhase.CHILD_PROCESS_STALL,
                is_stalled=True,
                is_blocked=False,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.STALLED,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process pid {pid} terminated by {sig_name}",
                next_step_recommendation=rec,
            )

        # Check binary missing (127)
        if returncode == 127:
            rec = "run migkit doctor --install to put prerequisite binary in PATH"
            return ProgressDiagnosis(
                phase=CausalPhase.CHILD_PROCESS_STALL,
                is_stalled=True,
                is_blocked=True,
                liveness=False,
                progress_rate=0.0,
                eta_status=EtaStatus.BLOCKED_BY_USER_ACTION,
                eta_seconds=None,
                fake_percentage_suppressed=True,
                blocking_operation=f"child process executable not found (exit 127): {command}",
                next_step_recommendation=rec,
            )

        # Fallback generic process failure
        rec = f"review stderr tail and re-run with safe backoff: {command}"
        return ProgressDiagnosis(
            phase=CausalPhase.CHILD_PROCESS_STALL,
            is_stalled=True,
            is_blocked=False,
            liveness=False,
            progress_rate=0.0,
            eta_status=EtaStatus.STALLED,
            eta_seconds=None,
            fake_percentage_suppressed=True,
            blocking_operation=f"child process pid {pid} failed with exit code {returncode}",
            next_step_recommendation=rec,
        )
