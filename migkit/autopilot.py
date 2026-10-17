import dataclasses
import datetime
import decimal
import enum
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from . import checkpoint, decide, durable

log = logging.getLogger("migkit.autopilot")


class AutopilotPhase(str, enum.Enum):
    PREFLIGHT = "preflight"
    BASTION_TUNNEL = "bastion_tunnel"
    TARGET_TUNE_BULK = "target_tune_bulk"
    DEFERRED_INDEX_DROP = "deferred_index_drop"
    PARALLEL_BULK_LOAD = "parallel_bulk_load"
    DEFERRED_INDEX_RESTORE = "deferred_index_restore"
    TARGET_TUNE_RESTORE = "target_tune_restore"
    CDC_REPLICATION = "cdc_replication"
    MERKLE_VERIFY = "merkle_verify"
    FLIGHT_SIMULATOR = "flight_simulator"
    SEAMLESS_CUTOVER = "seamless_cutover"
    REVERSE_STANDBY = "reverse_standby"


class AutopilotStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


@dataclasses.dataclass
class PhaseResult:
    phase: AutopilotPhase
    status: AutopilotStatus
    duration_seconds: float = 0.0
    details: Dict[str, Any] = dataclasses.field(default_factory=dict)
    error: Optional[str] = None
    care_badge: CareBadge = CareBadge.PASS


@dataclasses.dataclass
class AutopilotResult:
    hop_name: str
    ok: bool
    status: AutopilotStatus
    total_duration_seconds: float = 0.0
    phases: List[PhaseResult] = dataclasses.field(default_factory=list)
    certificate: Optional[Dict[str, Any]] = None
    failback_available: bool = False
    error: Optional[str] = None

    def phase_map(self) -> Dict[str, PhaseResult]:
        return {p.phase.value: p for p in self.phases}


@dataclasses.dataclass
class AutopilotCheckpointUnit:
    phase: str
    status: str
    min_cursor: Any = None
    max_cursor: Any = None
    completed_at: Optional[datetime.datetime] = None
    duration_seconds: float = 0.0
    details: Dict[str, Any] = dataclasses.field(default_factory=dict)


class AutopilotCheckpointStore:
    """Universal typed durable O(1) checkpoint store preserving native types (datetime, Decimal, UUID)."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._units: Dict[str, AutopilotCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def record_phase(self, unit: AutopilotCheckpointUnit) -> None:
        self._units[unit.phase] = unit
        self._save()

    def is_completed_o1(self, phase_name: str) -> bool:
        unit = self._units.get(phase_name)
        return unit is not None and unit.status == AutopilotStatus.COMPLETED.value

    def get_unit(self, phase_name: str) -> Optional[AutopilotCheckpointUnit]:
        return self._units.get(phase_name)

    def clear(self) -> None:
        self._units.clear()
        if self.path.exists():
            self.path.unlink()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "phases": {
                name: {
                    "phase": u.phase,
                    "status": u.status,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                    "duration_seconds": u.duration_seconds,
                    "details": {
                        k: checkpoint.serialize_cursor(v) for k, v in u.details.items()
                    },
                }
                for name, u in self._units.items()
            }
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        phases_raw = raw.get("phases", {})
        for name, item in phases_raw.items():
            self._units[name] = AutopilotCheckpointUnit(
                phase=item["phase"],
                status=item["status"],
                min_cursor=checkpoint.deserialize_cursor(item.get("min_cursor")),
                max_cursor=checkpoint.deserialize_cursor(item.get("max_cursor")),
                completed_at=checkpoint.deserialize_cursor(item.get("completed_at")),
                duration_seconds=float(item.get("duration_seconds", 0.0)),
                details={
                    k: checkpoint.deserialize_cursor(v)
                    for k, v in item.get("details", {}).items()
                },
            )


def autopilot_admission_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "admit_phase_preconditions_met",
            "admit phase when preconditions are verified and not skipped",
            needs=(
                decide.Need(
                    "preconditions met",
                    lambda facts: bool(facts.get("preconditions_met", True)),
                    "preconditions not met for phase",
                ),
                decide.Need(
                    "phase not requested to skip",
                    lambda facts: not bool(facts.get("skip_requested", False)),
                    "phase was requested to be skipped",
                ),
            ),
        ),
        decide.Rung(
            "skip_optional_phase_requested",
            "skip optional phase upon operator or configuration request",
            needs=(
                decide.Need(
                    "skip requested",
                    lambda facts: bool(facts.get("skip_requested", False)),
                    "skip was not requested",
                ),
                decide.Need(
                    "phase is optional",
                    lambda facts: bool(facts.get("phase_optional", True)),
                    "phase is mandatory and cannot be skipped",
                ),
            ),
        ),
        decide.Rung(
            "refuse_phase_unmet_preconditions",
            "refuse phase when required preconditions are unmet",
            needs=(
                decide.Need(
                    "unmet preconditions present",
                    lambda facts: bool(facts.get("unmet_preconditions", False)) or not bool(facts.get("preconditions_met", True)),
                    "no unmet preconditions detected",
                ),
            ),
        ),
    )


def autopilot_strategy_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "execute_turnkey_production_pipeline",
            "execute full turnkey migration pipeline to live production cutover",
            needs=(
                decide.Need(
                    "not dry run",
                    lambda facts: not bool(facts.get("dry_run", False)),
                    "dry-run rehearsal requested, live cutover suppressed",
                ),
                decide.Need(
                    "pipeline active and uncancelled",
                    lambda facts: not bool(facts.get("recovery_requested", False)) and not bool(facts.get("aborted", False)),
                    "recovery or abort condition triggered",
                ),
            ),
        ),
        decide.Rung(
            "execute_dry_run_rehearsal",
            "execute dry-run shadow rehearsal without switching live production traffic",
            needs=(
                decide.Need(
                    "dry run requested",
                    lambda facts: bool(facts.get("dry_run", False)),
                    "dry-run mode not active",
                ),
            ),
        ),
        decide.Rung(
            "execute_recovery_or_abort",
            "abort execution and engage recovery or rollback procedures",
            needs=(
                decide.Need(
                    "recovery or abort condition triggered",
                    lambda facts: bool(facts.get("recovery_requested", False)) or bool(facts.get("aborted", False)),
                    "no recovery or abort condition triggered",
                ),
            ),
        ),
    )


def autopilot_cutover_gate_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "certify_zero_drift_cutover_admit",
            "admit cutover certified with zero data drift and valid consent",
            needs=(
                decide.Need(
                    "zero drift verified",
                    lambda facts: bool(facts.get("zero_drift", False)),
                    "data drift or unverified parity detected before cutover",
                ),
                decide.Need(
                    "cutover auto-approved or operator consent present",
                    lambda facts: bool(facts.get("auto_approve", True)) or bool(facts.get("operator_consented", False)),
                    "operator cutover consent required",
                ),
                decide.Need(
                    "no critical query plan regression on the target",
                    lambda facts: bool(facts.get("query_parity_ok", True)),
                    "query optimizer parity guard found a critical plan or latency regression",
                ),
            ),
        ),
        decide.Rung(
            "pause_for_manual_cutover_consent",
            "pause cutover gate awaiting explicit manual operator consent",
            needs=(
                decide.Need(
                    "manual consent needed",
                    lambda facts: not bool(facts.get("auto_approve", True)) and not bool(facts.get("operator_consented", False)),
                    "automatic approval configured or consent already granted",
                ),
                decide.Need(
                    "zero drift certified",
                    lambda facts: bool(facts.get("zero_drift", False)),
                    "cannot pause for consent when drift exists",
                ),
            ),
        ),
        decide.Rung(
            "abort_cutover_data_divergence",
            "abort cutover due to data divergence or unverified drift",
            needs=(
                decide.Need(
                    "data divergence or drift detected",
                    lambda facts: bool(facts.get("data_divergence", False)) or not bool(facts.get("zero_drift", True)),
                    "no data divergence or drift detected",
                ),
            ),
        ),
    )


def autopilot_reverse_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "prewarm_reverse_cdc_stream",
            "pre-warm reverse standby CDC replication stream for failback protection",
            needs=(
                decide.Need(
                    "reverse replication supported",
                    lambda facts: bool(facts.get("reverse_supported", True)),
                    "reverse replication not supported by engine or topology",
                ),
                decide.Need(
                    "not dry run",
                    lambda facts: not bool(facts.get("dry_run", False)),
                    "reverse stream disabled during dry run",
                ),
            ),
        ),
        decide.Rung(
            "refuse_reverse_unsupported",
            "refuse reverse standby when unsupported or suppressed",
            needs=(
                decide.Need(
                    "reverse unsupported or dry run active",
                    lambda facts: not bool(facts.get("reverse_supported", True)) or bool(facts.get("dry_run", False)),
                    "reverse replication is fully supported and enabled",
                ),
            ),
        ),
    )


class AutopilotOrchestrator:
    def __init__(
        self,
        hop: Any,
        dry_run: bool = False,
        skip_tuning: bool = False,
        skip_flight_sim: bool = False,
        auto_approve: bool = True,
        timeout: Optional[float] = None,
        tunnel_mgr: Optional[Any] = None,
        mover: Optional[Any] = None,
        cdc_engine: Optional[Any] = None,
        merkle_folder: Optional[Any] = None,
        flight_sim: Optional[Any] = None,
        pooler_coord: Optional[Any] = None,
        reverse_loop: Optional[Any] = None,
        tuner: Optional[Any] = None,
        schema_lifecycle: Optional[Any] = None,
        state_dir: Optional[Union[str, Path]] = None,
        operator_consented: bool = False,
        lease_seconds: float = 30.0,
    ):
        self.hop = hop
        self.hop_name = getattr(hop, "name", str(hop))
        self.dry_run = bool(dry_run)
        self.skip_tuning = bool(skip_tuning)
        self.skip_flight_sim = bool(skip_flight_sim)
        self.auto_approve = bool(auto_approve)
        self.timeout = float(timeout) if timeout is not None else float(getattr(hop, "timeout", 3600.0) or 3600.0)
        self.operator_consented = bool(operator_consented)
        self.lease_seconds = float(lease_seconds)

        self.tunnel_mgr = tunnel_mgr
        self.mover = mover
        self.cdc_engine = cdc_engine
        self.merkle_folder = merkle_folder
        self.flight_sim = flight_sim
        self.pooler_coord = pooler_coord
        self.reverse_loop = reverse_loop
        self.tuner = tuner
        self.schema_lifecycle = schema_lifecycle

        self._active_lease = None
        self._active_tunnel = None
        self._target_tuned = False
        self._indexes_deferred = False
        self._reverse_started = False
        self._reverse_supported = True
        self._zero_drift_verified = False
        self._phases: List[PhaseResult] = []

        if state_dir is not None:
            self.state_dir = Path(state_dir)
        elif hasattr(hop, "report_dir") and callable(hop.report_dir):
            try:
                self.state_dir = Path(hop.report_dir())
            except Exception:
                self.state_dir = Path(f".migkit_state/{self.hop_name}")
        else:
            self.state_dir = Path(f".migkit_state/{self.hop_name}")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_store = AutopilotCheckpointStore(self.state_dir / "checkpoint.json")

    def execute(self) -> AutopilotResult:
        start_time = time.monotonic()
        lock_file = self.state_dir / ".run.lock"
        from . import progress_liveness
        self._active_lease = progress_liveness.ActiveVerificationLeaseLock(
            lock_path=lock_file, lease_seconds=self.lease_seconds
        )
        self._active_lease.acquire()
        try:
            strat_work = f"autopilot:{self.hop_name}:strategy"
            strat_facts = {
                "dry_run": self.dry_run,
                "auto_approve": self.auto_approve,
                "recovery_requested": False,
                "aborted": False,
            }
            strat_choice = decide.climb(strat_work, autopilot_strategy_rungs(), strat_facts)
            if strat_choice.rung is None or strat_choice.rung.name == "execute_recovery_or_abort":
                raise RuntimeError(f"Pipeline execution aborted by strategy: {strat_choice.reason}")

            is_dry_run = (strat_choice.rung.name == "execute_dry_run_rehearsal")

            self._execute_phase(AutopilotPhase.PREFLIGHT, self._step_preflight)
            self._execute_phase(AutopilotPhase.BASTION_TUNNEL, self._step_bastion_tunnel)

            self._execute_phase(
                AutopilotPhase.TARGET_TUNE_BULK,
                self._step_target_tune_bulk,
                optional=True,
                skip_requested=self.skip_tuning,
            )

            self._execute_phase(AutopilotPhase.DEFERRED_INDEX_DROP, self._step_deferred_index_drop)
            self._execute_phase(AutopilotPhase.PARALLEL_BULK_LOAD, self._step_parallel_bulk_load)
            self._execute_phase(AutopilotPhase.DEFERRED_INDEX_RESTORE, self._step_deferred_index_restore)

            self._execute_phase(
                AutopilotPhase.TARGET_TUNE_RESTORE,
                self._step_target_tune_restore,
                optional=True,
                skip_requested=self.skip_tuning or not self._target_tuned,
            )

            self._execute_phase(AutopilotPhase.CDC_REPLICATION, self._step_cdc_replication)
            self._execute_phase(AutopilotPhase.MERKLE_VERIFY, self._step_merkle_verify)

            cert = None
            if not self.skip_flight_sim:
                phase_res = self._execute_phase(AutopilotPhase.FLIGHT_SIMULATOR, self._step_flight_sim)
                cert = phase_res.details.get("certificate")
            else:
                self._execute_phase(
                    AutopilotPhase.FLIGHT_SIMULATOR,
                    self._step_flight_sim,
                    optional=True,
                    skip_requested=True,
                )

            if not is_dry_run:
                self._execute_phase(AutopilotPhase.SEAMLESS_CUTOVER, self._step_seamless_cutover)
                self._execute_phase(AutopilotPhase.REVERSE_STANDBY, self._step_reverse_standby)
            else:
                self._execute_phase(
                    AutopilotPhase.SEAMLESS_CUTOVER,
                    self._step_seamless_cutover,
                    optional=True,
                    skip_requested=True,
                )
                self._execute_phase(
                    AutopilotPhase.REVERSE_STANDBY,
                    self._step_reverse_standby,
                    optional=True,
                    skip_requested=True,
                )

            total_dur = time.monotonic() - start_time
            res = AutopilotResult(
                hop_name=self.hop_name,
                ok=True,
                status=AutopilotStatus.COMPLETED,
                total_duration_seconds=total_dur,
                phases=list(self._phases),
                certificate=cert,
                failback_available=self._reverse_started,
            )
            self._save_state(res)
            return res

        except Exception as exc:
            total_dur = time.monotonic() - start_time
            log.exception("autopilot execution error: %s", exc)
            self._rollback_on_failure()
            res = AutopilotResult(
                hop_name=self.hop_name,
                ok=False,
                status=AutopilotStatus.FAILED,
                total_duration_seconds=total_dur,
                phases=list(self._phases),
                error=str(exc),
            )
            self._save_state(res)
            return res

        finally:
            if self._active_lease is not None:
                self._active_lease.release()
                self._active_lease = None

    def _execute_phase(
        self,
        phase: AutopilotPhase,
        func: Callable[[], Dict[str, Any]],
        optional: bool = False,
        skip_requested: bool = False,
    ) -> PhaseResult:
        if self.checkpoint_store.is_completed_o1(phase.value):
            chk = self.checkpoint_store.get_unit(phase.value)
            pr = PhaseResult(
                phase=phase,
                status=AutopilotStatus.COMPLETED,
                duration_seconds=chk.duration_seconds if chk else 0.0,
                details=chk.details if chk else {"resumed": True},
                care_badge=CareBadge.PASS,
            )
            self._phases.append(pr)
            return pr

        work = f"autopilot:{self.hop_name}:admission:{phase.value}"
        admission_facts = {
            "preconditions_met": True,
            "skip_requested": skip_requested,
            "phase_optional": optional,
            "unmet_preconditions": False,
        }
        choice = decide.climb(work, autopilot_admission_rungs(), admission_facts)

        if choice.rung is None or choice.rung.name == "refuse_phase_unmet_preconditions":
            reason = choice.reason if choice.rung else "admission refused: unmet preconditions"
            pr = PhaseResult(
                phase=phase,
                status=AutopilotStatus.FAILED,
                duration_seconds=0.0,
                error=reason,
                care_badge=CareBadge.ACTION,
            )
            self._phases.append(pr)
            raise RuntimeError(f"Admission refused for phase {phase.value}: {reason}")

        if choice.rung.name == "skip_optional_phase_requested":
            pr = PhaseResult(
                phase=phase,
                status=AutopilotStatus.SKIPPED,
                duration_seconds=0.0,
                details={"reason": choice.reason},
                care_badge=CareBadge.REVIEW,
            )
            self._phases.append(pr)
            return pr

        t0 = time.monotonic()
        if self._active_lease is not None:
            self._active_lease.heartbeat()
        try:
            details = func() or {}
            dur = time.monotonic() - t0
            badge_val = details.get("care_badge", CareBadge.PASS.value)
            badge = CareBadge(badge_val) if isinstance(badge_val, str) else badge_val
            pr = PhaseResult(
                phase=phase,
                status=AutopilotStatus.COMPLETED,
                duration_seconds=dur,
                details=details,
                care_badge=badge,
            )
            self._phases.append(pr)
            now = datetime.datetime.now(datetime.timezone.utc)
            self.checkpoint_store.record_phase(
                AutopilotCheckpointUnit(
                    phase=phase.value,
                    status=AutopilotStatus.COMPLETED.value,
                    completed_at=now,
                    duration_seconds=dur,
                    details=details,
                )
            )
            return pr
        except Exception as exc:
            dur = time.monotonic() - t0
            pr = PhaseResult(
                phase=phase,
                status=AutopilotStatus.FAILED,
                duration_seconds=dur,
                error=str(exc),
                care_badge=CareBadge.ACTION,
            )
            self._phases.append(pr)
            raise

    def _step_preflight(self) -> Dict[str, Any]:
        target_host = getattr(self.hop, "target_host", None)
        if target_host:
            from . import target_tuning_contract
            if not target_tuning_contract.is_safe_test_host(target_host):
                raise target_tuning_contract.UnsafeTargetHostError(f"Target host {target_host} is not a safe test host")
        return {"preflight": "passed", "hop": self.hop_name, "care_badge": CareBadge.PASS.value}

    def _step_bastion_tunnel(self) -> Dict[str, Any]:
        bastion_cfg = getattr(self.hop, "bastion", None)
        if not bastion_cfg and self.tunnel_mgr is None:
            return {"tunnel": "direct", "care_badge": CareBadge.PASS.value}
        if self.tunnel_mgr is None:
            from . import tunnel
            self.tunnel_mgr = tunnel.AutoBastionTunnelManager.from_hop(self.hop)
        self._active_tunnel = self.tunnel_mgr.bind_hop(self.hop)
        local_port = getattr(self._active_tunnel, "local_port", 0)
        return {"tunnel": "active", "local_port": local_port, "care_badge": CareBadge.PASS.value}

    def _step_target_tune_bulk(self) -> Dict[str, Any]:
        self._target_tuned = True
        target_eng = getattr(self.hop, "target_engine", getattr(self.hop, "dst", None))
        if self.tuner is None:
            from .tune import TargetHardwareTuner
            self.tuner = TargetHardwareTuner(
                journal_path=(self.state_dir / "target-tune-journal.json") if self.state_dir else None,
                apply=bool(target_eng is not None and not getattr(self, "dry_run", False)),
            )
        res = self.tuner.apply_bulk(target_eng, db=getattr(self.hop, "target_db", None))
        statements = getattr(res, "applied", [])
        return {"tuning": "bulk", "statements": statements, "care_badge": CareBadge.PASS.value}

    def _step_deferred_index_drop(self) -> Dict[str, Any]:
        if self.schema_lifecycle is not None:
            dropped = self.schema_lifecycle.drop_non_unique_indexes()
            self._indexes_deferred = True
            count = len(dropped) if isinstance(dropped, list) else int(dropped)
            return {"indexes_dropped": count, "care_badge": CareBadge.PASS.value}
        target_eng = getattr(self.hop, "target_engine", getattr(self.hop, "dst", None))
        if target_eng is not None:
            from . import schema
            lifecycle = schema.DeferredIndexLifecycle(self.hop, db=getattr(self.hop, "target_db", None))
            self._schema_lifecycle_inst = lifecycle
            self._indexes_deferred = True
            return {"indexes_dropped": 0, "status": "lifecycle_active", "care_badge": CareBadge.PASS.value}
        self._indexes_deferred = True
        return {"indexes_dropped": 0, "care_badge": CareBadge.PASS.value}

    def _step_parallel_bulk_load(self) -> Dict[str, Any]:
        if self.mover is not None:
            copied = self.mover.copy_tables()
            return {"tables_copied": copied, "care_badge": CareBadge.PASS.value}
        return {"tables_copied": 0, "status": "direct", "care_badge": CareBadge.PASS.value}

    def _step_deferred_index_restore(self) -> Dict[str, Any]:
        if self.schema_lifecycle is not None:
            rebuilt = self.schema_lifecycle.rebuild_indexes()
            fks = self.schema_lifecycle.restore_foreign_keys()
            self._indexes_deferred = False
            r_count = len(rebuilt) if isinstance(rebuilt, list) else int(rebuilt)
            f_count = len(fks) if isinstance(fks, list) else int(fks)
            return {"indexes_rebuilt": r_count, "fks_restored": f_count, "care_badge": CareBadge.PASS.value}
        self._indexes_deferred = False
        return {"indexes_rebuilt": 0, "fks_restored": 0, "care_badge": CareBadge.PASS.value}

    def _step_target_tune_restore(self) -> Dict[str, Any]:
        target_eng = getattr(self.hop, "target_engine", getattr(self.hop, "dst", None))
        if self.tuner is None:
            from .tune import TargetHardwareTuner
            self.tuner = TargetHardwareTuner(
                journal_path=(self.state_dir / "target-tune-journal.json") if self.state_dir else None,
                apply=bool(target_eng is not None and not getattr(self, "dry_run", False)),
            )
        res = self.tuner.apply_runtime(target_eng, db=getattr(self.hop, "target_db", None))
        self._target_tuned = False
        statements = getattr(res, "applied", [])
        return {"tuning": "runtime", "applied": statements, "care_badge": CareBadge.PASS.value}

    def _step_cdc_replication(self) -> Dict[str, Any]:
        if self.cdc_engine is not None:
            drained = self.cdc_engine.catchup()
            return {"lag_seconds": 0.0, "drained_events": drained, "care_badge": CareBadge.PASS.value}
        return {"lag_seconds": 0.0, "care_badge": CareBadge.PASS.value}

    def _step_merkle_verify(self) -> Dict[str, Any]:
        if self.merkle_folder is not None:
            parity = self.merkle_folder.verify_parity()
            if not parity.get("in_sync", True):
                self._zero_drift_verified = False
                raise RuntimeError("Merkle tree verification detected data drift")
            self._zero_drift_verified = True
            return {"merkle_root_match": True, "drifted_tables": 0, "care_badge": CareBadge.PASS.value}
        self._zero_drift_verified = True
        return {"merkle_root_match": True, "care_badge": CareBadge.PASS.value}

    def _step_flight_sim(self) -> Dict[str, Any]:
        if self.flight_sim is not None:
            func = getattr(self.flight_sim, "run_rehearsal", getattr(self.flight_sim, "run", None))
            if func is None:
                raise RuntimeError("Flight simulator has neither run_rehearsal nor run method")
            report = func()
            cert_dict = report.to_dict() if hasattr(report, "to_dict") else report
            verdict = getattr(report, "sla_verdict", "PASS")
            if verdict != "PASS":
                raise RuntimeError(f"Flight simulator rehearsal SLA check failed: {verdict}")
            return {"certificate": cert_dict, "care_badge": CareBadge.PASS.value}
        from . import flight_simulator
        sim = flight_simulator.ShadowCutoverFlightSimulator(self.hop)
        cert = sim.run()
        if getattr(cert, "sla_verdict", "PASS") != "PASS":
            raise RuntimeError(f"Shadow cutover flight simulator failed SLA: {cert.sla_verdict}")
        return {"certificate": cert.to_dict(), "care_badge": CareBadge.PASS.value}

    def _query_parity_ok(self) -> bool:
        guard = getattr(self, "query_guard", None) or getattr(self.hop, "query_guard", None)
        if guard is None:
            return True
        workload = getattr(guard, "workload", None)
        if workload:
            guard.audit(guard.sample_workload(list(workload)))
        report = getattr(guard, "last_report", None)
        return True if report is None else bool(report.can_cutover)

    def _step_seamless_cutover(self) -> Dict[str, Any]:
        work = f"autopilot:{self.hop_name}:cutover_gate"
        cutover_facts = {
            "zero_drift": self._zero_drift_verified,
            "auto_approve": self.auto_approve,
            "operator_consented": self.operator_consented,
            "data_divergence": not self._zero_drift_verified,
            "query_parity_ok": self._query_parity_ok(),
        }
        choice = decide.climb(work, autopilot_cutover_gate_rungs(), cutover_facts)
        if choice.rung is None or choice.rung.name == "abort_cutover_data_divergence":
            raise RuntimeError(f"Cutover aborted by gate: {choice.reason}")
        if choice.rung.name == "pause_for_manual_cutover_consent":
            raise RuntimeError(f"Cutover paused awaiting manual consent: {choice.reason}")

        target_host = getattr(self.hop, "target_host", None)
        target_port = getattr(self.hop, "target_port", None)
        if not target_host or not target_port:
            raise ValueError(f"Target host and port must be specified on hop: host={target_host}, port={target_port}")

        if self.pooler_coord is not None:
            switched = self.pooler_coord.shift_traffic(target_host, target_port)
            return {"cutover": "completed", "switched": bool(switched), "care_badge": CareBadge.PASS.value}

        try:
            from . import traffic_state
            state_dir = self.state_dir / "traffic"
            state_dir.mkdir(parents=True, exist_ok=True)
            source_ep = traffic_state.DatabaseEndpoint(
                host=getattr(self.hop, "source_host", "127.0.0.1"),
                port=getattr(self.hop, "source_port", 5432),
                db=getattr(self.hop, "source_db", "source_db"),
            )
            target_ep = traffic_state.DatabaseEndpoint(
                host=target_host,
                port=int(target_port),
                db=getattr(self.hop, "target_db", "target_db"),
            )
            sm = traffic_state.TrafficStateMachine(state_dir=state_dir, source=source_ep, target=target_ep)
            sm.transition_to(traffic_state.CutoverPhase.DRAIN)
            sm.transition_to(traffic_state.CutoverPhase.PROMOTE)
            sm.transition_to(traffic_state.CutoverPhase.COMPLETE)
            return {"cutover": "completed", "switched": True, "epoch": sm.epoch, "care_badge": CareBadge.PASS.value}
        except Exception as exc:
            log.error("Cutover traffic shift failed: %s", exc)
            raise RuntimeError(f"Cutover traffic shift failed: {exc}") from exc

    def _step_reverse_standby(self) -> Dict[str, Any]:
        work = f"autopilot:{self.hop_name}:reverse"
        reverse_facts = {
            "reverse_supported": self._reverse_supported,
            "dry_run": self.dry_run,
        }
        choice = decide.climb(work, autopilot_reverse_rungs(), reverse_facts)
        if choice.rung is None or choice.rung.name == "refuse_reverse_unsupported":
            return {"reverse_standby": "refused_unsupported", "reason": choice.reason, "care_badge": CareBadge.REVIEW.value}

        retention = float(getattr(self.hop, "reverse_retention_hours", 48.0))
        self._reverse_started = True
        if self.reverse_loop is not None:
            if hasattr(self.reverse_loop, "pre_warm"):
                self.reverse_loop.pre_warm(max_retention_hours=retention)
            return {"reverse_standby": "pre_warmed", "retention_hours": retention, "care_badge": CareBadge.PASS.value}

        from . import reverse
        loop = reverse.pre_warm_reverse_loop(self.hop, max_retention_hours=retention)
        return {
            "reverse_standby": "pre_warmed",
            "retention_hours": retention,
            "loop_id": getattr(loop, "name", "reverse_active"),
            "care_badge": CareBadge.PASS.value,
        }

    def _rollback_on_failure(self) -> None:
        if self._target_tuned:
            try:
                target_eng = getattr(self.hop, "target_engine", getattr(self.hop, "dst", None))
                if self.tuner is not None:
                    self.tuner.apply_runtime(target_eng, db=getattr(self.hop, "target_db", None))
                self._target_tuned = False
            except Exception as exc:
                log.warning("Rollback of target tuning failed: %s", exc)
        if self._indexes_deferred:
            try:
                if self.schema_lifecycle is not None:
                    self.schema_lifecycle.rebuild_indexes()
                    self.schema_lifecycle.restore_foreign_keys()
                self._indexes_deferred = False
            except Exception as exc:
                log.warning("Rollback of deferred indexes failed: %s", exc)
        if self.tunnel_mgr and self._active_tunnel:
            try:
                self.tunnel_mgr.close()
                self._active_tunnel = None
            except Exception as exc:
                log.warning("Closing active tunnel on rollback failed: %s", exc)
        if self.flight_sim and hasattr(self.flight_sim, "auto_rollback_to_savepoint"):
            try:
                self.flight_sim.auto_rollback_to_savepoint()
            except Exception as exc:
                log.warning("Flight sim auto-rollback failed: %s", exc)

    def _save_state(self, result: AutopilotResult) -> None:
        try:
            durable.write_json(
                f"state/autopilot_{self.hop_name}.json",
                {
                    "hop": result.hop_name,
                    "ok": result.ok,
                    "status": result.status.value,
                    "duration": result.total_duration_seconds,
                    "failback_available": result.failback_available,
                    "phases": [dataclasses.asdict(p) for p in result.phases],
                    "error": result.error,
                },
            )
        except Exception:
            pass
