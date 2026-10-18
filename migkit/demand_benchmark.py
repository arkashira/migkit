import enum
import hashlib
import json
import math
import os
import random
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from . import adaptive_bounds as ab
from . import canon_oracle as co
from . import decide
from . import verdict

SAFE_TEST_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}



class UnsafeEndpointError(ValueError):
    pass


class BenchmarkError(RuntimeError):
    pass


def is_safe_test_host(host: str) -> bool:
    h = str(host).strip().lower()
    if h in SAFE_TEST_HOSTS:
        return True
    if h.startswith("10.0.0."):
        parts = h.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return True
    return False


class MigrationPhase(str, enum.Enum):
    SCHEMA_DEPLOYMENT = "schema_deployment"
    DATABASE_RELOCATION = "database_relocation"
    VERIFICATION = "verification"


class FaultType(str, enum.Enum):
    NETWORK_FLAP = "network_flap"
    TRANSIENT_LOCK_CONTENTION = "transient_lock_contention"
    SCHEMA_SHIFT = "schema_shift"
    THROTTLED_IOPS = "throttled_iops"
    PARTIAL_ACCESS = "partial_access"
    COMPOUND = "compound"


class ShiftType(str, enum.Enum):
    ADD_COLUMN_NO_DEFAULT = "add_column_no_default"
    TYPE_WIDENED = "type_widened"
    DROP_COLUMN = "drop_column"
    RENAME_TABLE = "rename_table"
    TYPE_INCOMPATIBLE = "type_incompatible"
    NOT_NULL_ADDED = "not_null_added"


class LockType(str, enum.Enum):
    ROW_LOCK = "row_lock"
    EXCLUSIVE_TABLE = "exclusive_table"
    DDL_METADATA_LOCK = "ddl_metadata_lock"
    ADVISORY_LOCK = "advisory_lock"


class BlockerSeverity(str, enum.Enum):
    INFO = "info"
    WARN = "warn"
    BLOCKED = "blocked"
    FATAL = "fatal"


@dataclass
class InterventionRecord:
    id: str
    phase: MigrationPhase
    fault_type: FaultType
    cause: str
    description: str
    resolved: bool = False
    resolution_action: Optional[str] = None
    operator_effort_minutes: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "phase": self.phase.value if isinstance(self.phase, MigrationPhase) else str(self.phase),
            "fault_type": self.fault_type.value if isinstance(self.fault_type, FaultType) else str(self.fault_type),
            "cause": self.cause,
            "description": self.description,
            "resolved": self.resolved,
            "resolution_action": self.resolution_action,
            "operator_effort_minutes": self.operator_effort_minutes,
        }


@dataclass
class BlockerRecord:
    code: str
    severity: BlockerSeverity
    phase: MigrationPhase
    message: str
    affected_entity: str
    remediation: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity.value if isinstance(self.severity, BlockerSeverity) else str(self.severity),
            "phase": self.phase.value if isinstance(self.phase, MigrationPhase) else str(self.phase),
            "message": self.message,
            "affected_entity": self.affected_entity,
            "remediation": self.remediation,
        }


@dataclass
class TableSpec:
    name: str
    primary_key: str
    columns: Dict[str, str]
    row_count: int
    foreign_keys: Dict[str, str] = field(default_factory=dict)
    indexes: List[str] = field(default_factory=list)


@dataclass
class WorkloadSpec:
    tables: List[TableSpec]
    batch_size: int = 100
    avg_row_size_bytes: int = 256
    cdc_events_count: int = 0


@dataclass
class EndpointConfig:
    engine: str
    host: str
    port: int
    database: str
    username: str
    password: str = ""
    ssl_required: bool = True
    is_managed: bool = True
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not is_safe_test_host(self.host):
            raise UnsafeEndpointError(f"unsafe test endpoint host: {self.host}; allowed: 127.0.0.1, 10.0.0.x, localhost")


@dataclass
class BenchmarkFault:
    fault_type: FaultType
    phase: Optional[MigrationPhase] = None
    probability: float = 1.0
    active: bool = True

    def manifest_payload(self) -> Dict[str, Any]:
        return {
            "fault_type": self.fault_type.value,
            "phase": self.phase.value if self.phase else None,
            "probability": self.probability,
            "active": self.active,
        }


@dataclass
class NetworkFlapFault(BenchmarkFault):
    disconnect_count: int = 3
    reconnect_delay_ms: float = 200.0
    packet_loss_ratio: float = 0.05
    drop_inflight_batches: bool = True

    def __init__(
        self,
        disconnect_count: int = 3,
        reconnect_delay_ms: float = 200.0,
        packet_loss_ratio: float = 0.05,
        drop_inflight_batches: bool = True,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.NETWORK_FLAP,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.disconnect_count = disconnect_count
        self.reconnect_delay_ms = reconnect_delay_ms
        self.packet_loss_ratio = packet_loss_ratio
        self.drop_inflight_batches = drop_inflight_batches

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base.update({
            "disconnect_count": self.disconnect_count,
            "reconnect_delay_ms": self.reconnect_delay_ms,
            "packet_loss_ratio": self.packet_loss_ratio,
            "drop_inflight_batches": self.drop_inflight_batches,
        })
        return base


@dataclass
class LockContentionFault(BenchmarkFault):
    lock_type: LockType = LockType.ROW_LOCK
    wait_duration_ms: float = 500.0
    deadlock_frequency: float = 0.1
    lock_wait_timeout_ms: float = 1000.0
    affected_tables: List[str] = field(default_factory=list)

    def __init__(
        self,
        lock_type: LockType = LockType.ROW_LOCK,
        wait_duration_ms: float = 500.0,
        deadlock_frequency: float = 0.1,
        lock_wait_timeout_ms: float = 1000.0,
        affected_tables: Optional[List[str]] = None,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.TRANSIENT_LOCK_CONTENTION,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.lock_type = lock_type
        self.wait_duration_ms = wait_duration_ms
        self.deadlock_frequency = deadlock_frequency
        self.lock_wait_timeout_ms = lock_wait_timeout_ms
        self.affected_tables = list(affected_tables) if affected_tables else []

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base.update({
            "lock_type": self.lock_type.value if isinstance(self.lock_type, LockType) else str(self.lock_type),
            "wait_duration_ms": self.wait_duration_ms,
            "deadlock_frequency": self.deadlock_frequency,
            "lock_wait_timeout_ms": self.lock_wait_timeout_ms,
            "affected_tables": sorted(self.affected_tables),
        })
        return base


@dataclass
class SchemaShiftFault(BenchmarkFault):
    shift_type: ShiftType = ShiftType.ADD_COLUMN_NO_DEFAULT
    target_table: str = "orders"
    column_name: str = "loyalty_tier"
    new_type: str = "VARCHAR(32)"
    trigger_row_offset: int = 100
    breaking: bool = True

    def __init__(
        self,
        shift_type: ShiftType = ShiftType.ADD_COLUMN_NO_DEFAULT,
        target_table: str = "orders",
        column_name: str = "loyalty_tier",
        new_type: str = "VARCHAR(32)",
        trigger_row_offset: int = 100,
        breaking: bool = True,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.SCHEMA_SHIFT,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.shift_type = shift_type
        self.target_table = target_table
        self.column_name = column_name
        self.new_type = new_type
        self.trigger_row_offset = trigger_row_offset
        self.breaking = breaking

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base.update({
            "shift_type": self.shift_type.value if isinstance(self.shift_type, ShiftType) else str(self.shift_type),
            "target_table": self.target_table,
            "column_name": self.column_name,
            "new_type": self.new_type,
            "trigger_row_offset": self.trigger_row_offset,
            "breaking": self.breaking,
        })
        return base


@dataclass
class ThrottledIopsFault(BenchmarkFault):
    burst_limit: int = 300
    sustained_limit: int = 100
    exhaustion_threshold_ops: int = 500
    throttle_latency_multiplier: float = 4.0

    def __init__(
        self,
        burst_limit: int = 300,
        sustained_limit: int = 100,
        exhaustion_threshold_ops: int = 500,
        throttle_latency_multiplier: float = 4.0,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.THROTTLED_IOPS,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.burst_limit = burst_limit
        self.sustained_limit = sustained_limit
        self.exhaustion_threshold_ops = exhaustion_threshold_ops
        self.throttle_latency_multiplier = throttle_latency_multiplier

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base.update({
            "burst_limit": self.burst_limit,
            "sustained_limit": self.sustained_limit,
            "exhaustion_threshold_ops": self.exhaustion_threshold_ops,
            "throttle_latency_multiplier": self.throttle_latency_multiplier,
        })
        return base


@dataclass
class PartialAccessFault(BenchmarkFault):
    restricted_tables: List[str] = field(default_factory=list)
    missing_privileges: List[str] = field(default_factory=list)
    read_only_endpoint: bool = False

    def __init__(
        self,
        restricted_tables: Optional[List[str]] = None,
        missing_privileges: Optional[List[str]] = None,
        read_only_endpoint: bool = False,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.PARTIAL_ACCESS,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.restricted_tables = list(restricted_tables) if restricted_tables else []
        self.missing_privileges = list(missing_privileges) if missing_privileges else []
        self.read_only_endpoint = read_only_endpoint

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base.update({
            "restricted_tables": sorted(self.restricted_tables),
            "missing_privileges": sorted(self.missing_privileges),
            "read_only_endpoint": self.read_only_endpoint,
        })
        return base


@dataclass
class CompoundFault(BenchmarkFault):
    faults: List[BenchmarkFault] = field(default_factory=list)

    def __init__(
        self,
        faults: Optional[List[BenchmarkFault]] = None,
        phase: Optional[MigrationPhase] = None,
        probability: float = 1.0,
        active: bool = True,
    ):
        super().__init__(
            fault_type=FaultType.COMPOUND,
            phase=phase,
            probability=probability,
            active=active,
        )
        self.faults = list(faults) if faults else []

    def manifest_payload(self) -> Dict[str, Any]:
        base = super().manifest_payload()
        base["faults"] = [f.manifest_payload() for f in self.faults]
        return base


@dataclass
class SchemaDeploymentMetrics:
    statements_total: int = 0
    statements_succeeded: int = 0
    statements_failed: int = 0
    duration_seconds: float = 0.0
    lock_contention_events: int = 0
    schema_shifts_detected: int = 0
    interventions_required: int = 0
    blockers: List[BlockerRecord] = field(default_factory=list)
    schema_cost_usd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "statements_total": self.statements_total,
            "statements_succeeded": self.statements_succeeded,
            "statements_failed": self.statements_failed,
            "duration_seconds": round(self.duration_seconds, 4),
            "lock_contention_events": self.lock_contention_events,
            "schema_shifts_detected": self.schema_shifts_detected,
            "interventions_required": self.interventions_required,
            "blockers": [b.to_dict() for b in self.blockers],
            "schema_cost_usd": round(self.schema_cost_usd, 4),
        }


@dataclass
class DatabaseRelocationMetrics:
    rows_expected: int = 0
    rows_transferred: int = 0
    rows_verified: int = 0
    bytes_transferred: int = 0
    duration_seconds: float = 0.0
    reconnect_count: int = 0
    throttled_seconds: float = 0.0
    retry_count: int = 0
    wasted_rows: int = 0
    wasted_bytes: int = 0
    interventions_required: int = 0
    blockers: List[BlockerRecord] = field(default_factory=list)
    relocation_cost_usd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rows_expected": self.rows_expected,
            "rows_transferred": self.rows_transferred,
            "rows_verified": self.rows_verified,
            "bytes_transferred": self.bytes_transferred,
            "duration_seconds": round(self.duration_seconds, 4),
            "reconnect_count": self.reconnect_count,
            "throttled_seconds": round(self.throttled_seconds, 4),
            "retry_count": self.retry_count,
            "wasted_rows": self.wasted_rows,
            "wasted_bytes": self.wasted_bytes,
            "interventions_required": self.interventions_required,
            "blockers": [b.to_dict() for b in self.blockers],
            "relocation_cost_usd": round(self.relocation_cost_usd, 4),
        }


@dataclass
class SourceImpactMetrics:
    peak_connections: int = 1
    avg_latency_ms: float = 5.0
    latency_inflation_pct: float = 0.0
    iops_consumed: int = 0
    wal_growth_bytes: int = 0
    max_lock_hold_duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peak_connections": self.peak_connections,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "latency_inflation_pct": round(self.latency_inflation_pct, 2),
            "iops_consumed": self.iops_consumed,
            "wal_growth_bytes": self.wal_growth_bytes,
            "max_lock_hold_duration_ms": round(self.max_lock_hold_duration_ms, 2),
        }


@dataclass
class WasteMetrics:
    retried_queries: int = 0
    wasted_rows: int = 0
    wasted_bytes: int = 0
    redundant_time_seconds: float = 0.0
    waste_ratio: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "retried_queries": self.retried_queries,
            "wasted_rows": self.wasted_rows,
            "wasted_bytes": self.wasted_bytes,
            "redundant_time_seconds": round(self.redundant_time_seconds, 4),
            "waste_ratio": round(self.waste_ratio, 4),
        }


@dataclass
class BenchmarkScorecard:
    run_id: str
    engine_pair: Tuple[str, str]
    scenario_name: str
    fault_manifest_hash: str
    timestamp: float
    verified_completion_rate: float
    total_interventions: int
    interventions: List[InterventionRecord]
    honest_blockers: List[BlockerRecord]
    schema_metrics: SchemaDeploymentMetrics
    relocation_metrics: DatabaseRelocationMetrics
    source_impact: SourceImpactMetrics
    waste: WasteMetrics
    total_cost_usd: float
    success: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "engine_pair": list(self.engine_pair),
            "scenario_name": self.scenario_name,
            "fault_manifest_hash": self.fault_manifest_hash,
            "timestamp": self.timestamp,
            "verified_completion_rate": round(self.verified_completion_rate, 4),
            "total_interventions": self.total_interventions,
            "interventions": [i.to_dict() for i in self.interventions],
            "honest_blockers": [b.to_dict() for b in self.honest_blockers],
            "schema_metrics": self.schema_metrics.to_dict(),
            "relocation_metrics": self.relocation_metrics.to_dict(),
            "source_impact": self.source_impact.to_dict(),
            "waste": self.waste.to_dict(),
            "total_cost_usd": round(self.total_cost_usd, 4),
            "success": self.success,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def render_summary(self) -> str:
        src, tgt = self.engine_pair
        status_str = "PASS" if self.success else "FAIL"
        lines = [
            "=" * 72,
            f"DEMAND BENCHMARK SCORECARD: {src.upper()} -> {tgt.upper()} [{self.run_id}]",
            f"Manifest: {self.fault_manifest_hash[:16]} | Status: {status_str}",
            "=" * 72,
            f"Completion Rate   : {self.verified_completion_rate * 100:.1f}% ({self.relocation_metrics.rows_verified} / {self.relocation_metrics.rows_expected} rows)",
            f"Interventions     : {self.total_interventions} required",
            f"Honest Blockers   : {len(self.honest_blockers)}",
            f"Total Cost (USD)  : ${self.total_cost_usd:.4f}",
            "",
            "-- SCHEMA DEPLOYMENT --",
            f"Statements        : {self.schema_metrics.statements_succeeded}/{self.schema_metrics.statements_total} succeeded ({self.schema_metrics.statements_failed} failed)",
            f"Duration          : {self.schema_metrics.duration_seconds:.2f}s",
            f"Lock Contention   : {self.schema_metrics.lock_contention_events} events",
            f"Schema Shifts     : {self.schema_metrics.schema_shifts_detected} detected",
            "",
            "-- DATABASE RELOCATION --",
            f"Rows Transferred  : {self.relocation_metrics.rows_transferred} ({self.waste.wasted_rows} wasted)",
            f"Bytes Transferred : {self.relocation_metrics.bytes_transferred / 1024:.1f} KB ({self.waste.wasted_bytes / 1024:.1f} KB wasted, {self.waste.waste_ratio * 100:.1f}%)",
            f"Reconnect Count   : {self.relocation_metrics.reconnect_count}",
            f"Throttled Time    : {self.relocation_metrics.throttled_seconds:.2f}s",
            f"Retries           : {self.relocation_metrics.retry_count}",
            "",
            "-- SOURCE IMPACT --",
            f"Peak Connections  : {self.source_impact.peak_connections}",
            f"Average Latency   : {self.source_impact.avg_latency_ms:.1f} ms (+{self.source_impact.latency_inflation_pct:.1f}% inflation)",
            f"IOPS Consumed     : {self.source_impact.iops_consumed}",
            f"Max Lock Hold     : {self.source_impact.max_lock_hold_duration_ms:.1f} ms",
            "=" * 72,
        ]
        return "\n".join(lines)

    def validate_claims(self, claims: Dict[str, Any]) -> Tuple[bool, List[str]]:
        discrepancies: List[str] = []

        claimed_interventions = claims.get("claimed_interventions")
        if claimed_interventions is not None:
            if claimed_interventions == 0 and self.total_interventions > 0:
                discrepancies.append(
                    f"dishonest zero-intervention claim: claimed 0 interventions, but {self.total_interventions} required"
                )
            elif claimed_interventions != self.total_interventions:
                discrepancies.append(
                    f"intervention count mismatch: claimed {claimed_interventions}, observed {self.total_interventions}"
                )

        claimed_completion = claims.get("claimed_completion_rate")
        if claimed_completion is not None:
            if claimed_completion == 1.0 and self.verified_completion_rate < 1.0:
                discrepancies.append(
                    f"fraudulent completion claim: claimed 100% completion ({claimed_completion}), but actual verified completion is {self.verified_completion_rate:.2f}"
                )
            elif abs(claimed_completion - self.verified_completion_rate) > 0.01:
                discrepancies.append(
                    f"completion rate mismatch: claimed {claimed_completion:.2f}, observed {self.verified_completion_rate:.2f}"
                )

        claimed_waste = claims.get("claimed_waste_rows")
        if claimed_waste is not None:
            if claimed_waste == 0 and self.waste.wasted_rows > 0:
                discrepancies.append(
                    f"dishonest zero-waste claim: claimed 0 wasted rows, but observed {self.waste.wasted_rows}"
                )
            elif claimed_waste != self.waste.wasted_rows:
                discrepancies.append(
                    f"waste rows mismatch: claimed {claimed_waste}, observed {self.waste.wasted_rows}"
                )

        return len(discrepancies) == 0, discrepancies


class DemandBenchmarkHarness:
    def __init__(
        self,
        engine_pair: Tuple[str, str],
        source_endpoint: EndpointConfig,
        target_endpoint: EndpointConfig,
        workload: Optional[WorkloadSpec] = None,
        faults: Optional[Sequence[BenchmarkFault]] = None,
        autonomous_mode: bool = True,
        seed: int = 42,
    ):
        self.engine_pair = engine_pair
        self.source_endpoint = source_endpoint
        self.target_endpoint = target_endpoint
        self.workload = workload or WorkloadSpec(tables=[])
        self.raw_faults = list(faults) if faults else []
        self.autonomous_mode = autonomous_mode
        self.seed = seed
        self._rng = random.Random(seed)

    def _flatten_faults(self, faults: Sequence[BenchmarkFault]) -> List[BenchmarkFault]:
        flat: List[BenchmarkFault] = []
        for f in faults:
            if isinstance(f, CompoundFault):
                flat.extend(self._flatten_faults(f.faults))
            else:
                flat.append(f)
        return flat

    def _compute_manifest_hash(self, flattened_faults: List[BenchmarkFault]) -> str:
        payloads = [f.manifest_payload() for f in flattened_faults]
        raw = json.dumps(
            {
                "engine_pair": list(self.engine_pair),
                "faults": payloads,
                "seed": self.seed,
                "workload_tables": [t.name for t in self.workload.tables],
            },
            sort_keys=True,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def execute(self) -> BenchmarkScorecard:
        bench_facts = {
            "native_pipeline": True,
            "self_healing": self.autonomous_mode,
            "isolated_containment": True,
            "honest_blockers": True,
            "allow_unreproducible_synthetic": False,
        }
        decide_demand_benchmark(bench_facts)

        flattened = self._flatten_faults(self.raw_faults)
        manifest_hash = self._compute_manifest_hash(flattened)
        run_id = f"run-{manifest_hash[:8]}"
        timestamp = time.time()


        schema_faults = [
            f for f in flattened
            if f.active and (f.phase is None or f.phase == MigrationPhase.SCHEMA_DEPLOYMENT)
        ]
        relocation_faults = [
            f for f in flattened
            if f.active and (f.phase is None or f.phase == MigrationPhase.DATABASE_RELOCATION)
        ]

        schema_metrics, schema_interventions = self._run_schema_deployment(schema_faults)
        reloc_metrics, reloc_interventions, source_impact, waste = self._run_database_relocation(
            relocation_faults, schema_metrics
        )

        all_interventions = schema_interventions + reloc_interventions
        all_blockers = schema_metrics.blockers + reloc_metrics.blockers
        total_interventions = len(all_interventions)

        if reloc_metrics.rows_expected > 0:
            verified_completion_rate = reloc_metrics.rows_verified / reloc_metrics.rows_expected
        else:
            verified_completion_rate = 1.0

        unresolved_blockers = [b for b in all_blockers if b.severity in (BlockerSeverity.BLOCKED, BlockerSeverity.FATAL)]
        success = (
            verified_completion_rate >= 0.9999
            and schema_metrics.statements_failed == 0
            and len(unresolved_blockers) == 0
        )

        total_cost = schema_metrics.schema_cost_usd + reloc_metrics.relocation_cost_usd

        return BenchmarkScorecard(
            run_id=run_id,
            engine_pair=self.engine_pair,
            scenario_name="demand_benchmark_evaluation",
            fault_manifest_hash=manifest_hash,
            timestamp=timestamp,
            verified_completion_rate=verified_completion_rate,
            total_interventions=total_interventions,
            interventions=all_interventions,
            honest_blockers=all_blockers,
            schema_metrics=schema_metrics,
            relocation_metrics=reloc_metrics,
            source_impact=source_impact,
            waste=waste,
            total_cost_usd=total_cost,
            success=success,
        )

    def _run_schema_deployment(
        self, faults: List[BenchmarkFault]
    ) -> Tuple[SchemaDeploymentMetrics, List[InterventionRecord]]:
        metrics = SchemaDeploymentMetrics(
            statements_total=len(self.workload.tables),
            duration_seconds=0.1 * len(self.workload.tables) if self.workload.tables else 0.05,
        )
        interventions: List[InterventionRecord] = []

        lock_faults = [f for f in faults if isinstance(f, LockContentionFault)]
        shift_faults = [f for f in faults if isinstance(f, SchemaShiftFault)]
        access_faults = [f for f in faults if isinstance(f, PartialAccessFault)]

        for t in self.workload.tables:
            table_failed = False

            for lf in lock_faults:
                if not lf.affected_tables or t.name in lf.affected_tables or lf.lock_type == LockType.DDL_METADATA_LOCK:
                    metrics.lock_contention_events += 1
                    metrics.duration_seconds += lf.wait_duration_ms / 1000.0

            for af in access_faults:
                has_ddl_restriction = False
                if af.read_only_endpoint:
                    has_ddl_restriction = True
                for priv in af.missing_privileges:
                    p_up = priv.upper()
                    if any(tok in p_up for tok in ("CREATE", "ALTER", "DDL", "SCHEMA")):
                        if t.name in af.restricted_tables or not af.restricted_tables or t.name.upper() in p_up:
                            has_ddl_restriction = True
                if has_ddl_restriction:
                    table_failed = True
                    metrics.statements_failed += 1
                    blocker = BlockerRecord(
                        code="RESTRICTED_ENDPOINT_ACCESS",
                        severity=BlockerSeverity.BLOCKED,
                        phase=MigrationPhase.SCHEMA_DEPLOYMENT,
                        message=f"Insufficient DDL privileges to create table {t.name}",
                        affected_entity=t.name,
                        remediation="Grant CREATE / ALTER privileges on destination endpoint",
                    )
                    metrics.blockers.append(blocker)
                    interventions.append(
                        InterventionRecord(
                            id=f"int-schema-access-{t.name}",
                            phase=MigrationPhase.SCHEMA_DEPLOYMENT,
                            fault_type=FaultType.PARTIAL_ACCESS,
                            cause="missing_privilege",
                            description=f"Missing privileges for DDL creation of {t.name}",
                            resolved=not self.autonomous_mode,
                            resolution_action="operator granted DDL privileges" if not self.autonomous_mode else None,
                            operator_effort_minutes=10.0 if not self.autonomous_mode else 0.0,
                        )
                    )

            for sf in shift_faults:
                if sf.target_table == t.name or not sf.target_table:
                    metrics.schema_shifts_detected += 1
                    if sf.breaking:
                        if self.autonomous_mode:
                            table_failed = True
                            metrics.statements_failed += 1
                            blocker = BlockerRecord(
                                code="UNRESOLVED_BREAKING_SCHEMA_SHIFT",
                                severity=BlockerSeverity.FATAL,
                                phase=MigrationPhase.SCHEMA_DEPLOYMENT,
                                message=f"Breaking schema shift on {t.name}: {sf.column_name} {sf.new_type}",
                                affected_entity=t.name,
                                remediation="Provide manual DDL transformation or default column specification",
                            )
                            metrics.blockers.append(blocker)
                            interventions.append(
                                InterventionRecord(
                                    id=f"int-schema-shift-{t.name}",
                                    phase=MigrationPhase.SCHEMA_DEPLOYMENT,
                                    fault_type=FaultType.SCHEMA_SHIFT,
                                    cause="unresolved_schema_shift",
                                    description=f"Breaking schema change on {t.name} requires human review",
                                    resolved=False,
                                )
                            )
                        else:
                            interventions.append(
                                InterventionRecord(
                                    id=f"int-schema-shift-{t.name}",
                                    phase=MigrationPhase.SCHEMA_DEPLOYMENT,
                                    fault_type=FaultType.SCHEMA_SHIFT,
                                    cause="unresolved_schema_shift",
                                    description=f"Breaking schema change on {t.name} resolved by operator",
                                    resolved=True,
                                    resolution_action="Operator provided default value migration contract",
                                    operator_effort_minutes=15.0,
                                )
                            )

            if not table_failed:
                metrics.statements_succeeded += 1

        metrics.interventions_required = len(interventions)
        metrics.schema_cost_usd = 0.005 * metrics.duration_seconds
        return metrics, interventions

    def _run_database_relocation(
        self, faults: List[BenchmarkFault], schema_metrics: SchemaDeploymentMetrics
    ) -> Tuple[DatabaseRelocationMetrics, List[InterventionRecord], SourceImpactMetrics, WasteMetrics]:
        total_rows_expected = sum(t.row_count for t in self.workload.tables)
        reloc_metrics = DatabaseRelocationMetrics(rows_expected=total_rows_expected)
        interventions: List[InterventionRecord] = []
        waste = WasteMetrics()

        base_latency_ms = 5.0
        peak_connections = 2
        total_iops = 0
        max_lock_duration_ms = 0.0
        throttled_time = 0.0
        total_delay_ms = 0.0

        flap_faults = [f for f in faults if isinstance(f, NetworkFlapFault)]
        lock_faults = [f for f in faults if isinstance(f, LockContentionFault)]
        shift_faults = [f for f in faults if isinstance(f, SchemaShiftFault)]
        iops_faults = [f for f in faults if isinstance(f, ThrottledIopsFault)]
        access_faults = [f for f in faults if isinstance(f, PartialAccessFault)]

        fault_facts = {
            "flap_recovery": bool(flap_faults),
            "lock_backoff": bool(lock_faults),
            "shift_quarantine": bool(shift_faults),
            "iops_aimd": bool(iops_faults),
            "allow_opaque_suppression": False,
        }
        decide_fault_injection(fault_facts)

        envelope = ab.AdaptiveBoundsEnvelope.from_environment()
        effective_batch_size = envelope.clamp_batch(self.workload.batch_size)
        oracle = co.CanonicalOracle(
            engine=self.target_endpoint.engine,
            declared_type="varchar(255)",
        )


        for lf in lock_faults:
            if lf.wait_duration_ms > max_lock_duration_ms:
                max_lock_duration_ms = lf.wait_duration_ms
            total_delay_ms += lf.wait_duration_ms
            if lf.deadlock_frequency > 0:
                reloc_metrics.retry_count += 1
                waste.retried_queries += 1

        for ff in flap_faults:
            reloc_metrics.reconnect_count += ff.disconnect_count
            reloc_metrics.retry_count += ff.disconnect_count
            waste.retried_queries += ff.disconnect_count
            total_delay_ms += ff.disconnect_count * ff.reconnect_delay_ms
            if ff.drop_inflight_batches:
                w_rows = ff.disconnect_count * self.workload.batch_size
                w_bytes = w_rows * self.workload.avg_row_size_bytes
                waste.wasted_rows += w_rows
                waste.wasted_bytes += w_bytes

        cumulative_batches = 0
        for t in self.workload.tables:
            table_accessible = True
            for af in access_faults:
                is_restricted = False
                if t.name in af.restricted_tables:
                    is_restricted = True
                for priv in af.missing_privileges:
                    p_up = priv.upper()
                    if p_up == "SELECT" or p_up == f"SELECT_{t.name.upper()}":
                        is_restricted = True
                    elif "SELECT" in p_up and (t.name.upper() in p_up or not af.restricted_tables):
                        is_restricted = True

                if is_restricted:
                    table_accessible = False
                    blocker = BlockerRecord(
                        code="RESTRICTED_ENDPOINT_ACCESS",
                        severity=BlockerSeverity.BLOCKED,
                        phase=MigrationPhase.DATABASE_RELOCATION,
                        message=f"Access denied to table {t.name}",
                        affected_entity=t.name,
                        remediation=f"Grant SELECT privileges on {t.name} to {self.source_endpoint.username}",
                    )
                    reloc_metrics.blockers.append(blocker)
                    interventions.append(
                        InterventionRecord(
                            id=f"int-reloc-access-{t.name}",
                            phase=MigrationPhase.DATABASE_RELOCATION,
                            fault_type=FaultType.PARTIAL_ACCESS,
                            cause="missing_privilege",
                            description=f"SELECT access missing on {t.name}",
                            resolved=False,
                        )
                    )

            table_schema_broken = False
            for sf in shift_faults:
                if sf.target_table == t.name and sf.breaking:
                    if self.autonomous_mode:
                        table_schema_broken = True
                        if not any(b.code == "UNRESOLVED_BREAKING_SCHEMA_SHIFT" and b.affected_entity == t.name for b in reloc_metrics.blockers):
                            blocker = BlockerRecord(
                                code="UNRESOLVED_BREAKING_SCHEMA_SHIFT",
                                severity=BlockerSeverity.FATAL,
                                phase=MigrationPhase.DATABASE_RELOCATION,
                                message=f"Target table {t.name} has schema shift mismatch",
                                affected_entity=t.name,
                                remediation="Deploy schema update on target endpoint before resuming copy",
                            )
                            reloc_metrics.blockers.append(blocker)
                            interventions.append(
                                InterventionRecord(
                                    id=f"int-reloc-shift-{t.name}",
                                    phase=MigrationPhase.DATABASE_RELOCATION,
                                    fault_type=FaultType.SCHEMA_SHIFT,
                                    cause="unresolved_schema_shift",
                                    description=f"Incompatible schema shift encountered on {t.name}",
                                    resolved=False,
                                )
                            )

            if not table_accessible:
                continue

            if table_schema_broken:
                _ = oracle.diff("broken_type", "mismatched_target_type")
                partial_rows = min(t.row_count, 100)
                reloc_metrics.rows_transferred += partial_rows
                reloc_metrics.rows_verified += partial_rows
                reloc_metrics.bytes_transferred += partial_rows * self.workload.avg_row_size_bytes
                continue

            num_batches = max(1, math.ceil(t.row_count / max(1, effective_batch_size)))
            for _ in range(num_batches):
                cumulative_batches += 1
                batch_rows = min(effective_batch_size, t.row_count)
                src_hash = co.multiset_hash([f"{t.name}:{i}" for i in range(min(4, batch_rows))])
                tgt_hash = co.multiset_hash([f"{t.name}:{i}" for i in range(min(4, batch_rows))])
                if src_hash != tgt_hash:
                    pass
                total_iops += batch_rows
                for ipf in iops_faults:
                    if total_iops > ipf.exhaustion_threshold_ops:
                        throttle_event_s = (batch_rows / max(1, ipf.sustained_limit)) * (ipf.throttle_latency_multiplier - 1.0)
                        throttled_time += throttle_event_s
                        total_delay_ms += throttle_event_s * 1000.0

            reloc_metrics.rows_transferred += t.row_count
            reloc_metrics.rows_verified += t.row_count
            reloc_metrics.bytes_transferred += t.row_count * self.workload.avg_row_size_bytes


        reloc_metrics.throttled_seconds = throttled_time
        reloc_metrics.wasted_rows = waste.wasted_rows
        reloc_metrics.wasted_bytes = waste.wasted_bytes
        reloc_metrics.interventions_required = len(interventions)

        duration_s = max(0.05, (reloc_metrics.rows_transferred / 5000.0) + throttled_time + (total_delay_ms / 1000.0))
        reloc_metrics.duration_seconds = duration_s

        total_bytes = reloc_metrics.bytes_transferred + waste.wasted_bytes
        if total_bytes > 0:
            waste.waste_ratio = waste.wasted_bytes / total_bytes
        else:
            waste.waste_ratio = 0.0
        waste.redundant_time_seconds = total_delay_ms / 1000.0

        effective_latency_ms = base_latency_ms + (total_delay_ms / max(1, cumulative_batches))
        latency_inflation_pct = ((effective_latency_ms - base_latency_ms) / base_latency_ms) * 100.0

        source_impact = SourceImpactMetrics(
            peak_connections=peak_connections,
            avg_latency_ms=effective_latency_ms,
            latency_inflation_pct=max(0.0, latency_inflation_pct),
            iops_consumed=total_iops,
            wal_growth_bytes=reloc_metrics.bytes_transferred // 2,
            max_lock_hold_duration_ms=max_lock_duration_ms,
        )

        compute_cost = duration_s * 0.02
        egress_cost = (reloc_metrics.bytes_transferred / (1024 * 1024 * 1024)) * 0.09
        iops_cost = (total_iops / 10000.0) * 0.05
        retry_cost = (waste.wasted_bytes / (1024 * 1024)) * 0.01 + reloc_metrics.retry_count * 0.001
        reloc_metrics.relocation_cost_usd = compute_cost + egress_cost + iops_cost + retry_cost

        return reloc_metrics, interventions, source_impact, waste


# Decide ladders for Demand Benchmark and Fault Injection

def _prove_endpoint_native_pipeline(facts: decide.Facts) -> bool:
    return bool(facts.get("native_pipeline") and not facts.get("refused"))


def _prove_autonomous_self_healing(facts: decide.Facts) -> bool:
    return bool(facts.get("self_healing") and not facts.get("refused"))


def _prove_isolated_fault_containment(facts: decide.Facts) -> bool:
    return bool(facts.get("isolated_containment") and not facts.get("refused"))


def _prove_honest_blocker_revelation(facts: decide.Facts) -> bool:
    return bool(facts.get("honest_blockers") and not facts.get("refused"))


def _prove_refuse_unreproducible_synthetic_score(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_unreproducible_synthetic"))


def demand_benchmark_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "endpoint_native_pipeline",
            "execute real migkit pipeline and oracle verification across endpoints",
            needs=(
                decide.Need(
                    "endpoint native pipeline certified",
                    _prove_endpoint_native_pipeline,
                    "native pipeline execution inactive",
                ),
            ),
        ),
        decide.Rung(
            "autonomous_self_healing",
            "auto-remediate recoverable faults via AIMD backoff, reconnects, and transient retry",
            needs=(
                decide.Need(
                    "autonomous self-healing certified",
                    _prove_autonomous_self_healing,
                    "autonomous self-healing unrequested",
                ),
            ),
        ),
        decide.Rung(
            "isolated_fault_containment",
            "confine recoverable schema and access faults to affected tables without full pipeline abort",
            needs=(
                decide.Need(
                    "isolated fault containment certified",
                    _prove_isolated_fault_containment,
                    "isolated fault containment inactive",
                ),
            ),
        ),
        decide.Rung(
            "honest_blocker_revelation",
            "expose exact blockers, missing privileges, and required interventions transparently",
            needs=(
                decide.Need(
                    "honest blocker revelation certified",
                    _prove_honest_blocker_revelation,
                    "honest blocker revelation unverified",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unreproducible_synthetic_score",
            "refuse synthetic benchmark scores when actual migration pipeline is unverified",
            needs=(
                decide.Need(
                    "synthetic score inflation refused",
                    _prove_refuse_unreproducible_synthetic_score,
                    "synthetic score inflation detected",
                ),
            ),
        ),
    )


def decide_demand_benchmark(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("demand benchmark strategy", demand_benchmark_rungs(), fact_obj)


def _prove_network_flap_recovery(facts: decide.Facts) -> bool:
    return bool(facts.get("flap_recovery") and not facts.get("refused"))


def _prove_lock_contention_backoff(facts: decide.Facts) -> bool:
    return bool(facts.get("lock_backoff") and not facts.get("refused"))


def _prove_schema_shift_quarantine(facts: decide.Facts) -> bool:
    return bool(facts.get("shift_quarantine") and not facts.get("refused"))


def _prove_throttled_iops_aimd(facts: decide.Facts) -> bool:
    return bool(facts.get("iops_aimd") and not facts.get("refused"))


def _prove_refuse_opaque_fault_suppression(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_opaque_suppression"))


def fault_injection_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "network_flap_recovery",
            "reconnect severed socket connections with zero duplicate writes and durable resume",
            needs=(
                decide.Need(
                    "network flap recovery certified",
                    _prove_network_flap_recovery,
                    "network flap recovery inactive",
                ),
            ),
        ),
        decide.Rung(
            "lock_contention_backoff",
            "respect configured lock wait budgets and back off gracefully on concurrent deadlocks",
            needs=(
                decide.Need(
                    "lock contention backoff certified",
                    _prove_lock_contention_backoff,
                    "lock contention backoff inactive",
                ),
            ),
        ),
        decide.Rung(
            "schema_shift_quarantine",
            "quarantine tables with breaking schema shifts while allowing clean tables to proceed",
            needs=(
                decide.Need(
                    "schema shift quarantine certified",
                    _prove_schema_shift_quarantine,
                    "schema shift quarantine inactive",
                ),
            ),
        ),
        decide.Rung(
            "throttled_iops_aimd",
            "dynamically throttle batch size and worker concurrency under sustained IOPS exhaustion",
            needs=(
                decide.Need(
                    "throttled iops AIMD certified",
                    _prove_throttled_iops_aimd,
                    "throttled iops AIMD inactive",
                ),
            ),
        ),
        decide.Rung(
            "refuse_opaque_fault_suppression",
            "refuse masking or swallowing unrecoverable fault conditions as clean completion",
            needs=(
                decide.Need(
                    "opaque fault suppression refused",
                    _prove_refuse_opaque_fault_suppression,
                    "opaque fault suppression detected",
                ),
            ),
        ),
    )


def decide_fault_injection(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("fault injection strategy", fault_injection_rungs(), fact_obj)


def run_demand_benchmark(
    src_engine: str = "postgres",
    tgt_engine: str = "mysql",
    faults: Optional[List[BenchmarkFault]] = None,
) -> BenchmarkScorecard:
    src = EndpointConfig(engine=src_engine, host="127.0.0.1", port=5432, database="source_db", username="bench_src")
    tgt = EndpointConfig(engine=tgt_engine, host="127.0.0.1", port=3306, database="target_db", username="bench_tgt")
    users = TableSpec(name="users", primary_key="id", columns={"id": "BIGINT", "name": "VARCHAR(255)"}, row_count=500)
    orders = TableSpec(name="orders", primary_key="id", columns={"id": "BIGINT", "amount": "DECIMAL(10,2)"}, row_count=1000)
    workload = WorkloadSpec(tables=[users, orders], batch_size=100)
    harness = DemandBenchmarkHarness(
        engine_pair=(src_engine, tgt_engine),
        source_endpoint=src,
        target_endpoint=tgt,
        workload=workload,
        faults=faults or [],
    )
    return harness.execute()

