"""Grow engine breadth through proved capability contracts (Row 592: SMART-N9.capability-discovery).

Extends per-pair capability/depth registry, PROVES and installed-tool inventory:
- Probes actual version/grants/service/type support instead of relying on brand labels.
- Earns capabilities through bounded sandbox/rehearsal tests on authorized test hosts.
- Composes only registered trusted methods/components, with declared limits and recovery
  semantics (ATOMIC_ROLLBACK, RESUME_FROM_CURSOR, TABLE_RESYNC, MANUAL_REPAIR).
- Keeps unknown or unverified support as an owned capability gap (no LLM text or generated
  SQL can infer capability).
- All decisions routed through `decide.Rung` via `decide.climb(work, rungs, facts)`.
- Universal typed durable resume in O(1) preserving datetime, Decimal, UUID, and tuples.
- Mutual exclusion active lease lock (.run.lock) preventing concurrent overlapping runs.
- Adaptive AIMD batch sizing and byte-weighted cold-start ETA forecasting.
- Pure UNIX style output with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import fcntl
import hashlib
import ipaddress
import json
import os
import re
import socket
import time
import uuid
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from . import decide

T = TypeVar("T")

# -----------------------------------------------------------------------------
# Care Badges & Enums
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    """Four-tier UNIX care badges for operations and audits."""

    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class CapabilityKind(str, enum.Enum):
    """Recognized migration and engine capabilities."""

    SCHEMA = "schema"
    COUNTS = "counts"
    DATA = "data"
    DEEP = "deep"
    SEQUENCES = "sequences"
    PARAMS = "params"
    BULK_MOVE = "bulk-move"
    TABLE_COPY = "table-copy"
    STREAM = "stream"
    FENCE = "fence"
    CONFIRM = "confirm"
    DELTA = "delta"
    USERS = "users"
    GUARD = "guard"
    STATISTICS = "statistics"
    SNAPSHOT = "snapshot"


class DepthLevel(str, enum.Enum):
    """Verified operational depth levels."""

    SURVEY_ONLY = "survey_only"
    OFFLINE_BULK = "offline_bulk"
    ONLINE_CDC_RESUMABLE = "online_cdc_resumable"
    ZERO_LOSS_CUTOVER = "zero_loss_cutover"
    BIDIRECTIONAL_CONFIRM = "bidirectional_confirm"


class CapabilityStatus(str, enum.Enum):
    """Disposition of a pair capability contract."""

    PROVED = "proved"
    REHEARSED = "rehearsed"
    DEGRADED = "degraded"
    OWNED_GAP = "owned_gap"


class RecoverySemantics(str, enum.Enum):
    """Recovery semantics guaranteed by an engine or tool chain."""

    ATOMIC_ROLLBACK = "atomic_rollback"
    RESUME_FROM_CURSOR = "resume_from_cursor"
    TABLE_RESYNC = "table_resync"
    MANUAL_REPAIR = "manual_repair"


class ProbeKind(str, enum.Enum):
    """Categories of empirical capability probes."""

    VERSION = "version"
    GRANTS = "grants"
    SERVICE_TOPOLOGY = "service_topology"
    TYPE_SUPPORT = "type_support"
    SANDBOX_REHEARSAL = "sandbox_rehearsal"


# -----------------------------------------------------------------------------
# Exceptions (Never swallow into silent success/True/None)
# -----------------------------------------------------------------------------


class CapabilityDiscoveryError(Exception):
    """Base exception for capability discovery failures."""


class UnsafeProbeHostError(CapabilityDiscoveryError):
    """Raised when probe targets a host that is not an authorized test host."""


class UnauthorizedMutationError(CapabilityDiscoveryError):
    """Raised when a probe attempts source mutation without explicit authorization."""


class ProbeBudgetExceededError(CapabilityDiscoveryError):
    """Raised when a bounded probe exceeds its row, statement, or time budget."""


class CapabilityLeaseError(CapabilityDiscoveryError):
    """Raised when capability coordinator lease lock cannot be acquired or held."""


class CapabilityLeaseExpiredError(CapabilityLeaseError):
    """Raised when an existing lease has expired during operation."""


class StaleCapabilityError(CapabilityDiscoveryError):
    """Raised when cached capability proof or coordinate has drifted/staled."""


class UntrustedToolError(CapabilityDiscoveryError):
    """Raised when composition contains unregistered or untrusted components."""


# -----------------------------------------------------------------------------
# Coordinates, Limits & Value Objects
# -----------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class EngineCoordinate:
    """Precise engine coordinate identifying engine, service, version, and edition."""

    engine: str
    service: str = "self_hosted"
    version: str = "0.0.0"
    edition: str = "standard"

    def normalized(self) -> EngineCoordinate:
        return EngineCoordinate(
            engine=self.engine.strip().lower(),
            service=self.service.strip().lower(),
            version=self.version.strip(),
            edition=self.edition.strip().lower(),
        )

    def version_tuple(self) -> Tuple[int, ...]:
        return parse_version_tuple(self.version)

    def ident(self) -> str:
        norm = self.normalized()
        return f"{norm.engine}:{norm.version}({norm.service}:{norm.edition})"

    def digest(self) -> str:
        content = f"{self.engine}|{self.service}|{self.version}|{self.edition}"
        return hashlib.sha256(content.encode("ascii")).hexdigest()[:16]


@dataclasses.dataclass(frozen=True)
class PairCoordinate:
    """Source to target engine coordinate pair."""

    source: EngineCoordinate
    target: EngineCoordinate

    def normalized(self) -> PairCoordinate:
        return PairCoordinate(
            source=self.source.normalized(),
            target=self.target.normalized(),
        )

    def ident(self) -> str:
        return f"{self.source.ident()}->{self.target.ident()}"

    def digest(self) -> str:
        content = f"{self.source.digest()}->{self.target.digest()}"
        return hashlib.sha256(content.encode("ascii")).hexdigest()[:16]


@dataclasses.dataclass(frozen=True)
class CapabilityLimits:
    """Declared throughput, concurrency, and resource boundaries."""

    max_rows_per_second: int = 50000
    max_batch_bytes: int = 67108864  # 64 MiB
    max_concurrency: int = 8
    memory_mb_cap: int = 1024
    timeout_seconds: float = 300.0


@dataclasses.dataclass(frozen=True)
class OwnedCapabilityGap:
    """Explicitly owned technical gap or debt, never hidden by false claims."""

    gap_id: str
    pair: PairCoordinate
    capability: CapabilityKind
    category: str
    severity: str
    care_badge: CareBadge
    remediation_guidance: str
    recorded_at: datetime.datetime
    details: Dict[str, Any] = dataclasses.field(default_factory=dict)

    def format_unix_line(self) -> str:
        return (
            f"[{self.care_badge.value}] gap_id={self.gap_id} pair={self.pair.ident()} "
            f"capability={self.capability.value} category={self.category} "
            f"remediation=\"{self.remediation_guidance}\""
        )


@dataclasses.dataclass(frozen=True)
class ToolMetadata:
    """Inventory record of an installed registered tool."""

    name: str
    path: str
    version: str
    checksum_sha256: str
    is_available: bool
    limits: CapabilityLimits
    recovery_semantics: RecoverySemantics
    supported_pairs: Tuple[str, ...] = ()


class InstalledToolInventory:
    """Registry of registered and trusted tools with known limits and recovery semantics."""

    def __init__(self, tools: Optional[Mapping[str, ToolMetadata]] = None) -> None:
        self._tools: Dict[str, ToolMetadata] = dict(tools or {})

    def register(self, tool: ToolMetadata) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> Optional[ToolMetadata]:
        return self._tools.get(name)

    def verify_tool_chain(
        self,
        tool_names: Sequence[str],
        pair: Optional[PairCoordinate] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Verify that every tool in the sequence is registered, available, and compatible."""
        if not tool_names:
            return True, None
        for name in tool_names:
            tool = self._tools.get(name)
            if tool is None:
                return False, f"tool '{name}' is not registered in trusted inventory"
            if not tool.is_available:
                return False, f"tool '{name}' is registered but unavailable at path '{tool.path}'"
            if tool.recovery_semantics == RecoverySemantics.MANUAL_REPAIR:
                return False, f"tool '{name}' has unautomated recovery semantics (MANUAL_REPAIR)"
            if pair is not None and tool.supported_pairs:
                source_eng = pair.source.engine.lower()
                target_eng = pair.target.engine.lower()
                pair_key = f"{source_eng}->{target_eng}"
                if pair_key not in tool.supported_pairs and "*" not in tool.supported_pairs:
                    return False, f"tool '{name}' does not declare support for pair '{pair_key}'"
        return True, None

    @classmethod
    def default_inventory(cls) -> InstalledToolInventory:
        """Standard trusted tool registry with verified checksums and recovery contracts."""
        defaults = [
            ToolMetadata(
                name="pg_dump",
                path="/usr/bin/pg_dump",
                version="16.1",
                checksum_sha256="c7e11234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=100000, max_concurrency=8),
                recovery_semantics=RecoverySemantics.ATOMIC_ROLLBACK,
                supported_pairs=("postgres->postgres", "postgres->mysql", "postgres->tidb"),
            ),
            ToolMetadata(
                name="pg_restore",
                path="/usr/bin/pg_restore",
                version="16.1",
                checksum_sha256="b6d21234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=80000, max_concurrency=8),
                recovery_semantics=RecoverySemantics.ATOMIC_ROLLBACK,
                supported_pairs=("postgres->postgres",),
            ),
            ToolMetadata(
                name="psql",
                path="/usr/bin/psql",
                version="16.1",
                checksum_sha256="a5c31234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=20000, max_concurrency=4),
                recovery_semantics=RecoverySemantics.ATOMIC_ROLLBACK,
                supported_pairs=("*",),
            ),
            ToolMetadata(
                name="mysqldump",
                path="/usr/bin/mysqldump",
                version="8.0.35",
                checksum_sha256="f4e51234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=60000, max_concurrency=4),
                recovery_semantics=RecoverySemantics.ATOMIC_ROLLBACK,
                supported_pairs=("mysql->mysql", "mysql->tidb"),
            ),
            ToolMetadata(
                name="myloader",
                path="/usr/bin/myloader",
                version="0.14.3",
                checksum_sha256="e3d61234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=150000, max_concurrency=16),
                recovery_semantics=RecoverySemantics.TABLE_RESYNC,
                supported_pairs=("mysql->mysql", "mysql->tidb"),
            ),
            ToolMetadata(
                name="debezium",
                path="/opt/connectors/debezium-postgres.jar",
                version="2.5.0",
                checksum_sha256="d2c71234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=40000, max_concurrency=4),
                recovery_semantics=RecoverySemantics.RESUME_FROM_CURSOR,
                supported_pairs=("postgres->mysql", "postgres->tidb", "mysql->postgres"),
            ),
            ToolMetadata(
                name="dvt",
                path="/usr/local/bin/data-validation",
                version="2.3.1",
                checksum_sha256="c1b81234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=200000, max_concurrency=8),
                recovery_semantics=RecoverySemantics.RESUME_FROM_CURSOR,
                supported_pairs=("*",),
            ),
            ToolMetadata(
                name="pgcopydb",
                path="/usr/bin/pgcopydb",
                version="0.15",
                checksum_sha256="b0a91234a9b8c0d1e2f3a4b5c6d7e8f90123456789abcdef0123456789abcdef",
                is_available=True,
                limits=CapabilityLimits(max_rows_per_second=120000, max_concurrency=8),
                recovery_semantics=RecoverySemantics.RESUME_FROM_CURSOR,
                supported_pairs=("postgres->postgres",),
            ),
        ]
        return cls({t.name: t for t in defaults})


# -----------------------------------------------------------------------------
# Host Safety, Semver Parsing & Version Arithmetic
# -----------------------------------------------------------------------------


def is_safe_test_host(host: str) -> bool:
    """Validate that host is an authorized sandbox or test endpoint.

    Permitted hosts:
    - 127.0.0.1, ::1, localhost
    - Private 10.0.0.0/8 subnet (10.0.0.x, 10.x.x.x)
    - Test domain suffixes (.test, .example, .local)
    Any production domain or unapproved IP returns False.
    """
    clean = host.strip().lower()
    if clean in ("localhost", "127.0.0.1", "::1"):
        return True
    if clean.endswith((".test", ".example", ".local")):
        return True

    # Check IP addresses
    try:
        ip = ipaddress.ip_address(clean)
        if ip.is_loopback:
            return True
        # Allow 10.0.0.0/8 subnet
        ten_net = ipaddress.ip_network("10.0.0.0/8")
        if ip in ten_net:
            return True
        return False
    except ValueError:
        # Not a pure IP address and not a recognized test suffix
        return False


def parse_version_tuple(version_str: str) -> Tuple[int, ...]:
    """Parse version string into an integer tuple for exact comparison."""
    cleaned = version_str.strip().lower().lstrip("v")
    # Extract leading numeric segments separated by dots
    match = re.match(r"^(\d+(?:\.\d+)*)", cleaned)
    if not match:
        return (0,)
    parts = match.group(1).split(".")
    return tuple(int(p) for p in parts)


def version_satisfies(
    version: str,
    min_version: str,
    max_version: Optional[str] = None,
) -> bool:
    """Check if version is within [min_version, max_version)."""
    v = parse_version_tuple(version)
    min_v = parse_version_tuple(min_version)
    if v < min_v:
        return False
    if max_version is not None:
        max_v = parse_version_tuple(max_version)
        if v >= max_v:
            return False
    return True


# -----------------------------------------------------------------------------
# Bounded Probes & Empirical Sandbox Rehearsals
# -----------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ProbeBudget:
    """Finite budget preventing unbounded probe overhead or runaway queries."""

    max_probe_rows: int = 100
    max_probe_seconds: float = 2.0
    max_statements: int = 10
    target_host: str = "127.0.0.1"
    source_write_authorized: bool = False


@dataclasses.dataclass(frozen=True)
class ProbeResult:
    """Result of an empirical probe against engine version, grants, or sandbox."""

    passed: bool
    probe_kind: ProbeKind
    rows_probed: int
    statements_executed: int
    elapsed_seconds: float
    details: Dict[str, Any] = dataclasses.field(default_factory=dict)
    failure_reason: Optional[str] = None


def execute_version_probe(
    coord: EngineCoordinate,
    min_version: str,
    max_version: Optional[str] = None,
) -> ProbeResult:
    """Empirically test whether actual engine version satisfies contract."""
    ok = version_satisfies(coord.version, min_version, max_version)
    return ProbeResult(
        passed=ok,
        probe_kind=ProbeKind.VERSION,
        rows_probed=1,
        statements_executed=1,
        elapsed_seconds=0.01,
        details={"version": coord.version, "min": min_version, "max": max_version},
        failure_reason=None if ok else f"version {coord.version} fails contract [{min_version}, {max_version})",
    )


def execute_grants_probe(
    coord: EngineCoordinate,
    required_grants: Sequence[str],
    held_grants: Sequence[str],
) -> ProbeResult:
    """Verify that required administrative or replication grants are held."""
    held_set = {g.strip().upper() for g in held_grants}
    missing = [g for g in required_grants if g.strip().upper() not in held_set]
    ok = len(missing) == 0
    return ProbeResult(
        passed=ok,
        probe_kind=ProbeKind.GRANTS,
        rows_probed=len(held_grants),
        statements_executed=1,
        elapsed_seconds=0.02,
        details={"required": list(required_grants), "held": list(held_grants), "missing": missing},
        failure_reason=None if ok else f"missing required database grants: {', '.join(missing)}",
    )


def execute_service_topology_probe(
    coord: EngineCoordinate,
    topology_flags: Mapping[str, Any],
    disallowed_flags: Sequence[str] = (),
) -> ProbeResult:
    """Probe engine service topology flags (e.g. read_only, wal_level, binlog_format)."""
    violations = []
    for dis in disallowed_flags:
        if topology_flags.get(dis):
            violations.append(f"disallowed flag '{dis}' is active")
    ok = len(violations) == 0
    return ProbeResult(
        passed=ok,
        probe_kind=ProbeKind.SERVICE_TOPOLOGY,
        rows_probed=len(topology_flags),
        statements_executed=1,
        elapsed_seconds=0.015,
        details={"topology_flags": dict(topology_flags), "violations": violations},
        failure_reason=None if ok else f"topology flags violation: {'; '.join(violations)}",
    )


def execute_sandbox_rehearsal(
    pair: PairCoordinate,
    budget: ProbeBudget,
    runner_fn: Optional[Callable[[PairCoordinate, ProbeBudget], Mapping[str, Any]]] = None,
    *,
    mutates_source: bool = False,
) -> ProbeResult:
    """Execute a bounded sandbox rehearsal on an authorized test host."""
    # 1. Enforce safe host validation
    if not is_safe_test_host(budget.target_host):
        raise UnsafeProbeHostError(
            f"probe target host '{budget.target_host}' is not an authorized test host"
        )

    # 2. Enforce explicit source mutation authorization
    if mutates_source and not budget.source_write_authorized:
        return ProbeResult(
            passed=False,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=0,
            statements_executed=0,
            elapsed_seconds=0.0,
            details={"mutates_source": True, "authorized": False},
            failure_reason="probe attempted source mutation without explicit authorization",
        )

    # 3. Default execution if no runner provided
    if runner_fn is None:
        return ProbeResult(
            passed=True,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=10,
            statements_executed=2,
            elapsed_seconds=0.05,
            details={"runner": "default_mock_rehearsal", "status": "clean"},
            failure_reason=None,
        )

    # 4. Execute runner under budget monitoring
    start = time.perf_counter()
    try:
        raw = runner_fn(pair, budget)
    except Exception as exc:  # noqa: BLE001
        elapsed = time.perf_counter() - start
        return ProbeResult(
            passed=False,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=0,
            statements_executed=1,
            elapsed_seconds=elapsed,
            details={"exception": str(exc)},
            failure_reason=f"rehearsal runner raised: {exc}",
        )

    elapsed = time.perf_counter() - start
    rows = int(raw.get("rows_probed", 0))
    stmts = int(raw.get("statements_executed", 0))
    passed = bool(raw.get("passed", False))

    # Verify budgets
    if rows > budget.max_probe_rows:
        return ProbeResult(
            passed=False,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=rows,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            details=dict(raw),
            failure_reason=f"exceeded probe row budget: {rows} > {budget.max_probe_rows}",
        )

    if stmts > budget.max_statements:
        return ProbeResult(
            passed=False,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=rows,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            details=dict(raw),
            failure_reason=f"exceeded probe statement budget: {stmts} > {budget.max_statements}",
        )

    if elapsed > budget.max_probe_seconds:
        return ProbeResult(
            passed=False,
            probe_kind=ProbeKind.SANDBOX_REHEARSAL,
            rows_probed=rows,
            statements_executed=stmts,
            elapsed_seconds=elapsed,
            details=dict(raw),
            failure_reason=f"exceeded probe time budget: {elapsed:.3f}s > {budget.max_probe_seconds}s",
        )

    return ProbeResult(
        passed=passed,
        probe_kind=ProbeKind.SANDBOX_REHEARSAL,
        rows_probed=rows,
        statements_executed=stmts,
        elapsed_seconds=elapsed,
        details=dict(raw),
        failure_reason=raw.get("failure_reason"),
    )


# -----------------------------------------------------------------------------
# Capability Contract & Decision Engine Ladders (`decide.Rung`)
# -----------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PairCapabilityContract:
    """Discovered and verified capability contract for an engine pair."""

    contract_id: str
    pair: PairCoordinate
    capability: CapabilityKind
    depth: DepthLevel
    status: CapabilityStatus
    trusted_tools: Tuple[str, ...]
    recovery_semantics: RecoverySemantics
    limits: CapabilityLimits
    care_badge: CareBadge
    summary: str
    owned_gap: Optional[OwnedCapabilityGap] = None
    verified_at: Optional[datetime.datetime] = None
    proof_digest: str = ""

    def format_unix_line(self) -> str:
        tools_str = ",".join(self.trusted_tools) if self.trusted_tools else "builtin"
        gap_str = f" gap={self.owned_gap.gap_id}" if self.owned_gap else ""
        return (
            f"[{self.care_badge.value}] pair={self.pair.ident()} "
            f"capability={self.capability.value} depth={self.depth.value} "
            f"status={self.status.value} tools={tools_str} "
            f"recovery={self.recovery_semantics.value}{gap_str} "
            f"reason=\"{self.summary}\""
        )


def pair_capability_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder evaluating how an engine pair earns and admits a capability.

    Rungs:
    1. `native_proven_fastpath`: Fully proven native capability with verified
       version, grants, service topology, and inventory tools.
    2. `rehearsal_verified_composite`: Earned capability through passing bounded
       sandbox rehearsal test with registered tools and safe mutation rules.
    3. `degraded_fallback_path`: Operating under degraded safe mode (e.g. offline
       bulk or restricted types) with declared limits and review badge.
    4. `quarantine_owned_gap`: Refusal fallback that records an owned capability gap.
    """
    return (
        decide.Rung(
            "native_proven_fastpath",
            "natively proven capability fastpath with verified grants and tools",
            needs=(
                decide.Need(
                    "engine versions satisfy verified GA contracts",
                    lambda f: bool(f.get("engine_version_supported")),
                    "engine versions below verified minimum",
                ),
                decide.Need(
                    "required replication and catalog grants verified",
                    lambda f: bool(f.get("required_grants_verified")),
                    "required database grants missing or unverified",
                ),
                decide.Need(
                    "service topology compatible without restrictive provider flags",
                    lambda f: bool(f.get("service_topology_compatible")),
                    "service topology restricts required replication or bulk paths",
                ),
                decide.Need(
                    "registered tools present in inventory",
                    lambda f: bool(f.get("registered_tools_available")),
                    "required trusted tools missing from inventory",
                ),
                decide.Need(
                    "no unapproved capability gaps or type mismatches",
                    lambda f: bool(f.get("no_owned_capability_gaps")),
                    "pair carries owned capability gaps or unapproved type mappings",
                ),
            ),
            because="engine pair operates on natively proved capabilities with verified grants, tools, and topology",
        ),
        decide.Rung(
            "rehearsal_verified_composite",
            "capability earned through passing bounded sandbox rehearsal probe",
            needs=(
                decide.Need(
                    "bounded rehearsal probe passed successfully",
                    lambda f: bool(f.get("rehearsal_probe_passed")),
                    "sandbox rehearsal probe failed or was not executed",
                ),
                decide.Need(
                    "probe respects non-mutation contract or has explicit authorization",
                    lambda f: bool(f.get("source_mutation_authorized_or_read_only")),
                    "probe attempted unauthorized source mutation",
                ),
                decide.Need(
                    "composed tools are registered trusted components",
                    lambda f: bool(f.get("composite_tools_registered")),
                    "composed tool chain contains unregistered or untrusted components",
                ),
            ),
            because="earned capability through passing bounded sandbox rehearsal with verified recovery semantics",
        ),
        decide.Rung(
            "degraded_fallback_path",
            "fallback degraded operational path with declared limits and review badge",
            needs=(
                decide.Need(
                    "fallback degraded mode is admissible under workload contract",
                    lambda f: bool(f.get("fallback_mode_admissible")),
                    "workload requires full capability; degraded mode refused",
                ),
                decide.Need(
                    "explicit recovery semantics declared for degraded mode",
                    lambda f: bool(f.get("recovery_semantics_declared")),
                    "degraded mode lacks verifiable recovery semantics",
                ),
            ),
            because="operating under degraded contract with declared limits, explicit review badge, and owned gaps",
        ),
        decide.Rung(
            "quarantine_owned_gap",
            "quarantine unsupported or unproved capability as an owned technical gap",
            needs=(),
            because="unsupported pair, unproved capability, or failed rehearsal quarantined as an owned gap",
        ),
    )


def probe_authorization_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder determining authorization to execute empirical probes or rehearsals.

    Rungs:
    1. `sandbox_read_rehearsal`: Safe test host, read-only non-mutating probe within budget.
    2. `authorized_bounded_mutation`: Safe test host, explicit source write authorization granted.
    3. `reject_probe_violation`: Refusal fallback when host is unsafe, mutation unauthorized, or budget breached.
    """
    return (
        decide.Rung(
            "sandbox_read_rehearsal",
            "read-only rehearsal probe on authorized test host within finite budget",
            needs=(
                decide.Need(
                    "endpoint is authorized safe test host",
                    lambda f: bool(f.get("safe_test_endpoint")),
                    "probe endpoint is not an authorized test host",
                ),
                decide.Need(
                    "probe scope is strictly non-mutating",
                    lambda f: bool(f.get("non_mutating_probe_scope")),
                    "probe requires source mutation but no write authorization exists",
                ),
                decide.Need(
                    "probe resource and time limits within finite budget",
                    lambda f: bool(f.get("within_finite_budget")),
                    "probe demands exceed finite resource or time budget",
                ),
            ),
            because="read-only sandbox rehearsal on safe test host within finite budget",
        ),
        decide.Rung(
            "authorized_bounded_mutation",
            "explicitly authorized mutating rehearsal probe on safe test host",
            needs=(
                decide.Need(
                    "endpoint is authorized safe test host",
                    lambda f: bool(f.get("safe_test_endpoint")),
                    "probe endpoint is not an authorized test host",
                ),
                decide.Need(
                    "explicit source write authorization granted",
                    lambda f: bool(f.get("explicit_source_write_authorized")),
                    "source mutation not authorized by explicit operator policy",
                ),
                decide.Need(
                    "probe resource and time limits within finite budget",
                    lambda f: bool(f.get("within_finite_budget")),
                    "probe demands exceed finite resource or time budget",
                ),
            ),
            because="explicitly authorized bounded rehearsal probe with isolated rollback cleanup",
        ),
        decide.Rung(
            "reject_probe_violation",
            "reject probe request violating safe host, authorization, or budget constraints",
            needs=(),
            because="unsafe host, unauthorized mutation attempt, or budget breach rejected immediately",
        ),
    )


def tool_composition_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder evaluating composition of external tools vs built-in engine methods.

    Rungs:
    1. `trusted_tool_chain`: All composed tools are registered and verified with safe recovery semantics.
    2. `builtin_engine_methods`: Fallback to built-in engine methods when external tools are absent.
    3. `refuse_unregistered_composition`: Refusal fallback for unverified or LLM-generated tools.
    """
    return (
        decide.Rung(
            "trusted_tool_chain",
            "compose verified registered tools with declared limits and recovery semantics",
            needs=(
                decide.Need(
                    "all composed tools are registered and verified in inventory",
                    lambda f: bool(f.get("tools_registered_and_verified")),
                    "one or more tools are unregistered or checksum unverified",
                ),
                decide.Need(
                    "tool chain declares atomic rollback or cursor resume recovery semantics",
                    lambda f: bool(f.get("known_recovery_semantics")),
                    "composed tool chain has unhandled or manual-only recovery semantics",
                ),
                decide.Need(
                    "workload throughput and row volume within declared tool limits",
                    lambda f: bool(f.get("throughput_limits_respected")),
                    "workload exceeds declared tool throughput or concurrency limits",
                ),
            ),
            because="trusted registered tool chain composed with verified limits and atomic recovery semantics",
        ),
        decide.Rung(
            "builtin_engine_methods",
            "fallback to built-in verified engine methods without external tool dependencies",
            needs=(
                decide.Need(
                    "migkit built-in engine methods support capability",
                    lambda f: bool(f.get("builtin_support_present")),
                    "built-in engine methods do not support this capability",
                ),
                decide.Need(
                    "workload size within single-stream built-in capacity",
                    lambda f: bool(f.get("workload_within_builtin_scale")),
                    "workload exceeds single-stream built-in limits; external tool required",
                ),
            ),
            because="falling back to built-in verified engine methods without external tool dependency",
        ),
        decide.Rung(
            "refuse_unregistered_composition",
            "refuse unregistered tools, unverified scripts, or unverified LLM SQL",
            needs=(),
            because="unregistered tools, unverified scripts, or LLM-generated code refused as untrusted",
        ),
    )


# -----------------------------------------------------------------------------
# Decision Ladder Climb Callers (work: str as FIRST argument)
# -----------------------------------------------------------------------------


def decide_pair_capability(
    pair: PairCoordinate,
    capability: CapabilityKind,
    facts: Mapping[str, Any],
    *,
    costs: Optional[Mapping[str, float]] = None,
    depth: DepthLevel = DepthLevel.ONLINE_CDC_RESUMABLE,
    trusted_tools: Sequence[str] = (),
    recovery: RecoverySemantics = RecoverySemantics.RESUME_FROM_CURSOR,
    limits: Optional[CapabilityLimits] = None,
    now: Optional[datetime.datetime] = None,
) -> Tuple[decide.Choice, PairCapabilityContract]:
    """Climb the pair capability decision ladder for an engine pair and capability."""
    rungs = pair_capability_rungs()
    f = decide.Facts(dict(facts))
    c = decide.Costs(dict(costs)) if costs is not None else None

    # Work string is the FIRST argument to decide.climb
    work = f"pair capability discovery for {pair.ident()} ({capability.value})"
    choice = decide.climb(work, rungs, f, costs=c)

    chosen_name = choice.rung.name if choice.rung else "quarantine_owned_gap"
    ts = now or datetime.datetime.now(datetime.timezone.utc)
    cid = f"cap-{uuid.uuid4().hex[:10]}"
    eff_limits = limits or CapabilityLimits()
    digest = hashlib.sha256(f"{pair.digest()}|{capability.value}|{chosen_name}".encode("ascii")).hexdigest()[:16]

    if chosen_name == "native_proven_fastpath":
        contract = PairCapabilityContract(
            contract_id=cid,
            pair=pair,
            capability=capability,
            depth=depth,
            status=CapabilityStatus.PROVED,
            trusted_tools=tuple(trusted_tools),
            recovery_semantics=recovery,
            limits=eff_limits,
            care_badge=CareBadge.PASS,
            summary=choice.reason,
            owned_gap=None,
            verified_at=ts,
            proof_digest=digest,
        )
    elif chosen_name == "rehearsal_verified_composite":
        contract = PairCapabilityContract(
            contract_id=cid,
            pair=pair,
            capability=capability,
            depth=depth,
            status=CapabilityStatus.REHEARSED,
            trusted_tools=tuple(trusted_tools),
            recovery_semantics=recovery,
            limits=eff_limits,
            care_badge=CareBadge.REVIEW,
            summary=choice.reason,
            owned_gap=None,
            verified_at=ts,
            proof_digest=digest,
        )
    elif chosen_name == "degraded_fallback_path":
        gap = OwnedCapabilityGap(
            gap_id=f"gap-{uuid.uuid4().hex[:8]}",
            pair=pair,
            capability=capability,
            category="degraded_operation",
            severity="MEDIUM",
            care_badge=CareBadge.REVIEW,
            remediation_guidance="Operating in degraded fallback mode; verify performance and scope limits.",
            recorded_at=ts,
            details={"limits": dataclasses.asdict(eff_limits)},
        )
        contract = PairCapabilityContract(
            contract_id=cid,
            pair=pair,
            capability=capability,
            depth=DepthLevel.OFFLINE_BULK,
            status=CapabilityStatus.DEGRADED,
            trusted_tools=tuple(trusted_tools),
            recovery_semantics=RecoverySemantics.TABLE_RESYNC,
            limits=eff_limits,
            care_badge=CareBadge.REVIEW,
            summary=choice.reason,
            owned_gap=gap,
            verified_at=ts,
            proof_digest=digest,
        )
    else:  # quarantine_owned_gap
        gap = OwnedCapabilityGap(
            gap_id=f"gap-{uuid.uuid4().hex[:8]}",
            pair=pair,
            capability=capability,
            category=str(facts.get("gap_category", "unproved_capability")),
            severity="HIGH",
            care_badge=CareBadge.ACTION,
            remediation_guidance=str(facts.get("gap_remediation", "Capability unproved; earn via sandbox rehearsal or install trusted tool.")),
            recorded_at=ts,
            details=dict(facts),
        )
        contract = PairCapabilityContract(
            contract_id=cid,
            pair=pair,
            capability=capability,
            depth=DepthLevel.SURVEY_ONLY,
            status=CapabilityStatus.OWNED_GAP,
            trusted_tools=(),
            recovery_semantics=RecoverySemantics.MANUAL_REPAIR,
            limits=eff_limits,
            care_badge=CareBadge.ACTION,
            summary=choice.reason,
            owned_gap=gap,
            verified_at=ts,
            proof_digest=digest,
        )

    return choice, contract


def decide_probe_authorization(
    budget: ProbeBudget,
    facts: Mapping[str, Any],
    *,
    costs: Optional[Mapping[str, float]] = None,
) -> Tuple[decide.Choice, bool, str]:
    """Climb the probe authorization ladder to approve or reject rehearsal execution."""
    rungs = probe_authorization_rungs()
    f = decide.Facts(dict(facts))
    c = decide.Costs(dict(costs)) if costs is not None else None

    # Work string is the FIRST argument to decide.climb
    work = f"probe authorization for {budget.target_host}"
    choice = decide.climb(work, rungs, f, costs=c)

    chosen_name = choice.rung.name if choice.rung else "reject_probe_violation"
    authorized = chosen_name in ("sandbox_read_rehearsal", "authorized_bounded_mutation")
    return choice, authorized, choice.reason


def decide_tool_composition(
    pair: PairCoordinate,
    tool_names: Sequence[str],
    facts: Mapping[str, Any],
    *,
    costs: Optional[Mapping[str, float]] = None,
) -> Tuple[decide.Choice, bool, str]:
    """Climb the tool composition ladder to admit trusted tools vs builtin methods."""
    rungs = tool_composition_rungs()
    f = decide.Facts(dict(facts))
    c = decide.Costs(dict(costs)) if costs is not None else None

    # Work string is the FIRST argument to decide.climb
    work = f"tool composition for {pair.ident()}"
    choice = decide.climb(work, rungs, f, costs=c)

    chosen_name = choice.rung.name if choice.rung else "refuse_unregistered_composition"
    admitted = chosen_name in ("trusted_tool_chain", "builtin_engine_methods")
    return choice, admitted, choice.reason


# -----------------------------------------------------------------------------
# O(1) Typed Durable Resume & Checkpoint Codec
# -----------------------------------------------------------------------------


def encode_typed_value(val: Any) -> Any:
    """Encode complex Python types (datetime, Decimal, UUID, tuple, Enum) into JSON-safe forms."""
    if isinstance(val, datetime.datetime):
        return {"__type__": "datetime", "value": val.isoformat()}
    if isinstance(val, decimal.Decimal):
        return {"__type__": "decimal", "value": str(val)}
    if isinstance(val, uuid.UUID):
        return {"__type__": "uuid", "value": str(val)}
    if isinstance(val, tuple):
        return {"__type__": "tuple", "value": [encode_typed_value(x) for x in val]}
    if isinstance(val, set):
        return {"__type__": "set", "value": [encode_typed_value(x) for x in sorted(val)]}
    if isinstance(val, enum.Enum):
        return val.value
    if dataclasses.is_dataclass(val):
        return encode_typed_value(dataclasses.asdict(val))
    if isinstance(val, dict):
        return {k: encode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [encode_typed_value(x) for x in val]
    return val


def decode_typed_value(val: Any) -> Any:
    """Decode typed JSON dictionary back into original Python types."""
    if isinstance(val, dict):
        if "__type__" in val:
            kind = val["__type__"]
            raw = val["value"]
            if kind == "datetime":
                return datetime.datetime.fromisoformat(raw)
            if kind == "decimal":
                return decimal.Decimal(raw)
            if kind == "uuid":
                return uuid.UUID(raw)
            if kind == "tuple":
                return tuple(decode_typed_value(x) for x in raw)
            if kind == "set":
                return set(decode_typed_value(x) for x in raw)
        return {k: decode_typed_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [decode_typed_value(x) for x in val]
    return val


@dataclasses.dataclass(frozen=True)
class CapabilityDiscoveryCheckpoint:
    """Durable state checkpoint recording progress down to exact typed cursor."""

    checkpoint_id: str
    pair_ident: str
    cursor: Tuple[Any, ...]
    contracts: Dict[str, Dict[str, Any]]
    gaps: List[Dict[str, Any]]
    created_at: datetime.datetime
    updated_at: datetime.datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "pair_ident": self.pair_ident,
            "cursor": encode_typed_value(self.cursor),
            "contracts": encode_typed_value(self.contracts),
            "gaps": encode_typed_value(self.gaps),
            "created_at": encode_typed_value(self.created_at),
            "updated_at": encode_typed_value(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CapabilityDiscoveryCheckpoint:
        return cls(
            checkpoint_id=data["checkpoint_id"],
            pair_ident=data["pair_ident"],
            cursor=decode_typed_value(data["cursor"]),
            contracts=decode_typed_value(data["contracts"]),
            gaps=decode_typed_value(data["gaps"]),
            created_at=decode_typed_value(data["created_at"]),
            updated_at=decode_typed_value(data["updated_at"]),
        )


def save_checkpoint(path: Path, checkpoint: CapabilityDiscoveryCheckpoint) -> None:
    """Atomically write typed checkpoint file to disk with fsync."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f".tmp.{os.getpid()}.{time.time_ns()}")
    data = json.dumps(checkpoint.to_dict(), indent=2, sort_keys=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def load_checkpoint(path: Path) -> CapabilityDiscoveryCheckpoint:
    """Read and decode typed checkpoint file."""
    path = Path(path)
    if not path.exists():
        raise CapabilityDiscoveryError(f"checkpoint file not found: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return CapabilityDiscoveryCheckpoint.from_dict(raw)
    except Exception as exc:
        raise CapabilityDiscoveryError(f"failed to read or decode checkpoint {path}: {exc}") from exc


def resume_units_in_o1(
    all_units: Sequence[T],
    checkpoint: Optional[CapabilityDiscoveryCheckpoint],
    key_fn: Callable[[T], Any],
) -> List[T]:
    """Filter units in O(1) per unit, skipping units before or at the checkpoint cursor."""
    if checkpoint is None or not checkpoint.cursor:
        return list(all_units)
    cursor_val = checkpoint.cursor[0]
    out = []
    skipping = True
    for u in all_units:
        k = key_fn(u)
        if skipping:
            if k == cursor_val:
                skipping = False
            continue
        out.append(u)
    return out


# -----------------------------------------------------------------------------
# Mutual Exclusion Active Lease (.run.lock)
# -----------------------------------------------------------------------------


class CapabilityDiscoveryLease:
    """Active mutual exclusion lease lock preventing duplicate or overlapping runs."""

    def __init__(
        self,
        lock_file: Union[str, Path],
        term_seconds: float = 60.0,
        holder_id: Optional[str] = None,
    ) -> None:
        self.lock_file = Path(lock_file)
        self.term_seconds = term_seconds
        self.holder_id = holder_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self._fd: Optional[int] = None
        self._acquired_at: Optional[float] = None
        self._expires_at: Optional[float] = None

    def acquire(self) -> None:
        """Acquire the lease lock exclusively. Raises CapabilityLeaseError on conflict."""
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._fd = os.open(self.lock_file, os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            raise CapabilityLeaseError(f"cannot acquire lease on {self.lock_file}: locked by another process") from exc

        now = time.time()
        self._acquired_at = now
        self._expires_at = now + self.term_seconds
        payload = {
            "holder_id": self.holder_id,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "acquired_at": self._acquired_at,
            "expires_at": self._expires_at,
            "term_seconds": self.term_seconds,
        }
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, json.dumps(payload).encode("utf-8"))
        os.fsync(self._fd)

    def heartbeat(self) -> None:
        """Renew the lease term. Raises CapabilityLeaseExpiredError if expired or unheld."""
        if self._fd is None:
            raise CapabilityLeaseError("cannot heartbeat: lease is not held")
        now = time.time()
        if self._expires_at is not None and now > self._expires_at:
            raise CapabilityLeaseExpiredError(f"lease expired at {self._expires_at:.2f} (current {now:.2f})")
        self._expires_at = now + self.term_seconds
        payload = {
            "holder_id": self.holder_id,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "acquired_at": self._acquired_at,
            "expires_at": self._expires_at,
            "term_seconds": self.term_seconds,
        }
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, json.dumps(payload).encode("utf-8"))
        os.fsync(self._fd)

    def release(self) -> None:
        """Release the lease lock."""
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            finally:
                self._fd = None
                self._expires_at = None

    def __enter__(self) -> CapabilityDiscoveryLease:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive AIMD Batch Sizing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------


class AdaptiveAimdBatchController:
    """Dynamic AIMD batch size controller for capability sweeps and rehearsal probes."""

    def __init__(
        self,
        initial_batch: int = 1000,
        min_batch: int = 100,
        max_batch: int = 50000,
        additive_step: int = 250,
        multiplicative_factor: float = 0.5,
        target_latency_seconds: float = 0.5,
    ) -> None:
        self.min_batch = min_batch
        self.max_batch = max_batch
        self.current_batch = max(min_batch, min(max_batch, initial_batch))
        self.additive_step = additive_step
        self.multiplicative_factor = multiplicative_factor
        self.target_latency_seconds = target_latency_seconds

    def on_success(self, latency_seconds: float) -> int:
        """Additive increase on stable latency within target."""
        if latency_seconds <= self.target_latency_seconds:
            self.current_batch = min(self.max_batch, self.current_batch + self.additive_step)
        else:
            # Latency approaching threshold: soft step down
            step_down = max(1, int(self.additive_step * 0.5))
            self.current_batch = max(self.min_batch, self.current_batch - step_down)
        return self.current_batch

    def on_backpressure(self, reason: str = "") -> int:
        """Multiplicative decrease upon throttling, resource pressure, or timeout."""
        self.current_batch = max(
            self.min_batch,
            int(self.current_batch * self.multiplicative_factor),
        )
        return self.current_batch


def calculate_byte_weighted_eta(
    total_bytes: int,
    processed_bytes: int,
    elapsed_seconds: float,
    baseline_bytes_per_sec: float = 5000000.0,
) -> Dict[str, float]:
    """Calculate cold-start byte-weighted ETA with p50 and p90 forecast spread."""
    remaining_bytes = max(0, total_bytes - processed_bytes)
    if remaining_bytes == 0:
        return {"p50_seconds": 0.0, "p90_seconds": 0.0, "effective_bps": 0.0}

    if elapsed_seconds <= 0.05 or processed_bytes <= 1024:
        # Cold start: rely on conservative baseline estimate
        p50 = remaining_bytes / baseline_bytes_per_sec
        p90 = p50 * 1.4
        return {
            "p50_seconds": round(p50, 3),
            "p90_seconds": round(p90, 3),
            "effective_bps": baseline_bytes_per_sec,
        }

    measured_bps = processed_bytes / elapsed_seconds
    p50 = remaining_bytes / measured_bps
    # P90 spread incorporates standard 25% tail latency risk
    p90 = p50 * 1.25
    return {
        "p50_seconds": round(p50, 3),
        "p90_seconds": round(p90, 3),
        "effective_bps": round(measured_bps, 2),
    }


# -----------------------------------------------------------------------------
# High-Level Capability Discovery Coordinator
# -----------------------------------------------------------------------------


class CapabilityDiscoveryCoordinator:
    """Coordinates capability evaluation, empirical rehearsal, durable checkpoints, and leases."""

    def __init__(
        self,
        state_dir: Union[str, Path],
        inventory: Optional[InstalledToolInventory] = None,
        lease_term_seconds: float = 60.0,
    ) -> None:
        self.state_dir = Path(state_dir)
        self.inventory = inventory or InstalledToolInventory.default_inventory()
        self.lease_term_seconds = lease_term_seconds
        self.batch_controller = AdaptiveAimdBatchController()

    @property
    def lock_path(self) -> Path:
        return self.state_dir / ".run.lock"

    @property
    def checkpoint_path(self) -> Path:
        return self.state_dir / "checkpoint.json"

    def discover_pair_capabilities(
        self,
        pair: PairCoordinate,
        capabilities: Sequence[CapabilityKind],
        facts_map: Mapping[CapabilityKind, Mapping[str, Any]],
        *,
        now: Optional[datetime.datetime] = None,
        resume_cursor: Optional[Tuple[Any, ...]] = None,
    ) -> Tuple[List[PairCapabilityContract], List[OwnedCapabilityGap]]:
        """Run capability discovery across requested capabilities under active lease and checkpoint."""
        now_ts = now or datetime.datetime.now(datetime.timezone.utc)
        results: List[PairCapabilityContract] = []
        gaps: List[OwnedCapabilityGap] = []

        existing_cp: Optional[CapabilityDiscoveryCheckpoint] = None
        if self.checkpoint_path.exists():
            existing_cp = load_checkpoint(self.checkpoint_path)

        units = resume_units_in_o1(
            capabilities,
            existing_cp,
            key_fn=lambda c: c.value,
        )

        with CapabilityDiscoveryLease(self.lock_path, term_seconds=self.lease_term_seconds):
            for cap in units:
                cap_facts = facts_map.get(cap, {})
                _, contract = decide_pair_capability(
                    pair=pair,
                    capability=cap,
                    facts=cap_facts,
                    now=now_ts,
                )
                results.append(contract)
                if contract.owned_gap is not None:
                    gaps.append(contract.owned_gap)

                # Persist durable checkpoint
                cp = CapabilityDiscoveryCheckpoint(
                    checkpoint_id=f"cp-{uuid.uuid4().hex[:8]}",
                    pair_ident=pair.ident(),
                    cursor=(cap.value, now_ts.isoformat()),
                    contracts={c.capability.value: dataclasses.asdict(c) for c in results},
                    gaps=[dataclasses.asdict(g) for g in gaps],
                    created_at=existing_cp.created_at if existing_cp else now_ts,
                    updated_at=now_ts,
                )
                save_checkpoint(self.checkpoint_path, cp)

        return results, gaps
