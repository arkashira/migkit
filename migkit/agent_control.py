"""Expose resumable constrained migration operations to agents (Row 641: RCH33.agent-control).

Provides a versioned structured read/plan/execute/reconcile interface over the
migration engine (`autonomous.Workflow`, `attempts.Ledger`, `durable`, `lease`,
`verdict`, `decide`) with operation IDs, scope contracts, and authorization
references.

Key guarantees:
- Agents request goals and inspect evidence; deterministic admission (`decide.Rung`)
  and effect tracking (`attempts.Ledger` + `autonomous.Workflow`) remain authoritative
  without introducing a second orchestration state machine.
- Duplicated agent calls, process restarts, and untrusted database text (prompt
  injection, SQL injection, command injection in identifiers or row payloads)
  cannot duplicate writes, widen scope, or bypass a refused contract.
- Universal typed durable resume in O(1) across ranges/cursors (preserving int,
  str, Decimal, UUID, datetime, date, and composite tuple keys), active lease
  mutual exclusion (.run.lock), AIMD adaptive batch sizing, byte-weighted
  cold-start ETA, and unified `verdict` semantics with 4-tier care badges
  (ACTION, REVIEW, BENIGN, PASS).
"""
from __future__ import annotations

import contextlib
import datetime
import decimal
import enum
import hashlib
import hmac
import json
import math
import os
import re
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from . import attempts, autonomous, decide, durable, lease, verdict

INTERFACE_VERSION = "1.0"
SUPPORTED_INTERFACE_VERSIONS: Tuple[str, ...] = ("1.0",)

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class AgentPhase(str, enum.Enum):
    READ = "read"
    PLAN = "plan"
    EXECUTE = "execute"
    RECONCILE = "reconcile"


class AdmissionVerdict(str, enum.Enum):
    ADMITTED = "admitted"
    DEDUPLICATED = "deduplicated"
    RECONCILE_REQUIRED = "reconcile_required"
    REFUSED = "refused"


class AgentControlError(RuntimeError):
    """Base error for constrained agent control operations."""


class UnsupportedInterfaceVersionError(AgentControlError):
    """Raised when an agent request uses an unsupported protocol version."""


class ScopeWideningRefusedError(AgentControlError):
    """Raised when an agent attempts to widen database, table, column, or row scope."""


class AuthorizationRefusedError(AgentControlError):
    """Raised when an authorization reference is missing, expired, or invalid."""


class RefusedContractBypassError(AgentControlError):
    """Raised when an agent attempts to execute or override a refused contract."""


class UntrustedInputInjectionError(AgentControlError):
    """Raised when untrusted identifiers or directives attempt injection."""


class UncertainEffectBlockError(AgentControlError):
    """Raised when an interrupted operation has uncertain target effects requiring reconcile."""


class ActiveLeaseConflictError(AgentControlError):
    """Raised when another active process holds the hop execution lease."""


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,127}$")

_INJECTION_PATTERNS: Tuple[Tuple[str, re.Pattern[str]], ...] = (
    ("prompt_ignore_rules", re.compile(r"ignore\s+((all|previous|prior|above|earlier)\s+)*(instructions|rules|contracts)", re.I)),
    ("prompt_system_override", re.compile(r"(system\s*prompt|developer\s*mode|override\s+contract|bypass\s+refused)", re.I)),
    ("prompt_scope_widen", re.compile(r"(widen\s+scope|grant\s+all|allow_write\s*=\s*true|authorize\s+all)", re.I)),
    ("sql_ddl_injection", re.compile(r"(;\s*(drop|alter|truncate|grant|revoke|create|delete|update|insert)\b|--\s|\bunion\s+select\b)", re.I)),
    ("shell_command_injection", re.compile(r"(\$\([^)]*\)|`[^`]+`|\|\|\s*(rm|sh|bash|curl|wget)\b|;\s*(rm|sh|bash)\b)", re.I)),
)


def admission_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for deterministic agent operation admission.

    Uses declared facts in `factbook.FACTS`:
    - `kept`: whether a committed operation effect already exists for this op_id
    - `refused`: why the contract or scope was refused, or empty string
    - `write refused`: why writes are refused for this request, or empty string
    - `go`: whether the operation is authorized to write
    - `authorize`: whether the authorization reference verified cleanly
    - `same_shape`: whether the requested scope matches the ratified contract scope
    """
    return (
        decide.Rung(
            "deduplicated",
            "reuse the committed effect recorded for this operation id",
            needs=(
                decide.Need(
                    "a committed effect for this operation id",
                    lambda f: bool(f.get("kept")) and not f.get("refused") and bool(f.get("same_shape")),
                    why="the operation id has no committed effect on this scope to reuse",
                ),
            ),
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "write-admitted",
            "admit the constrained write operation under the ratified contract",
            needs=(
                decide.Need(
                    "no refused contract on the scope",
                    lambda f: not f.get("refused"),
                    why=lambda f: str(f.get("refused") or "the contract was refused"),
                ),
                decide.Need(
                    "write allowed on the target",
                    lambda f: (
                        not f.get("write refused")
                        and bool(f.get("go"))
                        and bool(f.get("authorize"))
                        and bool(f.get("same_shape"))
                    ),
                    why=lambda f: str(
                        f.get("write refused")
                        or "the write operation lacks authorization or scope parity"
                    ),
                ),
            ),
            footprint=1,
            leaves="a recorded unit in the hop's ledger",
        ),
        decide.Rung(
            "read-only",
            "inspect or plan within the ratified scope without target writes",
            needs=(
                decide.Need(
                    "scope and contract holds for read",
                    lambda f: (
                        not f.get("refused")
                        and not f.get("write refused")
                        and bool(f.get("authorize"))
                        and bool(f.get("same_shape"))
                    ),
                    why=lambda f: str(
                        f.get("refused")
                        or f.get("write refused")
                        or "the scope or authorization does not hold for read"
                    ),
                ),
            ),
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "refuse-operation",
            "refuse the agent operation and hold the target untouched",
            footprint=0,
            leaves="nothing",
        ),
    )


def decide_admission(facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the agent operation admission ladder on `facts`."""
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("agent_operation_admission", admission_rungs(), fact_obj)


def scan_untrusted_text(value: Any) -> List[str]:
    """Return the matched injection pattern names found in `value`."""
    text = str(value or "")
    matched: List[str] = []
    for name, pattern in _INJECTION_PATTERNS:
        if pattern.search(text):
            matched.append(name)
    if "\x00" in text:
        matched.append("null_byte_injection")
    return matched


def validate_identifier(name: str, kind: str = "identifier") -> str:
    """Validate that a database, schema, table, or column identifier is safe."""
    text = str(name or "").strip()
    if not text:
        raise UntrustedInputInjectionError(f"empty {kind} is not allowed")
    findings = scan_untrusted_text(text)
    if findings:
        raise UntrustedInputInjectionError(
            f"untrusted {kind} {text!r} refused ({', '.join(findings)})"
        )
    parts = text.split(".")
    for part in parts:
        if not _SAFE_IDENTIFIER_RE.match(part):
            raise UntrustedInputInjectionError(
                f"untrusted {kind} {text!r} fails identifier syntax check"
            )
    return text


def quarantine_untrusted_data(payload: Any) -> Tuple[Any, List[str]]:
    """Sanitize and quarantine untrusted database text so it cannot act as control instructions.

    Returns `(sanitized_payload, detected_patterns)`.
    """
    detected: List[str] = []

    def _walk(item: Any) -> Any:
        if isinstance(item, str):
            hits = scan_untrusted_text(item)
            if hits:
                for h in hits:
                    if h not in detected:
                        detected.append(h)
                return {
                    "untrusted_db_text": True,
                    "injection_detected": True,
                    "patterns": list(hits),
                    "quarantined_value": item,
                }
            return item
        if isinstance(item, Mapping):
            out = {}
            for k, v in item.items():
                ks = str(k)
                khits = scan_untrusted_text(ks)
                if khits:
                    for h in khits:
                        if h not in detected:
                            detected.append(h)
                out[ks] = _walk(v)
            return out
        if isinstance(item, (list, tuple)):
            return [_walk(x) for x in item]
        return item

    return _walk(payload), detected


def encode_typed_cursor(value: Any) -> Dict[str, Any]:
    """Encode a cursor/range boundary preserving native Python types for O(1) resume."""
    if value is None:
        return {"__type__": "none", "value": None}
    if isinstance(value, bool):
        return {"__type__": "bool", "value": bool(value)}
    if isinstance(value, int):
        return {"__type__": "int", "value": int(value)}
    if isinstance(value, float):
        return {"__type__": "float", "value": float(value)}
    if isinstance(value, str):
        return {"__type__": "str", "value": value}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "value": [encode_typed_cursor(v) for v in value]}
    if isinstance(value, list):
        return {"__type__": "list", "value": [encode_typed_cursor(v) for v in value]}
    raise TypeError(f"unsupported cursor key type: {type(value).__name__}")


def decode_typed_cursor(data: Any) -> Any:
    """Decode a typed cursor/range boundary back to its exact native Python type."""
    if not isinstance(data, Mapping) or "__type__" not in data:
        return data
    kind = data["__type__"]
    raw = data.get("value")
    if kind == "none":
        return None
    if kind == "bool":
        return bool(raw)
    if kind == "int":
        return int(raw)
    if kind == "float":
        return float(raw)
    if kind == "str":
        return str(raw)
    if kind == "decimal":
        return decimal.Decimal(str(raw))
    if kind == "uuid":
        return uuid.UUID(str(raw))
    if kind == "datetime":
        return datetime.datetime.fromisoformat(str(raw))
    if kind == "date":
        return datetime.date.fromisoformat(str(raw))
    if kind == "tuple":
        return tuple(decode_typed_cursor(x) for x in (raw or ()))
    if kind == "list":
        return [decode_typed_cursor(x) for x in (raw or ())]
    raise ValueError(f"unknown typed cursor tag: {kind!r}")


@dataclass(frozen=True)
class ScopeContract:
    """Bounded migration scope that an agent cannot widen."""
    databases: Tuple[str, ...]
    tables: Tuple[str, ...]
    columns: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    row_filters: Dict[str, str] = field(default_factory=dict)
    max_rows_per_op: int = 10000
    allow_schema_ddl: bool = False
    source_host: str = "127.0.0.1"
    target_host: str = "10.0.0.10"

    def __post_init__(self) -> None:
        object.__setattr__(self, "databases", tuple(sorted({str(d) for d in self.databases})))
        object.__setattr__(self, "tables", tuple(sorted({str(t) for t in self.tables})))
        norm_cols = {
            str(k): tuple(sorted({str(c) for c in v}))
            for k, v in sorted((self.columns or {}).items())
        }
        object.__setattr__(self, "columns", norm_cols)
        norm_filters = {
            str(k): str(v).strip()
            for k, v in sorted((self.row_filters or {}).items())
            if str(v).strip()
        }
        object.__setattr__(self, "row_filters", norm_filters)

    def validate_identifiers(self) -> Optional[str]:
        """Validate all identifiers and row filters against untrusted injection."""
        try:
            if not self.databases:
                return "scope must name at least one database"
            if not self.tables:
                return "scope must name at least one table"
            for db in self.databases:
                validate_identifier(db, "database")
            for tbl in self.tables:
                validate_identifier(tbl, "table")
            for tbl, cols in self.columns.items():
                validate_identifier(tbl, "table")
                for col in cols:
                    validate_identifier(col, "column")
            for tbl, expr in self.row_filters.items():
                validate_identifier(tbl, "table")
                hits = scan_untrusted_text(expr)
                if hits:
                    return f"untrusted row_filter on {tbl!r} refused ({', '.join(hits)})"
            if int(self.max_rows_per_op) <= 0:
                return "max_rows_per_op must be positive"
        except UntrustedInputInjectionError as exc:
            return str(exc)
        return None

    def to_canonical_dict(self) -> Dict[str, Any]:
        return {
            "databases": list(self.databases),
            "tables": list(self.tables),
            "columns": {k: list(v) for k, v in sorted(self.columns.items())},
            "row_filters": dict(sorted(self.row_filters.items())),
            "max_rows_per_op": int(self.max_rows_per_op),
            "allow_schema_ddl": bool(self.allow_schema_ddl),
            "source_host": str(self.source_host),
            "target_host": str(self.target_host),
        }

    def fingerprint(self) -> str:
        payload = json.dumps(self.to_canonical_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("ascii", errors="replace")).hexdigest()[:32]

    def contains(self, requested: "ScopeContract") -> Tuple[bool, str]:
        """Check whether `requested` is strictly within `self` without widening."""
        bad_req = requested.validate_identifiers()
        if bad_req:
            return False, bad_req
        extra_dbs = sorted(set(requested.databases) - set(self.databases))
        if extra_dbs:
            return False, f"scope widened to unauthorized database(s): {', '.join(extra_dbs)}"
        extra_tables = sorted(set(requested.tables) - set(self.tables))
        if extra_tables:
            return False, f"scope widened to unauthorized table(s): {', '.join(extra_tables)}"
        for tbl, req_cols in requested.columns.items():
            if tbl not in self.tables:
                return False, f"column scope names unauthorized table {tbl}"
            if tbl in self.columns:
                allowed_cols = set(self.columns[tbl])
                extra_cols = sorted(set(req_cols) - allowed_cols)
                if extra_cols:
                    return False, f"scope widened on {tbl} to column(s): {', '.join(extra_cols)}"
        for tbl, allowed_cols in self.columns.items():
            if tbl in requested.tables and tbl not in requested.columns:
                return False, f"scope omitted column restriction on {tbl} ({', '.join(allowed_cols)})"
        for tbl, req_filter in self.row_filters.items():
            if tbl in requested.tables and requested.row_filters.get(tbl) != req_filter:
                return False, f"scope dropped or altered row filter on {tbl}"
        for tbl, req_filter in requested.row_filters.items():
            if tbl in self.row_filters and self.row_filters[tbl] != req_filter:
                return False, f"scope altered row filter on {tbl}"
        if int(requested.max_rows_per_op) > int(self.max_rows_per_op):
            return (
                False,
                f"scope widened max_rows_per_op from {self.max_rows_per_op} to {requested.max_rows_per_op}",
            )
        if requested.allow_schema_ddl and not self.allow_schema_ddl:
            return False, "scope widened allow_schema_ddl when contract forbids DDL"
        if requested.source_host != self.source_host or requested.target_host != self.target_host:
            return False, "scope changed endpoint hosts outside contract"
        return True, ""


@dataclass(frozen=True)
class AuthorizationReference:
    """Cryptographically bound operator authorization reference."""
    auth_id: str
    principal: str
    scope_fingerprint: str
    allowed_goals: Tuple[str, ...] = ("copy", "sync", "cutover")
    allowed_phases: Tuple[str, ...] = ("read", "plan", "execute", "reconcile")
    plan_id: Optional[str] = None
    allow_write: bool = False
    expires_epoch: Optional[float] = None
    token_digest: str = ""

    @classmethod
    def issue(
        cls,
        auth_id: str,
        principal: str,
        scope: ScopeContract,
        *,
        allowed_goals: Sequence[str] = ("copy", "sync", "cutover"),
        allowed_phases: Sequence[str] = ("read", "plan", "execute", "reconcile"),
        plan_id: Optional[str] = None,
        allow_write: bool = False,
        expires_epoch: Optional[float] = None,
        signing_context: str = "migkit-agent-control-v1",
    ) -> "AuthorizationReference":
        sfp = scope.fingerprint()
        goals = tuple(sorted({str(g) for g in allowed_goals}))
        phases = tuple(sorted({str(p) for p in allowed_phases}))
        digest = cls._compute_digest(
            auth_id=auth_id,
            principal=principal,
            scope_fingerprint=sfp,
            allowed_goals=goals,
            allowed_phases=phases,
            plan_id=plan_id,
            allow_write=allow_write,
            expires_epoch=expires_epoch,
            signing_context=signing_context,
        )
        return cls(
            auth_id=str(auth_id),
            principal=str(principal),
            scope_fingerprint=sfp,
            allowed_goals=goals,
            allowed_phases=phases,
            plan_id=plan_id,
            allow_write=bool(allow_write),
            expires_epoch=expires_epoch,
            token_digest=digest,
        )

    @staticmethod
    def _compute_digest(
        *,
        auth_id: str,
        principal: str,
        scope_fingerprint: str,
        allowed_goals: Tuple[str, ...],
        allowed_phases: Tuple[str, ...],
        plan_id: Optional[str],
        allow_write: bool,
        expires_epoch: Optional[float],
        signing_context: str = "migkit-agent-control-v1",
    ) -> str:
        msg = json.dumps(
            {
                "auth_id": str(auth_id),
                "principal": str(principal),
                "scope_fingerprint": str(scope_fingerprint),
                "allowed_goals": list(allowed_goals),
                "allowed_phases": list(allowed_phases),
                "plan_id": plan_id,
                "allow_write": bool(allow_write),
                "expires_epoch": None if expires_epoch is None else round(float(expires_epoch), 3),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hmac.new(
            signing_context.encode("ascii"),
            msg.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()[:32]

    def verify(
        self,
        *,
        phase: str,
        goal: str,
        contract_scope: ScopeContract,
        expected_plan_id: Optional[str] = None,
        now: Optional[float] = None,
        signing_context: str = "migkit-agent-control-v1",
    ) -> Tuple[bool, str]:
        if not self.auth_id or not self.principal:
            return False, "authorization reference is missing auth_id or principal"
        expected_digest = self._compute_digest(
            auth_id=self.auth_id,
            principal=self.principal,
            scope_fingerprint=self.scope_fingerprint,
            allowed_goals=tuple(sorted(set(self.allowed_goals))),
            allowed_phases=tuple(sorted(set(self.allowed_phases))),
            plan_id=self.plan_id,
            allow_write=self.allow_write,
            expires_epoch=self.expires_epoch,
            signing_context=signing_context,
        )
        if not hmac.compare_digest(str(self.token_digest), expected_digest):
            return False, "authorization token_digest signature mismatch"
        if self.scope_fingerprint != contract_scope.fingerprint():
            return False, "authorization scope_fingerprint does not match ratified contract scope"
        if phase not in self.allowed_phases:
            return False, f"authorization {self.auth_id} does not permit phase {phase!r}"
        if goal not in self.allowed_goals:
            return False, f"authorization {self.auth_id} does not permit goal {goal!r}"
        current_time = time.time() if now is None else float(now)
        if self.expires_epoch is not None and current_time >= float(self.expires_epoch):
            return False, f"authorization {self.auth_id} expired at {self.expires_epoch}"
        if phase == AgentPhase.EXECUTE.value:
            if not self.allow_write:
                return False, f"authorization {self.auth_id} does not grant write permission (allow_write=False)"
            if expected_plan_id and self.plan_id != expected_plan_id:
                return (
                    False,
                    f"authorization plan_id {self.plan_id!r} does not match ratified plan {expected_plan_id!r}",
                )
        return True, ""


@dataclass(frozen=True)
class RangeUnitSpec:
    """A single resumable range/chunk unit within a constrained operation."""
    unit_id: str
    database: str
    table: str
    start_key: Any
    end_key: Any
    estimated_rows: int = 100
    estimated_bytes: int = 16384


@dataclass(frozen=True)
class AgentOperationRequest:
    """Versioned structured agent operation request."""
    operation_id: str
    phase: str
    goal: str
    scope: ScopeContract
    authorization: AuthorizationReference
    interface_version: str = INTERFACE_VERSION
    plan_id: Optional[str] = None
    step: str = "move"
    database: str = ""
    range_units: Tuple[RangeUnitSpec, ...] = ()
    agent_notes: str = ""

    def request_fingerprint(self) -> str:
        """Deterministic digest of the operation's intent and scope (for idempotency checks)."""
        unit_sigs = [
            {
                "unit_id": u.unit_id,
                "database": u.database,
                "table": u.table,
                "start_key": encode_typed_cursor(u.start_key),
                "end_key": encode_typed_cursor(u.end_key),
            }
            for u in self.range_units
        ]
        payload = json.dumps(
            {
                "interface_version": self.interface_version,
                "phase": self.phase,
                "goal": self.goal,
                "step": self.step,
                "database": self.database,
                "plan_id": self.plan_id,
                "scope": self.scope.to_canonical_dict(),
                "units": unit_sigs,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("ascii", errors="replace")).hexdigest()[:32]


@dataclass
class AIMDBatchController:
    """Dynamic AIMD batch and chunk controller adapting to latency and row bytes."""
    min_batch_rows: int = 10
    max_batch_rows: int = 50000
    current_batch_rows: int = 500
    target_latency_seconds: float = 0.50
    additive_step: int = 250
    multiplicative_decrease: float = 0.50

    def observe(self, rows: int, elapsed_seconds: float, bytes_processed: int = 0) -> int:
        elapsed = max(0.0001, float(elapsed_seconds))
        if elapsed <= self.target_latency_seconds:
            grow = max(1, int(self.additive_step))
            if bytes_processed > 0 and rows > 0:
                avg_row_bytes = max(1.0, float(bytes_processed) / float(rows))
                if avg_row_bytes > 16384.0:
                    grow = max(1, grow // 4)
            self.current_batch_rows = min(self.max_batch_rows, self.current_batch_rows + grow)
        else:
            shrunk = int(float(self.current_batch_rows) * float(self.multiplicative_decrease))
            self.current_batch_rows = max(self.min_batch_rows, shrunk)
        return self.current_batch_rows


@dataclass
class ByteWeightedETAForecaster:
    """Instant cold-start byte-weighted EWMA ETA forecaster."""
    default_bytes_per_sec: float = 10.0 * 1024 * 1024
    ewma_alpha: float = 0.35
    ewma_rate_bps: Optional[float] = None
    samples: List[float] = field(default_factory=list)

    def observe(self, num_bytes: int, elapsed_seconds: float) -> float:
        if num_bytes <= 0 or elapsed_seconds <= 0.0:
            return self.ewma_rate_bps or self.default_bytes_per_sec
        rate = float(num_bytes) / max(0.0001, float(elapsed_seconds))
        self.samples.append(rate)
        if self.ewma_rate_bps is None:
            self.ewma_rate_bps = rate
        else:
            self.ewma_rate_bps = (
                self.ewma_alpha * rate + (1.0 - self.ewma_alpha) * self.ewma_rate_bps
            )
        return self.ewma_rate_bps

    def forecast_seconds(self, remaining_bytes: int) -> Dict[str, Any]:
        rem = max(0, int(remaining_bytes))
        if rem == 0:
            return {"eta_seconds": 0.0, "p90_eta_seconds": 0.0, "cold_start": self.ewma_rate_bps is None}
        rate = self.ewma_rate_bps if self.ewma_rate_bps is not None else self.default_bytes_per_sec
        eta = rem / max(1.0, rate)
        if len(self.samples) >= 2:
            ordered = sorted(self.samples)
            p10_rate = ordered[max(0, int(0.10 * (len(ordered) - 1)))]
            p90_eta = rem / max(1.0, min(rate, p10_rate))
        else:
            p90_eta = eta * 1.25
        return {
            "eta_seconds": round(eta, 4),
            "p90_eta_seconds": round(p90_eta, 4),
            "cold_start": self.ewma_rate_bps is None,
        }


@dataclass(frozen=True)
class AgentOperationResult:
    """Unified structured response mapping every agent phase to `verdict` semantics."""
    interface_version: str
    operation_id: str
    phase: str
    goal: str
    admission: str
    chosen_rung: str
    rung_reason: str
    verdict_status: str
    verdict_word: str
    exit_code: int
    verdict_means: str
    care_badge: str
    scope_fingerprint: str
    plan_id: Optional[str] = None
    writes_applied: int = 0
    units_completed: Tuple[str, ...] = ()
    units_skipped_o1: Tuple[str, ...] = ()
    reconciled_operations: Dict[str, str] = field(default_factory=dict)
    resume_cursors: Dict[str, Any] = field(default_factory=dict)
    quarantined_injections: Tuple[str, ...] = ()
    evidence: Dict[str, Any] = field(default_factory=dict)
    eta_forecast: Dict[str, Any] = field(default_factory=dict)
    batch_rows_next: int = 500
    next_step: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _map_verdict_semantics(
    *,
    status_key: str,
    records: Optional[Sequence[Mapping[str, Any]]] = None,
    quarantined_injections: Sequence[str] = (),
    deduplicated: bool = False,
    next_step: str = "",
) -> Tuple[str, str, int, str, str, str]:
    """Map an operation outcome or check record list to canonical `verdict` semantics."""
    if records:
        dec = verdict.decide(list(records))
        st = dec.status
        word = dec.word
        code = dec.exit
        means = verdict.MEANS.get(st, dec.why)
        step = next_step or dec.next or dec.why
    elif status_key in verdict.VERDICTS:
        v = verdict.VERDICTS[status_key]
        st = status_key
        word = v.word
        code = v.exit
        means = verdict.MEANS[status_key]
        step = next_step
    else:
        st = status_key if status_key in verdict.EXIT else "failed"
        word = st
        code = verdict.EXIT[st]
        means = verdict.MEANS[st]
        step = next_step

    if st in ("different", "error", "refused", "failed", "usage"):
        badge = CARE_ACTION
    elif st == "incomplete" or quarantined_injections:
        badge = CARE_REVIEW
    elif deduplicated:
        badge = CARE_BENIGN
    else:
        badge = CARE_PASS

    return st, word, int(code), means, badge, step


class ActiveOperationLease:
    """Mutual-exclusion `.run.lock` lease with PID and heartbeat verification."""

    def __init__(self, state_dir: Path, lease_seconds: float = 60.0) -> None:
        self.state_dir = Path(state_dir)
        self.lock_path = self.state_dir / ".run.lock"
        self.lease_seconds = float(lease_seconds)
        self.holder_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

    @contextlib.contextmanager
    def acquire(self, now_fn: Callable[[], float] = time.time):
        durable.private_dir(self.state_dir)
        with durable.locked(self.lock_path):
            existing = durable.read_state(self.lock_path, "cache")
            now_val = float(now_fn())
            if isinstance(existing, dict) and existing.get("holder"):
                expires = float(existing.get("expires") or 0.0)
                same_pid = (
                    existing.get("host") == socket.gethostname()
                    and int(existing.get("pid") or -1) == os.getpid()
                )
                if not same_pid and expires > now_val and lease.liveness(existing, now=now_val):
                    raise ActiveLeaseConflictError(
                        f"active operation lease held by {existing.get('holder')} until {expires}"
                    )
            rec = {
                "holder": self.holder_id,
                "host": socket.gethostname(),
                "pid": os.getpid(),
                "renewed": now_val,
                "expires": now_val + self.lease_seconds,
                "jitter": 0.0,
            }
            durable.write_json(self.lock_path, rec)
        try:
            yield rec
        finally:
            with durable.locked(self.lock_path):
                cur = durable.read_state(self.lock_path, "cache")
                if isinstance(cur, dict) and cur.get("holder") == self.holder_id:
                    self.lock_path.unlink(missing_ok=True)
                    durable.sync_dir(self.state_dir)

    def heartbeat(self, now_fn: Callable[[], float] = time.time) -> Dict[str, Any]:
        with durable.locked(self.lock_path):
            cur = durable.read_state(self.lock_path, "cache")
            if not isinstance(cur, dict) or cur.get("holder") != self.holder_id:
                raise ActiveLeaseConflictError("operation lease lost during execution")
            now_val = float(now_fn())
            cur["renewed"] = now_val
            cur["expires"] = now_val + self.lease_seconds
            durable.write_json(self.lock_path, cur)
            return cur


class ConstrainedMigrationController:
    """Authoritative read/plan/execute/reconcile controller over migkit's engine.

    Uses `autonomous.Workflow` (`workflow.json`) and `attempts.Ledger` (`attempts.json`)
    as the single durable state owners, with typed range checkpoints in `checkpoints.json`
    and contract metadata in `contract.json`.
    """

    def __init__(
        self,
        state_dir: Path,
        contract_scope: ScopeContract,
        *,
        goal: str = "copy",
        signing_context: str = "migkit-agent-control-v1",
        lease_seconds: float = 60.0,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        if goal not in autonomous.FLOWS:
            raise ValueError(f"unsupported goal {goal!r}; expected one of {autonomous.GOALS}")
        self.state_dir = durable.private_dir(Path(state_dir))
        self.contract_scope = contract_scope
        self.goal = goal
        self.signing_context = signing_context
        self.now_fn = now_fn
        self.lease = ActiveOperationLease(self.state_dir, lease_seconds=lease_seconds)
        self.aimd = AIMDBatchController()
        self.eta = ByteWeightedETAForecaster()

        self.workflow_path = self.state_dir / "workflow.json"
        self.ledger_path = self.state_dir / "attempts.json"
        self.contract_path = self.state_dir / "contract.json"
        self.checkpoint_path = self.state_dir / "checkpoints.json"

        self.ledger = attempts.Ledger(self.ledger_path)
        self._init_or_verify_contract()

    def _init_or_verify_contract(self) -> Dict[str, Any]:
        """Load or initialize the durable contract record without allowing scope tampering."""
        bad = self.contract_scope.validate_identifiers()
        existing = durable.read_state(self.contract_path, "state") if self.contract_path.exists() else None
        sfp = self.contract_scope.fingerprint()
        if isinstance(existing, dict) and existing.get("scope_fingerprint"):
            if existing["scope_fingerprint"] != sfp:
                raise ScopeWideningRefusedError(
                    "persisted contract scope_fingerprint disagrees with controller contract_scope"
                )
            if bad and not existing.get("refused_reason"):
                existing["refused_reason"] = bad
                durable.write_json(self.contract_path, existing, indent=1)
            return existing

        record = {
            "interface_version": INTERFACE_VERSION,
            "goal": self.goal,
            "scope": self.contract_scope.to_canonical_dict(),
            "scope_fingerprint": sfp,
            "plan_id": None,
            "refused_reason": bad or "",
            "created_at": float(self.now_fn()),
        }
        durable.write_json(self.contract_path, record, indent=1)
        return record

    def _load_contract(self) -> Dict[str, Any]:
        rec = durable.read_state(self.contract_path, "state")
        if not isinstance(rec, dict):
            return self._init_or_verify_contract()
        return rec

    def _save_contract(self, rec: Dict[str, Any]) -> None:
        durable.write_json(self.contract_path, rec, indent=1)

    def _load_workflow(self) -> autonomous.Workflow:
        _, phases = autonomous.compose(self.goal, self.contract_scope.databases)
        return autonomous.Workflow.load(
            self.workflow_path,
            self.goal,
            self.contract_scope.databases,
            phases,
        )

    def _load_checkpoints(self) -> Dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {"completed_units": {}, "cursors": {}}
        got = durable.read_state(self.checkpoint_path, "position")
        if not isinstance(got, dict):
            return {"completed_units": {}, "cursors": {}}
        got.setdefault("completed_units", {})
        got.setdefault("cursors", {})
        return got

    def _save_checkpoints(self, state: Dict[str, Any]) -> None:
        durable.write_json(self.checkpoint_path, state, indent=1)

    def mark_contract_refused(self, reason: str) -> None:
        """Durably mark the contract as refused so no subsequent agent call can bypass it."""
        rec = self._load_contract()
        rec["refused_reason"] = str(reason or "contract refused by check")
        self._save_contract(rec)

    def _check_version(self, req: AgentOperationRequest) -> Optional[str]:
        if req.interface_version not in SUPPORTED_INTERFACE_VERSIONS:
            return (
                f"unsupported interface_version {req.interface_version!r}; "
                f"supported: {', '.join(SUPPORTED_INTERFACE_VERSIONS)}"
            )
        return None

    def _evaluate_admission(
        self,
        req: AgentOperationRequest,
        contract: Mapping[str, Any],
    ) -> Tuple[decide.Choice, Dict[str, Any], Optional[Dict[str, Any]]]:
        """Build `factbook`-compliant facts for `decide_admission` and return `(choice, facts, owned)`."""
        ver_err = self._check_version(req)
        scope_ok, scope_why = self.contract_scope.contains(req.scope)
        auth_ok, auth_why = req.authorization.verify(
            phase=req.phase,
            goal=req.goal,
            contract_scope=self.contract_scope,
            expected_plan_id=contract.get("plan_id") if req.phase == AgentPhase.EXECUTE.value else None,
            now=self.now_fn(),
            signing_context=self.signing_context,
        )
        note_hits = scan_untrusted_text(req.agent_notes)
        note_err = (
            f"untrusted directive in agent_notes refused ({', '.join(note_hits)})"
            if note_hits
            else ""
        )
        op_id_hits = scan_untrusted_text(req.operation_id)
        op_id_err = (
            f"untrusted operation_id refused ({', '.join(op_id_hits)})"
            if op_id_hits or not str(req.operation_id).strip()
            else ""
        )

        refused_reasons = [
            r
            for r in (
                ver_err,
                contract.get("refused_reason"),
                "" if scope_ok else scope_why,
                note_err,
                op_id_err,
            )
            if r
        ]

        owned = self.ledger.owned(req.operation_id)
        req_fp = req.request_fingerprint()
        kept_committed = False
        if owned and owned.get("state") == "committed":
            if owned.get("request_fingerprint") != req_fp:
                refused_reasons.append(
                    f"operation_id {req.operation_id!r} replayed with altered scope or parameters"
                )
            else:
                kept_committed = True

        uncertain_ops = self.ledger.uncertain()
        write_refused_reasons: List[str] = []
        if not auth_ok:
            write_refused_reasons.append(auth_why)
        if req.phase == AgentPhase.EXECUTE.value:
            if not contract.get("plan_id"):
                write_refused_reasons.append("cannot execute before a plan is ratified")
            elif req.plan_id != contract.get("plan_id"):
                write_refused_reasons.append(
                    f"request plan_id {req.plan_id!r} does not match ratified plan {contract.get('plan_id')!r}"
                )
            if uncertain_ops and not kept_committed:
                write_refused_reasons.append(
                    f"uncertain operations require reconcile first: {', '.join(sorted(uncertain_ops))}"
                )

        facts = {
            "kept": kept_committed,
            "refused": "; ".join(refused_reasons),
            "write refused": "; ".join(write_refused_reasons),
            "go": req.phase == AgentPhase.EXECUTE.value and bool(req.authorization.allow_write),
            "authorize": bool(auth_ok),
            "same_shape": bool(scope_ok) and req.goal == self.goal,
        }
        choice = decide_admission(facts)
        return choice, facts, (owned if owned else None)

    def read(
        self,
        req: AgentOperationRequest,
        inspect_fn: Optional[Callable[[ScopeContract], Mapping[str, Any]]] = None,
    ) -> AgentOperationResult:
        """Execute a constrained read/evidence inspection operation."""
        with self.lease.acquire(self.now_fn):
            contract = self._load_contract()
            choice, facts, owned = self._evaluate_admission(req, contract)
            if choice.path == "refuse-operation":
                reason = facts.get("refused") or facts.get("write refused") or choice.reason
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="usage" if "interface_version" in reason else "refused",
                    next_step=f"resolve refusal ({reason}) and resubmit within authorized scope",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.READ.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.REFUSED.value,
                    chosen_rung=choice.path,
                    rung_reason=reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    next_step=step,
                )

            raw_evidence = dict(inspect_fn(req.scope)) if inspect_fn is not None else {}
            sanitized_evidence, injected = quarantine_untrusted_data(raw_evidence)
            wf = self._load_workflow()
            checkpoints = self._load_checkpoints()
            check_records = sanitized_evidence.get("records")
            st, word, code, means, badge, step = _map_verdict_semantics(
                status_key="done",
                records=check_records if isinstance(check_records, list) else None,
                quarantined_injections=injected,
                deduplicated=(choice.path == "deduplicated"),
                next_step="review evidence and call plan or execute within ratified scope",
            )
            return AgentOperationResult(
                interface_version=req.interface_version,
                operation_id=req.operation_id,
                phase=AgentPhase.READ.value,
                goal=req.goal,
                admission=(
                    AdmissionVerdict.DEDUPLICATED.value
                    if choice.path == "deduplicated"
                    else AdmissionVerdict.ADMITTED.value
                ),
                chosen_rung=choice.path,
                rung_reason=choice.reason,
                verdict_status=st,
                verdict_word=word,
                exit_code=code,
                verdict_means=means,
                care_badge=badge,
                scope_fingerprint=self.contract_scope.fingerprint(),
                plan_id=contract.get("plan_id"),
                resume_cursors={
                    k: decode_typed_cursor(v)
                    for k, v in (checkpoints.get("cursors") or {}).items()
                },
                quarantined_injections=tuple(injected),
                evidence={
                    "workflow_units": {u.key: u.row() for u in wf.units},
                    "uncertain_operations": list(sorted(self.ledger.uncertain())),
                    "inspected": sanitized_evidence,
                },
                next_step=step,
            )

    def plan(
        self,
        req: AgentOperationRequest,
        assess_fn: Optional[Callable[[ScopeContract], Sequence[Tuple[str, str]]]] = None,
    ) -> AgentOperationResult:
        """Build and ratify a constrained migration plan over `autonomous.Workflow`."""
        with self.lease.acquire(self.now_fn):
            contract = self._load_contract()
            choice, facts, owned = self._evaluate_admission(req, contract)
            if choice.path == "refuse-operation":
                reason = facts.get("refused") or facts.get("write refused") or choice.reason
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="usage" if "interface_version" in reason else "refused",
                    next_step=f"cannot ratify plan: {reason}",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.PLAN.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.REFUSED.value,
                    chosen_rung=choice.path,
                    rung_reason=reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    next_step=step,
                )

            if choice.path == "deduplicated" and owned:
                saved_plan_id = owned.get("plan_id") or contract.get("plan_id")
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="done",
                    deduplicated=True,
                    next_step=f"plan {saved_plan_id} already ratified; proceed to execute",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.PLAN.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.DEDUPLICATED.value,
                    chosen_rung=choice.path,
                    rung_reason=choice.reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=saved_plan_id,
                    evidence=dict(owned.get("evidence") or {}),
                    next_step=step,
                )

            raw_blockers = list(assess_fn(req.scope)) if assess_fn is not None else []
            sanitized_blockers, injected = quarantine_untrusted_data(raw_blockers)
            wf = self._load_workflow()
            assess_unit = wf.by_key().get("assess")
            if raw_blockers:
                refusal_msg = "; ".join(f"{db or 'hop'}: {why}" for db, why in raw_blockers)
                contract["refused_reason"] = f"assessment refused contract: {refusal_msg}"
                self._save_contract(contract)
                if assess_unit is not None:
                    assess_unit.state = "failed"
                    assess_unit.why = contract["refused_reason"]
                wf.save()
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="refused",
                    quarantined_injections=injected,
                    next_step="resolve assessment blockers before requesting a new plan",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.PLAN.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.REFUSED.value,
                    chosen_rung="refuse-operation",
                    rung_reason=contract["refused_reason"],
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=None,
                    quarantined_injections=tuple(injected),
                    evidence={"blockers": sanitized_blockers},
                    next_step=step,
                )

            plan_id = contract.get("plan_id") or (
                f"plan-{hashlib.sha256((req.operation_id + ':' + self.contract_scope.fingerprint()).encode('ascii')).hexdigest()[:12]}"
            )
            contract["plan_id"] = plan_id
            self._save_contract(contract)
            if assess_unit is not None:
                assess_unit.state = "done"
                assess_unit.way = "agent-plan"
                assess_unit.at = float(self.now_fn())
                wf.evidence["assess"] = list(autonomous.CONTRACTS["assess"][1])
            wf.decided(
                assess_unit or autonomous.Unit("assess"),
                "plan",
                f"ratified plan {plan_id} over {', '.join(self.contract_scope.databases)}",
                "agent-plan",
                plan_id=plan_id,
                scope_fingerprint=self.contract_scope.fingerprint(),
            )
            wf.save()

            total_est_bytes = sum(int(u.estimated_bytes) for u in req.range_units)
            eta_info = self.eta.forecast_seconds(total_est_bytes)
            plan_evidence = {
                "plan_id": plan_id,
                "phases": [u.key for u in wf.units],
                "databases": list(self.contract_scope.databases),
                "tables": list(self.contract_scope.tables),
                "scope_fingerprint": self.contract_scope.fingerprint(),
            }
            self.ledger.own(
                req.operation_id,
                state="committed",
                phase=AgentPhase.PLAN.value,
                plan_id=plan_id,
                request_fingerprint=req.request_fingerprint(),
                evidence=plan_evidence,
                at=float(self.now_fn()),
            )

            st, word, code, means, badge, step = _map_verdict_semantics(
                status_key="done",
                quarantined_injections=injected,
                next_step=f"issue write authorization bound to plan_id={plan_id} and call execute",
            )
            return AgentOperationResult(
                interface_version=req.interface_version,
                operation_id=req.operation_id,
                phase=AgentPhase.PLAN.value,
                goal=req.goal,
                admission=AdmissionVerdict.ADMITTED.value,
                chosen_rung=choice.path,
                rung_reason=choice.reason,
                verdict_status=st,
                verdict_word=word,
                exit_code=code,
                verdict_means=means,
                care_badge=badge,
                scope_fingerprint=self.contract_scope.fingerprint(),
                plan_id=plan_id,
                quarantined_injections=tuple(injected),
                evidence=plan_evidence,
                eta_forecast=eta_info,
                batch_rows_next=self.aimd.current_batch_rows,
                next_step=step,
            )

    def execute(
        self,
        req: AgentOperationRequest,
        write_unit_fn: Callable[[RangeUnitSpec, int], Mapping[str, Any]],
    ) -> AgentOperationResult:
        """Execute a constrained migration operation with O(1) typed resume and deduplication."""
        with self.lease.acquire(self.now_fn):
            contract = self._load_contract()
            choice, facts, owned = self._evaluate_admission(req, contract)

            if choice.path == "deduplicated" and owned is not None:
                checkpoints = self._load_checkpoints()
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key=str(owned.get("verdict_status") or "done"),
                    deduplicated=True,
                    next_step="operation already committed; duplicate call suppressed without target writes",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.EXECUTE.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.DEDUPLICATED.value,
                    chosen_rung=choice.path,
                    rung_reason=choice.reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    writes_applied=0,
                    units_completed=tuple(owned.get("units_completed") or ()),
                    units_skipped_o1=tuple(owned.get("units_completed") or ()),
                    resume_cursors={
                        k: decode_typed_cursor(v)
                        for k, v in (checkpoints.get("cursors") or {}).items()
                    },
                    quarantined_injections=tuple(owned.get("quarantined_injections") or ()),
                    evidence=dict(owned.get("evidence") or {}),
                    eta_forecast=dict(owned.get("eta_forecast") or {}),
                    batch_rows_next=self.aimd.current_batch_rows,
                    next_step=step,
                )

            if choice.path != "write-admitted":
                reason = facts.get("refused") or facts.get("write refused") or choice.reason
                needs_reconcile = "uncertain operations require reconcile first" in reason
                status_key = (
                    "usage"
                    if "interface_version" in reason
                    else ("error" if needs_reconcile else "refused")
                )
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key=status_key,
                    next_step=(
                        "run reconcile to settle uncertain target effects before retrying execute"
                        if needs_reconcile
                        else f"operation refused: {reason}"
                    ),
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.EXECUTE.value,
                    goal=req.goal,
                    admission=(
                        AdmissionVerdict.RECONCILE_REQUIRED.value
                        if needs_reconcile
                        else AdmissionVerdict.REFUSED.value
                    ),
                    chosen_rung=choice.path,
                    rung_reason=reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    writes_applied=0,
                    next_step=step,
                )

            for unit in req.range_units:
                if unit.database not in req.scope.databases:
                    raise ScopeWideningRefusedError(
                        f"range unit {unit.unit_id} targets database {unit.database!r} outside scope"
                    )
                if unit.table not in req.scope.tables:
                    raise ScopeWideningRefusedError(
                        f"range unit {unit.unit_id} targets table {unit.table!r} outside scope"
                    )
                if int(unit.estimated_rows) > int(req.scope.max_rows_per_op):
                    raise ScopeWideningRefusedError(
                        f"range unit {unit.unit_id} rows {unit.estimated_rows} exceeds max_rows_per_op {req.scope.max_rows_per_op}"
                    )

            wf = self._load_workflow()
            target_db = req.database or (req.scope.databases[0] if req.scope.databases else "")
            wf_key = f"{req.step}:{target_db}" if target_db and req.step != "assess" else req.step
            wf_unit = wf.by_key().get(wf_key)
            if wf_unit is not None:
                wf_unit.state = "running"
                wf.save()

            req_fp = req.request_fingerprint()
            self.ledger.own(
                req.operation_id,
                state="began",
                phase=AgentPhase.EXECUTE.value,
                plan_id=contract.get("plan_id"),
                step=req.step,
                database=target_db,
                request_fingerprint=req_fp,
                started_at=float(self.now_fn()),
            )

            checkpoints = self._load_checkpoints()
            completed_map: Dict[str, Any] = checkpoints["completed_units"]
            cursors_map: Dict[str, Any] = checkpoints["cursors"]

            units_completed: List[str] = []
            units_skipped_o1: List[str] = []
            all_injected: List[str] = []
            writes_applied = 0
            unit_outputs: List[Dict[str, Any]] = []
            verdict_records: List[Dict[str, Any]] = []

            remaining_bytes = sum(
                int(u.estimated_bytes)
                for u in req.range_units
                if u.unit_id not in completed_map
            )

            try:
                for unit in req.range_units:
                    self.lease.heartbeat(self.now_fn)
                    if unit.unit_id in completed_map:
                        units_skipped_o1.append(unit.unit_id)
                        units_completed.append(unit.unit_id)
                        continue

                    self.ledger.unit_opened(
                        unit.unit_id,
                        kind=req.step,
                        level="range",
                        parent=f"{unit.database}.{unit.table}",
                        parts=len(req.range_units),
                        at=float(self.now_fn()),
                    )
                    t0 = float(self.now_fn())
                    try:
                        raw_out = dict(write_unit_fn(unit, self.aimd.current_batch_rows))
                    except BaseException as exc:
                        self.ledger.unit_closed(
                            unit.unit_id,
                            ok=False,
                            reason=type(exc).__name__,
                            at=float(self.now_fn()),
                        )
                        self.ledger.own(
                            req.operation_id,
                            state="unknown",
                            error_type=type(exc).__name__,
                            failed_unit=unit.unit_id,
                            at=float(self.now_fn()),
                        )
                        if wf_unit is not None:
                            wf_unit.state = "failed-unknown"
                            wf_unit.why = f"{type(exc).__name__} on {unit.unit_id}"
                            wf.save()
                        raise

                    elapsed = max(0.0001, float(self.now_fn()) - t0)
                    rows_written = int(raw_out.get("rows_written", unit.estimated_rows))
                    bytes_written = int(raw_out.get("bytes_written", unit.estimated_bytes))
                    self.aimd.observe(rows_written, elapsed, bytes_written)
                    self.eta.observe(bytes_written, elapsed)
                    remaining_bytes = max(0, remaining_bytes - int(unit.estimated_bytes))

                    sanitized_out, injected = quarantine_untrusted_data(raw_out)
                    for pattern in injected:
                        if pattern not in all_injected:
                            all_injected.append(pattern)

                    writes_applied += 1
                    units_completed.append(unit.unit_id)
                    table_key = f"{unit.database}.{unit.table}"
                    encoded_end = encode_typed_cursor(unit.end_key)
                    completed_map[unit.unit_id] = {
                        "operation_id": req.operation_id,
                        "table": table_key,
                        "end_key": encoded_end,
                        "rows_written": rows_written,
                        "committed_at": float(self.now_fn()),
                    }
                    cursors_map[table_key] = encoded_end
                    self._save_checkpoints(checkpoints)
                    self.ledger.unit_closed(
                        unit.unit_id,
                        ok=True,
                        at=float(self.now_fn()),
                    )
                    unit_outputs.append(
                        {
                            "unit_id": unit.unit_id,
                            "table": table_key,
                            "rows_written": rows_written,
                            "output": sanitized_out,
                        }
                    )
                    if isinstance(sanitized_out.get("verdict_record"), dict):
                        verdict_records.append(sanitized_out["verdict_record"])
            except BaseException as exc:
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="error",
                    quarantined_injections=all_injected,
                    next_step=(
                        f"execution interrupted ({type(exc).__name__}); "
                        "call reconcile to verify target effects before retrying"
                    ),
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.EXECUTE.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.RECONCILE_REQUIRED.value,
                    chosen_rung=choice.path,
                    rung_reason=f"interrupted during {req.operation_id}: {type(exc).__name__}",
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    writes_applied=writes_applied,
                    units_completed=tuple(units_completed),
                    units_skipped_o1=tuple(units_skipped_o1),
                    resume_cursors={
                        k: decode_typed_cursor(v) for k, v in cursors_map.items()
                    },
                    quarantined_injections=tuple(all_injected),
                    evidence={"partial_units": unit_outputs},
                    eta_forecast=self.eta.forecast_seconds(remaining_bytes),
                    batch_rows_next=self.aimd.current_batch_rows,
                    next_step=step,
                )

            if wf_unit is not None:
                wf_unit.state = "done"
                wf_unit.way = "agent-execute"
                wf_unit.at = float(self.now_fn())
                if req.step in autonomous.CONTRACTS:
                    wf.evidence[wf_unit.key] = list(autonomous.CONTRACTS[req.step][1])
                wf.save()

            eta_info = self.eta.forecast_seconds(0)
            st, word, code, means, badge, step = _map_verdict_semantics(
                status_key="done",
                records=verdict_records if verdict_records else None,
                quarantined_injections=all_injected,
                next_step="execution committed; inspect evidence or run verify",
            )
            evidence_payload = {
                "units": unit_outputs,
                "workflow_unit": wf_key,
                "workflow_state": wf_unit.state if wf_unit is not None else "done",
            }
            self.ledger.own(
                req.operation_id,
                state="committed",
                phase=AgentPhase.EXECUTE.value,
                plan_id=contract.get("plan_id"),
                request_fingerprint=req_fp,
                writes_applied=writes_applied,
                units_completed=list(units_completed),
                verdict_status=st,
                quarantined_injections=list(all_injected),
                evidence=evidence_payload,
                eta_forecast=eta_info,
                committed_at=float(self.now_fn()),
            )
            return AgentOperationResult(
                interface_version=req.interface_version,
                operation_id=req.operation_id,
                phase=AgentPhase.EXECUTE.value,
                goal=req.goal,
                admission=AdmissionVerdict.ADMITTED.value,
                chosen_rung=choice.path,
                rung_reason=choice.reason,
                verdict_status=st,
                verdict_word=word,
                exit_code=code,
                verdict_means=means,
                care_badge=badge,
                scope_fingerprint=self.contract_scope.fingerprint(),
                plan_id=contract.get("plan_id"),
                writes_applied=writes_applied,
                units_completed=tuple(units_completed),
                units_skipped_o1=tuple(units_skipped_o1),
                resume_cursors={
                    k: decode_typed_cursor(v) for k, v in cursors_map.items()
                },
                quarantined_injections=tuple(all_injected),
                evidence=evidence_payload,
                eta_forecast=eta_info,
                batch_rows_next=self.aimd.current_batch_rows,
                next_step=step,
            )

    def reconcile(
        self,
        req: AgentOperationRequest,
        probe_effect_fn: Callable[[str, Mapping[str, Any]], Mapping[str, Any]],
    ) -> AgentOperationResult:
        """Reconcile uncertain operations and open range units on the target after restart/failure.

        `probe_effect_fn(op_id, record)` returns:
        - `{"state": "committed" | "nothing" | "running", "landed_units": {unit_id: end_key, ...}}`
        """
        with self.lease.acquire(self.now_fn):
            contract = self._load_contract()
            choice, facts, _ = self._evaluate_admission(req, contract)
            if choice.path == "refuse-operation":
                reason = facts.get("refused") or facts.get("write refused") or choice.reason
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="usage" if "interface_version" in reason else "refused",
                    next_step=f"reconcile refused: {reason}",
                )
                return AgentOperationResult(
                    interface_version=req.interface_version,
                    operation_id=req.operation_id,
                    phase=AgentPhase.RECONCILE.value,
                    goal=req.goal,
                    admission=AdmissionVerdict.REFUSED.value,
                    chosen_rung=choice.path,
                    rung_reason=reason,
                    verdict_status=st,
                    verdict_word=word,
                    exit_code=code,
                    verdict_means=means,
                    care_badge=badge,
                    scope_fingerprint=self.contract_scope.fingerprint(),
                    plan_id=contract.get("plan_id"),
                    next_step=step,
                )

            uncertain = self.ledger.uncertain()
            checkpoints = self._load_checkpoints()
            completed_map = checkpoints["completed_units"]
            cursors_map = checkpoints["cursors"]

            reconciled_states: Dict[str, str] = {}
            all_injected: List[str] = []
            still_unresolved: List[str] = []

            for op_id, rec in sorted(uncertain.items()):
                raw_probe = dict(probe_effect_fn(op_id, rec))
                sanitized_probe, injected = quarantine_untrusted_data(raw_probe)
                for pattern in injected:
                    if pattern not in all_injected:
                        all_injected.append(pattern)

                effect_state = str(sanitized_probe.get("state") or "")
                landed_units = sanitized_probe.get("landed_units") or {}
                if isinstance(landed_units, Mapping):
                    for unit_id, info in landed_units.items():
                        if isinstance(info, Mapping):
                            table_key = str(info.get("table") or "")
                            end_key = info.get("end_key")
                            rows_written = int(info.get("rows_written") or 0)
                        else:
                            table_key = str(rec.get("database") or "")
                            end_key = info
                            rows_written = 0
                        enc_end = encode_typed_cursor(end_key)
                        completed_map[str(unit_id)] = {
                            "operation_id": op_id,
                            "table": table_key,
                            "end_key": enc_end,
                            "rows_written": rows_written,
                            "committed_at": float(self.now_fn()),
                        }
                        if table_key:
                            cursors_map[table_key] = enc_end
                        self.ledger.unit_closed(str(unit_id), ok=True, at=float(self.now_fn()))

                if effect_state == "committed":
                    reconciled_states[op_id] = "committed"
                    self.ledger.own(
                        op_id,
                        state="committed",
                        verdict_status="done",
                        units_completed=list(completed_map.keys()),
                        reconciled_at=float(self.now_fn()),
                    )
                elif effect_state == "nothing":
                    reconciled_states[op_id] = "nothing"
                    failed_unit = rec.get("failed_unit")
                    if failed_unit:
                        self.ledger.unit_closed(
                            str(failed_unit),
                            ok=False,
                            reason="reconciled_not_landed",
                            at=float(self.now_fn()),
                        )
                    self.ledger.own(
                        op_id,
                        state="reconciled_nothing",
                        reconciled_at=float(self.now_fn()),
                    )
                else:
                    reconciled_states[op_id] = effect_state or "unknown"
                    still_unresolved.append(op_id)

            self._save_checkpoints(checkpoints)
            wf = self._load_workflow()
            for u in wf.units:
                if u.state == "failed-unknown" and not still_unresolved:
                    u.state = "pending"
                    u.why = "reconciled on target"
            wf.save()

            if still_unresolved:
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="error",
                    quarantined_injections=all_injected,
                    next_step=(
                        f"operations still running or unknown on target ({', '.join(still_unresolved)}); "
                        "wait and reconcile again before execute"
                    ),
                )
            else:
                st, word, code, means, badge, step = _map_verdict_semantics(
                    status_key="done",
                    quarantined_injections=all_injected,
                    next_step="all uncertain effects settled; safe to resume execute from durable cursor",
                )

            return AgentOperationResult(
                interface_version=req.interface_version,
                operation_id=req.operation_id,
                phase=AgentPhase.RECONCILE.value,
                goal=req.goal,
                admission=AdmissionVerdict.ADMITTED.value,
                chosen_rung=choice.path,
                rung_reason=choice.reason,
                verdict_status=st,
                verdict_word=word,
                exit_code=code,
                verdict_means=means,
                care_badge=badge,
                scope_fingerprint=self.contract_scope.fingerprint(),
                plan_id=contract.get("plan_id"),
                units_completed=tuple(sorted(completed_map.keys())),
                reconciled_operations=reconciled_states,
                resume_cursors={
                    k: decode_typed_cursor(v) for k, v in cursors_map.items()
                },
                quarantined_injections=tuple(all_injected),
                evidence={"reconciled": reconciled_states, "unresolved": still_unresolved},
                next_step=step,
            )
