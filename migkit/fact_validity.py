"""Make facts and decisions valid for a bounded context (Row 585: SMART-N2.fact-validity).

Extends Facts/factbook with:
- Provenance, observation time, server/config/schema epoch vector, confidence,
  and validity scope.
- Dependency map to selectively invalidate only decisions whose inputs changed
  after failover, DDL, permission, packet, quota, or network changes.
- Fresh re-read of critical inputs before irreversible steps (cutover,
  trigger drop, promotion, table truncation).
- Concurrent probe deduplication avoiding redundant source load.
- Dynamic bounding of probe time and source impact derived from measured facts.
- Automatic redaction of sensitive credentials, tokens, and secret values.
- All decisions routed through decide.Rung with decide.climb(work, rungs, facts).

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import fnmatch
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import decide, factbook


# -----------------------------------------------------------------------------
# Care Badges, Exceptions, and Enums
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class FactValidityError(RuntimeError):
    """Base exception for bounded context fact validity operations."""


class StaleFactError(FactValidityError, ValueError):
    """Raised when an operation requires a current fact but only a stale fact is available."""


class CrossContextLeakError(FactValidityError, ValueError):
    """Raised when a fact from a foreign bounded context is improperly accessed."""


class InvalidEpochMutationError(FactValidityError, ValueError):
    """Raised when an untracked or corrupted epoch transition is provided."""


class IrreversibleStepBlockedError(FactValidityError, RuntimeError):
    """Raised when an irreversible step cannot proceed due to stale inputs or detected drift."""


class ProbeExecutionError(FactValidityError, RuntimeError):
    """Raised when a fact probe fails or probe capacity is exceeded."""


class ValidityScope(str, enum.Enum):
    """Bounded context scope for facts and decisions."""
    SERVER = "server"
    DATABASE = "database"
    TABLE = "table"
    CONNECTION = "connection"
    STEP = "step"
    GLOBAL = "global"


class EpochKind(str, enum.Enum):
    """Categories of epoch shifts triggering contextual invalidation."""
    FAILOVER = "failover"
    DDL = "ddl"
    PERMISSION = "permission"
    PACKET = "packet"
    QUOTA = "quota"
    NETWORK = "network"


SENSITIVE_KEY_PATTERNS: Tuple[str, ...] = (
    "*password*",
    "*passwd*",
    "*secret*",
    "*token*",
    "*credential*",
    "*private_key*",
    "*auth*",
    "*api_key*",
)

REDACTED_TEXT: str = "[REDACTED]"


# -----------------------------------------------------------------------------
# Epoch Vectors and Fact Provenance
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class EpochVector:
    """Multi-dimensional epoch counter capturing system and schema boundaries."""
    context_id: str
    failover_epoch: int = 0
    ddl_epoch: int = 0
    permission_epoch: int = 0
    packet_epoch: int = 0
    quota_epoch: int = 0
    network_epoch: int = 0
    generation: int = 0

    def get_epoch(self, kind: Union[EpochKind, str]) -> int:
        k = kind.value if isinstance(kind, EpochKind) else str(kind)
        attr = f"{k}_epoch"
        if hasattr(self, attr):
            return int(getattr(self, attr))
        raise InvalidEpochMutationError(f"Unknown epoch kind: {kind}")

    def bump(self, kind: Union[EpochKind, str], delta: int = 1) -> EpochVector:
        if delta <= 0:
            raise InvalidEpochMutationError(f"Epoch bump delta must be positive, got {delta}")
        k = kind.value if isinstance(kind, EpochKind) else str(kind)
        attr = f"{k}_epoch"
        if not hasattr(self, attr):
            raise InvalidEpochMutationError(f"Cannot bump unknown epoch: {kind}")
        current = getattr(self, attr)
        updates = {
            attr: current + delta,
            "generation": self.generation + 1,
        }
        return dataclasses.replace(self, **updates)

    def is_compatible(self, other: Optional[EpochVector]) -> bool:
        if other is None:
            return False
        if self.context_id != other.context_id and self.context_id != "global" and other.context_id != "global":
            return False
        return (
            self.failover_epoch == other.failover_epoch
            and self.ddl_epoch == other.ddl_epoch
            and self.permission_epoch == other.permission_epoch
            and self.packet_epoch == other.packet_epoch
            and self.quota_epoch == other.quota_epoch
            and self.network_epoch == other.network_epoch
        )

    def diff(self, other: EpochVector) -> Set[EpochKind]:
        changed: Set[EpochKind] = set()
        for k in EpochKind:
            if self.get_epoch(k) != other.get_epoch(k):
                changed.add(k)
        return changed

    def as_dict(self) -> Dict[str, Any]:
        return {
            "context_id": self.context_id,
            "failover_epoch": self.failover_epoch,
            "ddl_epoch": self.ddl_epoch,
            "permission_epoch": self.permission_epoch,
            "packet_epoch": self.packet_epoch,
            "quota_epoch": self.quota_epoch,
            "network_epoch": self.network_epoch,
            "generation": self.generation,
        }


@dataclass(frozen=True)
class FactProvenance:
    """Lineage, observation timestamp, confidence, and scope boundary for a fact."""
    source: str
    how: str = "read"
    observed_at_s: float = field(default_factory=time.time)
    ttl_s: float = 300.0
    epoch_vector: Optional[EpochVector] = None
    confidence: float = 1.0
    validity_scope: str = "server"
    is_secret: bool = False

    def is_stale(self, now_s: Optional[float] = None, current_epoch: Optional[EpochVector] = None) -> bool:
        now = time.time() if now_s is None else float(now_s)
        if (now - self.observed_at_s) > self.ttl_s:
            return True
        if current_epoch is not None and self.epoch_vector is not None:
            if not self.epoch_vector.is_compatible(current_epoch):
                return True
        return False


def is_sensitive_key(key: str) -> bool:
    k = str(key).lower()
    return any(fnmatch.fnmatch(k, pat) for pat in SENSITIVE_KEY_PATTERNS)


def redact_value_if_needed(key: str, value: Any, is_secret: bool = False) -> Any:
    if is_secret or is_sensitive_key(key):
        return REDACTED_TEXT
    if isinstance(value, Mapping):
        return {k: redact_value_if_needed(str(k), v, is_secret) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value_if_needed(key, item, is_secret) for item in value]
    return value


@dataclass(frozen=True)
class ContextualFact:
    """Fact envelope carrying value, typed provenance, and contextual validity tests."""
    key: str
    value: Any
    provenance: FactProvenance

    def is_stale(self, now_s: Optional[float] = None, current_epoch: Optional[EpochVector] = None) -> bool:
        return self.provenance.is_stale(now_s, current_epoch)

    def is_valid_for_context(
        self,
        target_context_id: str,
        current_epoch: Optional[EpochVector] = None,
        now_s: Optional[float] = None,
        min_confidence: float = 0.5,
    ) -> bool:
        if self.provenance.validity_scope != ValidityScope.GLOBAL.value:
            if (
                self.provenance.epoch_vector is not None
                and self.provenance.epoch_vector.context_id != target_context_id
            ):
                return False
        if self.provenance.confidence < min_confidence:
            return False
        if self.is_stale(now_s, current_epoch):
            return False
        return True

    def redacted_value(self) -> Any:
        return redact_value_if_needed(self.key, self.value, self.provenance.is_secret)


# -----------------------------------------------------------------------------
# Decision Rung Ladders (Row 585 Deciders)
# -----------------------------------------------------------------------------

FACT_VALIDITY_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "trusted_in_bounded_context",
        "fact is valid, unexpired, and matches current epoch in bounded context",
        needs=(
            decide.Need(
                "fact scope matches bounded context",
                lambda f: bool(f.get("scope_matches", False)),
                "{rung} requires fact scope to match bounded context",
            ),
            decide.Need(
                "fact epoch vector matches current context epoch",
                lambda f: bool(f.get("epoch_compatible", False)),
                "{rung} requires compatible epoch vector",
            ),
            decide.Need(
                "fact observation time is within TTL window",
                lambda f: bool(f.get("ttl_fresh", False)),
                "{rung} requires observation time within TTL",
            ),
            decide.Need(
                "fact confidence meets or exceeds required threshold",
                lambda f: float(f.get("confidence", 0.0)) >= float(f.get("min_confidence", 0.5)),
                "{rung} requires confidence >= threshold",
            ),
        ),
        because="observation was captured in this bounded context under current epochs within TTL",
    ),
    decide.Rung(
        "refresh_stale_fact",
        "fact is within context but stale, commanding refresh probe",
        needs=(
            decide.Need(
                "fact is known to context but requires refresh due to expired TTL or epoch drift",
                lambda f: bool(f.get("is_stale", False)) and bool(f.get("can_refresh", False)),
                "{rung} requires a refreshable stale fact in context",
            ),
        ),
        because="stale facts cannot count as current proof and must be refreshed",
    ),
    decide.Rung(
        "refuse_cross_context_leak",
        "refuse fact originating outside authorized bounded context",
        needs=(
            decide.Need(
                "fact scope or origin originates outside authorized bounded context",
                lambda f: bool(f.get("cross_context_leak", False)),
                "{rung} requires cross-context mismatch or untrusted provenance",
            ),
        ),
        because="facts from foreign contexts must never leak into local decisions",
    ),
)


DEPENDENCY_INVALIDATION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "selective_invalidation_only",
        "selectively invalidate only decisions whose inputs changed after epoch mutation",
        needs=(
            decide.Need(
                "epoch transition is well-formed with tracked mutation types",
                lambda f: bool(f.get("valid_epoch_transition", False)),
                "{rung} requires well-formed tracked epoch mutation",
            ),
            decide.Need(
                "mutation impact is localized and not a catastrophic failover",
                lambda f: not bool(f.get("catastrophic_failover", False)),
                "{rung} requires localized non-catastrophic change",
            ),
            decide.Need(
                "dependency map is registered and covers affected domain",
                lambda f: bool(f.get("dependency_map_ready", False)),
                "{rung} requires active dependency map covering domain",
            ),
        ),
        because="selective invalidation preserves unrelated work without restarting whole migrations",
    ),
    decide.Rung(
        "context_wide_invalidation",
        "invalidate all facts and decisions in bounded context on catastrophic failover",
        needs=(
            decide.Need(
                "catastrophic failover or complete context reset triggered",
                lambda f: bool(f.get("catastrophic_failover", False)),
                "{rung} requires catastrophic failover or full reset event",
            ),
        ),
        because="catastrophic failover or unknown server switch invalidates entire context state",
    ),
    decide.Rung(
        "refuse_untracked_epoch_mutation",
        "refuse untracked or corrupted epoch mutation event",
        needs=(
            decide.Need(
                "epoch mutation event lacks valid context or has untracked corruption",
                lambda f: bool(f.get("untracked_mutation", False)),
                "{rung} requires untracked or corrupt epoch mutation event",
            ),
        ),
        because="untracked epoch mutations cannot safely guide selective or full invalidation",
    ),
)


CRITICAL_STEP_VERIFICATION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "proceed_irreversible_step",
        "proceed with irreversible step after fresh verification of all critical inputs",
        needs=(
            decide.Need(
                "all critical inputs are freshly verified under current epoch",
                lambda f: bool(f.get("critical_inputs_fresh", False)),
                "{rung} requires all critical inputs to be fresh under current epoch",
            ),
            decide.Need(
                "confidence on critical inputs is absolute",
                lambda f: float(f.get("critical_confidence", 0.0)) >= 1.0,
                "{rung} requires 1.0 confidence on critical inputs",
            ),
            decide.Need(
                "zero safety drift or precondition failures detected",
                lambda f: not bool(f.get("safety_drift_detected", False)),
                "{rung} requires zero safety drift or precondition mismatch",
            ),
            decide.Need(
                "no concurrent probes remain in-flight",
                lambda f: int(f.get("probes_in_flight", 0)) == 0,
                "{rung} requires zero in-flight probes",
            ),
        ),
        because="all critical inputs are verified fresh and concordant immediately prior to commitment",
    ),
    decide.Rung(
        "force_reread_critical_inputs",
        "force fresh re-read of critical inputs before allowing irreversible commitment",
        needs=(
            decide.Need(
                "step is irreversible and critical inputs require fresh observation",
                lambda f: bool(f.get("irreversible_step", False))
                and bool(f.get("has_stale_critical_inputs", False)),
                "{rung} requires irreversible step with stale or missing critical inputs",
            ),
        ),
        because="stale facts cannot count as proof before an irreversible migration step",
    ),
    decide.Rung(
        "refuse_irreversible_on_drift",
        "refuse irreversible step when fresh re-read detects drift, mismatch or failed precondition",
        needs=(
            decide.Need(
                "critical input re-read revealed unrecoverable drift or conflict",
                lambda f: bool(f.get("safety_drift_detected", False)),
                "{rung} requires detected safety drift or precondition failure",
            ),
        ),
        because="irreversible step must be blocked when underlying state has drifted from assumptions",
    ),
)


PROBE_DEDUPLICATION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "deduplicate_in_flight_probe",
        "await in-flight probe result to eliminate duplicate queries against source database",
        needs=(
            decide.Need(
                "identical probe is currently in-flight for this context and key",
                lambda f: bool(f.get("matching_probe_in_flight", False)),
                "{rung} requires identical in-flight probe",
            ),
        ),
        because="concurrent probes for the same fact are deduplicated to protect source from hammering",
    ),
    decide.Rung(
        "execute_bounded_probe",
        "execute fresh probe within measured source load and latency safety budget",
        needs=(
            decide.Need(
                "no matching probe is in-flight",
                lambda f: not bool(f.get("matching_probe_in_flight", False)),
                "{rung} requires no identical probe in flight",
            ),
            decide.Need(
                "source load and probe rate are within measured safety budget",
                lambda f: bool(f.get("source_load_safe", False)),
                "{rung} requires source load and rate within measured budget",
            ),
        ),
        because="probe is permitted when source load headroom allows safe execution",
    ),
    decide.Rung(
        "throttle_probe_budget_exceeded",
        "throttle or back off probes when measured source load exceeds safety budget",
        needs=(
            decide.Need(
                "source load or probe rate exceeds measured safety budget",
                lambda f: not bool(f.get("source_load_safe", False)),
                "{rung} requires source load or rate to exceed budget",
            ),
        ),
        because="source load must be bounded to prevent migration probing from starving production traffic",
    ),
)


# -----------------------------------------------------------------------------
# Source Load Tracking and Probe Deduplication
# -----------------------------------------------------------------------------

@dataclass
class SourceLoadTracker:
    """Tracks measured source database health and dynamically derives probe budgets."""
    measured_cpu_utilization: float = 0.25
    free_connections: int = 80
    measured_latency_ms: float = 3.5
    active_probes: int = 0
    max_connection_budget_share: float = 0.2
    cpu_safe_headroom: float = 0.85
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def derive_probe_concurrency_limit(self) -> int:
        """Derive maximum allowed concurrent probes from measured readings."""
        with self._lock:
            conn_budget = max(1, int(self.free_connections * self.max_connection_budget_share))
            cpu_headroom = max(0.01, self.cpu_safe_headroom - self.measured_cpu_utilization)
            limit = max(1, int(conn_budget * (cpu_headroom / self.cpu_safe_headroom)))
            return limit

    def is_safe_for_probe(self) -> bool:
        """Verify whether measured source state permits an additional probe."""
        with self._lock:
            if self.measured_cpu_utilization >= self.cpu_safe_headroom:
                return False
            limit = max(1, int(self.free_connections * self.max_connection_budget_share))
            if self.active_probes >= limit:
                return False
            return True

    def calculate_backoff_delay_s(self) -> float:
        """Compute dynamic backoff interval derived from measured query round-trip latency."""
        with self._lock:
            base_s = max(0.005, self.measured_latency_ms / 1000.0)
            overload_factor = max(1.0, (self.measured_cpu_utilization / max(0.1, 1.0 - self.cpu_safe_headroom)))
            return base_s * overload_factor


class ProbeDeduplicator:
    """Coordinates probe execution, deduplicates concurrent requests, and enforces bounds."""

    def __init__(self, load_tracker: Optional[SourceLoadTracker] = None) -> None:
        self.load_tracker = load_tracker or SourceLoadTracker()
        self._in_flight: Dict[Tuple[str, str], threading.Event] = {}
        self._results: Dict[Tuple[str, str], Any] = {}
        self._errors: Dict[Tuple[str, str], Exception] = {}
        self._lock = threading.Lock()
        self.probes_executed = 0
        self.probes_deduplicated = 0
        self.probes_throttled = 0

    def execute_or_deduplicate(
        self,
        context_id: str,
        fact_key: str,
        probe_fn: Callable[[], Any],
        timeout_s: float = 10.0,
    ) -> Any:
        probe_target = (context_id, fact_key)
        event_to_wait: Optional[threading.Event] = None
        is_initiator = False

        with self._lock:
            if probe_target in self._in_flight:
                event_to_wait = self._in_flight[probe_target]
            else:
                event = threading.Event()
                self._in_flight[probe_target] = event
                self._results.pop(probe_target, None)
                self._errors.pop(probe_target, None)
                event_to_wait = event
                is_initiator = True

        ladder_facts = decide.Facts({
            "matching_probe_in_flight": not is_initiator,
            "source_load_safe": self.load_tracker.is_safe_for_probe(),
        })

        choice = decide.climb(
            f"probe_dispatch:{context_id}:{fact_key}",
            PROBE_DEDUPLICATION_LADDER,
            ladder_facts,
        )

        if choice.rung is None or choice.rung.name == "throttle_probe_budget_exceeded":
            if is_initiator:
                with self._lock:
                    self._in_flight.pop(probe_target, None)
                    event_to_wait.set()
            with self._lock:
                self.probes_throttled += 1
            backoff = self.load_tracker.calculate_backoff_delay_s()
            raise ProbeExecutionError(
                f"Source probe budget exceeded for {fact_key} in {context_id}; "
                f"suggested backoff: {backoff:.3f}s (CPU: {self.load_tracker.measured_cpu_utilization:.2f})"
            )

        if not is_initiator:
            with self._lock:
                self.probes_deduplicated += 1
            completed = event_to_wait.wait(timeout=timeout_s)
            if not completed:
                raise ProbeExecutionError(f"Timed out awaiting deduplicated probe result for {fact_key}")
            with self._lock:
                if probe_target in self._errors:
                    raise self._errors[probe_target]
                return self._results.get(probe_target)

        # Initiator branch: execute the actual probe function
        with self.load_tracker._lock:
            self.load_tracker.active_probes += 1
        with self._lock:
            self.probes_executed += 1

        t0 = time.time()
        try:
            val = probe_fn()
            elapsed_ms = (time.time() - t0) * 1000.0
            with self.load_tracker._lock:
                self.load_tracker.measured_latency_ms = (
                    0.8 * self.load_tracker.measured_latency_ms + 0.2 * max(0.1, elapsed_ms)
                )
            with self._lock:
                self._results[probe_target] = val
            return val
        except Exception as exc:
            with self._lock:
                self._errors[probe_target] = exc
            raise
        finally:
            with self.load_tracker._lock:
                self.load_tracker.active_probes = max(0, self.load_tracker.active_probes - 1)
            with self._lock:
                self._in_flight.pop(probe_target, None)
                event_to_wait.set()


# -----------------------------------------------------------------------------
# Contextual Facts (Extends decide.Facts)
# -----------------------------------------------------------------------------

class ContextualFacts(decide.Facts):
    """Context-aware, epoch-tracked, and provenance-backed Facts dictionary."""

    def __init__(
        self,
        context_id: str = "default",
        epoch_vector: Optional[EpochVector] = None,
        known: Optional[Mapping[str, Any]] = None,
        deduplicator: Optional[ProbeDeduplicator] = None,
        default_ttl_s: float = 300.0,
        **readers: Callable[[], Any],
    ) -> None:
        super().__init__(known or {}, **readers)
        self.context_id = context_id
        self.epoch_vector = epoch_vector or EpochVector(context_id=context_id)
        self.default_ttl_s = default_ttl_s
        self._context_facts: Dict[str, ContextualFact] = {}
        self._deduplicator = deduplicator or ProbeDeduplicator()

        # Wrap existing known items in ContextualFact envelopes if not already wrapped
        if known:
            for k, v in known.items():
                if isinstance(v, ContextualFact):
                    self._context_facts[k] = v
                    super().__setitem__(k, v.value)
                else:
                    prov = FactProvenance(
                        source="given",
                        how="given",
                        observed_at_s=time.time(),
                        ttl_s=self.default_ttl_s,
                        epoch_vector=self.epoch_vector,
                        confidence=1.0,
                        validity_scope=ValidityScope.SERVER.value,
                        is_secret=is_sensitive_key(k),
                    )
                    self._context_facts[k] = ContextualFact(key=k, value=v, provenance=prov)

    def put_fact(
        self,
        key: str,
        value: Any,
        provenance: Optional[FactProvenance] = None,
    ) -> None:
        """Register or update a fact with explicit provenance."""
        prov = provenance or FactProvenance(
            source="caller",
            how="read",
            observed_at_s=time.time(),
            ttl_s=self.default_ttl_s,
            epoch_vector=self.epoch_vector,
            confidence=1.0,
            validity_scope=ValidityScope.SERVER.value,
            is_secret=is_sensitive_key(key),
        )
        ctx_fact = ContextualFact(key=key, value=value, provenance=prov)
        self._context_facts[key] = ctx_fact
        super().__setitem__(key, value)

    def get_contextual(self, key: str) -> Optional[ContextualFact]:
        return self._context_facts.get(key)

    def is_fact_valid(self, key: str, min_confidence: float = 0.5, now_s: Optional[float] = None) -> bool:
        """Evaluate fact validity in the bounded context using FACT_VALIDITY_LADDER."""
        cf = self._context_facts.get(key)
        if cf is None:
            return False

        now = time.time() if now_s is None else float(now_s)
        scope_matches = (
            cf.provenance.validity_scope == ValidityScope.GLOBAL.value
            or cf.provenance.epoch_vector is None
            or cf.provenance.epoch_vector.context_id == self.context_id
        )
        epoch_compatible = (
            cf.provenance.epoch_vector is None
            or cf.provenance.epoch_vector.is_compatible(self.epoch_vector)
        )
        ttl_fresh = not cf.provenance.is_stale(now_s=now, current_epoch=self.epoch_vector)
        is_stale = not ttl_fresh or not epoch_compatible

        check_facts = decide.Facts({
            "scope_matches": scope_matches,
            "epoch_compatible": epoch_compatible,
            "ttl_fresh": ttl_fresh,
            "confidence": cf.provenance.confidence,
            "min_confidence": min_confidence,
            "is_stale": is_stale,
            "can_refresh": key in self._readers,
            "cross_context_leak": not scope_matches,
        })

        choice = decide.climb(f"fact_validity:{self.context_id}:{key}", FACT_VALIDITY_LADDER, check_facts)
        return choice.rung is not None and choice.rung.name == "trusted_in_bounded_context"

    def __getitem__(self, key: str) -> Any:
        asked = self.__dict__.setdefault("asked", [])
        if key not in asked:
            asked.append(key)

        # Check existing contextual fact
        if key in self._context_facts:
            cf = self._context_facts[key]
            if self.is_fact_valid(key):
                return cf.value

            # Stale or epoch-invalidated: attempt refresh if reader exists
            if key in self._readers:
                val = self._deduplicator.execute_or_deduplicate(
                    self.context_id, key, self._readers[key]
                )
                self.put_fact(key, val)
                return val

            # No reader: stale facts cannot count as current proof
            raise StaleFactError(
                f"Fact '{key}' is stale in context '{self.context_id}' (observed {cf.provenance.observed_at_s:.1f}, "
                f"TTL {cf.provenance.ttl_s}s, epoch gen {cf.provenance.epoch_vector.generation if cf.provenance.epoch_vector else 0})"
            )

        # If not in _context_facts, check lazy readers
        if key in self._readers:
            val = self._deduplicator.execute_or_deduplicate(
                self.context_id, key, self._readers[key]
            )
            self.put_fact(key, val)
            return val

        return super().__getitem__(key)

    def checkpoint_asked(self) -> int:
        return len(getattr(self, "asked", []) or [])

    def asked_since(self, checkpoint: int) -> Tuple[str, ...]:
        current = getattr(self, "asked", []) or []
        return tuple(current[checkpoint:])

    def invalidate(self, key: str) -> None:
        """Remove a fact upon invalidation."""
        self._context_facts.pop(key, None)
        self.pop(key, None)

    def invalidate_keys(self, keys: Iterable[str]) -> None:
        for k in keys:
            self.invalidate(k)

    def bump_epoch(self, kind: EpochKind, delta: int = 1) -> EpochVector:
        self.epoch_vector = self.epoch_vector.bump(kind, delta)
        return self.epoch_vector

    def to_dict(self, redact: bool = True) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in self.items():
            if redact:
                cf = self._context_facts.get(k)
                is_sec = cf.provenance.is_secret if cf else is_sensitive_key(k)
                out[k] = redact_value_if_needed(k, v, is_sec)
            else:
                out[k] = v
        return out

    def account(self, redact: bool = True) -> Dict[str, Any]:
        base_acc = factbook.account(known=self.asked, factors=())
        provenance_summary = {}
        for k in self.asked:
            cf = self._context_facts.get(k)
            if cf:
                provenance_summary[k] = {
                    "source": cf.provenance.source,
                    "how": cf.provenance.how,
                    "confidence": cf.provenance.confidence,
                    "scope": cf.provenance.validity_scope,
                    "stale": cf.is_stale(current_epoch=self.epoch_vector),
                }
        base_acc["provenance"] = provenance_summary
        base_acc["context_id"] = self.context_id
        base_acc["epoch_vector"] = self.epoch_vector.as_dict()
        return base_acc


# -----------------------------------------------------------------------------
# Dependency Map and Selective Invalidation
# -----------------------------------------------------------------------------

DEFAULT_EPOCH_DOMAIN_MAP: Dict[EpochKind, Tuple[str, ...]] = {
    EpochKind.FAILOVER: (
        "*server*",
        "*primary*",
        "*replica*",
        "*host*",
        "*endpoint*",
        "*uuid*",
        "*system*",
        "*wal_position*",
        "*lsn*",
        "*binlog*",
        "*load*",
        "*free_connections*",
    ),
    EpochKind.DDL: (
        "*schema*",
        "*table*",
        "*column*",
        "*types*",
        "*partitions*",
        "*index*",
        "*fks*",
        "*triggers*",
        "*pk*",
        "*key*",
        "*views*",
        "*sequences*",
        "*unlogged*",
    ),
    EpochKind.PERMISSION: (
        "*grant*",
        "*role*",
        "*privilege*",
        "*acl*",
        "*authoriz*",
        "*backup_admin*",
        "*clone_admin*",
        "*owner*",
        "*connect*",
    ),
    EpochKind.PACKET: (
        "*packet*",
        "*max_allowed_packet*",
        "*chunk_size*",
        "*buffer_size*",
        "*frame*",
        "*mtu*",
    ),
    EpochKind.QUOTA: (
        "*quota*",
        "*rate_limit*",
        "*max_connections*",
        "*memory_limit*",
        "*throttle*",
        "*cpu_limit*",
        "*iops*",
    ),
    EpochKind.NETWORK: (
        "*network*",
        "*rtt*",
        "*bandwidth*",
        "*loss*",
        "*latency*",
        "*route*",
        "*gateway*",
        "*dns*",
        "*bastion*",
    ),
}


@dataclass(frozen=True)
class InvalidationReport:
    """Summary of selective invalidation execution."""
    context_id: str
    changed_epochs: Tuple[EpochKind, ...]
    invalidated_facts: Tuple[str, ...]
    retained_facts: Tuple[str, ...]
    invalidated_decisions: Tuple[str, ...]
    retained_decisions: Tuple[str, ...]
    care_badge: str
    choice_said: str


class DecisionDependencyMap:
    """Maintains fine-grained causal dependency graph between epochs, facts, and decisions."""

    def __init__(
        self,
        domain_patterns: Optional[Mapping[EpochKind, Sequence[str]]] = None,
    ) -> None:
        self._domain_patterns: Dict[EpochKind, Tuple[str, ...]] = dict(
            domain_patterns or DEFAULT_EPOCH_DOMAIN_MAP
        )
        self._decision_inputs: Dict[str, Set[str]] = {}
        self._fact_dependents: Dict[str, Set[str]] = collections.defaultdict(set)
        self._decision_epochs: Dict[str, EpochVector] = {}
        self._decision_irreversible: Dict[str, bool] = {}
        self._lock = threading.Lock()

    def record_decision(
        self,
        work_id: str,
        choice: decide.Choice,
        context_epochs: EpochVector,
        is_irreversible: bool = False,
        asked_facts: Optional[Iterable[str]] = None,
    ) -> None:
        """Register which facts were asked by a decision."""
        with self._lock:
            if asked_facts is not None:
                asked = set(str(k) for k in asked_facts)
            else:
                asked = set(str(k) for k in choice.asked)
            self._decision_inputs[work_id] = asked
            self._decision_epochs[work_id] = context_epochs
            self._decision_irreversible[work_id] = is_irreversible
            for f in asked:
                self._fact_dependents[f].add(work_id)

    def is_fact_affected_by_epochs(self, fact_key: str, changed_epochs: Set[EpochKind]) -> bool:
        fk = str(fact_key).lower()
        for epoch in changed_epochs:
            patterns = self._domain_patterns.get(epoch, ())
            if any(fnmatch.fnmatch(fk, pat) for pat in patterns):
                return True
        return False

    def selective_invalidate(
        self,
        context_id: str,
        changed_epochs: Set[EpochKind],
        current_epochs: EpochVector,
        facts: ContextualFacts,
        catastrophic_failover: bool = False,
    ) -> InvalidationReport:
        """Invalidate only decisions whose input facts changed."""
        with self._lock:
            valid_transition = bool(changed_epochs) and isinstance(current_epochs, EpochVector)
            untracked_mutation = not valid_transition or not context_id

            ladder_facts = decide.Facts({
                "valid_epoch_transition": valid_transition,
                "catastrophic_failover": catastrophic_failover,
                "dependency_map_ready": True,
                "untracked_mutation": untracked_mutation,
            })

            choice = decide.climb(
                f"dependency_invalidation:{context_id}",
                DEPENDENCY_INVALIDATION_LADDER,
                ladder_facts,
            )

            if choice.rung is None or choice.rung.name == "refuse_untracked_epoch_mutation":
                raise InvalidEpochMutationError(
                    f"Untracked or corrupted epoch mutation for context '{context_id}'"
                )

            all_known_facts = set(facts.keys()) | set(facts._context_facts.keys())
            all_known_decisions = set(self._decision_inputs.keys())

            if choice.rung.name == "context_wide_invalidation":
                # Full invalidation
                invalidated_facts = set(all_known_facts)
                retained_facts = set()
                invalidated_decisions = set(all_known_decisions)
                retained_decisions = set()
                care_badge = CARE_ACTION
            else:
                # Selective invalidation
                invalidated_facts = {
                    fk for fk in all_known_facts
                    if self.is_fact_affected_by_epochs(fk, changed_epochs)
                }
                retained_facts = all_known_facts - invalidated_facts

                invalidated_decisions = set()
                for fk in invalidated_facts:
                    invalidated_decisions.update(self._fact_dependents.get(fk, set()))

                retained_decisions = all_known_decisions - invalidated_decisions

                has_irreversible_invalidated = any(
                    self._decision_irreversible.get(d, False) for d in invalidated_decisions
                )
                if has_irreversible_invalidated:
                    care_badge = CARE_ACTION
                elif invalidated_decisions:
                    care_badge = CARE_REVIEW
                else:
                    care_badge = CARE_BENIGN

            # Apply invalidation to facts
            facts.invalidate_keys(invalidated_facts)

            # Purge invalidated decisions from dependency map
            for d in invalidated_decisions:
                self._decision_inputs.pop(d, None)
                self._decision_epochs.pop(d, None)
                self._decision_irreversible.pop(d, None)
            for fk in invalidated_facts:
                self._fact_dependents.pop(fk, None)

            return InvalidationReport(
                context_id=context_id,
                changed_epochs=tuple(sorted(changed_epochs, key=lambda e: e.value)),
                invalidated_facts=tuple(sorted(invalidated_facts)),
                retained_facts=tuple(sorted(retained_facts)),
                invalidated_decisions=tuple(sorted(invalidated_decisions)),
                retained_decisions=tuple(sorted(retained_decisions)),
                care_badge=care_badge,
                choice_said=choice.said,
            )


# -----------------------------------------------------------------------------
# Re-Read Critical Inputs Before Irreversible Steps
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class StepVerificationResult:
    """Certification verdict before irreversible migration operations."""
    step_name: str
    approved: bool
    care_badge: str
    fresh_critical_facts: Dict[str, Any]
    reason: str
    choice: decide.Choice


class CriticalStepVerifier:
    """Verifies critical preconditions and enforces fresh re-reads before irreversible steps."""

    def __init__(self, dependency_map: Optional[DecisionDependencyMap] = None) -> None:
        self.dependency_map = dependency_map or DecisionDependencyMap()

    def verify_before_irreversible_step(
        self,
        step_name: str,
        critical_keys: Sequence[str],
        facts: ContextualFacts,
        current_epochs: EpochVector,
        probe_readers: Optional[Mapping[str, Callable[[], Any]]] = None,
        expected_preconditions: Optional[Mapping[str, Any]] = None,
    ) -> StepVerificationResult:
        """Exhaustively verify and re-read all critical inputs before commitment."""
        probe_readers = probe_readers or {}
        expected_preconditions = expected_preconditions or {}

        # 1. Identify stale or missing critical inputs
        stale_or_missing = []
        for k in critical_keys:
            if not facts.is_fact_valid(k):
                stale_or_missing.append(k)

        first_stage_facts = decide.Facts({
            "irreversible_step": True,
            "has_stale_critical_inputs": bool(stale_or_missing),
            "critical_inputs_fresh": not bool(stale_or_missing),
            "critical_confidence": 1.0 if not stale_or_missing else 0.0,
            "safety_drift_detected": False,
            "probes_in_flight": facts._deduplicator.load_tracker.active_probes,
        })

        first_choice = decide.climb(
            f"critical_step_check:{step_name}",
            CRITICAL_STEP_VERIFICATION_LADDER,
            first_stage_facts,
        )

        # 2. Force fresh re-read if required
        fresh_values: Dict[str, Any] = {}
        if first_choice.rung and first_choice.rung.name == "force_reread_critical_inputs":
            for k in critical_keys:
                reader = probe_readers.get(k) or facts._readers.get(k)
                if reader is None:
                    raise StaleFactError(
                        f"Cannot re-read critical input '{k}' for irreversible step '{step_name}': no reader provided"
                    )
                fresh_val = facts._deduplicator.execute_or_deduplicate(
                    facts.context_id, k, reader
                )
                facts.put_fact(
                    k,
                    fresh_val,
                    FactProvenance(
                        source=f"re_read:{step_name}",
                        how="measured",
                        observed_at_s=time.time(),
                        ttl_s=facts.default_ttl_s,
                        epoch_vector=current_epochs,
                        confidence=1.0,
                        validity_scope=ValidityScope.STEP.value,
                        is_secret=is_sensitive_key(k),
                    ),
                )
                fresh_values[k] = fresh_val
        else:
            for k in critical_keys:
                fresh_values[k] = facts[k]

        # 3. Detect drift against safety preconditions
        drift_detected = False
        drift_reasons = []
        for k, expected in expected_preconditions.items():
            actual = fresh_values.get(k)
            if actual != expected:
                drift_detected = True
                drift_reasons.append(f"fact '{k}' drifted from expected {expected!r} to actual {actual!r}")

        # 4. Final verification gate
        final_stage_facts = decide.Facts({
            "irreversible_step": True,
            "has_stale_critical_inputs": False,
            "critical_inputs_fresh": True,
            "critical_confidence": 1.0,
            "safety_drift_detected": drift_detected,
            "probes_in_flight": facts._deduplicator.load_tracker.active_probes,
        })

        final_choice = decide.climb(
            f"critical_step_commitment:{step_name}",
            CRITICAL_STEP_VERIFICATION_LADDER,
            final_stage_facts,
        )

        approved = final_choice.rung is not None and final_choice.rung.name == "proceed_irreversible_step"
        care_badge = CARE_PASS if approved else CARE_ACTION
        reason = (
            "All critical inputs freshly verified under current epoch with zero drift"
            if approved
            else f"Blocked irreversible step: {'; '.join(drift_reasons) if drift_reasons else final_choice.reason}"
        )

        if not approved:
            raise IrreversibleStepBlockedError(reason)

        return StepVerificationResult(
            step_name=step_name,
            approved=approved,
            care_badge=care_badge,
            fresh_critical_facts=fresh_values,
            reason=reason,
            choice=final_choice,
        )
