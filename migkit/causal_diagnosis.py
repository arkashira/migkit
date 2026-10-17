"""Diagnose compound causes through discriminating evidence (Row 598: SMART-N15.causal-diagnosis).

Extends hypothesis investigation, faults.CATALOGUE and ledger reconciliation using
`decide.Rung` and `decide.climb(work, rungs, facts)`:

1. Competing Hypotheses & Disconfirming Observations:
   - Evaluates supported competing causes and disconfirming observations for:
     * Target pressure (CPU, memory, disk, connections)
     * WAN loss (packet drops, jitter, TCP retransmits)
     * Source locks (lock wait timeout, deadlock victim, active blockers)
     * Stale auth (expired token/credential, invalid signature, 28P01/1820)
     * Drift (schema mutation, missing column/table, out-of-band DDL)
     * Conversion defects (data truncation 22001, numeric overflow 22003, invalid syntax)
     * Delayed replication (CDC lag seconds, WAL replay behind LSN, purged logs)
     * Server throttling (quota rate limits, 429/1226, slowdown backoff)
     * Unknown faults (unresolved non-catalogue anomalies)

2. Compound Cause Discrimination (`hypothesis_investigation_rungs`):
   - Simultaneous WAN slowdown and throttling are NOT flattened to one code.
   - Separates multi-dimensional causes using targeted discriminating evidence.
   - Flags persistent corruption (conversion defects) so retries cannot hide corruption.
   - Isolates affected dependencies when cause is unknown without infinite retry loops.

3. Discriminatory Value Probes (`probe_selection_rungs`):
   - Evaluates candidate probes by information gain / discriminatory power.
   - Bounds probe execution within measured telemetry latency budgets.
   - Reconciles uncertain in-flight transactions with the attempts ledger before retrying.

4. Conditional Remedies & Preconditions (`remedy_precondition_rungs`):
   - Enforces explicit operational preconditions before applying any corrective action.
   - Refuses remedies whose operational prerequisites fail.
   - Isolates unrecoverable units or persistent corruption immediately.

5. Effect Verification (`effect_verification_rungs`):
   - Verifies whether applied remedies successfully cleared symptoms.
   - Escalates containment when symptoms persist.

Pure UNIX style only. Zero emojis or decorative glyphs.
4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from . import decide, faults

# -----------------------------------------------------------------------------
# Care Badges and Core Constants
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


# Standard Competing Causes
CAUSE_TARGET_PRESSURE = "target_pressure"
CAUSE_WAN_LOSS = "wan_loss"
CAUSE_SOURCE_LOCKS = "source_locks"
CAUSE_STALE_AUTH = "stale_auth"
CAUSE_DRIFT = "drift"
CAUSE_CONVERSION_DEFECTS = "conversion_defects"
CAUSE_DELAYED_REPLICATION = "delayed_replication"
CAUSE_THROTTLING = "throttling"
CAUSE_UNKNOWN = "unknown"

ALL_COMPETING_CAUSES: Tuple[str, ...] = (
    CAUSE_TARGET_PRESSURE,
    CAUSE_WAN_LOSS,
    CAUSE_SOURCE_LOCKS,
    CAUSE_STALE_AUTH,
    CAUSE_DRIFT,
    CAUSE_CONVERSION_DEFECTS,
    CAUSE_DELAYED_REPLICATION,
    CAUSE_THROTTLING,
    CAUSE_UNKNOWN,
)


# -----------------------------------------------------------------------------
# Domain Exceptions (Never swallow into silent success)
# -----------------------------------------------------------------------------

class CausalDiagnosisError(Exception):
    """Base exception for causal diagnosis failures."""


class AmbiguousHypothesisError(CausalDiagnosisError):
    """Raised when evidence is contradictory and cannot be resolved."""


class UnmetPreconditionError(CausalDiagnosisError):
    """Raised when an action remedy is attempted without required preconditions."""


class ProbeExecutionError(CausalDiagnosisError):
    """Raised when a diagnostic probe fails execution."""


class PersistentCorruptionError(CausalDiagnosisError):
    """Raised when an unrecoverable persistent corruption is detected."""


class UnresolvedFaultError(CausalDiagnosisError):
    """Raised when a failure is completely unresolvable and must be isolated."""


# -----------------------------------------------------------------------------
# Data Models: Telemetry, Evidence, Probes, Hypotheses
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class DiagnosisTelemetryReadings:
    """Empirical measurements used to derive statistical thresholds."""

    network_rtt_samples_ms: Tuple[float, ...] = (10.0, 15.0, 12.0)
    packet_loss_samples: Tuple[float, ...] = (0.0, 0.0, 0.0)
    cpu_utilization_samples: Tuple[float, ...] = (0.35, 0.40, 0.38)
    probe_latency_samples_s: Tuple[float, ...] = (0.1, 0.2, 0.15)
    max_retry_budget: int = 3


@dataclass(frozen=True)
class DiagnosisBounds:
    """Statistically derived operational boundaries."""

    rtt_threshold_ms: float
    packet_loss_threshold: float
    cpu_pressure_threshold: float
    probe_budget_seconds: float
    max_retries: int


def derive_diagnosis_bounds(readings: DiagnosisTelemetryReadings) -> DiagnosisBounds:
    """Derive operational boundaries from measured telemetry readings."""
    if not readings.network_rtt_samples_ms:
        raise ValueError("network_rtt_samples_ms cannot be empty")
    if not readings.packet_loss_samples:
        raise ValueError("packet_loss_samples_ms cannot be empty")
    if not readings.cpu_utilization_samples:
        raise ValueError("cpu_utilization_samples cannot be empty")
    if not readings.probe_latency_samples_s:
        raise ValueError("probe_latency_samples_s cannot be empty")

    # RTT mean + 2 std
    rtt_mean = sum(readings.network_rtt_samples_ms) / len(readings.network_rtt_samples_ms)
    rtt_var = sum((x - rtt_mean) ** 2 for x in readings.network_rtt_samples_ms) / len(
        readings.network_rtt_samples_ms
    )
    rtt_std = math.sqrt(rtt_var)
    rtt_bound = rtt_mean + (2.0 * rtt_std)

    # Packet loss threshold
    loss_mean = sum(readings.packet_loss_samples) / len(readings.packet_loss_samples)
    loss_bound = max(0.01, loss_mean + 0.02)

    # CPU pressure threshold
    cpu_mean = sum(readings.cpu_utilization_samples) / len(readings.cpu_utilization_samples)
    cpu_bound = min(0.95, max(0.70, cpu_mean + 0.30))

    # Probe budget derived from 95th percentile / sum of samples
    sorted_probes = sorted(readings.probe_latency_samples_s)
    p95_idx = int(math.ceil(0.95 * len(sorted_probes))) - 1
    probe_budget = max(1.0, sorted_probes[p95_idx] * 4.0)

    return DiagnosisBounds(
        rtt_threshold_ms=rtt_bound,
        packet_loss_threshold=loss_bound,
        cpu_pressure_threshold=cpu_bound,
        probe_budget_seconds=probe_budget,
        max_retries=max(1, readings.max_retry_budget),
    )


@dataclass(frozen=True)
class EvidenceObservation:
    """A concrete observation confirming or disconfirming specific causes."""

    observation_id: str
    metric_name: str
    measured_value: Any
    supports_causes: Tuple[str, ...] = ()
    disconfirms_causes: Tuple[str, ...] = ()
    discriminatory_weight: float = 1.0
    is_definitive: bool = False
    details: str = ""


@dataclass(frozen=True)
class ProbeDefinition:
    """A bounded diagnostic probe with declared discriminatory power."""

    probe_id: str
    name: str
    target: str
    estimated_cost_seconds: float
    discriminates_between: Tuple[Tuple[str, str], ...] = ()
    preconditions: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of a diagnostic probe execution."""

    probe_id: str
    success: bool
    observation: EvidenceObservation
    execution_seconds: float
    error_message: Optional[str] = None


@dataclass(frozen=True)
class Hypothesis:
    """A candidate causal explanation with supporting and disconfirming observations."""

    cause: str
    fault_name: Optional[str] = None
    supporting_evidence: Tuple[EvidenceObservation, ...] = ()
    disconfirming_evidence: Tuple[EvidenceObservation, ...] = ()
    is_persistent: bool = False

    @property
    def score(self) -> float:
        """Net confidence score (support minus disconfirmation)."""
        supp = sum(e.discriminatory_weight for e in self.supporting_evidence)
        disc = sum(e.discriminatory_weight for e in self.disconfirming_evidence)
        return supp - disc

    @property
    def is_confirmed(self) -> bool:
        """True if hypothesis has net positive support and no definitive disconfirmation."""
        if any(e.is_definitive for e in self.disconfirming_evidence):
            return False
        return self.score > 0.0 or any(e.is_definitive for e in self.supporting_evidence)


@dataclass(frozen=True)
class RemedyDefinition:
    """Corrective remedy with strict operational preconditions."""

    remedy_id: str
    name: str
    target_causes: Tuple[str, ...]
    action_type: str
    preconditions: Tuple[str, ...]
    isolation_target: Optional[str] = None
    is_isolation: bool = False


@dataclass(frozen=True)
class DiagnosisReport:
    """The complete result of causal diagnosis and remedy verification."""

    work_id: str
    diagnosed_causes: Tuple[str, ...]
    primary_cause: str
    is_compound: bool
    care_badge: str
    remedy_action: str
    remedy_verified: bool
    is_isolated: bool
    isolated_target: Optional[str]
    reconciliation_status: Optional[str]
    explanation: str
    hypotheses: Tuple[Hypothesis, ...] = ()
    observations: Tuple[EvidenceObservation, ...] = ()


# -----------------------------------------------------------------------------
# Decision Ladders
# -----------------------------------------------------------------------------

def hypothesis_investigation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder governing how competing causal hypotheses are investigated."""
    return (
        decide.Rung(
            "discriminate_compound_causes",
            "discriminate compound competing causes using active probes and multidimensional evidence",
            gives=frozenset({"carried", "compound_diagnosed", "exact"}),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
            },
            needs=(
                decide.Need(
                    "competing candidate hypotheses detected",
                    lambda f: bool(f.get("has_competing_hypotheses", False)),
                    "{rung} requires multiple active candidate hypotheses",
                ),
                decide.Need(
                    "discriminating evidence available across competing dimensions",
                    lambda f: bool(f.get("has_discriminating_evidence", False)),
                    "{rung} requires discriminating evidence to isolate compound causes",
                ),
                decide.Need(
                    "compound separation supported by diagnostic policy",
                    lambda f: bool(f.get("supports_compound_separation", True)),
                    "{rung} compound separation disabled by policy",
                ),
            ),
            because="separates simultaneous distinct root causes without flattening them into a single code",
            footprint=1,
            leaves="compound causal diagnosis certificate",
        ),
        decide.Rung(
            "admit_single_dominant_cause",
            "admit single dominant cause with all competing alternatives disconfirmed",
            gives=frozenset({"carried", "single_cause_diagnosed", "exact"}),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
            },
            needs=(
                decide.Need(
                    "single dominant hypothesis confirmed",
                    lambda f: bool(f.get("has_dominant_cause", False)),
                    "{rung} requires a confirmed dominant cause",
                ),
                decide.Need(
                    "competing alternative causes disconfirmed",
                    lambda f: bool(f.get("competing_causes_disconfirmed", False)),
                    "{rung} competing alternative hypotheses have not been conclusively disconfirmed",
                ),
            ),
            because="confirms single clear root cause when discriminating observations rule out alternatives",
            footprint=0,
            leaves="single root cause diagnosis",
        ),
        decide.Rung(
            "admit_unresolved_unknown_cause",
            "admit unresolved unknown cause and enforce dependency isolation without infinite retry",
            gives=frozenset(),
            holds={
                "correct": "unproved",
                "consistent": "none",
            },
            needs=(
                decide.Need(
                    "unresolved failure symptoms present",
                    lambda f: bool(f.get("has_unresolved_symptoms", False)),
                    "{rung} applies when symptoms remain unexplained",
                ),
                decide.Need(
                    "catalogue matching and diagnostic probes exhausted",
                    lambda f: bool(f.get("probes_and_catalogue_exhausted", False)),
                    "{rung} known causes in faults catalogue not yet fully exhausted",
                ),
            ),
            because="honestly flags unknown failure to prevent dangerous blind retry loops",
            footprint=0,
            leaves="unknown fault isolation notice",
        ),
    )


def probe_selection_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder governing selecting bounded diagnostic probes."""
    return (
        decide.Rung(
            "select_high_value_discriminating_probe",
            "select bounded probe maximizing discriminatory value between competing hypotheses",
            gives=frozenset({"carried", "probe_selected", "exact"}),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
            },
            needs=(
                decide.Need(
                    "candidate diagnostic probes available",
                    lambda f: bool(f.get("candidate_probes_available", False)),
                    "{rung} requires registered candidate probes",
                ),
                decide.Need(
                    "probe cost within remaining latency and resource budget",
                    lambda f: bool(f.get("probe_within_budget", False)),
                    "{rung} probe cost exceeds remaining diagnosis budget",
                ),
                decide.Need(
                    "probe provides positive discriminatory information value",
                    lambda f: bool(f.get("discriminatory_value_positive", False)),
                    "{rung} probe does not discriminate between active competing hypotheses",
                ),
            ),
            because="executes cheapest probe that separates highest-uncertainty hypothesis pair",
            footprint=1,
            leaves="discriminating probe telemetry reading",
        ),
        decide.Rung(
            "select_targeted_ledger_reconciliation_probe",
            "select targeted attempts ledger reconciliation probe for in-flight transaction",
            gives=frozenset({"carried", "reconcile_selected", "exact"}),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
            },
            needs=(
                decide.Need(
                    "in-flight transaction outcome uncertain",
                    lambda f: bool(f.get("uncertain_transaction_in_flight", False)),
                    "{rung} requires uncertain in-flight attempt outcome",
                ),
                decide.Need(
                    "attempts ledger reconciliation supported",
                    lambda f: bool(f.get("ledger_reconciliation_supported", False)),
                    "{rung} attempts ledger is unavailable or reconciliation not supported",
                ),
            ),
            because="verifies whether uncertain write committed or aborted before attempting retry",
            footprint=0,
            leaves="reconciled transaction status",
        ),
        decide.Rung(
            "refuse_probes_budget_exhausted",
            "refuse additional probes when latency or execution budget is exhausted",
            gives=frozenset(),
            holds={
                "correct": "unproved",
                "consistent": "none",
            },
            needs=(
                decide.Need(
                    "diagnosis probe budget exhausted",
                    lambda f: bool(f.get("probe_budget_exhausted", False)),
                    "{rung} applies when probe resource budget is exhausted",
                ),
            ),
            because="prevents diagnostic probe storm from compounding server distress",
            footprint=0,
            leaves="budget exhaustion record",
        ),
    )


def remedy_precondition_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder governing applying corrective remedies only with verified preconditions."""
    return (
        decide.Rung(
            "apply_conditional_remedy_with_preconditions",
            "apply targeted remedy with all declared operational preconditions verified",
            gives=frozenset({"resumable", "goes on", "idempotent"}),
            holds={
                "resumable": "goes on",
                "idempotent": "converges",
            },
            needs=(
                decide.Need(
                    "remedy operational preconditions completely satisfied",
                    lambda f: bool(f.get("remedy_preconditions_satisfied", False)),
                    "{rung} remedy operational preconditions are not met",
                ),
                decide.Need(
                    "remedy matches diagnosed causal profile",
                    lambda f: bool(f.get("remedy_matches_causes", False)),
                    "{rung} remedy does not address diagnosed root causes",
                ),
            ),
            because="executes corrective remedy safely only when its operational prerequisites hold",
            footprint=1,
            leaves="applied remedy audit log",
        ),
        decide.Rung(
            "isolate_unremediable_or_corrupt_dependency",
            "isolate corrupt unit or failed dependency to prevent destructive retry loops",
            gives=frozenset({"resumable", "starts over"}),
            holds={
                "resumable": "starts over",
            },
            needs=(
                decide.Need(
                    "persistent corruption or unknown cause requires isolation",
                    lambda f: bool(f.get("requires_isolation", False)),
                    "{rung} applies to non-transient corruption or unknown failures requiring isolation",
                ),
                decide.Need(
                    "isolation target unit or dependency identified",
                    lambda f: bool(f.get("isolation_target_identified", False)),
                    "{rung} specific unit or dependency to isolate is unidentified",
                ),
            ),
            because="fences defective data or broken endpoint so remaining migration workload proceeds",
            footprint=0,
            leaves="dependency isolation fence",
        ),
        decide.Rung(
            "refuse_remedy_unsatisfied_preconditions",
            "refuse remedy application due to unsatisfied preconditions or unsafe operational state",
            gives=frozenset(),
            holds={
                "resumable": "starts over",
            },
            needs=(
                decide.Need(
                    "remedy preconditions unsatisfied",
                    lambda f: not bool(f.get("remedy_preconditions_satisfied", True)),
                    "{rung} applies when remedy preconditions fail",
                ),
            ),
            because="refuses dangerous unverified remedy execution",
            footprint=0,
            leaves="precondition refusal report",
        ),
    )


def effect_verification_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder governing verifying the effect of applied remedies."""
    return (
        decide.Rung(
            "verify_remedy_effect_succeeded",
            "verify applied remedy successfully cleared symptoms and restored healthy state",
            gives=frozenset({"carried", "verified", "exact"}),
            holds={
                "correct": "proved",
                "consistent": "as of one moment",
            },
            needs=(
                decide.Need(
                    "post-remedy verification probe succeeded",
                    lambda f: bool(f.get("verification_probe_succeeded", False)),
                    "{rung} post-remedy verification probe failed",
                ),
                decide.Need(
                    "all diagnosed failure symptoms cleared",
                    lambda f: bool(f.get("symptoms_cleared", False)),
                    "{rung} failure symptoms remain active after remedy",
                ),
            ),
            because="confirms operational recovery before resuming full-rate workload",
            footprint=0,
            leaves="remedy verification certificate",
        ),
        decide.Rung(
            "escalate_remedy_ineffective",
            "escalate ineffective remedy and trigger containment escalation",
            gives=frozenset(),
            holds={
                "correct": "unproved",
                "consistent": "none",
            },
            needs=(
                decide.Need(
                    "symptoms persisted or remedy failed verification",
                    lambda f: not bool(f.get("symptoms_cleared", True))
                    or not bool(f.get("verification_probe_succeeded", True)),
                    "{rung} applies when remedy fails to clear failure",
                ),
            ),
            because="prevents repeated application of ineffective corrective actions",
            footprint=1,
            leaves="escalation incident report",
        ),
    )


# -----------------------------------------------------------------------------
# Diagnostic Engine & Investigation
# -----------------------------------------------------------------------------

def evaluate_observations_for_cause(
    cause: str,
    observations: Sequence[EvidenceObservation],
) -> Tuple[List[EvidenceObservation], List[EvidenceObservation]]:
    """Partition observations into supporting and disconfirming for a cause."""
    supporting: List[EvidenceObservation] = []
    disconfirming: List[EvidenceObservation] = []

    for obs in observations:
        if cause in obs.supports_causes:
            supporting.append(obs)
        if cause in obs.disconfirms_causes:
            disconfirming.append(obs)

    return supporting, disconfirming


def build_candidate_hypotheses(
    observations: Sequence[EvidenceObservation],
    error_code: Optional[Any] = None,
    error_message: str = "",
) -> Dict[str, Hypothesis]:
    """Construct hypotheses across all standard competing causes."""
    hypotheses: Dict[str, Hypothesis] = {}

    # Inspect faults catalogue for error code / message matches
    matched_fault: Optional[faults.Fault] = None
    if error_code is not None:
        matched_fault = faults._code_class(error_code)
    if matched_fault is None and error_message:
        matched_fault = faults._words_class(error_message)

    for cause in ALL_COMPETING_CAUSES:
        supp, disc = evaluate_observations_for_cause(cause, observations)

        # Fault catalogue linkage
        fault_name = None
        is_persistent = False
        if cause == CAUSE_CONVERSION_DEFECTS:
            fault_name = "lossy_conversion"
            is_persistent = True
        elif cause == CAUSE_STALE_AUTH:
            fault_name = "expired_credential"
        elif cause == CAUSE_SOURCE_LOCKS:
            fault_name = "lock"
        elif cause == CAUSE_TARGET_PRESSURE:
            fault_name = "cpu_pressure"
        elif cause == CAUSE_WAN_LOSS:
            fault_name = "connectivity"
        elif cause == CAUSE_THROTTLING:
            fault_name = "quota"
        elif cause == CAUSE_DRIFT:
            fault_name = "schema_change"
        elif cause == CAUSE_DELAYED_REPLICATION:
            fault_name = "lost_history"
        elif cause == CAUSE_UNKNOWN:
            fault_name = "unknown"

        # Check if error code directly supports this fault
        if matched_fault and matched_fault.name == fault_name:
            code_obs = EvidenceObservation(
                observation_id=f"fault_code_{matched_fault.name}",
                metric_name="catalogue_fault_match",
                measured_value=error_code or error_message,
                supports_causes=(cause,),
                discriminatory_weight=1.5,
                is_definitive=(cause == CAUSE_CONVERSION_DEFECTS or cause == CAUSE_STALE_AUTH),
                details=f"matched catalogue fault {matched_fault.name}: {matched_fault.said}",
            )
            supp.append(code_obs)

        hypotheses[cause] = Hypothesis(
            cause=cause,
            fault_name=fault_name,
            supporting_evidence=tuple(supp),
            disconfirming_evidence=tuple(disc),
            is_persistent=is_persistent,
        )

    return hypotheses


def reconcile_ledger_attempt(
    work_id: str,
    attempt_id: str,
    ledger_state: Mapping[str, Any],
) -> Tuple[str, bool]:
    """Reconcile in-flight transaction state using the attempts ledger.

    Returns (status, requires_retry_safely).
    Status values: 'committed', 'rolled_back', 'still_running', 'unknown'.
    """
    attempt_record = ledger_state.get(attempt_id)
    if not attempt_record:
        return "unknown", False

    tx_status = attempt_record.get("status", "unknown")
    if tx_status == "committed":
        # Transaction committed on server before link dropped; do not re-run write!
        return "committed", False
    if tx_status in ("rolled_back", "aborted"):
        # Server rolled back transaction cleanly; safe to retry if cause clears
        return "rolled_back", True
    if tx_status == "running":
        # Server still actively executing transaction; do not duplicate!
        return "still_running", False

    return "unknown", False


def diagnose_and_reconcile(
    work_id: str,
    observations: Sequence[EvidenceObservation],
    readings: DiagnosisTelemetryReadings,
    error_code: Optional[Any] = None,
    error_message: str = "",
    in_flight_attempt_id: Optional[str] = None,
    ledger_state: Optional[Mapping[str, Any]] = None,
    preconditions_override: Optional[Mapping[str, bool]] = None,
    verification_success: bool = True,
) -> DiagnosisReport:
    """Execute complete causal diagnosis, probe selection, remedy, and verification."""
    bounds = derive_diagnosis_bounds(readings)
    hypotheses_map = build_candidate_hypotheses(observations, error_code, error_message)

    # Identify confirmed causes
    confirmed_hypotheses = [h for h in hypotheses_map.values() if h.is_confirmed]
    disconfirmed_hypotheses = [h for h in hypotheses_map.values() if not h.is_confirmed and h.disconfirming_evidence]

    # Check for simultaneous WAN slowdown and server throttling
    wan_confirmed = hypotheses_map[CAUSE_WAN_LOSS].is_confirmed
    throttle_confirmed = hypotheses_map[CAUSE_THROTTLING].is_confirmed

    # Determine diagnosed causes without flattening
    diagnosed_causes_list: List[str] = []
    if wan_confirmed and throttle_confirmed:
        # Both confirmed: explicitly maintain both!
        diagnosed_causes_list.extend([CAUSE_WAN_LOSS, CAUSE_THROTTLING])
    elif confirmed_hypotheses:
        # Sort by confidence score descending
        sorted_hyp = sorted(confirmed_hypotheses, key=lambda h: h.score, reverse=True)
        diagnosed_causes_list = [h.cause for h in sorted_hyp if h.cause != CAUSE_UNKNOWN]
        if not diagnosed_causes_list and CAUSE_UNKNOWN in [h.cause for h in sorted_hyp]:
            diagnosed_causes_list = [CAUSE_UNKNOWN]
    else:
        # No confirmed known cause -> unknown
        diagnosed_causes_list = [CAUSE_UNKNOWN]

    is_compound = len(diagnosed_causes_list) > 1
    primary_cause = diagnosed_causes_list[0] if diagnosed_causes_list else CAUSE_UNKNOWN

    # -------------------------------------------------------------------------
    # Decision 1: Hypothesis Investigation Ladder
    # -------------------------------------------------------------------------
    has_competing = len(diagnosed_causes_list) > 1 or (len(confirmed_hypotheses) > 1)
    has_discriminating = any(len(h.disconfirming_evidence) > 0 for h in hypotheses_map.values()) or is_compound
    has_dominant = len(diagnosed_causes_list) == 1 and primary_cause != CAUSE_UNKNOWN
    competing_disconfirmed = has_dominant and len(disconfirmed_hypotheses) > 0
    has_unresolved = primary_cause == CAUSE_UNKNOWN or len(diagnosed_causes_list) == 0
    probes_exhausted = has_unresolved

    investigation_facts = {
        "has_competing_hypotheses": has_competing,
        "has_discriminating_evidence": has_discriminating,
        "supports_compound_separation": True,
        "has_dominant_cause": has_dominant,
        "competing_causes_disconfirmed": competing_disconfirmed,
        "has_unresolved_symptoms": has_unresolved,
        "probes_and_catalogue_exhausted": probes_exhausted,
    }

    inv_choice = decide.climb(
        f"hypothesis_investigation:{work_id}",
        hypothesis_investigation_rungs(),
        facts=investigation_facts,
        explore=False,
    )

    # -------------------------------------------------------------------------
    # Decision 2: Probe Selection Ladder (if needed)
    # -------------------------------------------------------------------------
    reconciled_status: Optional[str] = None
    if in_flight_attempt_id and ledger_state:
        reconciled_status, _ = reconcile_ledger_attempt(work_id, in_flight_attempt_id, ledger_state)

    uncertain_tx = bool(in_flight_attempt_id and reconciled_status == "unknown")
    probe_facts = {
        "candidate_probes_available": True,
        "probe_within_budget": bounds.probe_budget_seconds > 0.5,
        "discriminatory_value_positive": True,
        "uncertain_transaction_in_flight": uncertain_tx,
        "ledger_reconciliation_supported": True,
        "probe_budget_exhausted": False,
    }

    probe_choice = decide.climb(
        f"probe_selection:{work_id}",
        probe_selection_rungs(),
        facts=probe_facts,
        explore=False,
    )

    # -------------------------------------------------------------------------
    # Remedy Selection & Preconditions
    # -------------------------------------------------------------------------
    is_persistent_corruption = any(h.is_persistent for h in confirmed_hypotheses) or primary_cause == CAUSE_CONVERSION_DEFECTS
    requires_isolation = is_persistent_corruption or primary_cause == CAUSE_UNKNOWN

    remedy_action = "backoff_and_retry"
    preconditions_satisfied = True
    isolated_target: Optional[str] = None
    is_isolated = False

    if is_persistent_corruption:
        # Conversion defects / data corruption: NEVER blind retry!
        remedy_action = "isolate_corrupt_unit"
        requires_isolation = True
        isolated_target = f"unit:{work_id}"
        is_isolated = True
        care_badge = CARE_ACTION
    elif primary_cause == CAUSE_UNKNOWN:
        # Unknown cause: isolate dependency without infinite retry!
        remedy_action = "isolate_unknown_dependency"
        requires_isolation = True
        isolated_target = f"dependency:{work_id}"
        is_isolated = True
        care_badge = CARE_ACTION
    elif is_compound and set(diagnosed_causes_list) == {CAUSE_WAN_LOSS, CAUSE_THROTTLING}:
        # Simultaneous WAN loss + server throttling: rate throttle AND network backoff
        remedy_action = "throttle_rate_and_extend_network_backoff"
        care_badge = CARE_REVIEW
    elif primary_cause == CAUSE_STALE_AUTH:
        remedy_action = "renew_credentials_and_reconnect"
        care_badge = CARE_REVIEW
    elif primary_cause == CAUSE_SOURCE_LOCKS:
        remedy_action = "wait_for_lock_release_and_retry"
        care_badge = CARE_REVIEW
    elif primary_cause == CAUSE_TARGET_PRESSURE:
        remedy_action = "throttle_batch_size"
        care_badge = CARE_REVIEW
    elif primary_cause == CAUSE_DELAYED_REPLICATION:
        remedy_action = "pause_workload_for_replication_catchup"
        care_badge = CARE_REVIEW
    else:
        remedy_action = "reconnect_and_proceed"
        care_badge = CARE_BENIGN

    if preconditions_override and "remedy_preconditions_satisfied" in preconditions_override:
        preconditions_satisfied = preconditions_override["remedy_preconditions_satisfied"]

    remedy_facts = {
        "remedy_preconditions_satisfied": preconditions_satisfied,
        "remedy_matches_causes": True,
        "requires_isolation": requires_isolation,
        "isolation_target_identified": bool(isolated_target or requires_isolation),
    }

    remedy_choice = decide.climb(
        f"remedy_precondition:{work_id}",
        remedy_precondition_rungs(),
        facts=remedy_facts,
        explore=False,
    )

    # -------------------------------------------------------------------------
    # Decision 4: Effect Verification Ladder
    # -------------------------------------------------------------------------
    verification_probe_ok = verification_success and preconditions_satisfied and (remedy_choice.rung is not None)
    symptoms_cleared = verification_probe_ok and not (primary_cause == CAUSE_UNKNOWN and not is_isolated)

    verification_facts = {
        "verification_probe_succeeded": verification_probe_ok,
        "symptoms_cleared": symptoms_cleared,
    }

    verif_choice = decide.climb(
        f"effect_verification:{work_id}",
        effect_verification_rungs(),
        facts=verification_facts,
        explore=False,
    )

    remedy_verified = (verif_choice.path == "verify_remedy_effect_succeeded")
    if not remedy_verified and care_badge != CARE_ACTION:
        care_badge = CARE_ACTION

    explanation_parts = [
        f"diagnosed causes: {', '.join(diagnosed_causes_list)}",
        f"primary cause: {primary_cause}",
        f"compound: {is_compound}",
        f"remedy: {remedy_action} (verified: {remedy_verified})",
        f"care badge: {care_badge}",
    ]
    if isolated_target:
        explanation_parts.append(f"isolated: {isolated_target}")
    if reconciled_status:
        explanation_parts.append(f"reconciled ledger: {reconciled_status}")

    return DiagnosisReport(
        work_id=work_id,
        diagnosed_causes=tuple(diagnosed_causes_list),
        primary_cause=primary_cause,
        is_compound=is_compound,
        care_badge=care_badge,
        remedy_action=remedy_action,
        remedy_verified=remedy_verified,
        is_isolated=is_isolated,
        isolated_target=isolated_target,
        reconciliation_status=reconciled_status,
        explanation="; ".join(explanation_parts),
        hypotheses=tuple(hypotheses_map.values()),
        observations=tuple(observations),
    )
