"""Allocate recovery effort by progress and deadline value (Row 645: RCH37.failure-budget).

Extends classified recovery (`faults.classify`, `decide.recovery`) to allocate
retry, probe, reconcile, and repair budgets by remaining deadline,
irreversibility, progress rate,resource validity windows, and uncertainty:

- Budget allocation across four recovery dimensions (retry, probe, reconcile,
  repair) bounded by remaining deadline and the shortest active resource
  validity window so no retry loop can outlive all resource validity windows.
- Repeated same-state repairs are detected by state hash comparison and treated
  as a diagnosis failure (`ACTION`), never as success.
- Compound faults (two or more distinct fault classes in the same recovery
  epoch, such as credential expiry + WAN partition + schema drift) invalidate
  prior cost data (`decide.Costs` / cost model snapshots) so subsequent planning
  never relies on pre-fault throughput assumptions.
- O(1) typed durable checkpoint resume and active lease lock (`.run.lock`).
- Pure UNIX style output with 4-tier care badges: ACTION, REVIEW, BENIGN, PASS.
"""
from __future__ import annotations

import datetime
import decimal
import enum
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide, durable, faults


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class RecoveryStage(str, enum.Enum):
    RETRY = "retry"
    PROBE = "probe"
    RECONCILE = "reconcile"
    REPAIR = "repair"


class TerminationReason(str, enum.Enum):
    RECOVERED = "RECOVERED"
    DIAGNOSIS_FAILURE = "DIAGNOSIS_FAILURE"
    VALIDITY_WINDOW_EXPIRED = "VALIDITY_WINDOW_EXPIRED"
    DEADLINE_EXHAUSTED = "DEADLINE_EXHAUSTED"
    UNCERTAIN_UNRECONCILABLE = "UNCERTAIN_UNRECONCILABLE"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    UNRECOVERABLE_FAULT = "UNRECOVERABLE_FAULT"


class FailureBudgetError(RuntimeError):
    """Base error for failure budget allocation and execution."""


class DiagnosisFailureError(FailureBudgetError):
    """Raised when a repair leaves or returns the unit to an identical state."""


class LeaseLockHeldError(FailureBudgetError):
    """Raised when another active recovery controller holds the .run.lock lease."""


class UnsafeHostError(FailureBudgetError):
    """Raised when an endpoint host is outside approved safe ranges in test mode."""


SAFE_HOST_PREFIXES = ("127.", "10.0.0.", "localhost", "example.com")


def validate_safe_host(host: str) -> str:
    """Validates that a host string is safe and non-empty."""
    cleaned = str(host or "").strip()
    if not cleaned:
        raise UnsafeHostError("host must be non-empty")
    if not any(cleaned == p or cleaned.startswith(p) for p in SAFE_HOST_PREFIXES):
        raise UnsafeHostError(f"host {cleaned} is outside safe endpoint ranges")
    return cleaned


@dataclass(frozen=True)
class ResourceValidityWindow:
    """Validity window for a migration resource (credential, lease, snapshot, slot)."""
    name: str
    ttl_seconds: float
    renewable: bool = False
    max_renewals: int = 0
    renewal_cost_seconds: float = 1.0
    renewed_ttl_seconds: float = 0.0

    def effective_horizon(self, renewals_used: int = 0) -> float:
        """Maximum remaining lifetime of this resource including permitted renewals."""
        base = max(0.0, float(self.ttl_seconds))
        if not self.renewable or self.max_renewals <= renewals_used:
            return base
        remaining_renewals = self.max_renewals - renewals_used
        extra = remaining_renewals * max(0.0, float(self.renewed_ttl_seconds or self.ttl_seconds))
        return base + extra

    def immediate_horizon(self) -> float:
        """Remaining validity before the next renewal or expiry."""
        return max(0.0, float(self.ttl_seconds))


@dataclass(frozen=True)
class ProgressSnapshot:
    """Observed progress and cost metrics for a migration unit."""
    unit_id: str
    units_completed: int
    units_total: int
    elapsed_seconds: float
    deadline_seconds: float
    attempt_cost_seconds: float = 2.0
    probe_cost_seconds: float = 0.5
    reconcile_cost_seconds: float = 1.0
    repair_cost_seconds: float = 2.5
    irreversibility: float = 0.0
    uncertainty: float = 0.0

    @property
    def remaining_deadline(self) -> float:
        return max(0.0, float(self.deadline_seconds) - float(self.elapsed_seconds))

    @property
    def completion_ratio(self) -> float:
        if self.units_total <= 0:
            return 1.0
        return min(1.0, max(0.0, float(self.units_completed) / float(self.units_total)))

    @property
    def progress_rate(self) -> float:
        """Units completed per second."""
        if self.elapsed_seconds <= 0.0 or self.units_completed <= 0:
            return 0.0
        return float(self.units_completed) / float(self.elapsed_seconds)

    @property
    def units_remaining(self) -> int:
        return max(0, int(self.units_total) - int(self.units_completed))

    @property
    def estimated_remaining_work_seconds(self) -> float:
        rate = self.progress_rate
        if rate > 0.0:
            return float(self.units_remaining) / rate
        return float(self.units_remaining) * max(0.1, float(self.attempt_cost_seconds))


@dataclass(frozen=True)
class StageBudget:
    """Allocated effort budget for recovery stages."""
    retry_tries: int
    probe_tries: int
    reconcile_tries: int
    repair_tries: int
    max_total_seconds: float
    max_waste_seconds: float
    validity_ceiling_seconds: float
    strategy_path: str
    care_badge: CareBadge
    reason: str
    cost_data_invalidated: bool = False

    def total_tries(self) -> int:
        return (
            int(self.retry_tries)
            + int(self.probe_tries)
            + int(self.reconcile_tries)
            + int(self.repair_tries)
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "retry_tries": self.retry_tries,
            "probe_tries": self.probe_tries,
            "reconcile_tries": self.reconcile_tries,
            "repair_tries": self.repair_tries,
            "max_total_seconds": round(self.max_total_seconds, 3),
            "max_waste_seconds": round(self.max_waste_seconds, 3),
            "validity_ceiling_seconds": round(self.validity_ceiling_seconds, 3),
            "strategy_path": self.strategy_path,
            "care_badge": self.care_badge.value,
            "reason": self.reason,
            "cost_data_invalidated": self.cost_data_invalidated,
        }


@dataclass(frozen=True)
class RepairObservation:
    """Record of a single repair execution and the state before and after."""
    unit_id: str
    repair_action: str
    pre_state_hash: str
    post_state_hash: str
    took_seconds: float
    attempt_index: int


class RepairStateTracker:
    """Tracks state fingerprints across repairs to detect repeated same-state repairs.

    Treating a repeated same-state repair as success hides broken root-cause
    diagnosis. If a repair leaves the state unchanged (`pre_state_hash == post_state_hash`)
    or returns the unit to a state already repaired by the same action, it is
    flagged immediately as a diagnosis failure (`ACTION`).
    """

    def __init__(self) -> None:
        self._history: Dict[str, List[RepairObservation]] = {}
        self._seen_states: Dict[Tuple[str, str], Set[str]] = {}
        self._diagnosis_failures: Dict[str, str] = {}

    @staticmethod
    def fingerprint_state(state: Any) -> str:
        """Deterministic SHA-256 fingerprint of a unit's structural state."""
        canonical = json.dumps(state, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("ascii", errors="replace")).hexdigest()[:16]

    def record_repair(
        self,
        unit_id: str,
        repair_action: str,
        pre_state: Any,
        post_state: Any,
        took_seconds: float = 0.0,
    ) -> Tuple[bool, str]:
        """Records a repair outcome. Returns `(is_diagnosis_failure, explanation)`."""
        pre_hash = pre_state if isinstance(pre_state, str) else self.fingerprint_state(pre_state)
        post_hash = post_state if isinstance(post_state, str) else self.fingerprint_state(post_state)
        obs_list = self._history.setdefault(unit_id, [])
        seen = self._seen_states.setdefault((unit_id, repair_action), set())

        same_as_pre = pre_hash == post_hash
        already_repaired_from = pre_hash in seen or post_hash in seen
        seen.add(pre_hash)
        obs = RepairObservation(
            unit_id=unit_id,
            repair_action=repair_action,
            pre_state_hash=pre_hash,
            post_state_hash=post_hash,
            took_seconds=float(took_seconds),
            attempt_index=len(obs_list) + 1,
        )
        obs_list.append(obs)

        if same_as_pre:
            msg = (
                f"diagnosis failure on {unit_id}: repair {repair_action} left state"
                f" unchanged at {post_hash}"
            )
            self._diagnosis_failures[unit_id] = msg
            return True, msg
        if already_repaired_from:
            msg = (
                f"diagnosis failure on {unit_id}: repair {repair_action} revisited"
                f" prior state {post_hash} after {len(obs_list)} repairs"
            )
            self._diagnosis_failures[unit_id] = msg
            return True, msg
        return False, ""

    def repeat_count(self, unit_id: str) -> int:
        if unit_id in self._diagnosis_failures:
            return max(1, len(self._history.get(unit_id, ())))
        return 0

    def failure_reason(self, unit_id: str) -> str:
        return self._diagnosis_failures.get(unit_id, "")

    def history(self, unit_id: str) -> Tuple[RepairObservation, ...]:
        return tuple(self._history.get(unit_id, ()))


@dataclass
class CostLedger:
    """Tracks measured unit costs and invalidates them when compound faults strike."""
    costs: decide.Costs = field(default_factory=decide.Costs)
    generation: int = 0
    invalidated_epochs: List[Dict[str, Any]] = field(default_factory=list)

    def record(self, way: str, units: int, seconds: float) -> None:
        self.costs.saw(way, units, seconds)

    def get(self, way: str) -> Optional[float]:
        return self.costs.get(way)

    def invalidate(self, unit_id: str, fault_classes: Sequence[str], at_seconds: float) -> Dict[str, Any]:
        """Invalidates all prior cost measurements due to compound faults."""
        evicted = dict(self.costs)
        self.costs.clear()
        self.generation += 1
        entry = {
            "generation": self.generation,
            "unit_id": unit_id,
            "fault_classes": list(dict.fromkeys(fault_classes)),
            "evicted_ways": sorted(evicted.keys()),
            "at_seconds": round(float(at_seconds), 3),
            "reason": (
                "compound faults ("
                + ", ".join(dict.fromkeys(fault_classes))
                + ") invalidated prior cost measurements"
            ),
        }
        self.invalidated_epochs.append(entry)
        return entry


def recovery_budget_plan(facts: Union[Dict[str, Any], decide.Facts]) -> decide.Choice:
    """The `decide.Choice` selecting the recovery budget allocation mode.

    Climbs the failure budget ladder based on diagnosis health, validity windows,
    uncertainty, compound fault status, and progress value.
    """
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts or {}))
    rungs = (
        decide.Rung(
            "halt_diagnosis_failure",
            "stop immediately on repeated same-state repair diagnosis failure",
            because="a repair that leaves or revisits the same state proves the diagnosis is wrong",
            needs=(
                decide.Need(
                    "a repeated same-state repair observed",
                    lambda x: int(x.get("same_state_repair_repeats") or 0) > 0,
                    "no repeated same-state repair was observed",
                ),
            ),
        ),
        decide.Rung(
            "halt_validity_expired",
            "stop because remaining deadline or resource validity window is exhausted",
            because="no recovery loop may outlive its deadline or resource validity windows",
            needs=(
                decide.Need(
                    "no Diagnosis failure ahead",
                    lambda x: int(x.get("same_state_repair_repeats") or 0) == 0,
                    "a repeated same-state repair already triggered diagnosis failure",
                ),
                decide.Need(
                    "validity window shorter than one recovery step",
                    lambda x: float(x.get("effective_window_seconds") or 0.0)
                    < float(x.get("min_step_seconds") or 0.5),
                    "remaining deadline and resource validity windows still fit recovery steps",
                ),
            ),
        ),
        decide.Rung(
            "reconcile_uncertain",
            "allocate reconcile and probe budget before any retry or repair",
            because="an unknown write outcome must be reconciled on the target before replay",
            needs=(
                decide.Need(
                    "an uncertain write outcome with a way to check",
                    lambda x: bool(x.get("uncertain")) and bool(x.get("can_check")),
                    "the outcome is already known or no check is available",
                ),
                decide.Need(
                    "validity window fits reconciliation",
                    lambda x: float(x.get("effective_window_seconds") or 0.0)
                    >= float(x.get("min_step_seconds") or 0.5),
                    "remaining validity window cannot fit reconciliation",
                ),
            ),
        ),
        decide.Rung(
            "compound_replan_and_probe",
            "invalidate prior costs and allocate probe, reconcile, and replan budget",
            because="compound faults change link, auth, or schema conditions and invalidate prior cost data",
            needs=(
                decide.Need(
                    "compound faults with recovery admitted",
                    lambda x: bool(x.get("compound_fault")) and bool(x.get("admits_recovery")),
                    "fault is single-class or does not admit recovery",
                ),
                decide.Need(
                    "known or reconciled outcome",
                    lambda x: not bool(x.get("uncertain")) or bool(x.get("reconciled_known")),
                    "outcome is still uncertain and unreconciled",
                ),
                decide.Need(
                    "validity window fits compound recovery",
                    lambda x: float(x.get("effective_window_seconds") or 0.0)
                    >= float(x.get("min_step_seconds") or 0.5),
                    "remaining validity window cannot fit compound recovery",
                ),
            ),
        ),
        decide.Rung(
            "progress_weighted_recovery",
            "allocate retry, probe, reconcile, and repair budgets by progress and deadline value",
            because="positive progress and sufficient deadline headroom justify bounded recovery investment",
            needs=(
                decide.Need(
                    "single recoverable fault with viable progress value",
                    lambda x: bool(x.get("admits_recovery")) and bool(x.get("progress_viable")),
                    "fault does not admit recovery or progress rate cannot meet deadline",
                ),
                decide.Need(
                    "known or reconciled outcome",
                    lambda x: not bool(x.get("uncertain")) or bool(x.get("reconciled_known")),
                    "outcome is still uncertain and unreconciled",
                ),
                decide.Need(
                    "validity window fits recovery step",
                    lambda x: float(x.get("effective_window_seconds") or 0.0)
                    >= float(x.get("min_step_seconds") or 0.5),
                    "remaining validity window cannot fit recovery step",
                ),
            ),
        ),
        decide.Rung(
            "terminate_bounded",
            "stop cleanly with explainable state and zero further retry waste",
            because="recovery is not admitted, uncheckable while uncertain, or exceeds deadline value",
        ),
    )
    return decide.climb("recovery budget allocation", rungs, f)


def compute_validity_ceiling(
    progress: ProgressSnapshot,
    windows: Sequence[ResourceValidityWindow],
    renewals_used: Optional[Dict[str, int]] = None,
) -> Tuple[float, float, Optional[str]]:
    """Returns `(effective_ceiling, immediate_ceiling, limiting_resource_name)`.

    No retry loop can outlive the remaining deadline or the shortest active
    resource validity window.
    """
    renewals_used = renewals_used or {}
    deadline_left = progress.remaining_deadline
    if not windows:
        return deadline_left, deadline_left, "deadline"

    limiting_name = "deadline"
    effective_min = deadline_left
    immediate_min = deadline_left
    for w in windows:
        used = int(renewals_used.get(w.name, 0))
        eff = w.effective_horizon(used)
        imm = w.immediate_horizon()
        if eff < effective_min:
            effective_min = eff
            limiting_name = w.name
        if imm < immediate_min:
            immediate_min = imm
    return max(0.0, effective_min), max(0.0, immediate_min), limiting_name


def allocate_recovery_budget(
    fault_classes: Sequence[str],
    progress: ProgressSnapshot,
    windows: Sequence[ResourceValidityWindow] = (),
    can_check: bool = True,
    renewable_resources: Optional[Set[str]] = None,
    reconciled_state: Optional[str] = None,
    repair_tracker: Optional[RepairStateTracker] = None,
    cost_ledger: Optional[CostLedger] = None,
    renewals_used: Optional[Dict[str, int]] = None,
) -> Tuple[StageBudget, decide.Choice]:
    """Allocates retry, probe, reconcile, and repair budgets for one or more faults.

    Accounts for:
    - Remaining deadline and resource validity windows (`windows`).
    - Irreversibility (`progress.irreversibility` in [0, 1]).
    - Progress rate and sunk progress value (`progress.completion_ratio`, `progress.progress_rate`).
    - Uncertainty (`progress.uncertainty` in [0, 1] and fault classification).
    - Compound faults (multiple distinct fault classes invalidate `cost_ledger`).
    - Repeated same-state repairs (`repair_tracker`).
    """
    unique_faults = list(dict.fromkeys(f for f in fault_classes if f))
    if not unique_faults:
        unique_faults = ["unknown"]

    compound = len(unique_faults) > 1
    cost_invalidated = False
    if compound and cost_ledger is not None:
        cost_ledger.invalidate(progress.unit_id, unique_faults, progress.elapsed_seconds)
        cost_invalidated = True

    catalog_entries = [faults.CATALOGUE.get(name, faults.CATALOGUE["unknown"]) for name in unique_faults]
    uncertain = bool(progress.uncertainty >= 0.5) or any(
        f.name in ("uncertain_commit", "orphaned") for f in catalog_entries
    )
    reconciled_known = reconciled_state in (faults.COMMITTED, faults.NOTHING)
    if reconciled_known:
        uncertain = False

    renewable_set = set(renewable_resources or ())
    for w in windows:
        if w.renewable and w.max_renewals > int((renewals_used or {}).get(w.name, 0)):
            renewable_set.add(w.name)

    admits_recovery = True
    for f in catalog_entries:
        if f.name == "expired_credential":
            if "credential" not in renewable_set and not any(w.renewable for w in windows):
                admits_recovery = False
        elif f.Admits if hasattr(f, "Admits") else f.admits == ("stop",):
            admits_recovery = False

    if uncertain and not can_check:
        admits_recovery = False

    effective_ceiling, immediate_ceiling, limiting_resource = compute_validity_ceiling(
        progress, windows, renewals_used
    )
    min_step = min(
        max(0.1, progress.probe_cost_seconds),
        max(0.1, progress.reconcile_cost_seconds),
        max(0.1, progress.attempt_cost_seconds),
    )

    repeats = repair_tracker.repeat_count(progress.unit_id) if repair_tracker else 0

    # Progress viability: compare remaining work + recovery overhead against effective ceiling
    # Higher progress completion ratio increases the value of salvaging checkpoints,
    # while zero progress rate past half the deadline reduces retry appetite.
    progress_bonus = 0.25 + 0.75 * progress.completion_ratio
    rate_factor = 1.0 if progress.progress_rate > 0.0 else (0.5 if progress.elapsed_seconds < progress.deadline_seconds * 0.5 else 0.0)
    max_waste_share = min(0.6, max(0.1, progress_bonus * max(0.25, rate_factor)))
    max_waste_seconds = min(effective_ceiling, effective_ceiling * max_waste_share)
    progress_viable = (
        effective_ceiling >= min_step
        and max_waste_seconds >= min_step
        and (progress.progress_rate > 0.0 or progress.elapsed_seconds <= progress.deadline_seconds * 0.75)
    )

    facts = decide.Facts(
        {
            "unit_id": progress.unit_id,
            "fault_classes": tuple(unique_faults),
            "compound_fault": compound,
            "same_state_repair_repeats": repeats,
            "effective_window_seconds": effective_ceiling,
            "immediate_window_seconds": immediate_ceiling,
            "limiting_resource": limiting_resource,
            "min_step_seconds": min_step,
            "uncertain": uncertain,
            "can_check": bool(can_check),
            "reconciled_known": reconciled_known,
            "admits_recovery": admits_recovery,
            "progress_viable": progress_viable,
            "irreversibility": float(progress.irreversibility),
            "completion_ratio": progress.completion_ratio,
        }
    )
    choice = recovery_budget_plan(facts)
    path = choice.path

    if path == "halt_diagnosis_failure":
        why = (
            repair_tracker.failure_reason(progress.unit_id)
            if repair_tracker and repair_tracker.failure_reason(progress.unit_id)
            else f"repeated same-state repair on {progress.unit_id}"
        )
        return (
            StageBudget(
                retry_tries=0,
                probe_tries=0,
                reconcile_tries=0,
                repair_tries=0,
                max_total_seconds=0.0,
                max_waste_seconds=0.0,
                validity_ceiling_seconds=effective_ceiling,
                strategy_path=path,
                care_badge=CareBadge.ACTION,
                reason=why,
                cost_data_invalidated=cost_invalidated,
            ),
            choice,
        )

    if path in ("halt_validity_expired", "terminate_bounded"):
        why = (
            f"validity ceiling {effective_ceiling:.2f}s (limited by {limiting_resource})"
            f" cannot fit min step {min_step:.2f}s"
            if path == "halt_validity_expired"
            else f"recovery refused for {','.join(unique_faults)}: {choice.reason}"
        )
        return (
            StageBudget(
                retry_tries=0,
                probe_tries=0,
                reconcile_tries=0,
                repair_tries=0,
                max_total_seconds=0.0,
                max_waste_seconds=0.0,
                validity_ceiling_seconds=effective_ceiling,
                strategy_path=path,
                care_badge=CareBadge.ACTION,
                reason=why,
                cost_data_invalidated=cost_invalidated,
            ),
            choice,
        )

    irrev = min(1.0, max(0.0, float(progress.irreversibility)))
    uncert = min(1.0, max(0.0, float(progress.uncertainty) if not uncertain else max(0.6, float(progress.uncertainty))))

    if path == "reconcile_uncertain":
        reconcile_tries = max(1, min(3, int(max_waste_seconds // max(0.2, progress.reconcile_cost_seconds))))
        rem_after_rec = max(0.0, max_waste_seconds - reconcile_tries * progress.reconcile_cost_seconds)
        probe_tries = max(1, min(2, int(rem_after_rec // max(0.2, progress.probe_cost_seconds)))) if rem_after_rec >= progress.probe_cost_seconds else 0
        # No blind retries or repairs while state is still uncertain
        return (
            StageBudget(
                retry_tries=0,
                probe_tries=probe_tries,
                reconcile_tries=reconcile_tries,
                repair_tries=0,
                max_total_seconds=min(effective_ceiling, max_waste_seconds),
                max_waste_seconds=max_waste_seconds,
                validity_ceiling_seconds=effective_ceiling,
                strategy_path=path,
                care_badge=CareBadge.REVIEW,
                reason=(
                    f"uncertain effect on {progress.unit_id}; allocated"
                    f" reconcile={reconcile_tries}, probe={probe_tries}, retry=0"
                ),
                cost_data_invalidated=cost_invalidated,
            ),
            choice,
        )

    if path == "compound_replan_and_probe":
        # Compound faults invalidate prior cost data: prioritize probe + reconcile + repair (replan)
        # before a conservative bounded retry.
        budget_s = min(effective_ceiling, max_waste_seconds)
        probe_tries = 1 if budget_s >= progress.probe_cost_seconds else 0
        spent = probe_tries * progress.probe_cost_seconds
        reconcile_tries = 1 if (uncert > 0.0 or irrev > 0.3) and (budget_s - spent) >= progress.reconcile_cost_seconds else 0
        spent += reconcile_tries * progress.reconcile_cost_seconds
        repair_tries = 1 if (budget_s - spent) >= progress.repair_cost_seconds else 0
        spent += repair_tries * progress.repair_cost_seconds
        rem = max(0.0, budget_s - spent)
        retry_cap = 1 if irrev >= 0.7 else 2
        retry_tries = min(retry_cap, int(rem // max(0.2, progress.attempt_cost_seconds)))
        if retry_tries == 0 and repair_tries == 0 and probe_tries > 0 and budget_s >= progress.attempt_cost_seconds:
            retry_tries = 1
        return (
            StageBudget(
                retry_tries=retry_tries,
                probe_tries=probe_tries,
                reconcile_tries=reconcile_tries,
                repair_tries=repair_tries,
                max_total_seconds=budget_s,
                max_waste_seconds=max_waste_seconds,
                validity_ceiling_seconds=effective_ceiling,
                strategy_path=path,
                care_badge=CareBadge.REVIEW,
                reason=(
                    f"compound faults ({','.join(unique_faults)}) invalidated prior costs;"
                    f" allocated probe={probe_tries}, reconcile={reconcile_tries},"
                    f" repair={repair_tries}, retry={retry_tries} within {budget_s:.2f}s"
                ),
                cost_data_invalidated=True,
            ),
            choice,
        )

    # progress_weighted_recovery
    budget_s = min(effective_ceiling, max_waste_seconds)
    # High irreversibility shifts budget from blind retry to reconcile + probe
    reconcile_tries = 1 if (irrev >= 0.4 or uncert >= 0.3) and budget_s >= progress.reconcile_cost_seconds else 0
    spent = reconcile_tries * progress.reconcile_cost_seconds
    probe_tries = 1 if (irrev >= 0.5 or uncert >= 0.2 or any(f.name == "corrupted_evidence" for f in catalog_entries)) and (budget_s - spent) >= progress.probe_cost_seconds else 0
    spent += probe_tries * progress.probe_cost_seconds
    needs_repair = any("replan" in f.admits for f in catalog_entries)
    repair_tries = 1 if needs_repair and (budget_s - spent) >= progress.repair_cost_seconds else 0
    spent += repair_tries * progress.repair_cost_seconds
    rem = max(0.0, budget_s - spent)
    raw_retries = int(rem // max(0.2, progress.attempt_cost_seconds))
    irrev_cap = max(1, int(round(4 * (1.0 - 0.65 * irrev))))
    retry_tries = max(1 if rem >= progress.attempt_cost_seconds and any("retry" in f.admits or "pause" in f.admits or "throttle" in f.admits for f in catalog_entries) else 0, min(irrev_cap, raw_retries))

    return (
        StageBudget(
            retry_tries=retry_tries,
            probe_tries=probe_tries,
            reconcile_tries=reconcile_tries,
            repair_tries=repair_tries,
            max_total_seconds=budget_s,
            max_waste_seconds=max_waste_seconds,
            validity_ceiling_seconds=effective_ceiling,
            strategy_path=path,
            care_badge=CareBadge.PASS if retry_tries > 0 or repair_tries > 0 else CareBadge.REVIEW,
            reason=(
                f"progress-weighted budget for {','.join(unique_faults)}:"
                f" retry={retry_tries}, probe={probe_tries}, reconcile={reconcile_tries},"
                f" repair={repair_tries} within {budget_s:.2f}s (ceiling {effective_ceiling:.2f}s)"
            ),
            cost_data_invalidated=cost_invalidated,
        ),
        choice,
    )


# ---------------------------------------------------------------------------
# O(1) Typed Durable Checkpoint & Active Lease Lock (.run.lock)
# ---------------------------------------------------------------------------


def _encode_typed(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "value": [_encode_typed(v) for v in value]}
    if isinstance(value, list):
        return [_encode_typed(v) for v in value]
    if isinstance(value, dict):
        return {k: _encode_typed(v) for k, v in value.items()}
    return value


def _decode_typed(value: Any) -> Any:
    if isinstance(value, dict):
        kind = value.get("__type__")
        if kind == "datetime":
            return datetime.datetime.fromisoformat(value["value"])
        if kind == "uuid":
            return uuid.UUID(value["value"])
        if kind == "decimal":
            return decimal.Decimal(value["value"])
        if kind == "tuple":
            return tuple(_decode_typed(v) for v in value["value"])
        return {k: _decode_typed(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode_typed(v) for v in value]
    return value


class RecoveryBudgetCheckpoint:
    """Universal typed durable checkpoint with O(1) completed unit skipping."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self.completed_units: Set[str] = set()
        self.cursors: Dict[str, Any] = {}
        self.waste_ledger: Dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = durable.read_state(self.path, "state")
        if not raw:
            return
        self.completed_units = set(raw.get("completed_units") or ())
        self.cursors = _decode_typed(raw.get("cursors") or {})
        self.waste_ledger = {k: float(v) for k, v in (raw.get("waste_ledger") or {}).items()}

    def is_completed(self, unit_id: str) -> bool:
        """O(1) membership test for completed units."""
        return unit_id in self.completed_units

    def mark_completed(self, unit_id: str, cursor: Any, waste_seconds: float = 0.0) -> None:
        self.completed_units.add(unit_id)
        self.cursors[unit_id] = cursor
        self.waste_ledger[unit_id] = round(float(waste_seconds), 4)
        self.save()

    def get_cursor(self, unit_id: str) -> Any:
        return self.cursors.get(unit_id)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        durable.write_json(
            self.path,
            {
                "completed_units": sorted(self.completed_units),
                "cursors": _encode_typed(self.cursors),
                "waste_ledger": self.waste_ledger,
            },
            indent=1,
        )


class RecoveryLeaseLock:
    """Mutual exclusion .run.lock with heartbeat preventing concurrent recovery runs."""

    def __init__(
        self,
        work_dir: Union[str, Path],
        owner_id: str = "controller-1",
        ttl_seconds: float = 30.0,
        now_fn: Callable[[], float] = time.time,
    ) -> None:
        self.lock_path = Path(work_dir) / ".run.lock"
        self.owner_id = owner_id
        self.ttl_seconds = float(ttl_seconds)
        self._now = now_fn
        self._held = False

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        if self.lock_path.exists():
            try:
                existing = json.loads(self.lock_path.read_text(encoding="ascii"))
                expires_at = float(existing.get("expires_at") or 0.0)
                holder = existing.get("owner_id")
                if expires_at > now and holder != self.owner_id:
                    raise LeaseLockHeldError(
                        f"active lease lock {self.lock_path.name} held by {holder}"
                        f" until {expires_at:.2f}"
                    )
            except (ValueError, OSError):
                pass
        self._write_lease(now)
        self._held = True

    def heartbeat(self) -> None:
        if not self._held:
            raise LeaseLockHeldError("cannot heartbeat an unacquired lease lock")
        self._write_lease(self._now())

    def _write_lease(self, now: float) -> None:
        payload = {
            "owner_id": self.owner_id,
            "heartbeat_at": now,
            "expires_at": now + self.ttl_seconds,
            "pid": os.getpid(),
        }
        durable.write_json(self.lock_path, payload, indent=1)

    def release(self) -> None:
        if self._held and self.lock_path.exists():
            try:
                self.lock_path.unlink()
            except OSError:
                pass
        self._held = False

    def __enter__(self) -> RecoveryLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Compound Fault & Window-Bounded Recovery Executor
# ---------------------------------------------------------------------------


@dataclass
class RecoveryExecutionReport:
    """Explainable outcome of executing a budgeted recovery loop."""
    unit_id: str
    recovered: bool
    termination_reason: TerminationReason
    care_badge: CareBadge
    elapsed_seconds: float
    waste_seconds: float
    validity_ceiling_seconds: float
    allocated_budget: StageBudget
    faults_observed: List[str]
    compound_fault_detected: bool
    cost_data_invalidated: bool
    actions_taken: List[Dict[str, Any]]
    final_state_hash: str
    explanation: str

    def format_unix_line(self) -> str:
        """Pure UNIX terse summary line with 4-tier care badge and zero glyphs."""
        status = "RECOVERED" if self.recovered else self.termination_reason.value
        fault_str = ",".join(self.faults_observed) if self.faults_observed else "none"
        return (
            f"[{self.care_badge.value}] unit={self.unit_id} status={status}"
            f" faults={fault_str} compound={int(self.compound_fault_detected)}"
            f" cost_invalidated={int(self.cost_data_invalidated)}"
            f" elapsed_s={self.elapsed_seconds:.2f} waste_s={self.waste_seconds:.2f}"
            f" ceiling_s={self.validity_ceiling_seconds:.2f}"
            f" strategy={self.allocated_budget.strategy_path}"
            f" reason=\"{self.explanation}\""
        )


def execute_budgeted_recovery(
    unit_id: str,
    progress: ProgressSnapshot,
    windows: Sequence[ResourceValidityWindow],
    fault_sequence: Sequence[Sequence[str]],
    step_fn: Callable[[int, Sequence[str]], Tuple[bool, Any, float]],
    reconcile_fn: Optional[Callable[[], Tuple[str, float]]] = None,
    probe_fn: Optional[Callable[[], Tuple[bool, float]]] = None,
    repair_fn: Optional[Callable[[str, Any], Tuple[Any, float]]] = None,
    renew_fn: Optional[Callable[[str], Tuple[bool, float]]] = None,
    initial_state: Any = None,
    cost_ledger: Optional[CostLedger] = None,
    repair_tracker: Optional[RepairStateTracker] = None,
    checkpoint: Optional[RecoveryBudgetCheckpoint] = None,
    host: str = "127.0.0.1",
) -> RecoveryExecutionReport:
    """Executes a recovery loop strictly bounded by deadline and resource validity windows.

    Enforces:
    1. O(1) skip if `unit_id` is already marked complete in `checkpoint`.
    2. Compound fault cost invalidation whenever >1 distinct fault classes appear.
    3. Repeated same-state repair detection via `repair_tracker` -> immediate
       `DIAGNOSIS_FAILURE` (`ACTION`) termination.
    4. Hard validity bound: no retry, probe, reconcile, or repair step is started
       if its projected duration would exceed `remaining_deadline` or any
       non-renewable `ResourceValidityWindow` (or after renewals are exhausted).
    """
    validate_safe_host(host)
    repair_tracker = repair_tracker or RepairStateTracker()
    cost_ledger = cost_ledger or CostLedger()

    if checkpoint is not None and checkpoint.is_completed(unit_id):
        state_hash = repair_tracker.fingerprint_state(checkpoint.get_cursor(unit_id))
        noop_budget = StageBudget(
            retry_tries=0,
            probe_tries=0,
            reconcile_tries=0,
            repair_tries=0,
            max_total_seconds=0.0,
            max_waste_seconds=0.0,
            validity_ceiling_seconds=progress.remaining_deadline,
            strategy_path="cached_checkpoint_skip",
            care_badge=CareBadge.PASS,
            reason="unit skipped in O(1) from durable checkpoint",
            cost_data_invalidated=False,
        )
        return RecoveryExecutionReport(
            unit_id=unit_id,
            recovered=True,
            termination_reason=TerminationReason.RECOVERED,
            care_badge=CareBadge.PASS,
            elapsed_seconds=0.0,
            waste_seconds=0.0,
            validity_ceiling_seconds=progress.remaining_deadline,
            allocated_budget=noop_budget,
            faults_observed=[],
            compound_fault_detected=False,
            cost_data_invalidated=False,
            actions_taken=[{"action": "checkpoint_skip", "took_s": 0.0}],
            final_state_hash=state_hash,
            explanation="unit already completed in durable checkpoint",
        )

    active_windows = list(windows)
    renewals_used: Dict[str, int] = {}
    current_state = initial_state if initial_state is not None else {"unit": unit_id, "epoch": 0}
    elapsed = 0.0
    waste = 0.0
    all_faults: List[str] = []
    actions_taken: List[Dict[str, Any]] = []

    def advance_time(dt: float, is_waste: bool = True) -> None:
        nonlocal elapsed, waste, active_windows
        dt = max(0.0, float(dt))
        elapsed += dt
        if is_waste:
            waste += dt
        active_windows = [
            ResourceValidityWindow(
                name=w.name,
                ttl_seconds=max(0.0, w.ttl_seconds - dt),
                renewable=w.renewable,
                max_renewals=w.max_renewals,
                renewal_cost_seconds=w.renewal_cost_seconds,
                renewed_ttl_seconds=w.renewed_ttl_seconds,
            )
            for w in active_windows
        ]

    def check_and_renew_windows() -> Optional[str]:
        """Verifies all resource windows are valid, renewing expired renewable ones if within ceiling."""
        nonlocal active_windows
        for idx, w in enumerate(active_windows):
            if w.ttl_seconds > 0.0:
                continue
            used = renewals_used.get(w.name, 0)
            if w.renewable and used < w.max_renewals and renew_fn is not None:
                rem_deadline = max(0.0, progress.deadline_seconds - (progress.elapsed_seconds + elapsed))
                if rem_deadline < w.renewal_cost_seconds:
                    return f"resource {w.name} expired and renewal cost {w.renewal_cost_seconds:.2f}s exceeds deadline"
                ok, r_cost = renew_fn(w.name)
                advance_time(r_cost, is_waste=True)
                renewals_used[w.name] = used + 1
                actions_taken.append({"action": f"renew:{w.name}", "ok": ok, "took_s": round(r_cost, 3)})
                if not ok:
                    return f"renewal of resource {w.name} failed"
                new_ttl = w.renewed_ttl_seconds if w.renewed_ttl_seconds > 0 else 30.0
                active_windows[idx] = ResourceValidityWindow(
                    name=w.name,
                    ttl_seconds=new_ttl,
                    renewable=w.renewable,
                    max_renewals=w.max_renewals,
                    renewal_cost_seconds=w.renewal_cost_seconds,
                    renewed_ttl_seconds=w.renewed_ttl_seconds,
                )
            else:
                return f"resource validity window {w.name} expired (ttl=0.0s, renewals_used={used}/{w.max_renewals})"
        return None

    # Gather initial fault wave to allocate recovery budget
    initial_faults = list(fault_sequence[0]) if fault_sequence else []
    for f_name in initial_faults:
        if f_name not in all_faults:
            all_faults.append(f_name)

    cur_progress = ProgressSnapshot(
        unit_id=unit_id,
        units_completed=progress.units_completed,
        units_total=progress.units_total,
        elapsed_seconds=progress.elapsed_seconds + elapsed,
        deadline_seconds=progress.deadline_seconds,
        attempt_cost_seconds=progress.attempt_cost_seconds,
        probe_cost_seconds=progress.probe_cost_seconds,
        reconcile_cost_seconds=progress.reconcile_cost_seconds,
        repair_cost_seconds=progress.repair_cost_seconds,
        irreversibility=progress.irreversibility,
        uncertainty=progress.uncertainty,
    )

    budget, _ = allocate_recovery_budget(
        fault_classes=all_faults or ["connectivity"],
        progress=cur_progress,
        windows=active_windows,
        can_check=reconcile_fn is not None,
        repair_tracker=repair_tracker,
        cost_ledger=cost_ledger,
        renewals_used=renewals_used,
    )

    if budget.strategy_path == "halt_diagnosis_failure":
        return RecoveryExecutionReport(
            unit_id=unit_id,
            recovered=False,
            termination_reason=TerminationReason.DIAGNOSIS_FAILURE,
            care_badge=CareBadge.ACTION,
            elapsed_seconds=elapsed,
            waste_seconds=waste,
            validity_ceiling_seconds=budget.validity_ceiling_seconds,
            allocated_budget=budget,
            faults_observed=all_faults,
            compound_fault_detected=len(all_faults) > 1,
            cost_data_invalidated=budget.cost_data_invalidated,
            actions_taken=actions_taken,
            final_state_hash=repair_tracker.fingerprint_state(current_state),
            explanation=budget.reason,
        )

    if budget.strategy_path in ("halt_validity_expired", "terminate_bounded"):
        term = (
            TerminationReason.VALIDITY_WINDOW_EXPIRED
            if budget.strategy_path == "halt_validity_expired"
            else (
                TerminationReason.UNCERTAIN_UNRECONCILABLE
                if cur_progress.uncertainty > 0.0 and reconcile_fn is None
                else TerminationReason.UNRECOVERABLE_FAULT
            )
        )
        return RecoveryExecutionReport(
            unit_id=unit_id,
            recovered=False,
            termination_reason=term,
            care_badge=CareBadge.ACTION,
            elapsed_seconds=elapsed,
            waste_seconds=waste,
            validity_ceiling_seconds=budget.validity_ceiling_seconds,
            allocated_budget=budget,
            faults_observed=all_faults,
            compound_fault_detected=len(all_faults) > 1,
            cost_data_invalidated=budget.cost_data_invalidated,
            actions_taken=actions_taken,
            final_state_hash=repair_tracker.fingerprint_state(current_state),
            explanation=budget.reason,
        )

    retries_left = budget.retry_tries
    probes_left = budget.probe_tries
    reconciles_left = budget.reconcile_tries
    repairs_left = budget.repair_tries
    wave_idx = 0

    while True:
        # 1. Enforce resource validity windows before every step
        expired_msg = check_and_renew_windows()
        if expired_msg is not None:
            return RecoveryExecutionReport(
                unit_id=unit_id,
                recovered=False,
                termination_reason=TerminationReason.VALIDITY_WINDOW_EXPIRED,
                care_badge=CareBadge.ACTION,
                elapsed_seconds=elapsed,
                waste_seconds=waste,
                validity_ceiling_seconds=budget.validity_ceiling_seconds,
                allocated_budget=budget,
                faults_observed=all_faults,
                compound_fault_detected=len(all_faults) > 1,
                cost_data_invalidated=cost_ledger.generation > 0,
                actions_taken=actions_taken,
                final_state_hash=repair_tracker.fingerprint_state(current_state),
                explanation=expired_msg,
            )

        wave_faults = list(fault_sequence[wave_idx]) if wave_idx < len(fault_sequence) else []
        for f_name in wave_faults:
            if f_name not in all_faults:
                all_faults.append(f_name)

        if len(all_faults) > 1 and cost_ledger.generation == 0:
            cost_ledger.invalidate(unit_id, all_faults, progress.elapsed_seconds + elapsed)

        # Handle credential expiry in the current fault wave if renewable
        if "expired_credential" in wave_faults:
            cred_win = next((w for w in active_windows if "credential" in w.name), None)
            if cred_win is not None:
                # Force expiry check and renewal
                idx = active_windows.index(cred_win)
                active_windows[idx] = ResourceValidityWindow(
                    name=cred_win.name,
                    ttl_seconds=0.0,
                    renewable=cred_win.renewable,
                    max_renewals=cred_win.max_renewals,
                    renewal_cost_seconds=cred_win.renewal_cost_seconds,
                    renewed_ttl_seconds=cred_win.renewed_ttl_seconds,
                )
                exp_err = check_and_renew_windows()
                if exp_err is not None:
                    return RecoveryExecutionReport(
                        unit_id=unit_id,
                        recovered=False,
                        termination_reason=TerminationReason.VALIDITY_WINDOW_EXPIRED,
                        care_badge=CareBadge.ACTION,
                        elapsed_seconds=elapsed,
                        waste_seconds=waste,
                        validity_ceiling_seconds=budget.validity_ceiling_seconds,
                        allocated_budget=budget,
                        faults_observed=all_faults,
                        compound_fault_detected=len(all_faults) > 1,
                        cost_data_invalidated=cost_ledger.generation > 0,
                        actions_taken=actions_taken,
                        final_state_hash=repair_tracker.fingerprint_state(current_state),
                        explanation=exp_err,
                    )

        # 2. If reconcile is needed and budgeted, run reconcile first
        if reconciles_left > 0 and reconcile_fn is not None and (
            budget.strategy_path == "reconcile_uncertain" or "uncertain_commit" in wave_faults or progress.uncertainty > 0.0
        ):
            reconciles_left -= 1
            rec_state, rec_cost = reconcile_fn()
            advance_time(rec_cost, is_waste=True)
            actions_taken.append({"action": "reconcile", "state": rec_state, "took_s": round(rec_cost, 3)})
            if rec_state == faults.COMMITTED:
                if checkpoint is not None:
                    checkpoint.mark_completed(unit_id, current_state, waste)
                return RecoveryExecutionReport(
                    unit_id=unit_id,
                    recovered=True,
                    termination_reason=TerminationReason.RECOVERED,
                    care_badge=CareBadge.PASS,
                    elapsed_seconds=elapsed,
                    waste_seconds=waste,
                    validity_ceiling_seconds=budget.validity_ceiling_seconds,
                    allocated_budget=budget,
                    faults_observed=all_faults,
                    compound_fault_detected=len(all_faults) > 1,
                    cost_data_invalidated=cost_ledger.generation > 0,
                    actions_taken=actions_taken,
                    final_state_hash=repair_tracker.fingerprint_state(current_state),
                    explanation="reconciled uncertain write as committed on target",
                )
            if rec_state not in (faults.NOTHING, faults.RUNNING):
                return RecoveryExecutionReport(
                    unit_id=unit_id,
                    recovered=False,
                    termination_reason=TerminationReason.UNCERTAIN_UNRECONCILABLE,
                    care_badge=CareBadge.ACTION,
                    elapsed_seconds=elapsed,
                    waste_seconds=waste,
                    validity_ceiling_seconds=budget.validity_ceiling_seconds,
                    allocated_budget=budget,
                    faults_observed=all_faults,
                    compound_fault_detected=len(all_faults) > 1,
                    cost_data_invalidated=cost_ledger.generation > 0,
                    actions_taken=actions_taken,
                    final_state_hash=repair_tracker.fingerprint_state(current_state),
                    explanation="reconciliation could not settle uncertain write state",
                )

        # 3. If probe is needed (compound fault or connectivity/evidence fault) and budgeted
        if probes_left > 0 and probe_fn is not None and (
            len(all_faults) > 1 or "connectivity" in wave_faults or "corrupted_evidence" in wave_faults
        ):
            probes_left -= 1
            probe_ok, probe_cost = probe_fn()
            advance_time(probe_cost, is_waste=True)
            actions_taken.append({"action": "probe", "ok": probe_ok, "took_s": round(probe_cost, 3)})
            if not probe_ok and retries_left <= 0:
                return RecoveryExecutionReport(
                    unit_id=unit_id,
                    recovered=False,
                    termination_reason=TerminationReason.BUDGET_EXHAUSTED,
                    care_badge=CareBadge.ACTION,
                    elapsed_seconds=elapsed,
                    waste_seconds=waste,
                    validity_ceiling_seconds=budget.validity_ceiling_seconds,
                    allocated_budget=budget,
                    faults_observed=all_faults,
                    compound_fault_detected=len(all_faults) > 1,
                    cost_data_invalidated=cost_ledger.generation > 0,
                    actions_taken=actions_taken,
                    final_state_hash=repair_tracker.fingerprint_state(current_state),
                    explanation="probe failed and no retry budget remains",
                )

        # 4. If schema_change / replan fault is present, run repair/replan and check for same-state diagnosis failure
        if any(f in ("schema_change", "packet", "statement_timeout", "failover", "stale_checkpoint") for f in wave_faults):
            if repairs_left <= 0 or repair_fn is None:
                return RecoveryExecutionReport(
                    unit_id=unit_id,
                    recovered=False,
                    termination_reason=TerminationReason.BUDGET_EXHAUSTED,
                    care_badge=CareBadge.ACTION,
                    elapsed_seconds=elapsed,
                    waste_seconds=waste,
                    validity_ceiling_seconds=budget.validity_ceiling_seconds,
                    allocated_budget=budget,
                    faults_observed=all_faults,
                    compound_fault_detected=len(all_faults) > 1,
                    cost_data_invalidated=cost_ledger.generation > 0,
                    actions_taken=actions_taken,
                    final_state_hash=repair_tracker.fingerprint_state(current_state),
                    explanation="repair required for schema/plan drift but repair budget is exhausted",
                )
            repairs_left -= 1
            new_state, rep_cost = repair_fn("replan_schema", current_state)
            advance_time(rep_cost, is_waste=True)
            diag_failed, diag_msg = repair_tracker.record_repair(
                unit_id, "replan_schema", current_state, new_state, rep_cost
            )
            current_state = new_state
            actions_taken.append(
                {
                    "action": "repair:replan_schema",
                    "took_s": round(rep_cost, 3),
                    "diagnosis_failure": diag_failed,
                }
            )
            if diag_failed:
                return RecoveryExecutionReport(
                    unit_id=unit_id,
                    recovered=False,
                    termination_reason=TerminationReason.DIAGNOSIS_FAILURE,
                    care_badge=CareBadge.ACTION,
                    elapsed_seconds=elapsed,
                    waste_seconds=waste,
                    validity_ceiling_seconds=budget.validity_ceiling_seconds,
                    allocated_budget=budget,
                    faults_observed=all_faults,
                    compound_fault_detected=len(all_faults) > 1,
                    cost_data_invalidated=cost_ledger.generation > 0,
                    actions_taken=actions_taken,
                    final_state_hash=repair_tracker.fingerprint_state(current_state),
                    explanation=diag_msg,
                )

        # 5. Check that next execution attempt cannot outlive remaining deadline or any active resource window
        rem_deadline = max(0.0, progress.deadline_seconds - (progress.elapsed_seconds + elapsed))
        min_active_ttl = min((w.effective_horizon(renewals_used.get(w.name, 0)) for w in active_windows), default=rem_deadline)
        hard_ceiling = min(rem_deadline, min_active_ttl)
        if hard_ceiling < progress.attempt_cost_seconds:
            return RecoveryExecutionReport(
                unit_id=unit_id,
                recovered=False,
                termination_reason=TerminationReason.VALIDITY_WINDOW_EXPIRED,
                care_badge=CareBadge.ACTION,
                elapsed_seconds=elapsed,
                waste_seconds=waste,
                validity_ceiling_seconds=budget.validity_ceiling_seconds,
                allocated_budget=budget,
                faults_observed=all_faults,
                compound_fault_detected=len(all_faults) > 1,
                cost_data_invalidated=cost_ledger.generation > 0,
                actions_taken=actions_taken,
                final_state_hash=repair_tracker.fingerprint_state(current_state),
                explanation=(
                    f"stopping before retry: remaining validity ceiling {hard_ceiling:.2f}s"
                    f" is shorter than attempt cost {progress.attempt_cost_seconds:.2f}s"
                ),
            )

        if retries_left <= 0 and wave_idx > 0:
            return RecoveryExecutionReport(
                unit_id=unit_id,
                recovered=False,
                termination_reason=TerminationReason.BUDGET_EXHAUSTED,
                care_badge=CareBadge.ACTION,
                elapsed_seconds=elapsed,
                waste_seconds=waste,
                validity_ceiling_seconds=budget.validity_ceiling_seconds,
                allocated_budget=budget,
                faults_observed=all_faults,
                compound_fault_detected=len(all_faults) > 1,
                cost_data_invalidated=cost_ledger.generation > 0,
                actions_taken=actions_taken,
                final_state_hash=repair_tracker.fingerprint_state(current_state),
                explanation=f"retry budget ({budget.retry_tries}) exhausted after {wave_idx} waves",
            )

        if wave_idx > 0:
            retries_left -= 1

        ok, next_state, step_cost = step_fn(wave_idx, wave_faults)
        advance_time(step_cost, is_waste=not ok)
        current_state = next_state if next_state is not None else current_state
        actions_taken.append({"action": "attempt", "wave": wave_idx, "ok": ok, "took_s": round(step_cost, 3)})

        if ok:
            cost_ledger.record("post_recovery_way", max(1, progress.units_remaining), step_cost)
            if checkpoint is not None:
                checkpoint.mark_completed(unit_id, current_state, waste)
            return RecoveryExecutionReport(
                unit_id=unit_id,
                recovered=True,
                termination_reason=TerminationReason.RECOVERED,
                care_badge=CareBadge.PASS if not all_faults else CareBadge.REVIEW,
                elapsed_seconds=elapsed,
                waste_seconds=waste,
                validity_ceiling_seconds=budget.validity_ceiling_seconds,
                allocated_budget=budget,
                faults_observed=all_faults,
                compound_fault_detected=len(all_faults) > 1,
                cost_data_invalidated=cost_ledger.generation > 0,
                actions_taken=actions_taken,
                final_state_hash=repair_tracker.fingerprint_state(current_state),
                explanation=(
                    f"recovered after {len(actions_taken)} steps with"
                    f" {waste:.2f}s waste inside {budget.validity_ceiling_seconds:.2f}s ceiling"
                ),
            )

        wave_idx += 1
        if wave_idx > budget.total_tries() + 2:
            return RecoveryExecutionReport(
                unit_id=unit_id,
                recovered=False,
                termination_reason=TerminationReason.BUDGET_EXHAUSTED,
                care_badge=CareBadge.ACTION,
                elapsed_seconds=elapsed,
                waste_seconds=waste,
                validity_ceiling_seconds=budget.validity_ceiling_seconds,
                allocated_budget=budget,
                faults_observed=all_faults,
                compound_fault_detected=len(all_faults) > 1,
                cost_data_invalidated=cost_ledger.generation > 0,
                actions_taken=actions_taken,
                final_state_hash=repair_tracker.fingerprint_state(current_state),
                explanation="total stage budget exhausted without recovery",
            )
