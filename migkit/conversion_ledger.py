"""Scope-distinct conversion ledger (RCH18.conversion-ledger).

Maintains stable identities for code/ETL/application artefacts and keeps
submitted, extracted, parsed, converted, deployed, executed, and covered
counts strictly distinct.

Unsupported encodings, encrypted code, and dynamic SQL remain explicit debts
with explicit care badges (ACTION, REVIEW, BENIGN, PASS).
Every metric reports both numerator and denominator; no conversion percentage
can hide excluded or unparsed objects.
"""
from __future__ import annotations

import enum
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from migkit import decide, durable


class ConversionLedgerError(Exception):
    """Base exception for conversion ledger failures."""
    pass


class CorruptLockError(ConversionLedgerError):
    """Raised when an existing lease/lock file cannot be parsed, to prevent clobbering."""
    pass


class LockAcquisitionError(ConversionLedgerError):
    """Raised when lease/lock file cannot be read or acquired."""
    pass


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ArtefactKind(str, enum.Enum):
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    VIEW = "VIEW"
    TRIGGER = "TRIGGER"
    PACKAGE = "PACKAGE"
    PACKAGE_BODY = "PACKAGE_BODY"
    ETL_JOB = "ETL_JOB"
    APP_QUERY = "APP_QUERY"
    DYNAMIC_SQL_FRAGMENT = "DYNAMIC_SQL_FRAGMENT"
    DDL_SCRIPT = "DDL_SCRIPT"


class ArtefactCriticality(str, enum.Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class LifecycleStage(str, enum.Enum):
    SUBMITTED = "submitted"
    EXTRACTED = "extracted"
    PARSED = "parsed"
    CONVERTED = "converted"
    DEPLOYED = "deployed"
    EXECUTED = "executed"
    COVERED = "covered"


class DebtType(str, enum.Enum):
    UNSUPPORTED_ENCODING = "unsupported_encoding"
    ENCRYPTED_CODE = "encrypted_code"
    DYNAMIC_SQL = "dynamic_sql"
    SYNTAX_ERROR = "syntax_error"
    UNRESOLVED_DEPENDENCY = "unresolved_dependency"
    CONVERSION_REFUSED = "conversion_refused"
    VERIFICATION_FAILED = "verification_failed"


# Strict linear progression order
STAGE_ORDER: List[LifecycleStage] = [
    LifecycleStage.SUBMITTED,
    LifecycleStage.EXTRACTED,
    LifecycleStage.PARSED,
    LifecycleStage.CONVERTED,
    LifecycleStage.DEPLOYED,
    LifecycleStage.EXECUTED,
    LifecycleStage.COVERED,
]


class DebtRecord:
    def __init__(
        self,
        debt_id: str,
        artefact_id: str,
        debt_type: DebtType,
        care_badge: CareBadge,
        reason: str,
        offset_or_symbol: Optional[str] = None,
        remediation_hint: Optional[str] = None,
        resolved: bool = False,
        resolved_at: Optional[float] = None,
        created_at: Optional[float] = None,
    ):
        self.debt_id = debt_id
        self.artefact_id = artefact_id
        self.debt_type = debt_type
        self.care_badge = care_badge
        self.reason = reason
        self.offset_or_symbol = offset_or_symbol
        self.remediation_hint = remediation_hint
        self.resolved = resolved
        self.resolved_at = resolved_at
        self.created_at = created_at if created_at is not None else time.time()

    def resolve(self, note: Optional[str] = None) -> None:
        self.resolved = True
        self.resolved_at = time.time()
        if note:
            self.remediation_hint = note

    def to_dict(self) -> Dict[str, Any]:
        return {
            "debt_id": self.debt_id,
            "artefact_id": self.artefact_id,
            "debt_type": self.debt_type.value,
            "care_badge": self.care_badge.value,
            "reason": self.reason,
            "offset_or_symbol": self.offset_or_symbol,
            "remediation_hint": self.remediation_hint,
            "resolved": self.resolved,
            "resolved_at": self.resolved_at,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DebtRecord:
        return cls(
            debt_id=str(data["debt_id"]),
            artefact_id=str(data["artefact_id"]),
            debt_type=DebtType(data["debt_type"]),
            care_badge=CareBadge(data["care_badge"]),
            reason=str(data["reason"]),
            offset_or_symbol=data.get("offset_or_symbol"),
            remediation_hint=data.get("remediation_hint"),
            resolved=bool(data.get("resolved", False)),
            resolved_at=data.get("resolved_at"),
            created_at=data.get("created_at"),
        )


class ArtefactIdentity:
    def __init__(
        self,
        canonical_id: str,
        schema: str,
        name: str,
        kind: ArtefactKind,
        criticality: ArtefactCriticality = ArtefactCriticality.MEDIUM,
        source_hash: str = "",
        aliases: Optional[List[str]] = None,
        source_file: Optional[str] = None,
        line_number: Optional[int] = None,
    ):
        self.canonical_id = canonical_id
        self.schema = schema
        self.name = name
        self.kind = kind
        self.criticality = criticality
        self.source_hash = source_hash
        self.aliases: List[str] = list(aliases) if aliases else []
        self.source_file = source_file
        self.line_number = line_number

    @property
    def qualified_name(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    def record_rename(self, new_name: str, new_schema: Optional[str] = None) -> None:
        old_qualified = self.qualified_name
        if old_qualified not in self.aliases:
            self.aliases.append(old_qualified)
        self.name = new_name
        if new_schema is not None:
            self.schema = new_schema

    def to_dict(self) -> Dict[str, Any]:
        return {
            "canonical_id": self.canonical_id,
            "schema": self.schema,
            "name": self.name,
            "kind": self.kind.value,
            "criticality": self.criticality.value,
            "source_hash": self.source_hash,
            "aliases": list(self.aliases),
            "source_file": self.source_file,
            "line_number": self.line_number,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ArtefactIdentity:
        return cls(
            canonical_id=str(data["canonical_id"]),
            schema=str(data.get("schema", "")),
            name=str(data["name"]),
            kind=ArtefactKind(data["kind"]),
            criticality=ArtefactCriticality(data.get("criticality", ArtefactCriticality.MEDIUM.value)),
            source_hash=str(data.get("source_hash", "")),
            aliases=list(data.get("aliases", [])),
            source_file=data.get("source_file"),
            line_number=data.get("line_number"),
        )


class ArtefactRecord:
    def __init__(
        self,
        identity: ArtefactIdentity,
        current_stage: LifecycleStage = LifecycleStage.SUBMITTED,
        stages_reached: Optional[Set[LifecycleStage]] = None,
        stage_timestamps: Optional[Dict[str, float]] = None,
        stage_outcomes: Optional[Dict[str, bool]] = None,
        debts: Optional[List[DebtRecord]] = None,
        target_name: Optional[str] = None,
        target_hash: Optional[str] = None,
        target_code: Optional[str] = None,
        conversion_rung: Optional[str] = None,
        execution_elapsed_sec: Optional[float] = None,
        coverage_ratio: Optional[float] = None,
    ):
        self.identity = identity
        self.current_stage = current_stage
        self.stages_reached: Set[LifecycleStage] = set(stages_reached) if stages_reached else {LifecycleStage.SUBMITTED}
        self.stage_timestamps: Dict[str, float] = dict(stage_timestamps) if stage_timestamps else {
            LifecycleStage.SUBMITTED.value: time.time()
        }
        self.stage_outcomes: Dict[str, bool] = dict(stage_outcomes) if stage_outcomes else {
            LifecycleStage.SUBMITTED.value: True
        }
        self.debts: List[DebtRecord] = list(debts) if debts else []
        self.target_name = target_name
        self.target_hash = target_hash
        self.target_code = target_code
        self.conversion_rung = conversion_rung
        self.execution_elapsed_sec = execution_elapsed_sec
        self.coverage_ratio = coverage_ratio

    def advance_stage(self, stage: LifecycleStage, success: bool = True) -> None:
        self.current_stage = stage
        self.stages_reached.add(stage)
        self.stage_timestamps[stage.value] = time.time()
        self.stage_outcomes[stage.value] = success

    def add_debt(self, debt: DebtRecord) -> None:
        self.debts.append(debt)

    def active_debts(self) -> List[DebtRecord]:
        return [d for d in self.debts if not d.resolved]

    def has_active_action_debt(self) -> bool:
        return any(d.care_badge == CareBadge.ACTION for d in self.active_debts())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identity": self.identity.to_dict(),
            "current_stage": self.current_stage.value,
            "stages_reached": [s.value for s in self.stages_reached],
            "stage_timestamps": dict(self.stage_timestamps),
            "stage_outcomes": dict(self.stage_outcomes),
            "debts": [d.to_dict() for d in self.debts],
            "target_name": self.target_name,
            "target_hash": self.target_hash,
            "target_code": self.target_code,
            "conversion_rung": self.conversion_rung,
            "execution_elapsed_sec": self.execution_elapsed_sec,
            "coverage_ratio": self.coverage_ratio,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ArtefactRecord:
        identity = ArtefactIdentity.from_dict(data["identity"])
        stages_reached = {LifecycleStage(s) for s in data.get("stages_reached", [])}
        debts = [DebtRecord.from_dict(d) for d in data.get("debts", [])]
        return cls(
            identity=identity,
            current_stage=LifecycleStage(data["current_stage"]),
            stages_reached=stages_reached,
            stage_timestamps=data.get("stage_timestamps"),
            stage_outcomes=data.get("stage_outcomes"),
            debts=debts,
            target_name=data.get("target_name"),
            target_hash=data.get("target_hash"),
            target_code=data.get("target_code"),
            conversion_rung=data.get("conversion_rung"),
            execution_elapsed_sec=data.get("execution_elapsed_sec"),
            coverage_ratio=data.get("coverage_ratio"),
        )


class DenominatorRatio:
    def __init__(self, name: str, numerator: int, denominator: int):
        self.name = name
        self.numerator = numerator
        self.denominator = denominator
        self.percentage = (
            (float(numerator) / float(denominator) * 100.0) if denominator > 0 else 0.0
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "percentage": round(self.percentage, 4),
        }

    def format_unix(self) -> str:
        return f"{self.name:<32}: {self.numerator:>5}/{self.denominator:<5} ({self.percentage:>6.2f}%)"


class ScopeMetrics:
    def __init__(
        self,
        submitted: int,
        extracted: int,
        parsed: int,
        converted: int,
        deployed: int,
        executed: int,
        covered: int,
        unsupported_encoding_debt: int,
        encrypted_code_debt: int,
        dynamic_sql_debt: int,
        syntax_error_debt: int,
        other_debt: int,
        action_debt_count: int,
        review_debt_count: int,
        benign_debt_count: int,
    ):
        self.submitted = submitted
        self.extracted = extracted
        self.parsed = parsed
        self.converted = converted
        self.deployed = deployed
        self.executed = executed
        self.covered = covered

        # Gaps (unreached counts)
        self.unextracted = max(0, submitted - extracted)
        self.unparsed = max(0, extracted - parsed)
        self.unconverted = max(0, parsed - converted)
        self.undeployed = max(0, converted - deployed)
        self.unexecuted = max(0, deployed - executed)
        self.uncovered = max(0, executed - covered)

        # Debt counts
        self.unsupported_encoding_debt = unsupported_encoding_debt
        self.encrypted_code_debt = encrypted_code_debt
        self.dynamic_sql_debt = dynamic_sql_debt
        self.syntax_error_debt = syntax_error_debt
        self.other_debt = other_debt
        self.total_active_debts = (
            unsupported_encoding_debt
            + encrypted_code_debt
            + dynamic_sql_debt
            + syntax_error_debt
            + other_debt
        )
        self.action_debt_count = action_debt_count
        self.review_debt_count = review_debt_count
        self.benign_debt_count = benign_debt_count

        # Distinct Ratios reporting every denominator
        self.extracted_over_submitted = DenominatorRatio(
            "extracted_over_submitted", self.extracted, self.submitted
        )
        self.parsed_over_submitted = DenominatorRatio(
            "parsed_over_submitted", self.parsed, self.submitted
        )
        self.parsed_over_extracted = DenominatorRatio(
            "parsed_over_extracted", self.parsed, self.extracted
        )
        self.converted_over_parsed = DenominatorRatio(
            "converted_over_parsed", self.converted, self.parsed
        )
        self.converted_over_submitted = DenominatorRatio(
            "converted_over_submitted", self.converted, self.submitted
        )
        self.deployed_over_converted = DenominatorRatio(
            "deployed_over_converted", self.deployed, self.converted
        )
        self.executed_over_deployed = DenominatorRatio(
            "executed_over_deployed", self.executed, self.deployed
        )
        self.covered_over_executed = DenominatorRatio(
            "covered_over_executed", self.covered, self.executed
        )
        self.covered_over_converted = DenominatorRatio(
            "covered_over_converted", self.covered, self.converted
        )
        self.covered_over_submitted = DenominatorRatio(
            "covered_over_submitted", self.covered, self.submitted
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "counts": {
                "submitted": self.submitted,
                "extracted": self.extracted,
                "parsed": self.parsed,
                "converted": self.converted,
                "deployed": self.deployed,
                "executed": self.executed,
                "covered": self.covered,
            },
            "gaps": {
                "unextracted": self.unextracted,
                "unparsed": self.unparsed,
                "unconverted": self.unconverted,
                "undeployed": self.undeployed,
                "unexecuted": self.unexecuted,
                "uncovered": self.uncovered,
            },
            "debts": {
                "unsupported_encoding": self.unsupported_encoding_debt,
                "encrypted_code": self.encrypted_code_debt,
                "dynamic_sql": self.dynamic_sql_debt,
                "syntax_error": self.syntax_error_debt,
                "other": self.other_debt,
                "total_active": self.total_active_debts,
                "action": self.action_debt_count,
                "review": self.review_debt_count,
                "benign": self.benign_debt_count,
            },
            "ratios": {
                "extracted_over_submitted": self.extracted_over_submitted.to_dict(),
                "parsed_over_submitted": self.parsed_over_submitted.to_dict(),
                "parsed_over_extracted": self.parsed_over_extracted.to_dict(),
                "converted_over_parsed": self.converted_over_parsed.to_dict(),
                "converted_over_submitted": self.converted_over_submitted.to_dict(),
                "deployed_over_converted": self.deployed_over_converted.to_dict(),
                "executed_over_deployed": self.executed_over_deployed.to_dict(),
                "covered_over_executed": self.covered_over_executed.to_dict(),
                "covered_over_converted": self.covered_over_converted.to_dict(),
                "covered_over_submitted": self.covered_over_submitted.to_dict(),
            },
        }

    def format_unix(self) -> str:
        lines = [
            "------------------------------------------------------------------------",
            "SCOPE ACCOUNTING LEDGER (DISTINCT STAGES & EXPLICIT DENOMINATORS)",
            "------------------------------------------------------------------------",
            f"  SUBMITTED : {self.submitted:>5} | GAP UNEXTRACTED : {self.unextracted:>5}",
            f"  EXTRACTED : {self.extracted:>5} | GAP UNPARSED    : {self.unparsed:>5}",
            f"  PARSED    : {self.parsed:>5} | GAP UNCONVERTED : {self.unconverted:>5}",
            f"  CONVERTED : {self.converted:>5} | GAP UNDEPLOYED  : {self.undeployed:>5}",
            f"  DEPLOYED  : {self.deployed:>5} | GAP UNEXECUTED  : {self.unexecuted:>5}",
            f"  EXECUTED  : {self.executed:>5} | GAP UNCOVERED   : {self.uncovered:>5}",
            f"  COVERED   : {self.covered:>5} |",
            "------------------------------------------------------------------------",
            "DEBT REGISTRY (ACTIVE):",
            f"  UNSUPPORTED ENCODING : {self.unsupported_encoding_debt}",
            f"  ENCRYPTED CODE       : {self.encrypted_code_debt}",
            f"  DYNAMIC SQL          : {self.dynamic_sql_debt}",
            f"  SYNTAX ERRORS        : {self.syntax_error_debt}",
            f"  OTHER DEBTS          : {self.other_debt}",
            f"  TOTAL ACTIVE DEBTS   : {self.total_active_debts} (ACTION={self.action_debt_count}, REVIEW={self.review_debt_count}, BENIGN={self.benign_debt_count})",
            "------------------------------------------------------------------------",
            "CONVERSION RATIOS (REPORTING EVERY DENOMINATOR):",
            f"  {self.converted_over_parsed.format_unix()}",
            f"  {self.converted_over_submitted.format_unix()}",
            f"  {self.parsed_over_submitted.format_unix()}",
            f"  {self.covered_over_converted.format_unix()}",
            f"  {self.covered_over_submitted.format_unix()}",
            "------------------------------------------------------------------------",
        ]
        return "\n".join(lines)


class ReadinessVerdict:
    def __init__(
        self,
        is_ready: bool,
        care_badge: CareBadge,
        blockers: List[str],
        warnings: List[str],
        metrics: ScopeMetrics,
    ):
        self.is_ready = is_ready
        self.care_badge = care_badge
        self.blockers = list(blockers)
        self.warnings = list(warnings)
        self.metrics = metrics

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_ready": self.is_ready,
            "care_badge": self.care_badge.value,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": self.metrics.to_dict(),
        }

    def format_unix(self) -> str:
        lines = [
            f"[{self.care_badge.value}] READINESS VERDICT: {'READY' if self.is_ready else 'BLOCKED'}",
            f"  Converted / Parsed    : {self.metrics.converted_over_parsed.numerator}/{self.metrics.converted_over_parsed.denominator} ({self.metrics.converted_over_parsed.percentage:.2f}%)",
            f"  Converted / Submitted : {self.metrics.converted_over_submitted.numerator}/{self.metrics.converted_over_submitted.denominator} ({self.metrics.converted_over_submitted.percentage:.2f}%)",
            f"  Covered / Submitted   : {self.metrics.covered_over_submitted.numerator}/{self.metrics.covered_over_submitted.denominator} ({self.metrics.covered_over_submitted.percentage:.2f}%)",
            f"  Active Debts          : {self.metrics.total_active_debts} (ACTION={self.metrics.action_debt_count}, REVIEW={self.metrics.review_debt_count})",
        ]
        if self.blockers:
            lines.append("  BLOCKERS:")
            for b in self.blockers:
                lines.append(f"    - {b}")
        if self.warnings:
            lines.append("  WARNINGS:")
            for w in self.warnings:
                lines.append(f"    - {w}")
        return "\n".join(lines)


def conversion_scope_rungs() -> Tuple[decide.Rung, ...]:
    """Decide ladder for migration scope readiness and debt gating (Row 626: RCH18)."""
    return (
        decide.Rung(
            "certified_readiness_pass",
            "complete lifecycle verification with zero unparsed critical routines and zero unresolved debt",
            needs=(
                decide.Need(
                    "zero unparsed critical routines",
                    lambda f: int(f.get("unparsed_critical_count", 0)) == 0,
                    "{rung} requires all critical routines to be parsed",
                ),
                decide.Need(
                    "zero active action debts",
                    lambda f: int(f.get("action_debt_count", 0)) == 0,
                    "{rung} requires zero active ACTION debts",
                ),
                decide.Need(
                    "zero active review debts",
                    lambda f: int(f.get("review_debt_count", 0)) == 0,
                    "{rung} requires zero active REVIEW debts",
                ),
                decide.Need(
                    "all submitted artefacts parsed",
                    lambda f: int(f.get("parsed_count", 0)) >= int(f.get("submitted_count", 0)),
                    "{rung} requires 100% of submitted artefacts to be parsed",
                ),
                decide.Need(
                    "all parsed artefacts converted",
                    lambda f: int(f.get("converted_count", 0)) >= int(f.get("parsed_count", 0)),
                    "{rung} requires 100% of parsed artefacts to be converted",
                ),
            ),
            because="all artefacts successfully converted with zero debt and full lifecycle coverage",
        ),
        decide.Rung(
            "provisional_readiness_review",
            "critical routines parsed with zero ACTION debt; inspectable REVIEW debt tolerated under non-strict review",
            needs=(
                decide.Need(
                    "zero unparsed critical routines",
                    lambda f: int(f.get("unparsed_critical_count", 0)) == 0,
                    "{rung} requires all critical routines to be parsed",
                ),
                decide.Need(
                    "zero active action debts",
                    lambda f: int(f.get("action_debt_count", 0)) == 0,
                    "{rung} requires zero active ACTION debts",
                ),
                decide.Need(
                    "non-strict review allowed",
                    lambda f: not bool(f.get("strict_reviews", False)),
                    "{rung} refuses cutover when strict review policy is enabled with pending REVIEW debts",
                ),
            ),
            because="all critical routines converted with zero ACTION debt; REVIEW debts tolerated for inspection",
        ),
        decide.Rung(
            "critical_routine_blocked_action",
            "one or more critical routines failed or skipped parsing; blocks complete readiness unconditionally",
            needs=(
                decide.Need(
                    "critical unparsed routines present",
                    lambda f: int(f.get("unparsed_critical_count", 0)) > 0,
                    "{rung} requires at least one unparsed critical routine",
                ),
            ),
            because="unparsed critical routine blocks cutover readiness even if 100% of parsed code converted",
        ),
        decide.Rung(
            "action_debt_blocked_action",
            "unresolved ACTION debt or unparsed code blocks conversion readiness",
            because="unresolved ACTION debts or unparsed code require explicit remediation before cutover",
        ),
    )


def decide_conversion_scope(facts: Dict[str, Any]) -> decide.Choice:
    """Climbs conversion_scope_rungs decide ladder."""
    return decide.climb("conversion scope evaluation", conversion_scope_rungs(), facts)


class ActiveLeaseLock:
    """Mutual exclusion active lease lock (.run.lock) preventing duplicate runs."""

    def __init__(self, lock_file: Path, term_seconds: float = 30.0):
        self.lock_file = Path(lock_file)
        self.term_seconds = float(term_seconds)
        self._fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> bool:
        durable.private_dir(self.lock_file.parent)
        if self.lock_file.exists():
            try:
                with open(self.lock_file, "r", encoding="ascii") as f:
                    content = f.read().strip()
                if content:
                    payload = json.loads(content)
                    expires = float(payload.get("expires_at", 0))
                    if time.time() < expires:
                        return False
            except json.JSONDecodeError as exc:
                raise CorruptLockError(f"Corrupt lease file at {self.lock_file}: {exc}") from exc
            except OSError as exc:
                raise LockAcquisitionError(f"Cannot read lease file at {self.lock_file}: {exc}") from exc

        try:
            fd = os.open(str(self.lock_file), os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            payload = json.dumps(
                {
                    "pid": os.getpid(),
                    "acquired_at": time.time(),
                    "expires_at": time.time() + self.term_seconds,
                }
            )
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, payload.encode("ascii"))
            os.fsync(fd)
            self._fd = fd
            self._acquired = True
            return True
        except (BlockingIOError, OSError) as exc:
            if isinstance(exc, BlockingIOError) or getattr(exc, "errno", None) in (errno.EACCES, errno.EAGAIN):
                return False
            raise LockAcquisitionError(f"Failed to acquire lock on {self.lock_file}: {exc}") from exc

    def renew(self) -> bool:
        if not self._acquired or self._fd is None:
            return False
        try:
            payload = json.dumps(
                {
                    "pid": os.getpid(),
                    "renewed_at": time.time(),
                    "expires_at": time.time() + self.term_seconds,
                }
            )
            os.ftruncate(self._fd, 0)
            os.lseek(self._fd, 0, os.SEEK_SET)
            os.write(self._fd, payload.encode("ascii"))
            os.fsync(self._fd)
            return True
        except OSError:
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        self._acquired = False
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
        except OSError:
            pass

    def __enter__(self) -> "ActiveLeaseLock":
        if not self.acquire():
            raise RuntimeError(f"Could not acquire active lease lock on {self.lock_file}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class ConversionLedger:
    """Distinct scope accounting ledger for migration artefacts."""

    def __init__(
        self,
        ledger_id: str = "default",
        base_dir: Optional[Path] = None,
        lease_term_sec: float = 30.0,
    ):
        self.ledger_id = ledger_id
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.lease = ActiveLeaseLock(self.base_dir / ".run.lock", term_seconds=lease_term_sec)
        self._records: Dict[str, ArtefactRecord] = {}
        self._name_index: Dict[str, str] = {}  # qualified_name or alias -> canonical_id
        self._debt_counter: int = 0

    @staticmethod
    def compute_canonical_id(
        schema: str,
        name: str,
        kind: ArtefactKind,
        content: Optional[str] = None,
    ) -> str:
        basis = f"{schema.lower()}.{name.lower()}:{kind.value}"
        if content:
            h = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()[:16]
            return f"{basis}#{h}"
        return hashlib.sha256(basis.encode("ascii")).hexdigest()[:24]

    def submit_artefact(
        self,
        name: str,
        kind: ArtefactKind,
        schema: str = "public",
        content: Optional[str] = None,
        raw_bytes: Optional[bytes] = None,
        criticality: ArtefactCriticality = ArtefactCriticality.MEDIUM,
        source_file: Optional[str] = None,
        line_number: Optional[int] = None,
        canonical_id: Optional[str] = None,
    ) -> ArtefactRecord:
        """Submits an artefact into the migration scope."""
        source_hash = ""
        if content is not None:
            source_hash = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
        elif raw_bytes is not None:
            source_hash = hashlib.sha256(raw_bytes).hexdigest()

        cid = canonical_id or self.compute_canonical_id(schema, name, kind, content)
        if cid in self._records:
            # Idempotent re-submission: update content hash if changed
            rec = self._records[cid]
            if source_hash:
                rec.identity.source_hash = source_hash
            return rec

        identity = ArtefactIdentity(
            canonical_id=cid,
            schema=schema,
            name=name,
            kind=kind,
            criticality=criticality,
            source_hash=source_hash,
            source_file=source_file,
            line_number=line_number,
        )
        rec = ArtefactRecord(identity=identity)
        self._records[cid] = rec
        self._name_index[identity.qualified_name.lower()] = cid
        self._name_index[identity.name.lower()] = cid
        return rec

    def mark_extracted(self, canonical_id: str) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        rec.advance_stage(LifecycleStage.EXTRACTED, success=True)
        return rec

    def record_extraction_debt(
        self,
        canonical_id: str,
        debt_type: DebtType,
        reason: str,
        care_badge: CareBadge = CareBadge.ACTION,
        remediation_hint: Optional[str] = None,
    ) -> DebtRecord:
        rec = self._require_record(canonical_id)
        self._debt_counter += 1
        debt_id = f"debt-{self._debt_counter:05d}"
        debt = DebtRecord(
            debt_id=debt_id,
            artefact_id=canonical_id,
            debt_type=debt_type,
            care_badge=care_badge,
            reason=reason,
            remediation_hint=remediation_hint,
        )
        rec.add_debt(debt)
        rec.advance_stage(LifecycleStage.EXTRACTED, success=False)
        return debt

    def mark_parsed(self, canonical_id: str) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        rec.stages_reached.add(LifecycleStage.EXTRACTED)
        rec.advance_stage(LifecycleStage.PARSED, success=True)
        return rec

    def record_parse_debt(
        self,
        canonical_id: str,
        debt_type: DebtType,
        reason: str,
        care_badge: CareBadge = CareBadge.ACTION,
        offset_or_symbol: Optional[str] = None,
        remediation_hint: Optional[str] = None,
    ) -> DebtRecord:
        rec = self._require_record(canonical_id)
        rec.stages_reached.add(LifecycleStage.EXTRACTED)
        self._debt_counter += 1
        debt_id = f"debt-{self._debt_counter:05d}"
        debt = DebtRecord(
            debt_id=debt_id,
            artefact_id=canonical_id,
            debt_type=debt_type,
            care_badge=care_badge,
            reason=reason,
            offset_or_symbol=offset_or_symbol,
            remediation_hint=remediation_hint,
        )
        rec.add_debt(debt)
        rec.advance_stage(LifecycleStage.PARSED, success=False)
        return debt

    def mark_converted(
        self,
        canonical_id: str,
        target_code: str,
        target_name: Optional[str] = None,
        rung: Optional[str] = None,
    ) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        rec.stages_reached.add(LifecycleStage.EXTRACTED)
        rec.stages_reached.add(LifecycleStage.PARSED)
        rec.target_code = target_code
        rec.target_name = target_name or rec.identity.name
        rec.target_hash = hashlib.sha256(target_code.encode("utf-8")).hexdigest()
        rec.conversion_rung = rung
        rec.advance_stage(LifecycleStage.CONVERTED, success=True)
        return rec

    def record_conversion_debt(
        self,
        canonical_id: str,
        debt_type: DebtType,
        reason: str,
        care_badge: CareBadge = CareBadge.ACTION,
        remediation_hint: Optional[str] = None,
    ) -> DebtRecord:
        rec = self._require_record(canonical_id)
        self._debt_counter += 1
        debt_id = f"debt-{self._debt_counter:05d}"
        debt = DebtRecord(
            debt_id=debt_id,
            artefact_id=canonical_id,
            debt_type=debt_type,
            care_badge=care_badge,
            reason=reason,
            remediation_hint=remediation_hint,
        )
        rec.add_debt(debt)
        rec.advance_stage(LifecycleStage.CONVERTED, success=False)
        return debt

    def mark_deployed(self, canonical_id: str, target_name: Optional[str] = None) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        if target_name:
            rec.target_name = target_name
        rec.advance_stage(LifecycleStage.DEPLOYED, success=True)
        return rec

    def mark_executed(
        self, canonical_id: str, elapsed_sec: Optional[float] = None
    ) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        rec.execution_elapsed_sec = elapsed_sec
        rec.advance_stage(LifecycleStage.EXECUTED, success=True)
        return rec

    def mark_covered(
        self, canonical_id: str, coverage_ratio: float = 1.0
    ) -> ArtefactRecord:
        rec = self._require_record(canonical_id)
        rec.coverage_ratio = max(0.0, min(1.0, float(coverage_ratio)))
        rec.advance_stage(LifecycleStage.COVERED, success=True)
        return rec

    def record_debt(
        self,
        canonical_id: str,
        debt_type: DebtType,
        care_badge: CareBadge,
        reason: str,
        offset_or_symbol: Optional[str] = None,
        remediation_hint: Optional[str] = None,
    ) -> DebtRecord:
        rec = self._require_record(canonical_id)
        self._debt_counter += 1
        debt_id = f"debt-{self._debt_counter:05d}"
        debt = DebtRecord(
            debt_id=debt_id,
            artefact_id=canonical_id,
            debt_type=debt_type,
            care_badge=care_badge,
            reason=reason,
            offset_or_symbol=offset_or_symbol,
            remediation_hint=remediation_hint,
        )
        rec.add_debt(debt)
        return debt

    def resolve_debt(self, debt_id: str, note: Optional[str] = None) -> bool:
        for rec in self._records.values():
            for d in rec.debts:
                if d.debt_id == debt_id:
                    d.resolve(note)
                    return True
        return False

    def rename_artefact(
        self, old_name: str, new_name: str, schema: Optional[str] = None
    ) -> ArtefactRecord:
        """Renames an artefact while preserving canonical ID, stages, debts, and history."""
        cid = self.find_canonical_id(old_name, schema)
        if not cid:
            raise KeyError(f"Artefact '{old_name}' not found in ledger")
        rec = self._records[cid]
        rec.identity.record_rename(new_name=new_name, new_schema=schema)
        # Update lookup indices
        self._name_index[rec.identity.qualified_name.lower()] = cid
        self._name_index[rec.identity.name.lower()] = cid
        return rec

    def find_canonical_id(self, name: str, schema: Optional[str] = None) -> Optional[str]:
        if schema:
            key = f"{schema.lower()}.{name.lower()}"
            if key in self._name_index:
                return self._name_index[key]
        return self._name_index.get(name.lower())

    def get_artefact(self, canonical_id: str) -> Optional[ArtefactRecord]:
        return self._records.get(canonical_id)

    def filter_pending(self, stage: LifecycleStage) -> List[ArtefactRecord]:
        """O(1) / fast filter of pending units that haven't successfully reached stage."""
        return [
            rec
            for rec in self._records.values()
            if stage not in rec.stages_reached or not rec.stage_outcomes.get(stage.value, False)
        ]

    def compute_metrics(self) -> ScopeMetrics:
        """Computes strict scope metrics across all 7 lifecycle stages and active debts."""
        submitted = len(self._records)
        extracted = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.EXTRACTED.value, False))
        parsed = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.PARSED.value, False))
        converted = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.CONVERTED.value, False))
        deployed = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.DEPLOYED.value, False))
        executed = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.EXECUTED.value, False))
        covered = sum(1 for r in self._records.values() if r.stage_outcomes.get(LifecycleStage.COVERED.value, False))

        unsupported_enc_cnt = 0
        encrypted_cnt = 0
        dyn_sql_cnt = 0
        syntax_err_cnt = 0
        other_debt_cnt = 0
        action_debt_cnt = 0
        review_debt_cnt = 0
        benign_debt_cnt = 0

        for r in self._records.values():
            for d in r.active_debts():
                if d.debt_type == DebtType.UNSUPPORTED_ENCODING:
                    unsupported_enc_cnt += 1
                elif d.debt_type == DebtType.ENCRYPTED_CODE:
                    encrypted_cnt += 1
                elif d.debt_type == DebtType.DYNAMIC_SQL:
                    dyn_sql_cnt += 1
                elif d.debt_type == DebtType.SYNTAX_ERROR:
                    syntax_err_cnt += 1
                else:
                    other_debt_cnt += 1

                if d.care_badge == CareBadge.ACTION:
                    action_debt_cnt += 1
                elif d.care_badge == CareBadge.REVIEW:
                    review_debt_cnt += 1
                elif d.care_badge == CareBadge.BENIGN:
                    benign_debt_cnt += 1

        return ScopeMetrics(
            submitted=submitted,
            extracted=extracted,
            parsed=parsed,
            converted=converted,
            deployed=deployed,
            executed=executed,
            covered=covered,
            unsupported_encoding_debt=unsupported_enc_cnt,
            encrypted_code_debt=encrypted_cnt,
            dynamic_sql_debt=dyn_sql_cnt,
            syntax_error_debt=syntax_err_cnt,
            other_debt=other_debt_cnt,
            action_debt_count=action_debt_cnt,
            review_debt_count=review_debt_cnt,
            benign_debt_count=benign_debt_cnt,
        )

    def evaluate_readiness(self, strict_reviews: bool = False) -> ReadinessVerdict:
        """Evaluates readiness for cutover using conversion_scope_rungs decide ladder."""
        metrics = self.compute_metrics()
        blockers: List[str] = []
        warnings: List[str] = []
        unparsed_critical_count = 0

        # 1. Critical routine parsing check
        for r in self._records.values():
            if r.identity.criticality == ArtefactCriticality.CRITICAL:
                parsed_ok = r.stage_outcomes.get(LifecycleStage.PARSED.value, False)
                if not parsed_ok:
                    unparsed_critical_count += 1
                    blockers.append(
                        f"CRITICAL routine '{r.identity.qualified_name}' failed or skipped parsing (stage: {r.current_stage.value})"
                    )

        # 2. Check for active ACTION debts
        for r in self._records.values():
            for d in r.active_debts():
                if d.care_badge == CareBadge.ACTION:
                    blockers.append(
                        f"Unresolved ACTION debt [{d.debt_type.value}] on '{r.identity.qualified_name}': {d.reason}"
                    )
                elif d.care_badge == CareBadge.REVIEW:
                    warnings.append(
                        f"Unresolved REVIEW debt [{d.debt_type.value}] on '{r.identity.qualified_name}': {d.reason}"
                    )

        # 3. Decide ladder climb
        facts = {
            "submitted_count": metrics.submitted,
            "extracted_count": metrics.extracted,
            "parsed_count": metrics.parsed,
            "converted_count": metrics.converted,
            "deployed_count": metrics.deployed,
            "executed_count": metrics.executed,
            "covered_count": metrics.covered,
            "unparsed_critical_count": unparsed_critical_count,
            "action_debt_count": metrics.action_debt_count,
            "review_debt_count": metrics.review_debt_count,
            "strict_reviews": strict_reviews,
        }
        choice = decide_conversion_scope(facts)

        if choice.path == "certified_readiness_pass":
            is_ready = True
            badge = CareBadge.PASS
        elif choice.path == "provisional_readiness_review":
            is_ready = True
            badge = CareBadge.REVIEW
        elif choice.path == "critical_routine_blocked_action":
            is_ready = False
            badge = CareBadge.ACTION
        else:  # action_debt_blocked_action
            is_ready = False
            badge = CareBadge.ACTION if (metrics.action_debt_count > 0 or unparsed_critical_count > 0) else CareBadge.REVIEW

        return ReadinessVerdict(
            is_ready=is_ready,
            care_badge=badge,
            blockers=blockers,
            warnings=warnings,
            metrics=metrics,
        )

    def format_unix_report(self) -> str:
        verdict = self.evaluate_readiness()
        lines = [
            verdict.metrics.format_unix(),
            verdict.format_unix(),
        ]
        return "\n".join(lines)

    def save_to_file(self, file_path: Path) -> None:
        """Persists the complete conversion ledger to disk atomically with durable fsync."""
        file_path = Path(file_path)
        data = {
            "ledger_id": self.ledger_id,
            "saved_at": time.time(),
            "debt_counter": self._debt_counter,
            "records": [r.to_dict() for r in self._records.values()],
        }
        durable.write_json(file_path, data, indent=2, full=True)

    def load_from_file(self, file_path: Path) -> None:
        """Loads and reconstructs the conversion ledger from disk in O(1) state reconstruction."""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Ledger file not found: {file_path}")
        data = durable.read_state(file_path, kind="state")
        if data is None or not isinstance(data, dict):
            raise durable.Torn(f"{file_path} cannot be read whole or is empty")

        self.ledger_id = data.get("ledger_id", "default")
        self._debt_counter = int(data.get("debt_counter", 0))
        self._records.clear()
        self._name_index.clear()

        for r_dict in data.get("records", []):
            rec = ArtefactRecord.from_dict(r_dict)
            self._records[rec.identity.canonical_id] = rec
            self._name_index[rec.identity.qualified_name.lower()] = rec.identity.canonical_id
            self._name_index[rec.identity.name.lower()] = rec.identity.canonical_id
            for alias in rec.identity.aliases:
                self._name_index[alias.lower()] = rec.identity.canonical_id

    def _require_record(self, canonical_id: str) -> ArtefactRecord:
        rec = self._records.get(canonical_id)
        if not rec:
            raise KeyError(f"Canonical artefact ID '{canonical_id}' not found in ledger")
        return rec


def ledger_for_hop(hop, db: str) -> ConversionLedger:
    """Returns an active ConversionLedger for a given migration hop and database."""
    report_dir = hop.report_dir(db)
    ledger_path = report_dir / "conversion-ledger.json"
    ledger = ConversionLedger(
        ledger_id=f"{getattr(hop, 'name', 'hop')}_{db}",
        base_dir=report_dir,
    )
    if ledger_path.exists():
        ledger.load_from_file(ledger_path)
    return ledger
