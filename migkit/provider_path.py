"""Differentiate provider-native and generic migration paths (Row 609: RCH01.provider-path).

Chooses native backup/log restore, provider logical replication, coordinated
dump/restore, or generic CDC from measured source/target/version/network/privilege
and coverage facts using `decide.Rung` and `decide.climb(work, rungs, facts)`.

Builds a per-path coverage and secondary-object contract across views, sequences,
grants, routines, and triggers; enforces independent catalog validation when a
provider-native path lacks native secondary-object coverage or verification; and
models provider resource lifecycle and verified completion semantics so native
speed never earns unsupported coverage.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import hashlib
import json
import pathlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from . import checkpoint, decide, lease

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class MigrationPathKind(str, enum.Enum):
    NATIVE_BACKUP_LOG_RESTORE = "native_backup_log_restore"
    PROVIDER_LOGICAL_REPLICATION = "provider_logical_replication"
    MANAGED_DUMP_RESTORE = "managed_dump_restore"
    GENERIC_CDC_PIPELINE = "generic_cdc_pipeline"


class SecondaryObjectKind(str, enum.Enum):
    VIEW = "views"
    SEQUENCE = "sequences"
    GRANT = "grants"
    ROUTINE = "routines"
    TRIGGER = "triggers"


ALL_SECONDARY_KINDS: Tuple[SecondaryObjectKind, ...] = (
    SecondaryObjectKind.VIEW,
    SecondaryObjectKind.SEQUENCE,
    SecondaryObjectKind.GRANT,
    SecondaryObjectKind.ROUTINE,
    SecondaryObjectKind.TRIGGER,
)


class SecondaryValidationStrategy(str, enum.Enum):
    INDEPENDENT_SUPPLEMENT_AND_VERIFY = "independent_catalog_reconcile_and_verify"
    INDEPENDENT_VERIFY_ONLY = "independent_catalog_verify_only"
    REFUSE_UNVERIFIED_GAP = "refuse_unverified_coverage_gap"


class ProviderResourceKind(str, enum.Enum):
    BACKUP_RESTORE_JOB = "backup_restore_job"
    LOG_REPLAY_SERVICE = "log_replay_service"
    REPLICATION_TASK = "replication_task"
    STAGING_STORAGE = "staging_storage"
    REPLICATION_SLOT = "replication_slot"


class ProviderResourceState(str, enum.Enum):
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    DRAINING = "draining"
    COMPLETED = "completed"
    FAILED = "failed"
    TEARDOWN_PENDING = "teardown_pending"
    CLEANED_UP = "cleaned_up"


class ProviderPathError(Exception):
    """Base error for provider-path selection, coverage, and lifecycle failures."""


class UnsupportedCoverageError(ProviderPathError):
    """Raised when a path is claimed complete while omitting required coverage."""


class ProviderResourceLifecycleError(ProviderPathError):
    """Raised when provider resource state transitions or teardown fail."""


class UnsafeEndpointError(ProviderPathError):
    """Raised when an endpoint host violates loopback or RFC1918 10.0.0.x rules."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that endpoint host is loopback (127.0.0.1, localhost) or 10.0.0.x."""
    cleaned = (host or "").strip()
    if not cleaned:
        raise UnsafeEndpointError("endpoint host must not be empty")
    if cleaned in ("127.0.0.1", "localhost") or cleaned.startswith("10.0.0."):
        return cleaned
    raise UnsafeEndpointError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, or 10.0.0.x permitted"
    )


def _normalize_sql_expr(expr: str) -> str:
    """Normalize SQL definitions deterministically for cross-catalog verification."""
    tokens = (expr or "").strip().rstrip(";").split()
    return " ".join(tokens).lower()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("ascii", errors="replace")).hexdigest()


# -----------------------------------------------------------------------------
# Secondary-Object Domain Models & Per-Path Coverage Contracts
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ViewSpec:
    name: str
    definition_sql: str
    columns: Tuple[str, ...] = ()
    security_invoker: bool = False

    @property
    def normalized_hash(self) -> str:
        payload = f"{self.name.lower()}|{_normalize_sql_expr(self.definition_sql)}|{','.join(c.lower() for c in self.columns)}|{int(self.security_invoker)}"
        return _sha256_text(payload)


@dataclass(frozen=True)
class SequenceSpec:
    name: str
    last_value: int
    increment_by: int = 1
    min_value: int = 1
    max_value: Optional[int] = None
    is_called: bool = True


@dataclass(frozen=True)
class GrantSpec:
    principal: str
    object_type: str
    object_name: str
    privileges: Tuple[str, ...]
    with_grant_option: bool = False

    @property
    def key(self) -> Tuple[str, str, str]:
        return (
            self.principal.strip().lower(),
            self.object_type.strip().lower(),
            self.object_name.strip().lower(),
        )

    @property
    def normalized_privileges(self) -> FrozenSet[str]:
        return frozenset(p.strip().upper() for p in self.privileges if p.strip())


@dataclass(frozen=True)
class RoutineSpec:
    name: str
    kind: str  # "function" or "procedure"
    signature: str
    body_sql: str
    security_definer: bool = False

    @property
    def normalized_hash(self) -> str:
        payload = (
            f"{self.name.lower()}|{self.kind.lower()}|"
            f"{_normalize_sql_expr(self.signature)}|{_normalize_sql_expr(self.body_sql)}|"
            f"{int(self.security_definer)}"
        )
        return _sha256_text(payload)


@dataclass(frozen=True)
class TriggerSpec:
    name: str
    table_name: str
    timing: str  # "BEFORE", "AFTER", "INSTEAD OF"
    events: Tuple[str, ...]  # ("INSERT", "UPDATE", ...)
    action_sql: str
    enabled: bool = True

    @property
    def normalized_hash(self) -> str:
        ev = ",".join(sorted(e.strip().upper() for e in self.events))
        payload = (
            f"{self.name.lower()}|{self.table_name.lower()}|"
            f"{self.timing.strip().upper()}|{ev}|"
            f"{_normalize_sql_expr(self.action_sql)}|{int(self.enabled)}"
        )
        return _sha256_text(payload)


@dataclass(frozen=True)
class SecondaryObjectCatalog:
    """Complete snapshot of secondary objects in a source or target database."""

    views: Dict[str, ViewSpec] = field(default_factory=dict)
    sequences: Dict[str, SequenceSpec] = field(default_factory=dict)
    grants: Tuple[GrantSpec, ...] = ()
    routines: Dict[str, RoutineSpec] = field(default_factory=dict)
    triggers: Dict[str, TriggerSpec] = field(default_factory=dict)

    def required_kinds(self) -> FrozenSet[SecondaryObjectKind]:
        """Return the set of secondary-object categories actually present in this catalog."""
        present: Set[SecondaryObjectKind] = set()
        if self.views:
            present.add(SecondaryObjectKind.VIEW)
        if self.sequences:
            present.add(SecondaryObjectKind.SEQUENCE)
        if self.grants:
            present.add(SecondaryObjectKind.GRANT)
        if self.routines:
            present.add(SecondaryObjectKind.ROUTINE)
        if self.triggers:
            present.add(SecondaryObjectKind.TRIGGER)
        return frozenset(present)

    def count_for(self, kind: SecondaryObjectKind) -> int:
        if kind == SecondaryObjectKind.VIEW:
            return len(self.views)
        if kind == SecondaryObjectKind.SEQUENCE:
            return len(self.sequences)
        if kind == SecondaryObjectKind.GRANT:
            return len(self.grants)
        if kind == SecondaryObjectKind.ROUTINE:
            return len(self.routines)
        if kind == SecondaryObjectKind.TRIGGER:
            return len(self.triggers)
        raise ProviderPathError(f"unknown secondary object kind: {kind!r}")


@dataclass(frozen=True)
class PathCoverageContract:
    """Per-path coverage and secondary-object contract."""

    path_kind: MigrationPathKind
    provider: str
    carries_tables_and_indexes: bool
    natively_carried_secondary: FrozenSet[SecondaryObjectKind]
    omitted_secondary: FrozenSet[SecondaryObjectKind]
    supports_selective_filtering: bool
    supports_column_remapping: bool
    provider_verifies_secondary: bool
    requires_independent_validation: bool
    completion_fence_type: str

    def missing_for_required(
        self, required: Iterable[SecondaryObjectKind]
    ) -> FrozenSet[SecondaryObjectKind]:
        req = frozenset(required)
        return req - self.natively_carried_secondary


def build_path_coverage_contract(
    path_kind: MigrationPathKind,
    *,
    provider: str = "generic",
    required_secondary: Iterable[SecondaryObjectKind] = ALL_SECONDARY_KINDS,
    provider_claims_secondary_verify: bool = False,
    server_scoped_grants_present: bool = False,
) -> PathCoverageContract:
    """Build the explicit coverage and secondary-object contract for a candidate path."""
    req = frozenset(required_secondary)
    prov = (provider or "generic").strip().lower()

    if path_kind == MigrationPathKind.NATIVE_BACKUP_LOG_RESTORE:
        # Physical/database-level backup/log restore (e.g. AWS snapshot/restore, Azure LRS)
        # carries database-stored views, sequences, routines, triggers, and DB-local grants,
        # but omits server-level logins/grants when server_scoped_grants_present is True,
        # and never natively proves cross-instance grant/sequence readiness without a check.
        carried = {
            SecondaryObjectKind.VIEW,
            SecondaryObjectKind.SEQUENCE,
            SecondaryObjectKind.ROUTINE,
            SecondaryObjectKind.TRIGGER,
        }
        if not server_scoped_grants_present:
            carried.add(SecondaryObjectKind.GRANT)
        natively_carried = frozenset(carried)
        omitted = frozenset(ALL_SECONDARY_KINDS) - natively_carried
        needs_indep = bool(req & omitted) or not provider_claims_secondary_verify
        return PathCoverageContract(
            path_kind=path_kind,
            provider=prov,
            carries_tables_and_indexes=True,
            natively_carried_secondary=natively_carried,
            omitted_secondary=omitted,
            supports_selective_filtering=False,
            supports_column_remapping=False,
            provider_verifies_secondary=provider_claims_secondary_verify and not omitted,
            requires_independent_validation=needs_indep,
            completion_fence_type="recovery_lsn_or_lrs_watermark",
        )

    if path_kind == MigrationPathKind.PROVIDER_LOGICAL_REPLICATION:
        # Provider-managed logical replication (e.g. AWS DMS classic/homogeneous logical,
        # native PG publication/subscription) streams table rows and does NOT carry views,
        # sequences (last_value is not replicated by logical slots in standard pub/sub),
        # grants, routines, or triggers without an independent schema/object supplement.
        natively_carried = frozenset()
        omitted = frozenset(ALL_SECONDARY_KINDS)
        return PathCoverageContract(
            path_kind=path_kind,
            provider=prov,
            carries_tables_and_indexes=True,
            natively_carried_secondary=natively_carried,
            omitted_secondary=omitted,
            supports_selective_filtering=True,
            supports_column_remapping=False,
            provider_verifies_secondary=False,
            requires_independent_validation=True,
            completion_fence_type="logical_replication_lsn_or_gtid",
        )

    if path_kind == MigrationPathKind.MANAGED_DUMP_RESTORE:
        # Dump and restore carries full DDL including views, sequences, grants, routines,
        # triggers when full schema dump is enabled, but still requires independent
        # post-restore catalog verification to catch partial restore errors or lagging sequences.
        natively_carried = frozenset(ALL_SECONDARY_KINDS)
        omitted = frozenset()
        return PathCoverageContract(
            path_kind=path_kind,
            provider=prov,
            carries_tables_and_indexes=True,
            natively_carried_secondary=natively_carried,
            omitted_secondary=omitted,
            supports_selective_filtering=True,
            supports_column_remapping=False,
            provider_verifies_secondary=False,
            requires_independent_validation=True,
            completion_fence_type="snapshot_transaction_scn_or_lsn",
        )

    if path_kind == MigrationPathKind.GENERIC_CDC_PIPELINE:
        # Generic CDC pipeline with migkit's catalog extractor carries tables + streams changes
        # and explicitly extracts and verifies all secondary object classes.
        natively_carried = frozenset(ALL_SECONDARY_KINDS)
        omitted = frozenset()
        return PathCoverageContract(
            path_kind=path_kind,
            provider=prov,
            carries_tables_and_indexes=True,
            natively_carried_secondary=natively_carried,
            omitted_secondary=omitted,
            supports_selective_filtering=True,
            supports_column_remapping=True,
            provider_verifies_secondary=True,
            requires_independent_validation=True,
            completion_fence_type="high_watermark_lsn_or_gtid_fence",
        )

    raise ProviderPathError(f"unsupported migration path kind: {path_kind!r}")


# -----------------------------------------------------------------------------
# Decide Ladders: Path Selection & Secondary Validation Strategy
# -----------------------------------------------------------------------------

def provider_path_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for choosing between provider-native and generic paths.

    Every rung checks source/target compatibility, version support, network reachability,
    privileges, and secondary-object coverage so native speed never earns unsupported
    coverage.
    """
    return (
        decide.Rung(
            "native_backup_log_restore",
            "provider-native backup and log restore",
            gives=frozenset(
                {"carried", "consistent as of", "online", "indexes built", "whole server"}
            ),
            needs=(
                decide.Need(
                    "homogeneous source and target engine",
                    lambda f: bool(f.get("source_engine"))
                    and f.get("source_engine") == f.get("target_engine"),
                    "{rung} requires identical source and target engine families",
                ),
                decide.Need(
                    "provider backup or log replay API offered for the pair",
                    lambda f: bool(f.get("provider_backup_log_api")),
                    "{rung} is not offered by the provider for this source/target pair",
                ),
                decide.Need(
                    "compatible major and storage format version",
                    lambda f: bool(f.get("version_compatible_for_physical", True))
                    and int(f.get("target_major_version", 0))
                    >= int(f.get("source_major_version", 0))
                    and (
                        not bool(f.get("requires_exact_major_match", False))
                        or int(f.get("target_major_version", 0))
                        == int(f.get("source_major_version", 0))
                    ),
                    "{rung} cannot restore across incompatible major or storage versions",
                ),
                decide.Need(
                    "staging storage and network path between source backup and target",
                    lambda f: bool(f.get("storage_or_network_path_ready")),
                    "{rung} lacks staging storage or network path from source backup to target",
                ),
                decide.Need(
                    "provider backup read and target restore privileges",
                    lambda f: bool(f.get("provider_restore_privilege")),
                    "{rung} lacks provider backup export or target restore privileges",
                ),
                decide.Need(
                    "whole-database scope without table filtering or column reshaping",
                    lambda f: not bool(f.get("requires_selective_table_or_row_transform", False)),
                    "{rung} copies whole database files and cannot filter rows or remap columns",
                ),
                decide.Need(
                    "complete secondary-object coverage or independent supplement",
                    lambda f: bool(f.get("native_backup_covers_required_objects", True))
                    or bool(f.get("independent_secondary_supplement_available", False)),
                    "{rung} omits required secondary objects and no independent supplement is enabled",
                ),
            ),
            because="restores physical backup and log segments directly without decoding rows",
            footprint=2,
            leaves="a provider restore job and staging snapshot cleaned up on completion",
        ),
        decide.Rung(
            "provider_logical_replication",
            "provider-native logical replication",
            gives=frozenset({"carried", "consistent as of", "online"}),
            needs=(
                decide.Need(
                    "logical replication wire compatibility between source and target",
                    lambda f: bool(f.get("logical_replication_engine_compatible")),
                    "{rung} requires logical replication protocol support between source and target",
                ),
                decide.Need(
                    "engine versions supporting logical decoding or row-based binlog",
                    lambda f: bool(f.get("version_supports_logical_replication")),
                    "{rung} is not supported by the source or target engine version",
                ),
                decide.Need(
                    "network route from target or replication instance to source",
                    lambda f: bool(f.get("network_replication_reachable")),
                    "{rung} has no network route between the replication endpoint and the source",
                ),
                decide.Need(
                    "replication slot, log retention, and replication privileges",
                    lambda f: bool(f.get("replication_privilege_and_wal")),
                    "{rung} lacks replication privilege, logical WAL level, or slot capacity",
                ),
                decide.Need(
                    "no custom column masking or non-native row reshape",
                    lambda f: not bool(f.get("requires_custom_column_masking_or_reshape", False)),
                    "{rung} cannot apply custom column masking or non-native row reshaping",
                ),
                decide.Need(
                    "secondary-object contract covered or supplemented independently",
                    lambda f: bool(f.get("logical_replication_covers_required_objects", False))
                    or bool(f.get("independent_secondary_supplement_available", False)),
                    "{rung} omits views, sequences, grants, routines, or triggers without independent supplement",
                ),
            ),
            because="streams logical changes natively with low lag once schema and secondary objects are covered",
            footprint=2,
            leaves="a provider replication slot and subscription or replication task",
        ),
        decide.Rung(
            "managed_dump_restore",
            "coordinated dump and restore",
            gives=frozenset({"carried", "consistent as of", "indexes built", "secondary objects"}),
            needs=(
                decide.Need(
                    "dump and restore compatibility between source and target",
                    lambda f: bool(f.get("dump_restore_engine_compatible")),
                    "{rung} requires dump/restore format compatibility between source and target",
                ),
                decide.Need(
                    "target version >= minimum dump restore version",
                    lambda f: int(f.get("target_major_version", 0))
                    >= int(f.get("min_dump_target_version", 0))
                    and int(f.get("target_major_version", 0)) > 0,
                    "{rung} target version is older than the dump stream requires",
                ),
                decide.Need(
                    "client network reachability to both source and target",
                    lambda f: bool(f.get("client_network_reachable")),
                    "{rung} cannot reach both source and target over client SQL connections",
                ),
                decide.Need(
                    "consistent dump read and target DDL/write privileges",
                    lambda f: bool(f.get("dump_restore_privileges")),
                    "{rung} lacks source dump read or target DDL/write privileges",
                ),
                decide.Need(
                    "snapshot window permits bounded offline or read-held dump",
                    lambda f: bool(f.get("snapshot_window_permits_dump", True)),
                    "{rung} exceeds the allowed write-freeze or snapshot window",
                ),
                decide.Need(
                    "no online continuous CDC tail required during dump",
                    lambda f: not bool(f.get("requires_continuous_cdc_stream", False)),
                    "{rung} is a point-in-time snapshot move and does not stream ongoing writes",
                ),
            ),
            because="carries data, indexes, and secondary DDL in one coordinated snapshot",
            footprint=1,
            leaves="a local dump directory removed after verification",
        ),
        decide.Rung(
            "generic_cdc_pipeline",
            "generic snapshot and CDC pipeline",
            gives=frozenset(
                {
                    "carried",
                    "consistent as of",
                    "online",
                    "row filter",
                    "column mapping",
                    "resume by chunk",
                    "resume by key",
                    "secondary objects",
                }
            ),
            needs=(
                decide.Need(
                    "client SQL reachability to source and target endpoints",
                    lambda f: bool(f.get("client_network_reachable")),
                    "{rung} cannot reach source and target endpoints from the mover",
                ),
                decide.Need(
                    "source table/change-capture read and target write privileges",
                    lambda f: bool(f.get("generic_cdc_privileges")),
                    "{rung} lacks source table/change read or target write privileges",
                ),
                decide.Need(
                    "catalog secondary-object extraction and verification enabled",
                    lambda f: bool(f.get("generic_covers_required_objects", True)),
                    "{rung} requires catalog access to extract and verify required secondary objects",
                ),
            ),
            because="carries selective filters, mappings, and full secondary-object coverage across any pair",
            footprint=1,
            leaves="checkpointed range state and change stream cursor",
        ),
    )


def secondary_validation_rungs() -> Tuple[decide.Rung, ...]:
    """Return `decide.Rung` ladder for secondary-object supplementation and validation."""
    return (
        decide.Rung(
            "independent_catalog_reconcile_and_verify",
            "independent secondary-object supplement and catalog verification",
            needs=(
                decide.Need(
                    "path has secondary-object coverage or verification gaps",
                    lambda f: bool(f.get("path_has_coverage_or_verification_gaps")),
                    "{rung} is only needed when the path omits secondary objects or verification",
                ),
                decide.Need(
                    "source and target catalog introspection and DDL apply access",
                    lambda f: bool(f.get("catalog_introspection_available"))
                    and bool(f.get("secondary_ddl_apply_available", True)),
                    "{rung} requires catalog introspection and target DDL apply privileges",
                ),
            ),
            because="extracts omitted views, sequences, grants, routines, and triggers and verifies parity",
            footprint=1,
        ),
        decide.Rung(
            "independent_catalog_verify_only",
            "independent secondary-object catalog verification",
            needs=(
                decide.Need(
                    "all required secondary objects already carried by path",
                    lambda f: not bool(f.get("path_has_unsupplemented_coverage_gaps", False)),
                    "{rung} cannot verify alone when required secondary objects were never carried",
                ),
                decide.Need(
                    "source and target catalog introspection available",
                    lambda f: bool(f.get("catalog_introspection_available")),
                    "{rung} requires source and target catalog read access",
                ),
            ),
            because="independently verifies views, sequences, grants, routines, and triggers before cutover",
            footprint=0,
        ),
        decide.Rung(
            "refuse_unverified_coverage_gap",
            "block completion on unverified secondary-object gap",
            needs=(
                decide.Need(
                    "unverified or unsupplemented secondary-object gap present",
                    lambda f: bool(f.get("must_block_unverified_gap")),
                    "{rung} is only taken when secondary coverage cannot be supplemented or verified",
                ),
            ),
            because="prevents certifying migration completion when secondary objects are omitted or unverified",
            footprint=0,
        ),
    )


# -----------------------------------------------------------------------------
# Measured Path Selection & Cost Evaluation
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class PathSelectionResult:
    choice: decide.Choice
    validation_choice: decide.Choice
    chosen_path: Optional[MigrationPathKind]
    validation_strategy: SecondaryValidationStrategy
    coverage_contract: Optional[PathCoverageContract]
    estimated_seconds: Optional[float]
    care_badge: CareBadge


def _derive_measured_costs(facts_map: Mapping[str, Any]) -> Optional[decide.Costs]:
    """Derive per-path measured seconds per unit from observed byte/row throughput facts."""
    dataset_bytes = float(facts_map.get("dataset_bytes") or 0.0)
    if dataset_bytes <= 0.0:
        return None

    costs = decide.Costs()
    rate_keys = (
        ("native_backup_log_restore", "measured_native_backup_bytes_per_sec"),
        ("provider_logical_replication", "measured_logical_rep_bytes_per_sec"),
        ("managed_dump_restore", "measured_dump_restore_bytes_per_sec"),
        ("generic_cdc_pipeline", "measured_generic_cdc_bytes_per_sec"),
    )
    any_measured = False
    for rung_name, rate_key in rate_keys:
        rate = float(facts_map.get(rate_key) or 0.0)
        if rate > 0.0:
            # Include any measured secondary supplement + validation overhead
            supplement_sec = float(facts_map.get(f"{rung_name}_validation_overhead_sec") or 0.0)
            total_sec = (dataset_bytes / rate) + max(0.0, supplement_sec)
            costs.saw(rung_name, 1, total_sec)
            any_measured = True
    return costs if any_measured else None


def choose_migration_path(
    facts_input: Mapping[str, Any],
    *,
    required_secondary: Iterable[SecondaryObjectKind] = (),
    work: str = "migration path selection",
) -> PathSelectionResult:
    """Choose between provider-native and generic paths via `decide.climb`.

    Automatically evaluates each candidate path's secondary-object contract against
    `required_secondary` so a fast native path is refused whenever it omits required
    views, sequences, grants, routines, or triggers without an independent supplement.
    """
    raw = dict(facts_input)
    if "source_host" in raw:
        validate_safe_endpoint(str(raw["source_host"]))
    if "target_host" in raw:
        validate_safe_endpoint(str(raw["target_host"]))

    req_kinds = frozenset(required_secondary) | frozenset(
        SecondaryObjectKind(k) if not isinstance(k, SecondaryObjectKind) else k
        for k in raw.get("required_secondary_kinds", ())
    )
    provider = str(raw.get("provider") or "generic")
    server_grants = bool(raw.get("server_scoped_grants_present", False))

    backup_contract = build_path_coverage_contract(
        MigrationPathKind.NATIVE_BACKUP_LOG_RESTORE,
        provider=provider,
        required_secondary=req_kinds,
        provider_claims_secondary_verify=bool(raw.get("provider_claims_secondary_verify", False)),
        server_scoped_grants_present=server_grants,
    )
    logical_contract = build_path_coverage_contract(
        MigrationPathKind.PROVIDER_LOGICAL_REPLICATION,
        provider=provider,
        required_secondary=req_kinds,
        provider_claims_secondary_verify=False,
        server_scoped_grants_present=server_grants,
    )

    raw.setdefault(
        "native_backup_covers_required_objects",
        len(backup_contract.missing_for_required(req_kinds)) == 0,
    )
    raw.setdefault(
        "logical_replication_covers_required_objects",
        len(logical_contract.missing_for_required(req_kinds)) == 0,
    )

    facts = decide.Facts(raw)
    costs = _derive_measured_costs(raw)
    rungs = provider_path_rungs()
    choice = decide.climb(work, rungs, facts, costs=costs)

    if choice.rung is None:
        val_facts = decide.Facts(
            {
                "path_has_coverage_or_verification_gaps": True,
                "path_has_unsupplemented_coverage_gaps": True,
                "catalog_introspection_available": False,
                "must_block_unverified_gap": True,
            }
        )
        val_choice = decide.climb(
            "secondary validation strategy", secondary_validation_rungs(), val_facts
        )
        return PathSelectionResult(
            choice=choice,
            validation_choice=val_choice,
            chosen_path=None,
            validation_strategy=SecondaryValidationStrategy.REFUSE_UNVERIFIED_GAP,
            coverage_contract=None,
            estimated_seconds=None,
            care_badge=CareBadge.ACTION,
        )

    chosen_kind = MigrationPathKind(choice.path)
    contract = build_path_coverage_contract(
        chosen_kind,
        provider=provider,
        required_secondary=req_kinds,
        provider_claims_secondary_verify=bool(raw.get("provider_claims_secondary_verify", False)),
        server_scoped_grants_present=server_grants,
    )
    missing_secondary = contract.missing_for_required(req_kinds)
    supplement_ok = bool(raw.get("independent_secondary_supplement_available", False))
    catalog_ok = bool(raw.get("catalog_introspection_available", True))

    has_gaps = bool(missing_secondary) or contract.requires_independent_validation
    unsupplemented_gaps = bool(missing_secondary) and not supplement_ok
    must_block = unsupplemented_gaps or not catalog_ok

    val_facts = decide.Facts(
        {
            "path_has_coverage_or_verification_gaps": bool(missing_secondary),
            "path_has_unsupplemented_coverage_gaps": unsupplemented_gaps,
            "catalog_introspection_available": catalog_ok,
            "secondary_ddl_apply_available": supplement_ok,
            "must_block_unverified_gap": must_block,
        }
    )
    val_choice = decide.climb(
        "secondary validation strategy", secondary_validation_rungs(), val_facts
    )
    val_strategy = (
        SecondaryValidationStrategy(val_choice.path)
        if val_choice.rung is not None
        else SecondaryValidationStrategy.REFUSE_UNVERIFIED_GAP
    )

    if val_strategy == SecondaryValidationStrategy.REFUSE_UNVERIFIED_GAP:
        badge = CareBadge.ACTION
    elif val_strategy == SecondaryValidationStrategy.INDEPENDENT_SUPPLEMENT_AND_VERIFY:
        badge = CareBadge.REVIEW
    elif has_gaps:
        badge = CareBadge.PASS
    else:
        badge = CareBadge.PASS

    est_sec = choice.cost()
    return PathSelectionResult(
        choice=choice,
        validation_choice=val_choice,
        chosen_path=chosen_kind,
        validation_strategy=val_strategy,
        coverage_contract=contract,
        estimated_seconds=float(est_sec) if isinstance(est_sec, (int, float)) else None,
        care_badge=badge,
    )


# -----------------------------------------------------------------------------
# Independent Secondary-Object Validation & Supplementation
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class SecondaryObjectDefect:
    kind: SecondaryObjectKind
    object_name: str
    defect_type: str  # "omitted", "definition_mismatch", "sequence_lag", "grant_deficit", "disabled"
    detail: str
    care_badge: CareBadge = CareBadge.ACTION


@dataclass(frozen=True)
class SecondaryValidationReport:
    verified: bool
    care_badge: CareBadge
    checked_counts: Dict[str, int]
    defects: Tuple[SecondaryObjectDefect, ...]
    summary: str


def verify_secondary_objects(
    source: SecondaryObjectCatalog,
    target: SecondaryObjectCatalog,
    *,
    required_kinds: Iterable[SecondaryObjectKind] = ALL_SECONDARY_KINDS,
    ignored_principals: Iterable[str] = ("rdsadmin", "cloudsqladmin", "azure_superuser"),
) -> SecondaryValidationReport:
    """Independently validate views, sequences, grants, routines, and triggers on target vs source."""
    req = frozenset(required_kinds)
    ignored = {p.strip().lower() for p in ignored_principals if p.strip()}
    defects: List[SecondaryObjectDefect] = []
    checked_counts: Dict[str, int] = {}

    if SecondaryObjectKind.VIEW in req:
        checked_counts[SecondaryObjectKind.VIEW.value] = len(source.views)
        target_views_lower = {k.lower(): v for k, v in target.views.items()}
        for name, src_view in sorted(source.views.items()):
            tgt_view = target_views_lower.get(name.lower())
            if tgt_view is None:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.VIEW,
                        object_name=name,
                        defect_type="omitted",
                        detail=f"view '{name}' present on source but omitted on target",
                    )
                )
            elif tgt_view.normalized_hash != src_view.normalized_hash:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.VIEW,
                        object_name=name,
                        defect_type="definition_mismatch",
                        detail=f"view '{name}' definition or columns differ on target",
                    )
                )

    if SecondaryObjectKind.SEQUENCE in req:
        checked_counts[SecondaryObjectKind.SEQUENCE.value] = len(source.sequences)
        target_seq_lower = {k.lower(): s for k, s in target.sequences.items()}
        for name, src_seq in sorted(source.sequences.items()):
            tgt_seq = target_seq_lower.get(name.lower())
            if tgt_seq is None:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.SEQUENCE,
                        object_name=name,
                        defect_type="omitted",
                        detail=f"sequence '{name}' present on source (last_value={src_seq.last_value}) but omitted on target",
                    )
                )
            else:
                if tgt_seq.last_value < src_seq.last_value:
                    defects.append(
                        SecondaryObjectDefect(
                            kind=SecondaryObjectKind.SEQUENCE,
                            object_name=name,
                            defect_type="sequence_lag",
                            detail=(
                                f"sequence '{name}' target last_value={tgt_seq.last_value} lags "
                                f"source last_value={src_seq.last_value}"
                            ),
                        )
                    )
                if tgt_seq.increment_by != src_seq.increment_by:
                    defects.append(
                        SecondaryObjectDefect(
                            kind=SecondaryObjectKind.SEQUENCE,
                            object_name=name,
                            defect_type="definition_mismatch",
                            detail=(
                                f"sequence '{name}' increment_by={tgt_seq.increment_by} != "
                                f"source increment_by={src_seq.increment_by}"
                            ),
                        )
                    )

    if SecondaryObjectKind.GRANT in req:
        src_grants = [g for g in source.grants if g.principal.strip().lower() not in ignored]
        checked_counts[SecondaryObjectKind.GRANT.value] = len(src_grants)
        tgt_grant_map: Dict[Tuple[str, str, str], Set[str]] = {}
        tgt_grant_opt: Dict[Tuple[str, str, str], bool] = {}
        for g in target.grants:
            if g.principal.strip().lower() in ignored:
                continue
            tgt_grant_map.setdefault(g.key, set()).update(g.normalized_privileges)
            tgt_grant_opt[g.key] = tgt_grant_opt.get(g.key, False) or g.with_grant_option

        for src_g in src_grants:
            have_privs = tgt_grant_map.get(src_g.key)
            if not have_privs:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.GRANT,
                        object_name=f"{src_g.principal}:{src_g.object_type}:{src_g.object_name}",
                        defect_type="omitted",
                        detail=(
                            f"grant for principal '{src_g.principal}' on {src_g.object_type} "
                            f"'{src_g.object_name}' ({sorted(src_g.normalized_privileges)}) omitted on target"
                        ),
                    )
                )
            else:
                missing_privs = src_g.normalized_privileges - have_privs
                if missing_privs:
                    defects.append(
                        SecondaryObjectDefect(
                            kind=SecondaryObjectKind.GRANT,
                            object_name=f"{src_g.principal}:{src_g.object_type}:{src_g.object_name}",
                            defect_type="grant_deficit",
                            detail=(
                                f"grant for '{src_g.principal}' on '{src_g.object_name}' missing "
                                f"privileges {sorted(missing_privs)}"
                            ),
                        )
                    )
                elif src_g.with_grant_option and not tgt_grant_opt.get(src_g.key, False):
                    defects.append(
                        SecondaryObjectDefect(
                            kind=SecondaryObjectKind.GRANT,
                            object_name=f"{src_g.principal}:{src_g.object_type}:{src_g.object_name}",
                            defect_type="grant_deficit",
                            detail=f"grant for '{src_g.principal}' on '{src_g.object_name}' lost WITH GRANT OPTION",
                        )
                    )

    if SecondaryObjectKind.ROUTINE in req:
        checked_counts[SecondaryObjectKind.ROUTINE.value] = len(source.routines)
        target_routines_lower = {k.lower(): r for k, r in target.routines.items()}
        for name, src_rt in sorted(source.routines.items()):
            tgt_rt = target_routines_lower.get(name.lower())
            if tgt_rt is None:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.ROUTINE,
                        object_name=name,
                        defect_type="omitted",
                        detail=f"routine '{name}' present on source but omitted on target",
                    )
                )
            elif tgt_rt.normalized_hash != src_rt.normalized_hash:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.ROUTINE,
                        object_name=name,
                        defect_type="definition_mismatch",
                        detail=f"routine '{name}' signature or body differs on target",
                    )
                )

    if SecondaryObjectKind.TRIGGER in req:
        checked_counts[SecondaryObjectKind.TRIGGER.value] = len(source.triggers)
        target_triggers_lower = {k.lower(): t for k, t in target.triggers.items()}
        for name, src_tr in sorted(source.triggers.items()):
            tgt_tr = target_triggers_lower.get(name.lower())
            if tgt_tr is None:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.TRIGGER,
                        object_name=name,
                        defect_type="omitted",
                        detail=f"trigger '{name}' on '{src_tr.table_name}' omitted on target",
                    )
                )
            elif not tgt_tr.enabled and src_tr.enabled:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.TRIGGER,
                        object_name=name,
                        defect_type="disabled",
                        detail=f"trigger '{name}' on '{src_tr.table_name}' is disabled on target",
                    )
                )
            elif tgt_tr.normalized_hash != src_tr.normalized_hash:
                defects.append(
                    SecondaryObjectDefect(
                        kind=SecondaryObjectKind.TRIGGER,
                        object_name=name,
                        defect_type="definition_mismatch",
                        detail=f"trigger '{name}' definition differs on target",
                    )
                )

    if defects:
        return SecondaryValidationReport(
            verified=False,
            care_badge=CareBadge.ACTION,
            checked_counts=checked_counts,
            defects=tuple(defects),
            summary=f"ACTION: {len(defects)} secondary-object defect(s) detected: {defects[0].detail}",
        )

    return SecondaryValidationReport(
        verified=True,
        care_badge=CareBadge.PASS,
        checked_counts=checked_counts,
        defects=(),
        summary="PASS: all required views, sequences, grants, routines, and triggers verified",
    )


def supplement_missing_secondary_objects(
    source: SecondaryObjectCatalog,
    target: SecondaryObjectCatalog,
    *,
    kinds_to_supplement: Iterable[SecondaryObjectKind] = ALL_SECONDARY_KINDS,
    sequence_headroom: int = 0,
) -> SecondaryObjectCatalog:
    """Synthesize and apply missing or lagging secondary objects from source onto target catalog."""
    if sequence_headroom < 0:
        raise ProviderPathError("sequence_headroom must be non-negative")
    kinds = frozenset(kinds_to_supplement)

    new_views = dict(target.views)
    if SecondaryObjectKind.VIEW in kinds:
        for name, v in source.views.items():
            new_views[name] = v

    new_sequences = dict(target.sequences)
    if SecondaryObjectKind.SEQUENCE in kinds:
        for name, s in source.sequences.items():
            existing = new_sequences.get(name)
            target_val = max(
                s.last_value + sequence_headroom,
                existing.last_value if existing is not None else s.last_value,
            )
            new_sequences[name] = dataclasses.replace(s, last_value=target_val)

    new_grants = list(target.grants)
    if SecondaryObjectKind.GRANT in kinds:
        existing_keys = {
            (g.key, g.normalized_privileges, g.with_grant_option) for g in new_grants
        }
        for g in source.grants:
            sig = (g.key, g.normalized_privileges, g.with_grant_option)
            if sig not in existing_keys:
                new_grants.append(g)
                existing_keys.add(sig)

    new_routines = dict(target.routines)
    if SecondaryObjectKind.ROUTINE in kinds:
        for name, r in source.routines.items():
            new_routines[name] = r

    new_triggers = dict(target.triggers)
    if SecondaryObjectKind.TRIGGER in kinds:
        for name, t in source.triggers.items():
            new_triggers[name] = t

    return SecondaryObjectCatalog(
        views=new_views,
        sequences=new_sequences,
        grants=tuple(new_grants),
        routines=new_routines,
        triggers=new_triggers,
    )


# -----------------------------------------------------------------------------
# Provider Resource Lifecycle & Verified Completion Semantics
# -----------------------------------------------------------------------------

_ALLOWED_RESOURCE_TRANSITIONS: Dict[ProviderResourceState, FrozenSet[ProviderResourceState]] = {
    ProviderResourceState.PROVISIONING: frozenset(
        {
            ProviderResourceState.ACTIVE,
            ProviderResourceState.FAILED,
            ProviderResourceState.TEARDOWN_PENDING,
        }
    ),
    ProviderResourceState.ACTIVE: frozenset(
        {
            ProviderResourceState.DRAINING,
            ProviderResourceState.COMPLETED,
            ProviderResourceState.FAILED,
            ProviderResourceState.TEARDOWN_PENDING,
        }
    ),
    ProviderResourceState.DRAINING: frozenset(
        {
            ProviderResourceState.COMPLETED,
            ProviderResourceState.FAILED,
            ProviderResourceState.TEARDOWN_PENDING,
        }
    ),
    ProviderResourceState.COMPLETED: frozenset(
        {
            ProviderResourceState.TEARDOWN_PENDING,
            ProviderResourceState.CLEANED_UP,
        }
    ),
    ProviderResourceState.FAILED: frozenset(
        {
            ProviderResourceState.TEARDOWN_PENDING,
            ProviderResourceState.CLEANED_UP,
        }
    ),
    ProviderResourceState.TEARDOWN_PENDING: frozenset(
        {
            ProviderResourceState.CLEANED_UP,
        }
    ),
    ProviderResourceState.CLEANED_UP: frozenset(),
}


@dataclass(frozen=True)
class ProviderResourceRecord:
    resource_id: str
    kind: ProviderResourceKind
    provider: str
    state: ProviderResourceState
    ephemeral: bool = True
    fence_position: Optional[str] = None
    error_detail: Optional[str] = None


class ProviderResourceLifecycle:
    """Tracks provider-native and mover resources from provisioning through teardown."""

    def __init__(self) -> None:
        self._resources: Dict[str, ProviderResourceRecord] = {}

    @property
    def resources(self) -> Dict[str, ProviderResourceRecord]:
        return dict(self._resources)

    def register(
        self,
        resource_id: str,
        kind: ProviderResourceKind,
        provider: str,
        *,
        ephemeral: bool = True,
        initial_state: ProviderResourceState = ProviderResourceState.PROVISIONING,
    ) -> ProviderResourceRecord:
        if not resource_id or not resource_id.strip():
            raise ProviderResourceLifecycleError("resource_id must not be empty")
        if resource_id in self._resources:
            raise ProviderResourceLifecycleError(f"duplicate provider resource_id '{resource_id}'")
        rec = ProviderResourceRecord(
            resource_id=resource_id,
            kind=kind,
            provider=provider,
            state=initial_state,
            ephemeral=ephemeral,
        )
        self._resources[resource_id] = rec
        return rec

    def transition(
        self,
        resource_id: str,
        new_state: ProviderResourceState,
        *,
        fence_position: Optional[str] = None,
        error_detail: Optional[str] = None,
    ) -> ProviderResourceRecord:
        if resource_id not in self._resources:
            raise ProviderResourceLifecycleError(f"unknown provider resource_id '{resource_id}'")
        current = self._resources[resource_id]
        allowed = _ALLOWED_RESOURCE_TRANSITIONS.get(current.state, frozenset())
        if new_state not in allowed:
            raise ProviderResourceLifecycleError(
                f"invalid transition for '{resource_id}' from {current.state.value} to {new_state.value}"
            )
        updated = dataclasses.replace(
            current,
            state=new_state,
            fence_position=fence_position if fence_position is not None else current.fence_position,
            error_detail=error_detail if error_detail is not None else current.error_detail,
        )
        self._resources[resource_id] = updated
        return updated

    def cleanup_all_ephemeral(self) -> Tuple[str, ...]:
        """Transition all ephemeral resources to CLEANED_UP and return their IDs."""
        cleaned: List[str] = []
        for rid, rec in list(self._resources.items()):
            if not rec.ephemeral or rec.state == ProviderResourceState.CLEANED_UP:
                continue
            if rec.state != ProviderResourceState.TEARDOWN_PENDING:
                self.transition(rid, ProviderResourceState.TEARDOWN_PENDING)
            self.transition(rid, ProviderResourceState.CLEANED_UP)
            cleaned.append(rid)
        return tuple(cleaned)

    def uncleaned_ephemeral_ids(self) -> Tuple[str, ...]:
        return tuple(
            rid
            for rid, rec in sorted(self._resources.items())
            if rec.ephemeral and rec.state != ProviderResourceState.CLEANED_UP
        )


@dataclass(frozen=True)
class PathCompletionVerdict:
    completed: bool
    care_badge: CareBadge
    path_kind: MigrationPathKind
    provider_reported_complete: bool
    fence_reached: bool
    primary_data_verified: bool
    secondary_validation: SecondaryValidationReport
    uncleaned_ephemeral_resources: Tuple[str, ...]
    reason: str


def evaluate_path_completion(
    *,
    path_selection: PathSelectionResult,
    provider_reported_complete: bool,
    source_fence_position: str,
    target_replayed_position: str,
    primary_data_verified: bool,
    source_catalog: SecondaryObjectCatalog,
    target_catalog: SecondaryObjectCatalog,
    lifecycle: ProviderResourceLifecycle,
    required_secondary: Optional[Iterable[SecondaryObjectKind]] = None,
) -> PathCompletionVerdict:
    """Evaluate end-to-end completion semantics across provider status, fence, and coverage.

    A provider reporting 'complete' or 'available' never certifies migration completion
    until:
    1. Target replay position reaches or exceeds `source_fence_position`,
    2. Primary table data is verified,
    3. All required secondary objects (`views`, `sequences`, `grants`, `routines`,
       `triggers`) pass independent validation,
    4. Ephemeral provider resources are cleaned up (or flagged for REVIEW if lingering).
    """
    if path_selection.chosen_path is None:
        raise ProviderPathError("cannot evaluate completion when no migration path was chosen")

    req_kinds = (
        frozenset(required_secondary)
        if required_secondary is not None
        else source_catalog.required_kinds()
    )
    sec_report = verify_secondary_objects(
        source_catalog, target_catalog, required_kinds=req_kinds
    )
    fence_ok = bool(source_fence_position and target_replayed_position) and (
        str(target_replayed_position) >= str(source_fence_position)
    )
    uncleaned = lifecycle.uncleaned_ephemeral_ids()

    if not provider_reported_complete:
        return PathCompletionVerdict(
            completed=False,
            care_badge=CareBadge.ACTION,
            path_kind=path_selection.chosen_path,
            provider_reported_complete=False,
            fence_reached=fence_ok,
            primary_data_verified=primary_data_verified,
            secondary_validation=sec_report,
            uncleaned_ephemeral_resources=uncleaned,
            reason="ACTION: provider job has not reached terminal completion state",
        )

    if not fence_ok:
        return PathCompletionVerdict(
            completed=False,
            care_badge=CareBadge.ACTION,
            path_kind=path_selection.chosen_path,
            provider_reported_complete=True,
            fence_reached=False,
            primary_data_verified=primary_data_verified,
            secondary_validation=sec_report,
            uncleaned_ephemeral_resources=uncleaned,
            reason=(
                f"ACTION: target replayed position '{target_replayed_position}' trails "
                f"source cutover fence '{source_fence_position}'"
            ),
        )

    if not primary_data_verified:
        return PathCompletionVerdict(
            completed=False,
            care_badge=CareBadge.ACTION,
            path_kind=path_selection.chosen_path,
            provider_reported_complete=True,
            fence_reached=True,
            primary_data_verified=False,
            secondary_validation=sec_report,
            uncleaned_ephemeral_resources=uncleaned,
            reason="ACTION: primary table data verification has not passed",
        )

    if not sec_report.verified:
        return PathCompletionVerdict(
            completed=False,
            care_badge=CareBadge.ACTION,
            path_kind=path_selection.chosen_path,
            provider_reported_complete=True,
            fence_reached=True,
            primary_data_verified=True,
            secondary_validation=sec_report,
            uncleaned_ephemeral_resources=uncleaned,
            reason=sec_report.summary,
        )

    if uncleaned:
        return PathCompletionVerdict(
            completed=True,
            care_badge=CareBadge.REVIEW,
            path_kind=path_selection.chosen_path,
            provider_reported_complete=True,
            fence_reached=True,
            primary_data_verified=True,
            secondary_validation=sec_report,
            uncleaned_ephemeral_resources=uncleaned,
            reason=f"REVIEW: data and secondary objects verified, but ephemeral resources remain: {uncleaned}",
        )

    return PathCompletionVerdict(
        completed=True,
        care_badge=CareBadge.PASS,
        path_kind=path_selection.chosen_path,
        provider_reported_complete=True,
        fence_reached=True,
        primary_data_verified=True,
        secondary_validation=sec_report,
        uncleaned_ephemeral_resources=(),
        reason="PASS: provider path fence, primary data, secondary objects, and resource teardown verified",
    )


# -----------------------------------------------------------------------------
# O(1) Typed Checkpoint & Lease Integration (via migkit.checkpoint & migkit.lease)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ProviderPathCheckpointUnit:
    unit_id: str
    path_kind: MigrationPathKind
    min_cursor: Any
    max_cursor: Any
    fence_position: str
    secondary_verified: bool
    completed_at: datetime.datetime


class ProviderPathCheckpointStore:
    """Durable $O(1)$ checkpoint store delegating cursor typing to `migkit.checkpoint`."""

    def __init__(self, path: pathlib.Path) -> None:
        self.path = pathlib.Path(path)
        self._units: Dict[str, ProviderPathCheckpointUnit] = {}
        if self.path.exists():
            self._load()

    def _load(self) -> None:
        raw = json.loads(self.path.read_text(encoding="ascii"))
        units_raw = raw.get("units", {})
        for uid, item in units_raw.items():
            self._units[uid] = ProviderPathCheckpointUnit(
                unit_id=uid,
                path_kind=MigrationPathKind(item["path_kind"]),
                min_cursor=checkpoint.deserialize_cursor(item["min_cursor"]),
                max_cursor=checkpoint.deserialize_cursor(item["max_cursor"]),
                fence_position=str(item["fence_position"]),
                secondary_verified=bool(item["secondary_verified"]),
                completed_at= checkpoint.deserialize_cursor(item["completed_at"]),
            )

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "units": {
                uid: {
                    "path_kind": u.path_kind.value,
                    "min_cursor": checkpoint.serialize_cursor(u.min_cursor),
                    "max_cursor": checkpoint.serialize_cursor(u.max_cursor),
                    "fence_position": u.fence_position,
                    "secondary_verified": u.secondary_verified,
                    "completed_at": checkpoint.serialize_cursor(u.completed_at),
                }
                for uid, u in self._units.items()
            }
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, ensure_ascii=True), encoding="ascii")
        tmp.replace(self.path)

    def record_unit(self, unit: ProviderPathCheckpointUnit) -> None:
        self._units[unit.unit_id] = unit
        self._save()

    def is_completed_o1(self, unit_id: str) -> bool:
        return unit_id in self._units

    def get_unit(self, unit_id: str) -> Optional[ProviderPathCheckpointUnit]:
        return self._units.get(unit_id)


def acquire_provider_path_lease(
    lock_path: pathlib.Path,
    *,
    what: str = "provider-path verification",
    term_seconds: float = 60.0,
) -> lease.Lease:
    """Acquire a mutual-exclusion hop lease using `migkit.lease.Lease`."""
    return lease.Lease(pathlib.Path(lock_path), what=what, term=term_seconds).acquire()
