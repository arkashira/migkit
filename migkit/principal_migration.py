"""Principal security semantics migration (Row 632: RCH24.principal-migration).

Migrates security semantics without indiscriminate grant copying.
Models user, role, grant, ownership, default privilege, and DEFINER/INVOKER
semantics separately from credentials and provider-managed principals.

Maps identities and approved conflict policies explicitly.
Discovers and resolves missing sequence privileges, RLS, definer changes,
and duplicate account names within policy, preventing unintended privilege
widening or silent use of migrator ownership.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import copy
import dataclasses
import enum
import hashlib
import json
import logging
import os
import re
import socket
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from . import decide

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Decision Engine Rungs (Row 632: RCH24)
# -----------------------------------------------------------------------------

def principal_security_policy_rungs(facts: Optional[decide.Facts] = None) -> Tuple[decide.Rung, ...]:
    """Ladder for certifying principal security migration policies."""
    return (
        decide.Rung(
            "strict_unwidened",
            "execute strict unwidened principal migration",
            needs=(
                decide.Need("no_privilege_widening", lambda f: bool(f.get("no_privilege_widening"))),
                decide.Need("no_silent_ownership", lambda f: bool(f.get("no_silent_ownership"))),
                decide.Need("mapped_principals_valid", lambda f: bool(f.get("mapped_principals_valid"))),
            ),
        ),
        decide.Rung(
            "remediated_mapping",
            "execute principal migration with explicit remediated mappings",
            needs=(
                decide.Need("remediation_mapped", lambda f: bool(f.get("remediation_mapped"))),
                decide.Need("migrator_ownership_remediated", lambda f: bool(f.get("migrator_ownership_remediated"))),
            ),
        ),
        decide.Rung(
            "quarantined_approval",
            "quarantine principal conflicts pending explicit operator signoff",
            needs=(
                decide.Need("conflicts_quarantined", lambda f: bool(f.get("conflicts_quarantined"))),
            ),
        ),
        decide.Rung(
            "unresolved_safety_violation",
            "refuse migration due to unresolved security or provider account violation",
        ),
    )


def decide_principal_security_policy(facts: Union[Dict[str, Any], decide.Facts]) -> decide.Choice:
    """Evaluates principal security policy via decide ladder."""
    f = facts if isinstance(facts, decide.Facts) else decide.Facts(facts)
    return decide.climb("principal_security_policy", principal_security_policy_rungs(f), f)



# -----------------------------------------------------------------------------
# Care Badges and Status Enums
# -----------------------------------------------------------------------------

class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class EngineDialect(str, enum.Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    GENERIC = "generic"


class PrincipalType(str, enum.Enum):
    ROLE = "ROLE"
    USER = "USER"
    GROUP = "GROUP"


class SecurityType(str, enum.Enum):
    DEFINER = "DEFINER"
    INVOKER = "INVOKER"


class ConflictAction(str, enum.Enum):
    FAIL_ON_CONFLICT = "FAIL_ON_CONFLICT"
    REMAP_EXPLICIT = "REMAP_EXPLICIT"
    PRESERVE_EXISTING = "PRESERVE_EXISTING"
    OVERWRITE_APPROVED = "OVERWRITE_APPROVED"
    MERGE_LEAST_PRIVILEGE = "MERGE_LEAST_PRIVILEGE"
    QUARANTINE = "QUARANTINE"


class DuplicateAccountResolution(str, enum.Enum):
    DISALLOW = "DISALLOW"
    MERGE_LEAST_PRIVILEGE = "MERGE_LEAST_PRIVILEGE"
    QUALIFY_BY_HOST = "QUALIFY_BY_HOST"
    QUARANTINE = "QUARANTINE"


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class PrincipalMigrationError(Exception):
    """Base exception for principal security migration failures."""


class PrivilegeWideningError(PrincipalMigrationError):
    """Raised when an operation would widen privileges beyond policy approval."""


class SilentMigratorOwnershipError(PrincipalMigrationError):
    """Raised when an object would silently retain migrator ownership."""


class IdentityConflictError(PrincipalMigrationError):
    """Raised when identity mapping encounters unapproved collisions."""


class LeaseLockError(PrincipalMigrationError):
    """Raised when lease lock cannot be acquired or refreshed."""


# -----------------------------------------------------------------------------
# Provider-Managed System Principals Constants
# -----------------------------------------------------------------------------

PROVIDER_MANAGED_PREFIXES: Dict[str, Tuple[str, ...]] = {
    "postgresql": ("pg_", "rds", "tencentdb", "cloudsql", "azure", "alloydb"),
    "postgres": ("pg_", "rds", "tencentdb", "cloudsql", "azure", "alloydb"),
    "mysql": ("AWS_", "rds", "tencent", "cloudsql", "azure_"),
    "generic": ("sys_", "system_"),
}

PROVIDER_MANAGED_EXACT: Dict[str, FrozenSet[str]] = {
    "postgresql": frozenset({
        "postgres",
        "root",
        "rdsadmin",
        "rds_superuser",
        "rds_replication",
        "cloudsqladmin",
        "cloudsqlsuperuser",
        "azure_pg_admin",
        "azure_superuser",
    }),
    "postgres": frozenset({
        "postgres",
        "root",
        "rdsadmin",
        "rds_superuser",
        "rds_replication",
        "cloudsqladmin",
        "cloudsqlsuperuser",
        "azure_pg_admin",
        "azure_superuser",
    }),
    "mysql": frozenset({
        "root",
        "mysql.sys",
        "mysql.session",
        "mysql.infoschema",
        "mariadb.sys",
        "rdsadmin",
        "rds_superuser_role",
        "rdswriteforwarduser",
        "tencentroot",
        "tencentdba",
        "cloudsqladmin",
    }),
    "generic": frozenset({"admin", "system", "root"}),
}

DEFAULT_MIGRATOR_ACCOUNTS = frozenset({
    "dts_migration",
    "migkit_admin",
    "migkit_worker",
    "migration_user",
    "postgres",
    "root",
    "rdsadmin",
})

SAFE_HOST_PREFIXES = ("127.0.0.1", "10.", "172.16.", "192.168.", "localhost")


# -----------------------------------------------------------------------------
# Core Security Semantics Dataclasses (Separate from Credentials)
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class PrincipalSpec:
    """Security principal definition with zero embedded credentials."""
    name: str
    principal_type: PrincipalType = PrincipalType.ROLE
    is_provider_managed: bool = False
    login: bool = True
    superuser: bool = False
    createdb: bool = False
    createrole: bool = False
    replication: bool = False
    bypassrls: bool = False
    inherit: bool = True
    connection_limit: int = -1
    member_of: Tuple[str, ...] = ()
    custom_attributes: Dict[str, Any] = dataclasses.field(default_factory=dict)
    credential_managed_externally: bool = True

    def normalized_name(self) -> str:
        return self.name.strip()


@dataclasses.dataclass(frozen=True)
class GrantSpec:
    """Object privilege grant without indiscriminate wildcards."""
    grantee: str
    object_type: str  # TABLE, SEQUENCE, FUNCTION, VIEW, SCHEMA, DATABASE
    schema: str
    object_name: str
    privilege: str    # SELECT, INSERT, UPDATE, DELETE, USAGE, EXECUTE, REFERENCES
    columns: Tuple[str, ...] = ()
    grantable: bool = False
    grantor: Optional[str] = None

    def signature(self) -> Tuple[str, str, str, str, str, Tuple[str, ...], bool]:
        return (
            self.grantee.strip(),
            self.object_type.upper().strip(),
            self.schema.strip(),
            self.object_name.strip(),
            self.privilege.upper().strip(),
            tuple(sorted(self.columns)),
            self.grantable,
        )


@dataclasses.dataclass(frozen=True)
class OwnershipSpec:
    """Object ownership specification."""
    object_type: str  # TABLE, SEQUENCE, VIEW, FUNCTION, TYPE, SCHEMA
    schema: str
    object_name: str
    owner: str

    def object_key(self) -> str:
        sch = f"{self.schema}." if self.schema else ""
        return f"{self.object_type.upper()}:{sch}{self.object_name}"


@dataclasses.dataclass(frozen=True)
class DefaultPrivilegeSpec:
    """Default privilege rule for future objects created by target_role."""
    target_role: str
    object_type: str  # TABLES, SEQUENCES, FUNCTIONS, TYPES, SCHEMAS
    grantee: str
    privilege: str
    schema: Optional[str] = None
    grantable: bool = False

    def signature(self) -> Tuple[str, Optional[str], str, str, str, bool]:
        return (
            self.target_role.strip(),
            self.schema.strip() if self.schema else None,
            self.object_type.upper().strip(),
            self.grantee.strip(),
            self.privilege.upper().strip(),
            self.grantable,
        )


@dataclasses.dataclass(frozen=True)
class RoutineSecuritySpec:
    """Definer and execution security semantics for routines and views."""
    schema: str
    routine_name: str
    routine_type: str  # FUNCTION, PROCEDURE, VIEW, TRIGGER
    security_type: SecurityType
    definer: str

    def object_key(self) -> str:
        sch = f"{self.schema}." if self.schema else ""
        return f"{self.routine_type.upper()}:{sch}{self.routine_name}"


@dataclasses.dataclass(frozen=True)
class RLSPolicySpec:
    """Row Level Security (RLS) state and policy definition."""
    schema: str
    table_name: str
    rls_enabled: bool = True
    force_rls: bool = False
    policy_name: str = ""
    command: str = "ALL"  # ALL, SELECT, INSERT, UPDATE, DELETE
    roles: Tuple[str, ...] = ()
    using_expression: Optional[str] = None
    with_check_expression: Optional[str] = None

    def table_key(self) -> str:
        sch = f"{self.schema}." if self.schema else ""
        return f"{sch}{self.table_name}"


@dataclasses.dataclass(frozen=True)
class SequenceDependency:
    """Explicit link between table column and identity/serial sequence."""
    schema: str
    sequence_name: str
    table_schema: str
    table_name: str
    column_name: str

    def sequence_key(self) -> str:
        return f"{self.schema}.{self.sequence_name}"

    def table_key(self) -> str:
        return f"{self.table_schema}.{self.table_name}"


# -----------------------------------------------------------------------------
# Catalog Security State Container
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class CatalogSecurityState:
    """Full snapshot of security semantics on an endpoint (source or target)."""
    principals: Dict[str, PrincipalSpec] = dataclasses.field(default_factory=dict)
    grants: List[GrantSpec] = dataclasses.field(default_factory=list)
    ownerships: List[OwnershipSpec] = dataclasses.field(default_factory=list)
    default_privileges: List[DefaultPrivilegeSpec] = dataclasses.field(default_factory=list)
    routine_securities: List[RoutineSecuritySpec] = dataclasses.field(default_factory=list)
    rls_policies: List[RLSPolicySpec] = dataclasses.field(default_factory=list)
    sequence_dependencies: List[SequenceDependency] = dataclasses.field(default_factory=list)


# -----------------------------------------------------------------------------
# Policy and Audit Findings
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class IdentityMappingRule:
    """Explicit mapping from source identity to target identity."""
    source_name: str
    target_name: str
    notes: str = ""


@dataclasses.dataclass(frozen=True)
class PrincipalMigrationPolicy:
    """Governance policy governing principal and security migration."""
    migrator_accounts: FrozenSet[str] = DEFAULT_MIGRATOR_ACCOUNTS
    approved_identity_mappings: Dict[str, str] = dataclasses.field(default_factory=dict)
    disallowed_privilege_widening: bool = True
    allow_superuser_migration: bool = False
    allow_bypassrls_migration: bool = False
    conflict_action: ConflictAction = ConflictAction.FAIL_ON_CONFLICT
    duplicate_account_resolution: DuplicateAccountResolution = DuplicateAccountResolution.DISALLOW
    auto_resolve_sequence_dependencies: bool = True
    auto_reassign_migrator_ownership: bool = True
    target_default_owner: Optional[str] = None
    safe_host_prefixes: Tuple[str, ...] = SAFE_HOST_PREFIXES


@dataclasses.dataclass(frozen=True)
class SecurityFinding:
    """Auditing finding with standard care badge and targeted remediation."""
    badge: CareBadge
    category: str
    object_name: str
    message: str
    action_required: Optional[str] = None
    remediation_ddl: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "badge": self.badge.value,
            "category": self.category,
            "object_name": self.object_name,
            "message": self.message,
            "action_required": self.action_required,
            "remediation_ddl": self.remediation_ddl,
        }


@dataclasses.dataclass
class MigrationPlan:
    """Compiled migration plan for security semantics."""
    findings: List[SecurityFinding] = dataclasses.field(default_factory=list)
    planned_principals: List[PrincipalSpec] = dataclasses.field(default_factory=list)
    planned_grants: List[GrantSpec] = dataclasses.field(default_factory=list)
    planned_ownerships: List[OwnershipSpec] = dataclasses.field(default_factory=list)
    planned_default_privileges: List[DefaultPrivilegeSpec] = dataclasses.field(default_factory=list)
    planned_routines: List[RoutineSecuritySpec] = dataclasses.field(default_factory=list)
    planned_rls: List[RLSPolicySpec] = dataclasses.field(default_factory=list)
    remediation_ddl: List[str] = dataclasses.field(default_factory=list)

    @property
    def has_blocking_actions(self) -> bool:
        return any(f.badge == CareBadge.ACTION for f in self.findings)

    @property
    def summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {b.value: 0 for b in CareBadge}
        for f in self.findings:
            counts[f.badge.value] = counts.get(f.badge.value, 0) + 1
        return counts

    def format_terse(self) -> str:
        """Pure UNIX style report output. No emojis or decorative glyphs."""
        lines: List[str] = []
        counts = self.summary
        lines.append(
            f"SUMMARY: {counts.get('PASS', 0)} PASS, {counts.get('BENIGN', 0)} BENIGN, "
            f"{counts.get('REVIEW', 0)} REVIEW, {counts.get('ACTION', 0)} ACTION"
        )
        for f in self.findings:
            if f.badge in (CareBadge.ACTION, CareBadge.REVIEW):
                action_part = f" -> ACTION: {f.action_required}" if f.action_required else ""
                lines.append(f"{f.badge.value} [{f.category}] {f.object_name}: {f.message}{action_part}")
        if not lines or len(lines) == 1:
            lines.append("PASS: security semantics verified without privilege widening or migrator leakage")
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# Principal Security Migrator Engine
# -----------------------------------------------------------------------------

class PrincipalSecurityMigrator:
    """Orchestrates security semantics migration between source and target."""

    def __init__(
        self,
        dialect: Union[str, EngineDialect] = EngineDialect.POSTGRESQL,
        policy: Optional[PrincipalMigrationPolicy] = None,
    ):
        if isinstance(dialect, str):
            self.dialect = EngineDialect(dialect.lower())
        else:
            self.dialect = dialect
        self.policy = policy or PrincipalMigrationPolicy()

    def is_provider_managed(self, name: str) -> bool:
        """Determines if principal is cloud-provider or database engine internal."""
        dialect_key = self.dialect.value
        prefixes = PROVIDER_MANAGED_PREFIXES.get(dialect_key, ())
        exact_set = PROVIDER_MANAGED_EXACT.get(dialect_key, frozenset())

        n = name.strip()
        # For MySQL, strip host component if present for system check
        if "@" in n and self.dialect == EngineDialect.MYSQL:
            user_part, _ = n.split("@", 1)
            user_part = user_part.strip("'\"`")
            if user_part in exact_set or any(user_part.startswith(p) for p in prefixes):
                return True

        if n in exact_set:
            return True
        return any(n.startswith(p) for p in prefixes)

    def is_migrator_account(self, name: str) -> bool:
        """Checks if account matches configured migration tooling accounts."""
        clean = name.strip()
        if "@" in clean and self.dialect == EngineDialect.MYSQL:
            clean = clean.split("@", 1)[0].strip("'\"`")
        return clean in self.policy.migrator_accounts

    def map_identity(self, source_identity: str) -> str:
        """Maps source principal identity to target identity under policy."""
        s = source_identity.strip()
        if s in self.policy.approved_identity_mappings:
            return self.policy.approved_identity_mappings[s]

        # For MySQL identities user@host, support mapping user part
        if "@" in s and self.dialect == EngineDialect.MYSQL:
            user, host = s.split("@", 1)
            user_clean = user.strip("'\"`")
            if user_clean in self.policy.approved_identity_mappings:
                mapped_user = self.policy.approved_identity_mappings[user_clean]
                return f"{mapped_user}@{host}"

        return s

    def validate_host_safety(self, host: str) -> bool:
        """Validates host against safe test/enterprise prefixes."""
        clean = host.strip("'\"`")
        if clean == "%":
            return True
        return any(clean.startswith(prefix) for prefix in self.policy.safe_host_prefixes)

    def detect_duplicate_accounts(
        self, principals: Sequence[PrincipalSpec]
    ) -> List[SecurityFinding]:
        """Discovers duplicate account names or ambigious multi-host collissions."""
        findings: List[SecurityFinding] = []
        name_groups: Dict[str, List[PrincipalSpec]] = collections.defaultdict(list)

        for p in principals:
            raw = p.name.strip()
            # Group by bare username
            bare = raw.split("@")[0].strip("'\"`").lower()
            name_groups[bare].append(p)

        for bare, group in sorted(name_groups.items()):
            if len(group) > 1:
                names = [g.name for g in group]
                if self.policy.duplicate_account_resolution == DuplicateAccountResolution.DISALLOW:
                    findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="DUPLICATE_ACCOUNT",
                            object_name=bare,
                            message=f"duplicate account variants detected: {names}",
                            action_required="disambiguate account identities or set approved merge policy",
                        )
                    )
                elif self.policy.duplicate_account_resolution == DuplicateAccountResolution.MERGE_LEAST_PRIVILEGE:
                    findings.append(
                        SecurityFinding(
                            badge=CareBadge.REVIEW,
                            category="DUPLICATE_ACCOUNT",
                            object_name=bare,
                            message=f"merging duplicate account variants under least privilege: {names}",
                            action_required="verify combined role memberships post-merge",
                        )
                    )
                elif self.policy.duplicate_account_resolution == DuplicateAccountResolution.QUALIFY_BY_HOST:
                    findings.append(
                        SecurityFinding(
                            badge=CareBadge.BENIGN,
                            category="DUPLICATE_ACCOUNT",
                            object_name=bare,
                            message=f"preserving host-qualified separation for {names}",
                        )
                    )
                else:  # QUARANTINE
                    findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="DUPLICATE_ACCOUNT",
                            object_name=bare,
                            message=f"quarantined ambiguous account variants: {names}",
                            action_required="manual identity review required before migration",
                        )
                    )
        return findings

    # -------------------------------------------------------------------------
    # Comprehensive Auditing and Planning
    # -------------------------------------------------------------------------

    def audit_and_plan(
        self,
        source: CatalogSecurityState,
        target: Optional[CatalogSecurityState] = None,
    ) -> MigrationPlan:
        """Audits security semantics and generates an un-widened, migrator-clean plan."""
        plan = MigrationPlan()
        tgt = target or CatalogSecurityState()

        # 1. Audit and map principals
        self._audit_principals(source, tgt, plan)

        # 2. Audit ownerships and eliminate silent migrator ownership
        self._audit_ownerships(source, tgt, plan)

        # 3. Audit object grants and prevent privilege widening
        self._audit_grants(source, tgt, plan)

        # 4. Resolve missing sequence privileges
        if self.policy.auto_resolve_sequence_dependencies:
            self._audit_sequence_privileges(source, tgt, plan)

        # 5. Audit Row Level Security (RLS)
        self._audit_rls(source, tgt, plan)

        # 6. Audit DEFINER / INVOKER routine semantics
        self._audit_routine_security(source, tgt, plan)

        # 7. Audit default privileges
        self._audit_default_privileges(source, tgt, plan)

        # 8. Generate remediation DDL
        plan.remediation_ddl = self.generate_ddl(plan)

        return plan

    # -------------------------------------------------------------------------
    # Internal Audit Phases
    # -------------------------------------------------------------------------

    def _audit_principals(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        # Check duplicate accounts in source
        plan.findings.extend(self.detect_duplicate_accounts(list(src.principals.values())))

        src_user_principals = [
            p for p in src.principals.values() if not self.is_provider_managed(p.name)
        ]

        for p in src_user_principals:
            target_identity = self.map_identity(p.name)

            # Prevent superuser widening
            if p.superuser and not self.policy.allow_superuser_migration:
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.ACTION,
                        category="PRIVILEGE_WIDENING",
                        object_name=p.name,
                        message=f"source principal holds SUPERUSER; widening into target environment disallowed",
                        action_required="downgrade role to least privilege application role",
                    )
                )

            # Prevent BYPASSRLS widening
            if p.bypassrls and not self.policy.allow_bypassrls_migration:
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.ACTION,
                        category="PRIVILEGE_WIDENING",
                        object_name=p.name,
                        message=f"source principal holds BYPASSRLS; carrying without security review disallowed",
                        action_required="strip BYPASSRLS or verify tenant isolation exception",
                    )
                )

            # Check if target already has principal
            if target_identity in tgt.principals:
                tgt_p = tgt.principals[target_identity]
                if self.policy.conflict_action == ConflictAction.FAIL_ON_CONFLICT:
                    if tgt_p.superuser and not p.superuser:
                        plan.findings.append(
                            SecurityFinding(
                                badge=CareBadge.ACTION,
                                category="IDENTITY_CONFLICT",
                                object_name=target_identity,
                                message="target principal already exists with wider privileges (SUPERUSER)",
                                action_required="refuse migration or adopt least privilege target role",
                            )
                        )
                    else:
                        plan.findings.append(
                            SecurityFinding(
                                badge=CareBadge.REVIEW,
                                category="IDENTITY_CONFLICT",
                                object_name=target_identity,
                                message="target principal already exists; adopting existing target definition",
                            )
                        )
            else:
                # Principal needs creation on target
                mapped_members = tuple(sorted([self.map_identity(m) for m in p.member_of]))
                clean_p = dataclasses.replace(
                    p,
                    name=target_identity,
                    superuser=False if not self.policy.allow_superuser_migration else p.superuser,
                    bypassrls=False if not self.policy.allow_bypassrls_migration else p.bypassrls,
                    member_of=mapped_members,
                    credential_managed_externally=True,
                )
                plan.planned_principals.append(clean_p)
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.PASS,
                        category="PRINCIPAL_MAPPING",
                        object_name=target_identity,
                        message=f"planned mapped principal without credentials: {p.name} -> {target_identity}",
                    )
                )

    def _audit_ownerships(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        tgt_owner_map = {o.object_key(): o for o in tgt.ownerships}

        for src_own in src.ownerships:
            key = src_own.object_key()
            mapped_owner = self.map_identity(src_own.owner)

            # Check if source owner was provider-managed or migrator
            if self.is_migrator_account(src_own.owner) or self.is_provider_managed(src_own.owner):
                if self.policy.target_default_owner:
                    mapped_owner = self.policy.target_default_owner

            # Check target state
            tgt_own = tgt_owner_map.get(key)
            if tgt_own:
                if self.is_migrator_account(tgt_own.owner):
                    # Target has silent migrator ownership!
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="MIGRATOR_OWNERSHIP",
                            object_name=key,
                            message=(
                                f"object is owned by migrator account '{tgt_own.owner}'; "
                                f"target owner must be remapped to application owner '{mapped_owner}'"
                            ),
                            action_required=f"reassign ownership to '{mapped_owner}'",
                            remediation_ddl=self._render_owner_ddl(src_own.object_type, src_own.schema, src_own.object_name, mapped_owner),
                        )
                    )
                    plan.planned_ownerships.append(
                        OwnershipSpec(
                            object_type=src_own.object_type,
                            schema=src_own.schema,
                            object_name=src_own.object_name,
                            owner=mapped_owner,
                        )
                    )
                elif tgt_own.owner != mapped_owner:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.REVIEW,
                            category="OWNERSHIP_DRIFT",
                            object_name=key,
                            message=f"ownership difference: target is '{tgt_own.owner}', expected '{mapped_owner}'",
                            remediation_ddl=self._render_owner_ddl(src_own.object_type, src_own.schema, src_own.object_name, mapped_owner),
                        )
                    )
                    plan.planned_ownerships.append(
                        OwnershipSpec(
                            object_type=src_own.object_type,
                            schema=src_own.schema,
                            object_name=src_own.object_name,
                            owner=mapped_owner,
                        )
                    )
                else:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.PASS,
                            category="OWNERSHIP_PARITY",
                            object_name=key,
                            message=f"ownership verified matching '{mapped_owner}'",
                        )
                    )
            else:
                # Object ownership to be established on target
                plan.planned_ownerships.append(
                    OwnershipSpec(
                        object_type=src_own.object_type,
                        schema=src_own.schema,
                        object_name=src_own.object_name,
                        owner=mapped_owner,
                    )
                )

    def _audit_grants(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        tgt_grant_sigs = {g.signature(): g for g in tgt.grants}

        for src_g in src.grants:
            # Skip provider-managed grantees
            if self.is_provider_managed(src_g.grantee):
                continue

            mapped_grantee = self.map_identity(src_g.grantee)
            planned_g = dataclasses.replace(src_g, grantee=mapped_grantee)
            sig = planned_g.signature()

            if sig in tgt_grant_sigs:
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.PASS,
                        category="GRANT_PARITY",
                        object_name=f"{src_g.schema}.{src_g.object_name}",
                        message=f"grant {src_g.privilege} on {src_g.object_type} present on target for {mapped_grantee}",
                    )
                )
            else:
                plan.planned_grants.append(planned_g)

        # Detect privilege widening on target
        src_mapped_sigs = {
            dataclasses.replace(g, grantee=self.map_identity(g.grantee)).signature()
            for g in src.grants
        }

        # Hierarchy of privileges to check excessive widening
        dangerous_privs = {"ALL", "ALL PRIVILEGES", "SUPERUSER"}

        for tgt_g in tgt.grants:
            if self.is_provider_managed(tgt_g.grantee):
                continue
            if tgt_g.signature() not in src_mapped_sigs:
                if tgt_g.privilege.upper() in dangerous_privs or tgt_g.grantable:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="PRIVILEGE_WIDENING",
                            object_name=f"{tgt_g.schema}.{tgt_g.object_name}",
                            message=(
                                f"target has extra grant with excessive privilege: {tgt_g.privilege} "
                                f"(grantable={tgt_g.grantable}) for {tgt_g.grantee}"
                            ),
                            action_required="revoke unexpected wide privilege on target",
                            remediation_ddl=self._render_revoke_ddl(tgt_g),
                        )
                    )
                else:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.BENIGN,
                            category="EXTRA_GRANT",
                            object_name=f"{tgt_g.schema}.{tgt_g.object_name}",
                            message=f"target has extra non-widening grant: {tgt_g.privilege} for {tgt_g.grantee}",
                        )
                    )

    def _audit_sequence_privileges(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        """Ensures roles with INSERT/UPDATE table rights have USAGE on underlying sequences."""
        dep_map: Dict[Tuple[str, str], List[SequenceDependency]] = collections.defaultdict(list)
        all_deps = list(src.sequence_dependencies) + list(tgt.sequence_dependencies)

        for dep in all_deps:
            dep_map[(dep.table_schema, dep.table_name)].append(dep)

        existing_tgt_seq_grants = {
            (g.grantee, g.schema, g.object_name, g.privilege.upper())
            for g in tgt.grants
            if g.object_type.upper() == "SEQUENCE"
        }

        # Check all table grants in source or plan
        combined_table_grants = list(src.grants) + list(plan.planned_grants)
        seen_required_seq: Set[Tuple[str, str, str, str]] = set()

        for g in combined_table_grants:
            if g.object_type.upper() != "TABLE":
                continue
            if g.privilege.upper() in ("INSERT", "UPDATE", "ALL", "ALL PRIVILEGES"):
                grantee = self.map_identity(g.grantee)
                deps = dep_map.get((g.schema, g.object_name), [])
                for d in deps:
                    req_key = (grantee, d.schema, d.sequence_name, "USAGE")
                    if req_key not in seen_required_seq and req_key not in existing_tgt_seq_grants:
                        seen_required_seq.add(req_key)
                        seq_grant = GrantSpec(
                            grantee=grantee,
                            object_type="SEQUENCE",
                            schema=d.schema,
                            object_name=d.sequence_name,
                            privilege="USAGE",
                            grantable=False,
                        )
                        plan.planned_grants.append(seq_grant)
                        plan.findings.append(
                            SecurityFinding(
                                badge=CareBadge.ACTION,
                                category="MISSING_SEQUENCE_GRANT",
                                object_name=f"{d.schema}.{d.sequence_name}",
                                message=(
                                    f"table {g.schema}.{g.object_name} requires USAGE on sequence "
                                    f"{d.schema}.{d.sequence_name} for grantee {grantee} (permission denied hazard)"
                                ),
                                action_required="grant USAGE on sequence to table writer",
                                remediation_ddl=self._render_grant_ddl(seq_grant),
                            )
                        )

    def _audit_rls(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        """Audits Row Level Security enablement and policy synchronization."""
        tgt_rls_map = {p.table_key(): p for p in tgt.rls_policies if p.policy_name == ""}
        tgt_policies_map = {
            (p.schema, p.table_name, p.policy_name): p
            for p in tgt.rls_policies
            if p.policy_name != ""
        }

        for src_rls in src.rls_policies:
            if src_rls.policy_name == "":
                # Table-level RLS enablement check
                key = src_rls.table_key()
                tgt_tbl = tgt_rls_map.get(key)
                if not tgt_tbl or not tgt_tbl.rls_enabled:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="RLS_DRIFT",
                            object_name=key,
                            message=f"table has RLS enabled on source but disabled on target",
                            action_required="enable row level security on target table",
                            remediation_ddl=f"ALTER TABLE {self._quote_ident(src_rls.schema)}.{self._quote_ident(src_rls.table_name)} ENABLE ROW LEVEL SECURITY;",
                        )
                    )
                    plan.planned_rls.append(src_rls)
            else:
                # Specific policy check
                pkey = (src_rls.schema, src_rls.table_name, src_rls.policy_name)
                mapped_roles = tuple(sorted([self.map_identity(r) for r in src_rls.roles]))
                planned_policy = dataclasses.replace(src_rls, roles=mapped_roles)

                if pkey not in tgt_policies_map:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="RLS_DRIFT",
                            object_name=f"{src_rls.table_key()}:{src_rls.policy_name}",
                            message=f"RLS policy '{src_rls.policy_name}' missing on target table",
                            action_required="create missing RLS policy on target",
                            remediation_ddl=self._render_rls_policy_ddl(planned_policy),
                        )
                    )
                    plan.planned_rls.append(planned_policy)
                else:
                    tgt_p = tgt_policies_map[pkey]
                    # Check definition equality
                    if (
                        tgt_p.using_expression != src_rls.using_expression
                        or tgt_p.with_check_expression != src_rls.with_check_expression
                        or set(tgt_p.roles) != set(mapped_roles)
                    ):
                        plan.findings.append(
                            SecurityFinding(
                                badge=CareBadge.ACTION,
                                category="RLS_DRIFT",
                                object_name=f"{src_rls.table_key()}:{src_rls.policy_name}",
                                message=f"RLS policy expressions or role scoping differ between source and target",
                                action_required="reconcile RLS policy expression",
                                remediation_ddl=self._render_rls_policy_ddl(planned_policy, replace=True),
                            )
                        )
                        plan.planned_rls.append(planned_policy)
                    else:
                        plan.findings.append(
                            SecurityFinding(
                                badge=CareBadge.PASS,
                                category="RLS_PARITY",
                                object_name=f"{src_rls.table_key()}:{src_rls.policy_name}",
                                message="RLS policy parity verified",
                            )
                        )

    def _audit_routine_security(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        """Audits routine DEFINER/INVOKER execution context."""
        tgt_routines = {r.object_key(): r for r in tgt.routine_securities}

        for src_r in src.routine_securities:
            key = src_r.object_key()
            mapped_definer = self.map_identity(src_r.definer)
            planned_r = dataclasses.replace(src_r, definer=mapped_definer)

            tgt_r = tgt_routines.get(key)
            if tgt_r:
                # 1. Definer became migrator account
                if self.is_migrator_account(tgt_r.definer):
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="DEFINER_DRIFT",
                            object_name=key,
                            message=(
                                f"routine definer is migrator account '{tgt_r.definer}'; "
                                f"must be restored to application definer '{mapped_definer}'"
                            ),
                            action_required=f"reassign routine definer to '{mapped_definer}'",
                            remediation_ddl=self._render_routine_security_ddl(planned_r),
                        )
                    )
                    plan.planned_routines.append(planned_r)
                # 2. DEFINER changed to INVOKER
                elif (
                    src_r.security_type == SecurityType.DEFINER
                    and tgt_r.security_type == SecurityType.INVOKER
                ):
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.ACTION,
                            category="DEFINER_DRIFT",
                            object_name=key,
                            message=(
                                "routine security mode demoted from DEFINER to INVOKER; "
                                "routine will execute with caller permissions and likely fail"
                            ),
                            action_required="restore routine to SECURITY DEFINER",
                            remediation_ddl=self._render_routine_security_ddl(planned_r),
                        )
                    )
                    plan.planned_routines.append(planned_r)
                elif tgt_r.definer != mapped_definer:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.REVIEW,
                            category="DEFINER_DRIFT",
                            object_name=key,
                            message=f"routine definer mismatch: target is '{tgt_r.definer}', source mapped is '{mapped_definer}'",
                            remediation_ddl=self._render_routine_security_ddl(planned_r),
                        )
                    )
                    plan.planned_routines.append(planned_r)
                else:
                    plan.findings.append(
                        SecurityFinding(
                            badge=CareBadge.PASS,
                            category="DEFINER_PARITY",
                            object_name=key,
                            message=f"routine security semantics verified ({src_r.security_type.value} as {mapped_definer})",
                        )
                    )
            else:
                plan.planned_routines.append(planned_r)

    def _audit_default_privileges(
        self,
        src: CatalogSecurityState,
        tgt: CatalogSecurityState,
        plan: MigrationPlan,
    ) -> None:
        """Audits default privilege rules for future objects."""
        tgt_sigs = {dp.signature() for dp in tgt.default_privileges}

        for src_dp in src.default_privileges:
            mapped_target_role = self.map_identity(src_dp.target_role)
            mapped_grantee = self.map_identity(src_dp.grantee)
            planned_dp = dataclasses.replace(
                src_dp,
                target_role=mapped_target_role,
                grantee=mapped_grantee,
            )

            if planned_dp.signature() in tgt_sigs:
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.PASS,
                        category="DEFAULT_PRIVILEGE_PARITY",
                        object_name=f"FOR {mapped_target_role} ON {src_dp.object_type}",
                        message=f"default privilege {src_dp.privilege} to {mapped_grantee} verified",
                    )
                )
            else:
                plan.planned_default_privileges.append(planned_dp)
                plan.findings.append(
                    SecurityFinding(
                        badge=CareBadge.ACTION,
                        category="DEFAULT_PRIVILEGE_GAP",
                        object_name=f"FOR {mapped_target_role} ON {src_dp.object_type}",
                        message=(
                            f"missing default privilege on target: future {src_dp.object_type} created by "
                            f"{mapped_target_role} will not grant {src_dp.privilege} to {mapped_grantee}"
                        ),
                        action_required="apply ALTER DEFAULT PRIVILEGES statement",
                        remediation_ddl=self._render_default_privilege_ddl(planned_dp),
                    )
                )

    # -------------------------------------------------------------------------
    # DDL Generation
    # -------------------------------------------------------------------------

    def generate_ddl(self, plan: MigrationPlan) -> List[str]:
        """Generates deterministic remediation DDL statements."""
        ddl: List[str] = []

        # 1. Create principals
        for p in plan.planned_principals:
            ddl.append(self._render_create_principal_ddl(p))
            for m in p.member_of:
                ddl.append(self._render_grant_role_ddl(m, p.name))

        # 2. Ownership fixes
        for o in plan.planned_ownerships:
            stmt = self._render_owner_ddl(o.object_type, o.schema, o.object_name, o.owner)
            if stmt and stmt not in ddl:
                ddl.append(stmt)

        # 3. Object grants
        for g in plan.planned_grants:
            stmt = self._render_grant_ddl(g)
            if stmt and stmt not in ddl:
                ddl.append(stmt)

        # 4. RLS policies
        for rls in plan.planned_rls:
            if rls.policy_name == "":
                stmt = f"ALTER TABLE {self._quote_ident(rls.schema)}.{self._quote_ident(rls.table_name)} ENABLE ROW LEVEL SECURITY;"
                if stmt not in ddl:
                    ddl.append(stmt)
            else:
                stmt = self._render_rls_policy_ddl(rls)
                if stmt not in ddl:
                    ddl.append(stmt)

        # 5. Routine securities
        for r in plan.planned_routines:
            stmt = self._render_routine_security_ddl(r)
            if stmt and stmt not in ddl:
                ddl.append(stmt)

        # 6. Default privileges
        for dp in plan.planned_default_privileges:
            stmt = self._render_default_privilege_ddl(dp)
            if stmt and stmt not in ddl:
                ddl.append(stmt)

        return ddl

    # -------------------------------------------------------------------------
    # Statement Renderers (Safe SQL Quoting, Zero Secrets)
    # -------------------------------------------------------------------------

    def _quote_ident(self, name: str) -> str:
        if self.dialect == EngineDialect.MYSQL:
            return "`" + name.replace("`", "``") + "`"
        return '"' + name.replace('"', '""') + '"'

    def _format_principal(self, name: str) -> str:
        if self.dialect == EngineDialect.MYSQL:
            if "@" in name:
                u, h = name.split("@", 1)
                u = u.strip("'\"`")
                h = h.strip("'\"`")
                return f"'{u}'@'{h}'"
            return f"'{name}'@'%'"
        return self._quote_ident(name)

    def _render_create_principal_ddl(self, p: PrincipalSpec) -> str:
        if self.dialect == EngineDialect.MYSQL:
            user_expr = self._format_principal(p.name)
            return f"CREATE USER IF NOT EXISTS {user_expr};"

        clauses: List[str] = []
        clauses.append("LOGIN" if p.login else "NOLOGIN")
        if p.superuser:
            clauses.append("SUPERUSER")
        if p.createdb:
            clauses.append("CREATEDB")
        if p.createrole:
            clauses.append("CREATEROLE")
        if p.replication:
            clauses.append("REPLICATION")
        if p.bypassrls:
            clauses.append("BYPASSRLS")
        if not p.inherit:
            clauses.append("NOINHERIT")
        if p.connection_limit >= 0:
            clauses.append(f"CONNECTION LIMIT {p.connection_limit}")

        clause_str = " ".join(clauses)
        return f"CREATE ROLE {self._quote_ident(p.name)} WITH {clause_str};"

    def _render_grant_role_ddl(self, role: str, grantee: str) -> str:
        if self.dialect == EngineDialect.MYSQL:
            return f"GRANT {self._quote_ident(role)} TO {self._format_principal(grantee)};"
        return f"GRANT {self._quote_ident(role)} TO {self._quote_ident(grantee)};"

    def _render_owner_ddl(self, obj_type: str, schema: str, name: str, owner: str) -> str:
        ot = obj_type.upper()
        sch_prefix = f"{self._quote_ident(schema)}." if schema else ""
        obj_ref = f"{sch_prefix}{self._quote_ident(name)}"

        if self.dialect == EngineDialect.MYSQL:
            # MySQL ownership mapped via routine/view definer or schema permissions
            return f"-- MySQL object {ot} {obj_ref} owner remapped to {self._format_principal(owner)}"

        if ot in ("TABLE", "VIEW", "SEQUENCE", "MATERIALIZED VIEW", "FOREIGN TABLE"):
            return f"ALTER {ot} {obj_ref} OWNER TO {self._quote_ident(owner)};"
        elif ot in ("FUNCTION", "PROCEDURE", "ROUTINE"):
            return f"ALTER {ot} {obj_ref} OWNER TO {self._quote_ident(owner)};"
        elif ot == "SCHEMA":
            return f"ALTER SCHEMA {self._quote_ident(name)} OWNER TO {self._quote_ident(owner)};"
        return f"ALTER {ot} {obj_ref} OWNER TO {self._quote_ident(owner)};"

    def _render_grant_ddl(self, g: GrantSpec) -> str:
        ot = g.object_type.upper()
        sch_prefix = f"{self._quote_ident(g.schema)}." if g.schema else ""
        obj_ref = f"{sch_prefix}{self._quote_ident(g.object_name)}"

        col_clause = f" ({', '.join(self._quote_ident(c) for c in g.columns)})" if g.columns else ""
        with_grant = " WITH GRANT OPTION" if g.grantable else ""

        if self.dialect == EngineDialect.MYSQL:
            grantee_expr = self._format_principal(g.grantee)
            return f"GRANT {g.privilege.upper()}{col_clause} ON {obj_ref} TO {grantee_expr}{with_grant};"

        if ot in ("TABLE", "SEQUENCE", "VIEW", "FOREIGN TABLE"):
            return f"GRANT {g.privilege.upper()}{col_clause} ON {ot} {obj_ref} TO {self._quote_ident(g.grantee)}{with_grant};"
        elif ot in ("FUNCTION", "PROCEDURE", "ROUTINE"):
            return f"GRANT {g.privilege.upper()} ON {ot} {obj_ref} TO {self._quote_ident(g.grantee)}{with_grant};"
        elif ot == "SCHEMA":
            return f"GRANT {g.privilege.upper()} ON SCHEMA {self._quote_ident(g.object_name)} TO {self._quote_ident(g.grantee)}{with_grant};"
        return f"GRANT {g.privilege.upper()}{col_clause} ON {obj_ref} TO {self._quote_ident(g.grantee)}{with_grant};"

    def _render_revoke_ddl(self, g: GrantSpec) -> str:
        ot = g.object_type.upper()
        sch_prefix = f"{self._quote_ident(g.schema)}." if g.schema else ""
        obj_ref = f"{sch_prefix}{self._quote_ident(g.object_name)}"

        if self.dialect == EngineDialect.MYSQL:
            grantee_expr = self._format_principal(g.grantee)
            return f"REVOKE {g.privilege.upper()} ON {obj_ref} FROM {grantee_expr};"

        if ot in ("TABLE", "SEQUENCE", "VIEW"):
            return f"REVOKE {g.privilege.upper()} ON {ot} {obj_ref} FROM {self._quote_ident(g.grantee)};"
        return f"REVOKE {g.privilege.upper()} ON {obj_ref} FROM {self._quote_ident(g.grantee)};"

    def _render_rls_policy_ddl(self, p: RLSPolicySpec, replace: bool = False) -> str:
        sch_prefix = f"{self._quote_ident(p.schema)}." if p.schema else ""
        tbl_ref = f"{sch_prefix}{self._quote_ident(p.table_name)}"
        roles_clause = ", ".join(self._quote_ident(r) for r in p.roles) if p.roles else "PUBLIC"

        clauses: List[str] = [f"FOR {p.command.upper()}"]
        clauses.append(f"TO {roles_clause}")
        if p.using_expression:
            clauses.append(f"USING ({p.using_expression})")
        if p.with_check_expression:
            clauses.append(f"WITH CHECK ({p.with_check_expression})")

        clause_str = " ".join(clauses)
        if replace:
            drop_stmt = f"DROP POLICY IF EXISTS {self._quote_ident(p.policy_name)} ON {tbl_ref};\n"
            return drop_stmt + f"CREATE POLICY {self._quote_ident(p.policy_name)} ON {tbl_ref} {clause_str};"
        return f"CREATE POLICY {self._quote_ident(p.policy_name)} ON {tbl_ref} {clause_str};"

    def _render_routine_security_ddl(self, r: RoutineSecuritySpec) -> str:
        sch_prefix = f"{self._quote_ident(r.schema)}." if r.schema else ""
        obj_ref = f"{sch_prefix}{self._quote_ident(r.routine_name)}"

        if self.dialect == EngineDialect.MYSQL:
            definer_expr = self._format_principal(r.definer)
            return f"ALTER {r.routine_type.upper()} {obj_ref} DEFINER = {definer_expr} SQL SECURITY {r.security_type.value};"

        # In PostgreSQL, SECURITY DEFINER is set on functions/procedures
        sec_kw = "SECURITY DEFINER" if r.security_type == SecurityType.DEFINER else "SECURITY INVOKER"
        return f"ALTER {r.routine_type.upper()} {obj_ref} {sec_kw};"

    def _render_default_privilege_ddl(self, dp: DefaultPrivilegeSpec) -> str:
        sch_clause = f" IN SCHEMA {self._quote_ident(dp.schema)}" if dp.schema else ""
        with_grant = " WITH GRANT OPTION" if dp.grantable else ""
        return (
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {self._quote_ident(dp.target_role)}{sch_clause} "
            f"GRANT {dp.privilege.upper()} ON {dp.object_type.upper()} TO {self._quote_ident(dp.grantee)}{with_grant};"
        )


# -----------------------------------------------------------------------------
# Mutual Exclusion Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

class PrincipalMigrationLease:
    """Active mutual exclusion verification lease lock (.run.lock) with heartbeat."""

    def __init__(self, lock_dir: Union[str, Path], ttl_seconds: float = 30.0):
        self.lock_dir = Path(lock_dir)
        self.lock_file = self.lock_dir / ".run.lock"
        self.ttl_seconds = ttl_seconds
        self.owner_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.acquired = False

    def acquire(self) -> "PrincipalMigrationLease":
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        now = time.time()

        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                heartbeat = float(data.get("heartbeat", 0))
                owner = data.get("owner", "unknown")
                if now - heartbeat < self.ttl_seconds:
                    raise LeaseLockError(
                        f"active lease locked by owner '{owner}' "
                        f"(heartbeat {now - heartbeat:.1f}s ago, TTL {self.ttl_seconds}s)"
                    )
            except (json.JSONDecodeError, OSError, ValueError):
                pass

        payload = {
            "owner": self.owner_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "heartbeat": now,
            "ttl": self.ttl_seconds,
        }
        raw = json.dumps(payload, indent=2)
        tmp = self.lock_dir / f".run.lock.tmp.{uuid.uuid4().hex[:8]}"
        try:
            tmp.write_text(raw, encoding="utf-8")
            os.replace(tmp, self.lock_file)
            self.acquired = True
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
        return self

    def heartbeat(self) -> None:
        if not self.acquired or not self.lock_file.exists():
            raise LeaseLockError("cannot heartbeat: lease not held")
        try:
            data = json.loads(self.lock_file.read_text(encoding="utf-8"))
            if data.get("owner") != self.owner_id:
                raise LeaseLockError("cannot heartbeat: lease stolen by another process")
        except (json.JSONDecodeError, OSError) as e:
            raise LeaseLockError(f"lease file unreadable during heartbeat: {e}")

        payload = {
            "owner": self.owner_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "heartbeat": time.time(),
            "ttl": self.ttl_seconds,
        }
        tmp = self.lock_dir / f".run.lock.tmp.{uuid.uuid4().hex[:8]}"
        try:
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(tmp, self.lock_file)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def release(self) -> None:
        if not self.acquired:
            return
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                if data.get("owner") == self.owner_id:
                    self.lock_file.unlink(missing_ok=True)
            except (json.JSONDecodeError, OSError):
                self.lock_file.unlink(missing_ok=True)
        self.acquired = False

    def __enter__(self) -> "PrincipalMigrationLease":
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Durable Typed Checkpoint in O(1)
# -----------------------------------------------------------------------------

@dataclasses.dataclass
class DurablePrincipalMigrationCheckpoint:
    """Universal Typed Durable Resume in O(1) for principal security audits."""

    session_id: str
    checkpoint_dir: Path
    verified_hashes: Dict[str, str] = dataclasses.field(default_factory=dict)
    state_version: int = 1

    def _state_file(self) -> Path:
        return self.checkpoint_dir / f"principal_migration_{self.session_id}.json"

    def record_verified(self, category: str, object_name: str, payload_hash: str) -> None:
        key = f"{category}:{object_name}"
        self.verified_hashes[key] = payload_hash

    def is_verified(self, category: str, object_name: str, current_hash: str) -> bool:
        key = f"{category}:{object_name}"
        return self.verified_hashes.get(key) == current_hash

    def save(self) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "session_id": self.session_id,
            "state_version": self.state_version,
            "verified_hashes": self.verified_hashes,
            "updated_at": time.time(),
        }
        tmp = self.checkpoint_dir / f".checkpoint.{uuid.uuid4().hex[:8]}.tmp"
        try:
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(tmp, self._state_file())
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    @classmethod
    def load(cls, checkpoint_dir: Union[str, Path], session_id: str) -> "DurablePrincipalMigrationCheckpoint":
        cp_dir = Path(checkpoint_dir)
        target = cp_dir / f"principal_migration_{session_id}.json"
        if not target.exists():
            return cls(session_id=session_id, checkpoint_dir=cp_dir)
        try:
            data = json.loads(target.read_text(encoding="utf-8"))
            return cls(
                session_id=session_id,
                checkpoint_dir=cp_dir,
                verified_hashes=data.get("verified_hashes", {}),
                state_version=int(data.get("state_version", 1)),
            )
        except (json.JSONDecodeError, OSError):
            return cls(session_id=session_id, checkpoint_dir=cp_dir)
