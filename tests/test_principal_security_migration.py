"""Tests for Principal Security Semantics Migration (Row 632: RCH24.principal-migration).

Proves that:
1. Security semantics (user, role, grant, ownership, default privilege, DEFINER/INVOKER)
   are modeled separately from credentials and provider-managed principals.
2. Identities and conflict policies map explicitly, tested with application roles.
3. Silent migrator ownership is detected and remediated to target application owners.
4. Unintended privilege widening (SUPERUSER, BYPASSRLS, excessive grants) is blocked.
5. Missing sequence privileges (permission denied hazards) are automatically discovered and resolved.
6. Row Level Security (RLS) enablement and policy drifts are detected and reconciled.
7. DEFINER / INVOKER shifts and migrator definers are caught and corrected.
8. Duplicate account names and multi-host collisions are discovered and resolved by policy.
9. Default privilege gaps for future objects are discovered and generated.
10. UNIX style reporting with pure ASCII care badges (PASS, BENIGN, REVIEW, ACTION).
11. Active mutual exclusion lease (.run.lock) and O(1) typed durable resume.
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pytest

from migkit.principal_migration import (
    CareBadge,
    CatalogSecurityState,
    ConflictAction,
    DefaultPrivilegeSpec,
    DuplicateAccountResolution,
    DurablePrincipalMigrationCheckpoint,
    EngineDialect,
    GrantSpec,
    IdentityConflictError,
    IdentityMappingRule,
    LeaseLockError,
    MigrationPlan,
    OwnershipSpec,
    PrincipalMigrationError,
    PrincipalMigrationLease,
    PrincipalMigrationPolicy,
    PrincipalSecurityMigrator,
    PrincipalSpec,
    PrincipalType,
    PrivilegeWideningError,
    RLSPolicySpec,
    RoutineSecuritySpec,
    SecurityFinding,
    SecurityType,
    SequenceDependency,
    SilentMigratorOwnershipError,
)

PROVES = [
    ("principal-migration.security-semantics", "postgres"),
    ("principal-migration.security-semantics", "mysql"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def base_policy() -> PrincipalMigrationPolicy:
    return PrincipalMigrationPolicy(
        approved_identity_mappings={
            "legacy_app_owner": "app_owner",
            "legacy_app_ro": "app_ro",
            "legacy_app_rw": "app_rw",
        },
        target_default_owner="app_owner",
        disallowed_privilege_widening=True,
        allow_superuser_migration=False,
        allow_bypassrls_migration=False,
        conflict_action=ConflictAction.FAIL_ON_CONFLICT,
        duplicate_account_resolution=DuplicateAccountResolution.DISALLOW,
        auto_resolve_sequence_dependencies=True,
        auto_reassign_migrator_ownership=True,
    )


@pytest.fixture
def pg_migrator(base_policy: PrincipalMigrationPolicy) -> PrincipalSecurityMigrator:
    return PrincipalSecurityMigrator(dialect=EngineDialect.POSTGRESQL, policy=base_policy)


@pytest.fixture
def mysql_migrator(base_policy: PrincipalMigrationPolicy) -> PrincipalSecurityMigrator:
    return PrincipalSecurityMigrator(dialect=EngineDialect.MYSQL, policy=base_policy)


# -----------------------------------------------------------------------------
# Test 1: Credentials Separation & Provider-Managed Principal Exclusion
# -----------------------------------------------------------------------------

def test_principal_credentials_separation(pg_migrator: PrincipalSecurityMigrator):
    """Verify security semantics model principals without embedding credentials."""
    p = PrincipalSpec(
        name="app_user",
        principal_type=PrincipalType.ROLE,
        login=True,
        superuser=False,
        connection_limit=50,
        member_of=("app_ro_group",),
    )
    assert p.credential_managed_externally is True
    assert not hasattr(p, "password")
    assert not hasattr(p, "password_hash")
    assert p.normalized_name() == "app_user"


def test_provider_managed_principals_exclusion(pg_migrator: PrincipalSecurityMigrator):
    """Verify cloud-provider managed and engine-internal accounts are recognized."""
    # PostgreSQL system principals
    assert pg_migrator.is_provider_managed("postgres") is True
    assert pg_migrator.is_provider_managed("rdsadmin") is True
    assert pg_migrator.is_provider_managed("rds_superuser") is True
    assert pg_migrator.is_provider_managed("cloudsqladmin") is True
    assert pg_migrator.is_provider_managed("azure_pg_admin") is True
    assert pg_migrator.is_provider_managed("pg_read_all_data") is True
    assert pg_migrator.is_provider_managed("app_owner") is False

    # MySQL system principals
    my_migrator = PrincipalSecurityMigrator(dialect=EngineDialect.MYSQL)
    assert my_migrator.is_provider_managed("root@'127.0.0.1'") is True
    assert my_migrator.is_provider_managed("mysql.sys@'localhost'") is True
    assert my_migrator.is_provider_managed("rdsadmin@'127.0.0.1'") is True
    assert my_migrator.is_provider_managed("app_user@'10.0.0.1'") is False


# -----------------------------------------------------------------------------
# Test 2: Explicit Identity Mapping with Application Roles
# -----------------------------------------------------------------------------

def test_explicit_identity_mapping_with_application_roles(pg_migrator: PrincipalSecurityMigrator):
    """Verify source principals are remapped explicitly to target application roles."""
    src = CatalogSecurityState(
        principals={
            "legacy_app_ro": PrincipalSpec(name="legacy_app_ro", login=True),
            "legacy_app_rw": PrincipalSpec(name="legacy_app_rw", login=True),
        },
        grants=[
            GrantSpec(
                grantee="legacy_app_ro",
                object_type="TABLE",
                schema="public",
                object_name="customers",
                privilege="SELECT",
            ),
            GrantSpec(
                grantee="legacy_app_rw",
                object_type="TABLE",
                schema="public",
                object_name="customers",
                privilege="INSERT",
            ),
        ],
    )

    plan = pg_migrator.audit_and_plan(src)
    planned_names = {p.name for p in plan.planned_principals}
    assert "app_ro" in planned_names
    assert "app_rw" in planned_names
    assert "legacy_app_ro" not in planned_names

    planned_grantees = {g.grantee for g in plan.planned_grants}
    assert "app_ro" in planned_grantees
    assert "app_rw" in planned_grantees


# -----------------------------------------------------------------------------
# Test 3: Silent Migrator Ownership Detection and Remediation
# -----------------------------------------------------------------------------

def test_silent_migrator_ownership_detection_and_remediation(pg_migrator: PrincipalSecurityMigrator):
    """Detect when tables silently arrived owned by migration account and reassign."""
    src = CatalogSecurityState(
        ownerships=[
            OwnershipSpec(
                object_type="TABLE",
                schema="public",
                object_name="orders",
                owner="legacy_app_owner",
            ),
            OwnershipSpec(
                object_type="SEQUENCE",
                schema="public",
                object_name="orders_id_seq",
                owner="legacy_app_owner",
            ),
        ]
    )

    # Target currently holds ownership under migrator account
    tgt = CatalogSecurityState(
        ownerships=[
            OwnershipSpec(
                object_type="TABLE",
                schema="public",
                object_name="orders",
                owner="dts_migration",
            ),
            OwnershipSpec(
                object_type="SEQUENCE",
                schema="public",
                object_name="orders_id_seq",
                owner="dts_migration",
            ),
        ]
    )

    plan = pg_migrator.audit_and_plan(src, tgt)
    actions = [f for f in plan.findings if f.category == "MIGRATOR_OWNERSHIP"]
    assert len(actions) == 2
    assert all(a.badge == CareBadge.ACTION for a in actions)
    assert any("orders" in a.object_name for a in actions)

    # Verify remediation DDL
    ddl = "\n".join(plan.remediation_ddl)
    assert 'ALTER TABLE "public"."orders" OWNER TO "app_owner";' in ddl
    assert 'ALTER SEQUENCE "public"."orders_id_seq" OWNER TO "app_owner";' in ddl


# -----------------------------------------------------------------------------
# Test 4: Prevent Unintended Privilege Widening (SUPERUSER, BYPASSRLS)
# -----------------------------------------------------------------------------

def test_prevent_unintended_privilege_widening_roles(pg_migrator: PrincipalSecurityMigrator):
    """Disallow carrying dangerous flags (SUPERUSER, BYPASSRLS) into target environment."""
    src = CatalogSecurityState(
        principals={
            "app_worker": PrincipalSpec(
                name="app_worker",
                login=True,
                superuser=True,
                bypassrls=True,
            ),
        }
    )

    plan = pg_migrator.audit_and_plan(src)
    widening_findings = [f for f in plan.findings if f.category == "PRIVILEGE_WIDENING"]
    assert len(widening_findings) >= 2
    assert all(f.badge == CareBadge.ACTION for f in widening_findings)

    # Planned principal has superuser and bypassrls stripped
    planned = [p for p in plan.planned_principals if p.name == "app_worker"][0]
    assert planned.superuser is False
    assert planned.bypassrls is False


def test_prevent_unintended_privilege_widening_grants(pg_migrator: PrincipalSecurityMigrator):
    """Detect when target environment holds extra unexpected wide grants."""
    src = CatalogSecurityState(
        grants=[
            GrantSpec(
                grantee="app_ro",
                object_type="TABLE",
                schema="public",
                object_name="invoices",
                privilege="SELECT",
            ),
        ]
    )

    # Target has unexpected wide privilege WITH GRANT OPTION
    tgt = CatalogSecurityState(
        grants=[
            GrantSpec(
                grantee="app_ro",
                object_type="TABLE",
                schema="public",
                object_name="invoices",
                privilege="ALL PRIVILEGES",
                grantable=True,
            ),
        ]
    )

    plan = pg_migrator.audit_and_plan(src, tgt)
    widening = [f for f in plan.findings if f.category == "PRIVILEGE_WIDENING"]
    assert len(widening) == 1
    assert widening[0].badge == CareBadge.ACTION
    assert "excessive privilege" in widening[0].message
    assert 'REVOKE ALL PRIVILEGES ON "public"."invoices" FROM "app_ro";' in plan.findings[0].remediation_ddl or \
           'REVOKE ALL PRIVILEGES' in widening[0].remediation_ddl


# -----------------------------------------------------------------------------
# Test 5: Missing Sequence Privileges Resolution (Permission Denied Hazard)
# -----------------------------------------------------------------------------

def test_missing_sequence_privilege_resolution(pg_migrator: PrincipalSecurityMigrator):
    """Automatically discover sequence dependencies and plan USAGE permissions."""
    src = CatalogSecurityState(
        grants=[
            GrantSpec(
                grantee="app_rw",
                object_type="TABLE",
                schema="public",
                object_name="order_items",
                privilege="INSERT",
            ),
        ],
        sequence_dependencies=[
            SequenceDependency(
                schema="public",
                sequence_name="order_items_id_seq",
                table_schema="public",
                table_name="order_items",
                column_name="id",
            ),
        ],
    )

    # Target has the table grant but is missing the sequence grant
    tgt = CatalogSecurityState(
        grants=[
            GrantSpec(
                grantee="app_rw",
                object_type="TABLE",
                schema="public",
                object_name="order_items",
                privilege="INSERT",
            ),
        ]
    )

    plan = pg_migrator.audit_and_plan(src, tgt)
    seq_findings = [f for f in plan.findings if f.category == "MISSING_SEQUENCE_GRANT"]
    assert len(seq_findings) == 1
    assert seq_findings[0].badge == CareBadge.ACTION
    assert "permission denied hazard" in seq_findings[0].message

    # Sequence grant is planned
    seq_grants = [g for g in plan.planned_grants if g.object_type == "SEQUENCE"]
    assert len(seq_grants) == 1
    assert seq_grants[0].privilege == "USAGE"
    assert seq_grants[0].object_name == "order_items_id_seq"

    # Generates DDL
    ddl = "\n".join(plan.remediation_ddl)
    assert 'GRANT USAGE ON SEQUENCE "public"."order_items_id_seq" TO "app_rw";' in ddl


# -----------------------------------------------------------------------------
# Test 6: Row Level Security (RLS) Enablement & Policy Drift
# -----------------------------------------------------------------------------

def test_rls_enablement_and_policy_drift_resolution(pg_migrator: PrincipalSecurityMigrator):
    """Reconcile table RLS enablement and missing/drifted security policies."""
    src = CatalogSecurityState(
        rls_policies=[
            # Table-level enablement
            RLSPolicySpec(schema="public", table_name="tenants", rls_enabled=True),
            # Specific policy
            RLSPolicySpec(
                schema="public",
                table_name="tenants",
                rls_enabled=True,
                policy_name="tenant_isolation_policy",
                command="SELECT",
                roles=("app_ro",),
                using_expression="tenant_id = current_setting('app.tenant_id')",
            ),
        ]
    )

    # Target has RLS disabled and policy missing
    tgt = CatalogSecurityState(
        rls_policies=[
            RLSPolicySpec(schema="public", table_name="tenants", rls_enabled=False),
        ]
    )

    plan = pg_migrator.audit_and_plan(src, tgt)
    rls_findings = [f for f in plan.findings if f.category == "RLS_DRIFT"]
    assert len(rls_findings) == 2
    assert all(f.badge == CareBadge.ACTION for f in rls_findings)

    ddl = "\n".join(plan.remediation_ddl)
    assert 'ALTER TABLE "public"."tenants" ENABLE ROW LEVEL SECURITY;' in ddl
    assert 'CREATE POLICY "tenant_isolation_policy" ON "public"."tenants"' in ddl
    assert 'USING (tenant_id = current_setting(\'app.tenant_id\'))' in ddl


# -----------------------------------------------------------------------------
# Test 7: Routine DEFINER / INVOKER Drift & Migrator Definers
# -----------------------------------------------------------------------------

def test_definer_invoker_drift_resolution(pg_migrator: PrincipalSecurityMigrator):
    """Detect when a routine definer changed to migrator or demoted to INVOKER."""
    src = CatalogSecurityState(
        routine_securities=[
            RoutineSecuritySpec(
                schema="public",
                routine_name="process_payment",
                routine_type="FUNCTION",
                security_type=SecurityType.DEFINER,
                definer="legacy_app_owner",
            ),
        ]
    )

    # Target routine demoted to INVOKER and owned by migrator account
    tgt = CatalogSecurityState(
        routine_securities=[
            RoutineSecuritySpec(
                schema="public",
                routine_name="process_payment",
                routine_type="FUNCTION",
                security_type=SecurityType.INVOKER,
                definer="dts_migration",
            ),
        ]
    )

    plan = pg_migrator.audit_and_plan(src, tgt)
    definer_findings = [f for f in plan.findings if f.category == "DEFINER_DRIFT"]
    assert len(definer_findings) >= 1
    assert any(f.badge == CareBadge.ACTION for f in definer_findings)

    ddl = "\n".join(plan.remediation_ddl)
    assert 'ALTER FUNCTION "public"."process_payment" SECURITY DEFINER;' in ddl


# -----------------------------------------------------------------------------
# Test 8: Duplicate Account Name Policies
# -----------------------------------------------------------------------------

def test_duplicate_account_detection_and_resolution(mysql_migrator: PrincipalSecurityMigrator):
    """Discover duplicate account variants and enforce resolution policy."""
    principals = [
        PrincipalSpec(name="'app'@'127.0.0.1'"),
        PrincipalSpec(name="'app'@'10.0.0.2'"),
    ]

    # Under default DISALLOW -> ACTION
    findings = mysql_migrator.detect_duplicate_accounts(principals)
    assert len(findings) == 1
    assert findings[0].badge == CareBadge.ACTION
    assert "duplicate account variants detected" in findings[0].message

    # Under QUALIFY_BY_HOST -> BENIGN
    policy_qualify = PrincipalMigrationPolicy(
        duplicate_account_resolution=DuplicateAccountResolution.QUALIFY_BY_HOST
    )
    mig_qualify = PrincipalSecurityMigrator(dialect=EngineDialect.MYSQL, policy=policy_qualify)
    findings_qualify = mig_qualify.detect_duplicate_accounts(principals)
    assert len(findings_qualify) == 1
    assert findings_qualify[0].badge == CareBadge.BENIGN

    # Under MERGE_LEAST_PRIVILEGE -> REVIEW
    policy_merge = PrincipalMigrationPolicy(
        duplicate_account_resolution=DuplicateAccountResolution.MERGE_LEAST_PRIVILEGE
    )
    mig_merge = PrincipalSecurityMigrator(dialect=EngineDialect.MYSQL, policy=policy_merge)
    findings_merge = mig_merge.detect_duplicate_accounts(principals)
    assert len(findings_merge) == 1
    assert findings_merge[0].badge == CareBadge.REVIEW


# -----------------------------------------------------------------------------
# Test 9: Default Privilege Gaps
# -----------------------------------------------------------------------------

def test_default_privileges_gap_resolution(pg_migrator: PrincipalSecurityMigrator):
    """Discover missing default privileges for future objects."""
    src = CatalogSecurityState(
        default_privileges=[
            DefaultPrivilegeSpec(
                target_role="legacy_app_owner",
                schema="public",
                object_type="TABLES",
                grantee="legacy_app_ro",
                privilege="SELECT",
            ),
        ]
    )

    tgt = CatalogSecurityState()  # Target has none configured

    plan = pg_migrator.audit_and_plan(src, tgt)
    dp_findings = [f for f in plan.findings if f.category == "DEFAULT_PRIVILEGE_GAP"]
    assert len(dp_findings) == 1
    assert dp_findings[0].badge == CareBadge.ACTION

    ddl = "\n".join(plan.remediation_ddl)
    assert 'ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "public" GRANT SELECT ON TABLES TO "app_ro";' in ddl


# -----------------------------------------------------------------------------
# Test 10: MySQL Dialect Security Migration
# -----------------------------------------------------------------------------

def test_mysql_dialect_security_migration(mysql_migrator: PrincipalSecurityMigrator):
    """Test MySQL specific identity formatting, safe host checks, and definer DDL."""
    assert mysql_migrator.validate_host_safety("127.0.0.1") is True
    assert mysql_migrator.validate_host_safety("10.0.0.5") is True
    assert mysql_migrator.validate_host_safety("198.51.100.1") is False

    src = CatalogSecurityState(
        routine_securities=[
            RoutineSecuritySpec(
                schema="shop",
                routine_name="calculate_discount",
                routine_type="PROCEDURE",
                security_type=SecurityType.DEFINER,
                definer="'app_owner'@'%'",
            ),
        ]
    )
    tgt = CatalogSecurityState(
        routine_securities=[
            RoutineSecuritySpec(
                schema="shop",
                routine_name="calculate_discount",
                routine_type="PROCEDURE",
                security_type=SecurityType.DEFINER,
                definer="'dts_migration'@'%'",
            ),
        ]
    )

    plan = mysql_migrator.audit_and_plan(src, tgt)
    ddl = "\n".join(plan.remediation_ddl)
    assert "ALTER PROCEDURE `shop`.`calculate_discount` DEFINER = 'app_owner'@'%' SQL SECURITY DEFINER;" in ddl


# -----------------------------------------------------------------------------
# Test 11: Pure UNIX Terse Reporting (Zero Emojis, Pure ASCII)
# -----------------------------------------------------------------------------

def test_pure_unix_terse_output_zero_glyphs(pg_migrator: PrincipalSecurityMigrator):
    """Verify output contains strictly UNIX style formatting with 4-tier care badges."""
    plan = MigrationPlan()
    plan.findings.append(
        SecurityFinding(
            badge=CareBadge.ACTION,
            category="MIGRATOR_OWNERSHIP",
            object_name="TABLE:public.orders",
            message="object owned by dts_migration",
            action_required="reassign to app_owner",
        )
    )
    plan.findings.append(
        SecurityFinding(
            badge=CareBadge.PASS,
            category="PRINCIPAL_MAPPING",
            object_name="app_ro",
            message="principal mapped cleanly",
        )
    )

    terse = plan.format_terse()
    # Check ASCII encoding
    assert terse.encode("ascii")

    # Check 4-tier care badges
    assert "ACTION [MIGRATOR_OWNERSHIP]" in terse
    assert "SUMMARY: 1 PASS, 0 BENIGN, 0 REVIEW, 1 ACTION" in terse

    # Strictly no decorative icons or emoji glyphs
    forbidden_glyphs = [
        "\u2713", "\u2717", "\u26a0", "\u274c", "\u2705", "\U0001f680", "\U0001f6e1", "\U0001f512"
    ]
    for glyph in forbidden_glyphs:
        assert glyph not in terse


# -----------------------------------------------------------------------------
# Test 12: Active Mutual Exclusion Lease (.run.lock)
# -----------------------------------------------------------------------------

def test_principal_migration_lease_mutual_exclusion():
    """Verify active lease prevents concurrent runs and heartbeat works."""
    with tempfile.TemporaryDirectory() as tmpdir:
        lease1 = PrincipalMigrationLease(tmpdir, ttl_seconds=10.0)
        lease1.acquire()
        assert lease1.acquired is True
        assert (Path(tmpdir) / ".run.lock").exists()

        # Attempt second acquire while active -> must raise LeaseLockError
        lease2 = PrincipalMigrationLease(tmpdir, ttl_seconds=10.0)
        with pytest.raises(LeaseLockError) as exc_info:
            lease2.acquire()
        assert "active lease locked" in str(exc_info.value)

        # Heartbeat works
        lease1.heartbeat()

        # Release frees the lease
        lease1.release()
        assert not (Path(tmpdir) / ".run.lock").exists()

        # Now lease2 can acquire
        lease2.acquire()
        assert lease2.acquired is True
        lease2.release()


# -----------------------------------------------------------------------------
# Test 13: Durable Typed Checkpoint in O(1)
# -----------------------------------------------------------------------------

def test_durable_checkpoint_resume_o1():
    """Verify state can be persisted and verified in O(1) across runs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        session_id = "test_sess_001"
        cp = DurablePrincipalMigrationCheckpoint(session_id=session_id, checkpoint_dir=Path(tmpdir))

        cp.record_verified("TABLE", "public.users", "hash_abc_123")
        cp.save()

        # Reload checkpoint
        loaded = DurablePrincipalMigrationCheckpoint.load(tmpdir, session_id)
        assert loaded.is_verified("TABLE", "public.users", "hash_abc_123") is True
        assert loaded.is_verified("TABLE", "public.users", "hash_different") is False
        assert loaded.is_verified("TABLE", "public.orders", "hash_abc_123") is False
