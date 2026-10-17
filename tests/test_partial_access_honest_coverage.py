import hashlib
import hmac
import json
import pytest

from migkit.partial_access import (
    AbsentReason,
    AbsentSourceEvidence,
    ApprovedExportManifest,
    AuthorizedCollectorRegistry,
    ExportFilePart,
    ExportObjectManifest,
    HonestCoverageVerdict,
    HonestCoverageViolationError,
    ManifestCompletenessError,
    ManifestVersionError,
    NormalizedTargetMapping,
    PartialAccessError,
    PartialAccessReconciler,
    PartialAccessScope,
    PlaceholderDataProhibitedError,
    SchemaIntegrityError,
    SecurityScopeViolationError,
    SourceLineage,
    UnauthorizedCollectorError,
)


def test_partial_access_scope_network_and_permission_boundaries():
    scope = PartialAccessScope(
        scope_id="scope-saas-01",
        source_id="saas-crm",
        accessible_objects={"accounts", "contacts"},
        inaccessible_objects={"billing_secrets", "internal_audit"},
        permitted_columns={
            "accounts": {"id", "name", "created_at"},
            "contacts": {"id", "account_id", "email"},
        },
        inaccessible_columns={
            "accounts": {"tax_identifier", "bank_account"},
        },
        predicates={"accounts": "tenant_id = 't-100'"},
        allowed_networks={"10.0.0.0/24", "127.0.0.1"},
        allowed_roles={"exporter_role"},
    )

    assert scope.is_object_accessible("accounts")
    assert scope.is_object_accessible("contacts")
    assert not scope.is_object_accessible("billing_secrets")
    assert not scope.is_object_accessible("internal_audit")
    assert not scope.is_object_accessible("unknown_table")

    assert scope.is_column_accessible("accounts", "name")
    assert not scope.is_column_accessible("accounts", "tax_identifier")
    assert not scope.is_column_accessible("accounts", "nonexistent")

    scope.validate_network_access("10.0.0.15")
    scope.validate_network_access("127.0.0.1")

    with pytest.raises(SecurityScopeViolationError):
        scope.validate_network_access("192.0.2.50")

    scope.validate_permission_boundary(["exporter_role"])
    with pytest.raises(SecurityScopeViolationError):
        scope.validate_permission_boundary(["exporter_role", "admin_super_user"])


def test_source_lineage_preservation_and_chaining():
    lineage = SourceLineage(
        source_system="saas-crm-tenant100",
        export_batch_id="batch-20261017-001",
        collected_at=1771142400.0,
        collector_id="collector-agent-01",
        extraction_method="owner_delegated_api",
        tenant_id="t-100",
    )

    assert lineage.lineage_hash is not None
    assert len(lineage.lineage_hash) == 64

    child_lineage = lineage.extend_chain(
        step="normalize_target_mapping",
        details={"engine": "postgres", "target_table": "crm_accounts"},
    )

    assert len(child_lineage.provenance_chain) == 1
    assert child_lineage.provenance_chain[0]["step"] == "normalize_target_mapping"
    assert child_lineage.lineage_hash != lineage.lineage_hash

    dumped = child_lineage.to_dict()
    restored = SourceLineage.from_dict(dumped)
    assert restored.source_system == lineage.source_system
    assert restored.export_batch_id == lineage.export_batch_id
    assert restored.lineage_hash == child_lineage.lineage_hash


def test_authorized_collector_registry_registration_and_signing():
    registry = AuthorizedCollectorRegistry()
    secret = b"test-collector-secret-key-32b01"

    registry.register_collector(
        collector_id="col-01",
        allowed_scopes={"scope-saas-01"},
        secret_key=secret,
        allowed_networks={"10.0.0.10", "127.0.0.1"},
        roles={"exporter_role"},
    )

    assert registry.is_collector_authorized("col-01", "scope-saas-01", "10.0.0.10")
    assert not registry.is_collector_authorized("col-01", "scope-other", "10.0.0.10")
    assert not registry.is_collector_authorized("col-01", "scope-saas-01", "10.0.0.99")
    assert not registry.is_collector_authorized("unknown-col", "scope-saas-01", "10.0.0.10")

    manifest = ApprovedExportManifest(
        manifest_id="mf-001",
        collector_id="col-01",
        scope_id="scope-saas-01",
        lineage=SourceLineage(
            source_system="saas-crm",
            export_batch_id="batch-01",
            collected_at=1771142400.0,
            collector_id="col-01",
            extraction_method="owner_dump",
        ),
        objects={},
    )

    signed_manifest = registry.sign_manifest(manifest, "col-01")
    assert signed_manifest.manifest_signature is not None
    assert registry.verify_manifest(signed_manifest)

    # Tampered manifest should fail verification
    tampered_manifest = ApprovedExportManifest(
        manifest_id="mf-001-tampered",
        collector_id="col-01",
        scope_id="scope-saas-01",
        lineage=signed_manifest.lineage,
        objects=signed_manifest.objects,
        manifest_signature=signed_manifest.manifest_signature,
    )
    assert not registry.verify_manifest(tampered_manifest)

    # Unregistered collector cannot sign
    with pytest.raises(UnauthorizedCollectorError):
        registry.sign_manifest(manifest, "unregistered-collector")


def test_approved_export_manifest_validation_and_schema_integrity():
    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-01",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    part1 = ExportFilePart(
        part_number=1,
        file_path="accounts_part1.json",
        record_count=2,
        sha256_hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    )

    obj_accounts = ExportObjectManifest(
        object_name="accounts",
        record_count=2,
        data_hash="abc123hash",
        schema_definition={"id": "BIGINT", "name": "VARCHAR", "created_at": "TIMESTAMP"},
        file_parts=[part1],
        columns=["id", "name", "created_at"],
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-001",
        manifest_version=1,
        collector_id="col-01",
        scope_id="scope-saas-01",
        lineage=lineage,
        objects={"accounts": obj_accounts},
    )

    # Completeness check
    complete, missing = manifest.validate_completeness(["accounts"])
    assert complete
    assert missing == []

    complete, missing = manifest.validate_completeness(["accounts", "contacts"])
    assert not complete
    assert missing == ["contacts"]

    # Version check
    assert manifest.validate_version(1)
    with pytest.raises(ManifestVersionError):
        manifest.validate_version(2)

    # Schema integrity check
    expected_schemas = {
        "accounts": {"id": "BIGINT", "name": "VARCHAR", "created_at": "TIMESTAMP"},
    }
    assert manifest.validate_schema_integrity(expected_schemas)

    drifted_schemas = {
        "accounts": {"id": "BIGINT", "name": "VARCHAR", "created_at": "INTEGER"},
    }
    with pytest.raises(SchemaIntegrityError):
        manifest.validate_schema_integrity(drifted_schemas)

    missing_col_schemas = {
        "accounts": {"id": "BIGINT", "name": "VARCHAR", "created_at": "TIMESTAMP", "extra_col": "TEXT"},
    }
    with pytest.raises(SchemaIntegrityError):
        manifest.validate_schema_integrity(missing_col_schemas)


def test_approved_export_manifest_serialization_roundtrip():
    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-01",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    obj_accounts = ExportObjectManifest(
        object_name="accounts",
        record_count=1,
        data_hash="hash123",
        schema_definition={"id": "BIGINT", "name": "VARCHAR"},
        columns=["id", "name"],
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-roundtrip",
        collector_id="col-01",
        scope_id="scope-01",
        lineage=lineage,
        objects={"accounts": obj_accounts},
        manifest_signature="sig-abc",
    )

    d = manifest.to_dict()
    restored = ApprovedExportManifest.from_dict(d)
    assert restored.manifest_id == manifest.manifest_id
    assert restored.collector_id == manifest.collector_id
    assert restored.manifest_signature == "sig-abc"
    assert "accounts" in restored.objects
    assert restored.objects["accounts"].record_count == 1


def test_placeholder_data_detection_and_rejection():
    # Attempting to invent synthetic/placeholder data to fill inaccessible gaps must be blocked
    records_with_placeholder_flags = [
        {"id": 1, "name": "Real Account"},
        {"id": -1, "name": "[PLACEHOLDER]", "__is_placeholder__": True},
    ]

    with pytest.raises(PlaceholderDataProhibitedError):
        NormalizedTargetMapping.detect_and_reject_placeholders(records_with_placeholder_flags)

    records_with_mock_sentinels = [
        {"id": 1, "name": "Real Account"},
        {"id": 999999999, "name": "__MOCK_SYNTHETIC_RECORD__"},
    ]

    with pytest.raises(PlaceholderDataProhibitedError):
        NormalizedTargetMapping.detect_and_reject_placeholders(records_with_mock_sentinels)

    clean_records = [
        {"id": 1, "name": "Account Alpha"},
        {"id": 2, "name": "Account Beta"},
    ]
    # Clean records should pass without error
    NormalizedTargetMapping.detect_and_reject_placeholders(clean_records)


def test_honest_coverage_verdict_cannot_be_whole_source_same():
    absent = AbsentSourceEvidence(
        object_name="billing_secrets",
        reason=AbsentReason.RESTRICTED_ACCESS_NO_API,
        detail="Owner credentials required; no API available for direct export.",
        target_record_count=100,
        source_record_count=None,
    )

    # Constructing a verdict with absent evidence and verdict="same" must raise HonestCoverageViolationError
    with pytest.raises(HonestCoverageViolationError):
        HonestCoverageVerdict(
            verdict="same",
            exit_code=0,
            is_whole_source_same=True,
            accessible_objects_verified=["accounts"],
            absent_source_evidence=[absent],
            total_upstream_objects=2,
        )

    # Even if someone sets verdict="incomplete" but is_whole_source_same=True, must raise
    with pytest.raises(HonestCoverageViolationError):
        HonestCoverageVerdict(
            verdict="incomplete",
            exit_code=3,
            is_whole_source_same=True,
            accessible_objects_verified=["accounts"],
            absent_source_evidence=[absent],
            total_upstream_objects=2,
        )

    # Valid honest coverage verdict
    honest_verdict = HonestCoverageVerdict(
        verdict="incomplete",
        exit_code=3,
        is_whole_source_same=False,
        accessible_objects_verified=["accounts"],
        absent_source_evidence=[absent],
        total_upstream_objects=2,
        object_verdicts={"accounts": "same", "billing_secrets": "absent"},
    )

    assert honest_verdict.verdict == "incomplete"
    assert honest_verdict.exit_code == 3
    assert not honest_verdict.is_whole_source_same
    assert honest_verdict.coverage_ratio == 0.5
    assert len(honest_verdict.absent_source_evidence) == 1
    assert honest_verdict.absent_source_evidence[0].object_name == "billing_secrets"


def test_partial_access_reconciler_honest_incomplete_match():
    scope = PartialAccessScope(
        scope_id="scope-crm-01",
        source_id="saas-crm",
        accessible_objects={"accounts"},
        inaccessible_objects={"billing_secrets", "internal_audit"},
        permitted_columns={"accounts": {"id", "name"}},
        allowed_networks={"10.0.0.1", "127.0.0.1"},
        allowed_roles={"exporter_role"},
    )

    registry = AuthorizedCollectorRegistry()
    secret = b"test-collector-secret-key-32b01"
    registry.register_collector(
        collector_id="col-01",
        allowed_scopes={"scope-crm-01"},
        secret_key=secret,
        allowed_networks={"10.0.0.1", "127.0.0.1"},
        roles={"exporter_role"},
    )

    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-01",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    accounts_data = [
        {"id": 1, "name": "Acme Corp"},
        {"id": 2, "name": "Globex Corp"},
    ]

    obj_accounts = ExportObjectManifest.from_records(
        object_name="accounts",
        records=accounts_data,
        schema_definition={"id": "BIGINT", "name": "VARCHAR"},
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-001",
        collector_id="col-01",
        scope_id="scope-crm-01",
        lineage=lineage,
        objects={"accounts": obj_accounts},
    )
    signed_manifest = registry.sign_manifest(manifest, "col-01")

    # Target data has matching accounts, plus the inaccessible tables
    target_data = {
        "accounts": [
            {"id": 1, "name": "Acme Corp"},
            {"id": 2, "name": "Globex Corp"},
        ],
        "billing_secrets": [
            {"id": 99, "secret_key": "tok_123"},
        ],
        "internal_audit": [
            {"id": 1, "action": "LOGIN"},
        ],
    }

    reconciler = PartialAccessReconciler(
        scope=scope,
        manifest=signed_manifest,
        registry=registry,
        client_ip="10.0.0.1",
        known_upstream_objects={"accounts", "billing_secrets", "internal_audit"},
    )

    verdict = reconciler.reconcile(
        source_data={"accounts": accounts_data},
        target_data=target_data,
    )

    # In-scope objects matched, but because source was partial, whole-source same is impossible
    assert verdict.verdict == "incomplete"
    assert verdict.exit_code == 3
    assert not verdict.is_whole_source_same
    assert "accounts" in verdict.accessible_objects_verified
    assert verdict.object_verdicts["accounts"] == "same"
    assert len(verdict.absent_source_evidence) == 2

    absent_names = {ev.object_name for ev in verdict.absent_source_evidence}
    assert absent_names == {"billing_secrets", "internal_audit"}


def test_partial_access_reconciler_discrepancy_on_in_scope_object():
    scope = PartialAccessScope(
        scope_id="scope-crm-02",
        source_id="saas-crm",
        accessible_objects={"accounts"},
        inaccessible_objects={"billing_secrets"},
        permitted_columns={"accounts": {"id", "name"}},
        allowed_networks={"127.0.0.1"},
        allowed_roles={"exporter_role"},
    )

    registry = AuthorizedCollectorRegistry()
    secret = b"test-collector-secret-key-32b01"
    registry.register_collector(
        collector_id="col-01",
        allowed_scopes={"scope-crm-02"},
        secret_key=secret,
        allowed_networks={"127.0.0.1"},
        roles={"exporter_role"},
    )

    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-02",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    accounts_data = [{"id": 1, "name": "Acme Corp"}]
    obj_accounts = ExportObjectManifest.from_records(
        object_name="accounts",
        records=accounts_data,
        schema_definition={"id": "BIGINT", "name": "VARCHAR"},
    )

    manifest = registry.sign_manifest(
        ApprovedExportManifest(
            manifest_id="mf-002",
            collector_id="col-01",
            scope_id="scope-crm-02",
            lineage=lineage,
            objects={"accounts": obj_accounts},
        ),
        "col-01",
    )

    target_data = {
        "accounts": [{"id": 1, "name": "Acme Corp Drifted"}],
        "billing_secrets": [{"id": 99, "secret": "tok_123"}],
    }

    reconciler = PartialAccessReconciler(
        scope=scope,
        manifest=manifest,
        registry=registry,
        client_ip="127.0.0.1",
        known_upstream_objects={"accounts", "billing_secrets"},
    )

    verdict = reconciler.reconcile(
        source_data={"accounts": accounts_data},
        target_data=target_data,
    )

    # Discrepancy on in-scope accounts must yield different with exit code 1
    assert verdict.verdict == "different"
    assert verdict.exit_code == 1
    assert not verdict.is_whole_source_same
    assert verdict.object_verdicts["accounts"] == "different"
    assert len(verdict.discrepancies) > 0
    assert len(verdict.absent_source_evidence) == 1


def test_reconciler_blocks_placeholder_injection():
    scope = PartialAccessScope(
        scope_id="scope-crm-03",
        source_id="saas-crm",
        accessible_objects={"accounts"},
        inaccessible_objects={"billing_secrets"},
        allowed_networks={"127.0.0.1"},
    )

    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-03",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    fake_records = [
        {"id": 1, "name": "Acme"},
        {"id": -1, "name": "[PLACEHOLDER]", "__is_placeholder__": True},
    ]

    obj_accounts = ExportObjectManifest.from_records(
        object_name="accounts",
        records=fake_records,
        schema_definition={"id": "BIGINT", "name": "VARCHAR"},
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-fake",
        collector_id="col-01",
        scope_id="scope-crm-03",
        lineage=lineage,
        objects={"accounts": obj_accounts},
    )

    reconciler = PartialAccessReconciler(
        scope=scope,
        manifest=manifest,
        client_ip="127.0.0.1",
    )

    with pytest.raises(PlaceholderDataProhibitedError):
        reconciler.reconcile(
            source_data={"accounts": fake_records},
            target_data={"accounts": fake_records},
        )


def test_reconciler_validates_collector_network_and_permissions():
    scope = PartialAccessScope(
        scope_id="scope-sec-01",
        source_id="saas-crm",
        accessible_objects={"accounts"},
        allowed_networks={"10.0.0.5"},
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-sec",
        collector_id="col-sec",
        scope_id="scope-sec-01",
        lineage=SourceLineage(
            source_system="saas-crm",
            export_batch_id="b-01",
            collected_at=1771142400.0,
            collector_id="col-sec",
            extraction_method="owner_dump",
        ),
        objects={},
    )

    # Client IP outside allowed networks
    reconciler = PartialAccessReconciler(
        scope=scope,
        manifest=manifest,
        client_ip="10.0.0.254",
    )

    with pytest.raises(SecurityScopeViolationError):
        reconciler.reconcile(source_data={}, target_data={})


def test_target_mapping_preserves_source_lineage():
    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="batch-crm-42",
        collected_at=1771142400.0,
        collector_id="col-42",
        extraction_method="owner_delegated_api",
        tenant_id="tenant-acme",
    )

    raw_records = [
        {"id": 101, "name": "Acme Corp", "extra_meta": "ignore"},
        {"id": 102, "name": "Globex Corp", "extra_meta": "ignore"},
    ]

    mapping = NormalizedTargetMapping(
        source_object="accounts",
        target_object="dim_accounts",
        column_transforms={
            "id": "account_id",
            "name": "company_name",
        },
        lineage=lineage,
    )

    normalized_envelope = mapping.transform(raw_records)
    assert normalized_envelope.target_object == "dim_accounts"
    assert len(normalized_envelope.records) == 2

    # Check that record fields are mapped cleanly
    assert normalized_envelope.records[0] == {"account_id": 101, "company_name": "Acme Corp"}
    assert normalized_envelope.records[1] == {"account_id": 102, "company_name": "Globex Corp"}

    # Lineage is preserved and chained
    assert normalized_envelope.lineage.source_system == "saas-crm"
    assert normalized_envelope.lineage.tenant_id == "tenant-acme"
    assert any(step["step"] == "normalize_target_mapping" for step in normalized_envelope.lineage.provenance_chain)


def test_collector_revocation_and_unauthorized_scope():
    registry = AuthorizedCollectorRegistry()
    secret = b"test-secret-key-32b0123456789012"
    registry.register_collector(
        collector_id="col-alpha",
        allowed_scopes={"scope-a"},
        secret_key=secret,
        allowed_networks={"10.0.0.5"},
    )

    manifest_scope_b = ApprovedExportManifest(
        manifest_id="mf-b",
        collector_id="col-alpha",
        scope_id="scope-b",
        lineage=SourceLineage(
            source_system="saas-crm",
            export_batch_id="b-b",
            collected_at=1771142400.0,
            collector_id="col-alpha",
            extraction_method="owner_dump",
        ),
        objects={},
    )

    # col-alpha is not authorized for scope-b
    assert not registry.is_collector_authorized("col-alpha", "scope-b", "10.0.0.5")

    # Revoke collector
    registry.revoke_collector("col-alpha")
    assert not registry.is_collector_authorized("col-alpha", "scope-a", "10.0.0.5")

    with pytest.raises(UnauthorizedCollectorError):
        registry.sign_manifest(manifest_scope_b, "col-alpha")


def test_honest_coverage_verdict_dict_serialization():
    absent = AbsentSourceEvidence(
        object_name="private_orders",
        reason=AbsentReason.OWNER_ONLY_DATA,
        detail="Owner restricted dataset",
        target_record_count=50,
    )
    verdict = HonestCoverageVerdict(
        verdict="incomplete",
        exit_code=3,
        is_whole_source_same=False,
        accessible_objects_verified=["public_catalog"],
        absent_source_evidence=[absent],
        total_upstream_objects=2,
        object_verdicts={"public_catalog": "same", "private_orders": "absent"},
    )

    d = verdict.to_dict()
    assert d["verdict"] == "incomplete"
    assert d["exit_code"] == 3
    assert not d["is_whole_source_same"]
    assert d["coverage_ratio"] == 0.5
    assert len(d["absent_source_evidence"]) == 1
    assert d["absent_source_evidence"][0]["object_name"] == "private_orders"
    assert d["absent_source_evidence"][0]["reason"] == "owner_only_data"


def test_column_level_restriction_excludes_inaccessible_columns():
    scope = PartialAccessScope(
        scope_id="scope-col-01",
        source_id="saas-crm",
        accessible_objects={"users"},
        permitted_columns={"users": {"id", "email"}},
        inaccessible_columns={"users": {"password_hash", "sso_token"}},
        allowed_networks={"127.0.0.1"},
    )

    assert scope.is_column_accessible("users", "id")
    assert scope.is_column_accessible("users", "email")
    assert not scope.is_column_accessible("users", "password_hash")
    assert not scope.is_column_accessible("users", "sso_token")
    assert not scope.is_column_accessible("users", "unlisted_column")

    lineage = SourceLineage(
        source_system="saas-crm",
        export_batch_id="b-col-01",
        collected_at=1771142400.0,
        collector_id="col-01",
        extraction_method="owner_dump",
    )

    # Source has permitted columns plus password_hash is omitted
    source_users = [{"id": 1, "email": "alice@example.com"}]
    # Target has permitted columns matching, but also has internal password_hash
    target_users = [{"id": 1, "email": "alice@example.com", "password_hash": "argon2$..."}]

    obj_users = ExportObjectManifest.from_records(
        object_name="users",
        records=source_users,
        schema_definition={"id": "BIGINT", "email": "VARCHAR"},
    )

    manifest = ApprovedExportManifest(
        manifest_id="mf-col",
        collector_id="col-01",
        scope_id="scope-col-01",
        lineage=lineage,
        objects={"users": obj_users},
    )

    reconciler = PartialAccessReconciler(
        scope=scope,
        manifest=manifest,
        client_ip="127.0.0.1",
        known_upstream_objects={"users"},
    )

    verdict = reconciler.reconcile(
        source_data={"users": source_users},
        target_data={"users": target_users},
    )

    # Because password_hash was marked inaccessible in scope, reconciler filtered it out during comparison,
    # so users is evaluated as 'same' within scope!
    assert verdict.object_verdicts["users"] == "same"
    assert "users" in verdict.accessible_objects_verified

