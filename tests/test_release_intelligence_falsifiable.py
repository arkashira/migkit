"""Tests for Row 642: RCH34.release-intelligence (Keep support and competitor knowledge versioned and falsifiable).

Verifies:
1. Official URL, version, release date, status, and changed capability predicates are
   versioned, fingerprinted, and diffed deterministically.
2. Documentation updates produce reviewable contract/probe tasks, never automatic
   installs or automatic production changes.
3. Conflicting regional and legacy documentation snapshots are retained in full
   lineage and invalidate unsupported broad claims.
4. GA, Preview, roadmap, marketing, and inference claims are strictly separated.
5. A preview feature never turns into an unconditional supported capability.
6. A vendor rename or redirected documentation URL never fabricates coverage.
7. Decision engine rungs in ADMISSION_LADDER reach both holds and refuses under
   tests.rung_branches.watched().
8. O(1) typed durable checkpoint resume, active lease lock (.run.lock) with heartbeat,
   AIMD batch controller, Merkle catalog diffing, and normalized tool wrapper.
"""

from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path

import pytest

from migkit import release_intelligence as ri
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("release_intelligence", "ADMISSION_LADDER", "verified_conditional_ga", "holds"),
    ("release_intelligence", "ADMISSION_LADDER", "verified_conditional_ga", "refuses"),
    ("release_intelligence", "ADMISSION_LADDER", "guarded_preview_gate", "holds"),
    ("release_intelligence", "ADMISSION_LADDER", "guarded_preview_gate", "refuses"),
    ("release_intelligence", "ADMISSION_LADDER", "reviewable_contract_probe_queue", "holds"),
]


# -----------------------------------------------------------------------------
# 1. Decision Engine Rung Branch Verification (watched)
# -----------------------------------------------------------------------------

def test_admission_ladder_rungs_hold_and_refuse_under_watched() -> None:
    with watched() as seen:
        # 1. verified_conditional_ga holds when GA, valid URL, explicit predicates, probe passed
        c_ga = ri.decide_claim_admission(
            tier=ri.KnowledgeTier.GA,
            official_url_valid=True,
            has_explicit_predicates=True,
            probe_passed=True,
        )
        assert c_ga.path == "verified_conditional_ga"

        # 2. verified_conditional_ga refuses and guarded_preview_gate holds on valid preview sandbox
        c_prev = ri.decide_claim_admission(
            tier=ri.KnowledgeTier.PREVIEW,
            official_url_valid=True,
            has_explicit_predicates=True,
            probe_passed=False,
            require_unconditional_support=False,
        )
        assert c_prev.path == "guarded_preview_gate"

        # 3. guarded_preview_gate refuses when caller demands unconditional support -> falls to queue
        c_prev_refused = ri.decide_claim_admission(
            tier=ri.KnowledgeTier.PREVIEW,
            official_url_valid=True,
            has_explicit_predicates=True,
            probe_passed=True,
            require_unconditional_support=True,
        )
        assert c_prev_refused.path == "reviewable_contract_probe_queue"

    assert set(branch(seen, "verified_conditional_ga")) == {HOLDS, REFUSES}
    assert set(branch(seen, "guarded_preview_gate")) == {HOLDS, REFUSES}
    assert HOLDS in branch(seen, "reviewable_contract_probe_queue")
    assert "unconditional" in (why(seen, "guarded_preview_gate") or "")


# -----------------------------------------------------------------------------
# 2. Versioned Metadata, Changed Capability Predicates, and Non-Automatic Tasks
# -----------------------------------------------------------------------------

def test_versioned_metadata_and_changed_capability_predicates_produce_probe_tasks() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    v1_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="postgres",
        target_engine="postgres",
        min_version="14.0",
        max_version="16.9",
        region_scope=("global",),
        required_settings=("wal_level=logical",),
        excluded_data_types=("xml", "pg_lsn"),
        requires_primary_key=True,
        supports_ddl_replication=False,
        max_lob_bytes=64 * 1024 * 1024,
        change_kind=ri.PredicateChangeKind.ADDED,
        falsifier_probe_id="probe_pg_logical_stream_v1",
    )
    snap_v1 = ri.ReleaseNoteSnapshot(
        snapshot_id="dms-2026-03-v3.5.3",
        vendor="aws",
        product="dms",
        official_url="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html#v3.5.3",
        version="3.5.3",
        release_date=datetime.date(2026, 3, 15),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
        predicates=(v1_pred,),
        probe_verified=True,
        probe_evidence_digest="sha256:1111aaaa",
    )

    tasks_v1 = registry.ingest_documentation_update(snap_v1)
    assert len(tasks_v1) == 1
    assert tasks_v1[0].task_kind == ri.TaskKind.FALSIFIER_PROBE
    assert tasks_v1[0].requires_human_review is True
    assert tasks_v1[0].auto_install_permitted is False
    assert tasks_v1[0].auto_production_change_permitted is False
    assert tasks_v1[0].care_badge == ri.CareBadge.REVIEW

    # Ingest v3.5.4 narrowing predicate (adds jsonb exclusion and lowers max_lob_bytes)
    v2_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="postgres",
        target_engine="postgres",
        min_version="14.0",
        max_version="17.2",
        region_scope=("global",),
        required_settings=("wal_level=logical",),
        excluded_data_types=("xml", "pg_lsn", "tsquery"),
        requires_primary_key=True,
        supports_ddl_replication=False,
        max_lob_bytes=32 * 1024 * 1024,
        change_kind=ri.PredicateChangeKind.NARROWED,
        falsifier_probe_id="probe_pg_logical_stream_v2",
    )
    snap_v2 = ri.ReleaseNoteSnapshot(
        snapshot_id="dms-2026-06-v3.5.4",
        vendor="aws",
        product="dms",
        official_url="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html#v3.5.4",
        version="3.5.4",
        release_date=datetime.date(2026, 6, 20),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
        predicates=(v2_pred,),
        probe_verified=True,
        probe_evidence_digest="sha256:2222bbbb",
    )
    tasks_v2 = registry.ingest_documentation_update(snap_v2)
    assert len(tasks_v2) >= 1
    assert len(registry.predicate_diffs) == 1
    diff = registry.predicate_diffs[0]
    assert diff.from_version == "3.5.3"
    assert diff.to_version == "3.5.4"
    assert len(diff.narrowed_predicates) == 1
    assert diff.care_badge == ri.CareBadge.ACTION
    assert snap_v1.content_fingerprint != snap_v2.content_fingerprint


def test_documentation_updates_never_trigger_automatic_installs_or_production_changes() -> None:
    registry = ri.ReleaseIntelligenceRegistry()
    snap = ri.ReleaseNoteSnapshot(
        snapshot_id="gcp-dms-2026-08",
        vendor="gcp",
        product="cloud-dms",
        official_url="https://docs.cloud.google.com/database-migration/docs/release-notes#2026-08",
        version="2026.08",
        release_date=datetime.date(2026, 8, 1),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
    )

    with pytest.raises(ri.AutomaticMutationRefusedError, match="automatic install"):
        registry.ingest_documentation_update(snap, attempt_auto_install=True)

    with pytest.raises(ri.AutomaticMutationRefusedError, match="automatic production change"):
        registry.ingest_documentation_update(snap, attempt_auto_production_change=True)

    assert registry.blocked_mutation_counts == {
        "auto_install_blocked": 1,
        "auto_production_change_blocked": 1,
    }
    assert "gcp-dms-2026-08" not in registry.snapshots

    # Direct construction of a ContractProbeTask with auto_install_permitted=True also fails
    with pytest.raises(ri.AutomaticMutationRefusedError):
        ri.ContractProbeTask(
            task_id="bad-task",
            snapshot_id="gcp-dms-2026-08",
            vendor="gcp",
            product="cloud-dms",
            version="2026.08",
            official_url="https://docs.cloud.google.com/database-migration/docs/release-notes",
            task_kind=ri.TaskKind.CONTRACT_REVIEW,
            target_capability="stream",
            changed_predicates=(),
            falsifier_probe_command="migkit probe",
            requires_human_review=True,
            auto_install_permitted=True,
        )


# -----------------------------------------------------------------------------
# 3. Conflicting Regional & Legacy Docs Retained; Broad Claims Invalidated
# -----------------------------------------------------------------------------

def test_conflicting_regional_and_legacy_docs_are_retained_and_invalidate_broad_claims() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    # Global doc claims broad DDL replication support and includes a sweeping marketing claim
    global_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="mysql",
        target_engine="mysql",
        min_version="8.0",
        max_version="8.4",
        region_scope=("global",),
        requires_primary_key=False,
        supports_ddl_replication=True,
        falsifier_probe_id="probe_dts_mysql_global",
    )
    snap_global = ri.ReleaseNoteSnapshot(
        snapshot_id="tencent-dts-global-2026-05",
        vendor="tencent",
        product="dts",
        official_url="https://www.tencentcloud.com/document/product/571/42597#mysql-cdc",
        version="2026.05",
        release_date=datetime.date(2026, 5, 10),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
        predicates=(global_pred,),
        broad_claims=("Supports all data types and seamless zero-downtime DDL across all regions",),
        region="global",
        probe_verified=True,
    )
    registry.ingest_documentation_update(snap_global)

    # Regional Hong Kong proxy doc (S112) states primary key is required and geometry/DDL restricted
    hk_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="mysql",
        target_engine="mysql",
        min_version="8.0",
        max_version="8.0",
        region_scope=("ap-east-1",),
        excluded_data_types=("geometry", "multipolygon"),
        requires_primary_key=True,
        supports_ddl_replication=False,
        change_kind=ri.PredicateChangeKind.NARROWED,
        falsifier_probe_id="probe_dts_mysql_hk",
    )
    snap_hk = ri.ReleaseNoteSnapshot(
        snapshot_id="tencent-dts-hk-2026-05",
        vendor="tencent",
        product="dts",
        official_url="https://proxy-hk.tencentcloud.com/ind/document/product/571/42597#mysql-limits",
        version="2026.05",
        release_date=datetime.date(2026, 5, 12),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.REGIONAL_VARIANT,
        predicates=(hk_pred,),
        region="ap-east-1",
        probe_verified=True,
    )
    registry.ingest_documentation_update(snap_hk)

    # Legacy doc also retained in lineage
    legacy_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="mysql",
        target_engine="mysql",
        min_version="5.7",
        max_version="8.0",
        region_scope=("global",),
        excluded_data_types=("json", "geometry"),
        requires_primary_key=True,
        supports_ddl_replication=False,
        max_lob_bytes=16 * 1024 * 1024,
        falsifier_probe_id="probe_dts_mysql_legacy",
    )
    snap_legacy = ri.ReleaseNoteSnapshot(
        snapshot_id="tencent-dts-legacy-2024",
        vendor="tencent",
        product="dts",
        official_url="https://www.tencentcloud.com/document/product/571/legacy-limits",
        version="2024.11",
        release_date=datetime.date(2024, 11, 1),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.LEGACY_RETAINED,
        predicates=(legacy_pred,),
        region="global",
        is_legacy_doc=True,
        probe_verified=True,
    )
    registry.ingest_documentation_update(snap_legacy)

    # All 3 snapshots (global, regional hk, legacy) MUST be retained
    assert set(registry.snapshots.keys()) == {
        "tencent-dts-global-2026-05",
        "tencent-dts-hk-2026-05",
        "tencent-dts-legacy-2024",
    }

    # Conflicts and invalidated broad claims must be recorded
    assert len(registry.conflicts) >= 2
    conflict_kinds = {c.conflict_kind for c in registry.conflicts}
    assert "regional_divergence" in conflict_kinds
    assert "legacy_contradiction" in conflict_kinds

    assert len(registry.invalidated_claims) >= 1
    inv = registry.invalidated_claims[0]
    assert inv.care_badge == ri.CareBadge.ACTION
    assert "all data types" in inv.claim_text.lower()

    # Evaluating capability while regional/legacy conflict is unresolved refuses GA admission
    verdict = registry.evaluate_capability(
        vendor="tencent",
        product="dts",
        capability="stream",
        source_engine="mysql",
        target_engine="mysql",
        version="8.0",
    )
    assert verdict.standing == ri.SupportStanding.REFUSED_CONFLICTING_DOCS
    assert verdict.is_unconditionally_supported is False
    assert verdict.is_conditionally_supported is False
    assert verdict.care_badge == ri.CareBadge.ACTION
    assert len(verdict.retained_conflict_ids) >= 2


# -----------------------------------------------------------------------------
# 4. Explicit Separation of GA, Preview, Roadmap, Marketing, and Inference
#    & Proof That Preview Never Turns Into Unconditional Support
# -----------------------------------------------------------------------------

def test_preview_never_turns_into_unconditional_supported_capability() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    preview_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="oracle",
        target_engine="snowflake",
        min_version="19.0",
        max_version="21.0",
        requires_primary_key=True,
        change_kind=ri.PredicateChangeKind.ADDED,
        falsifier_probe_id="probe_oracle_snowflake_preview",
    )
    snap_preview = ri.ReleaseNoteSnapshot(
        snapshot_id="qlik-may2026-preview-ora-sf",
        vendor="qlik",
        product="replicate",
        official_url="https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm#preview-ora",
        version="2026.5",
        release_date=datetime.date(2026, 5, 1),
        tier=ri.KnowledgeTier.PREVIEW,
        status=ri.ReleaseDocStatus.ACTIVE_PREVIEW,
        predicates=(preview_pred,),
        probe_verified=True,
        probe_evidence_digest="sha256:preview999",
    )
    registry.ingest_documentation_update(snap_preview)

    # 1. Explicit promotion call is deterministically refused
    with pytest.raises(ri.PreviewPromotionRefusedError, match="never become an unconditional"):
        registry.promote_preview_to_unconditional_support("qlik-may2026-preview-ora-sf")

    # 2. Requesting unconditional support on a Preview feature is refused
    v_uncond = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="oracle",
        target_engine="snowflake",
        version="19.0",
        require_unconditional_support=True,
        allow_preview_sandbox=True,
    )
    assert v_uncond.standing == ri.SupportStanding.REFUSED_UNCONDITIONAL_ON_PREVIEW
    assert v_uncond.is_unconditionally_supported is False
    assert v_uncond.is_conditionally_supported is False
    assert v_uncond.is_preview_only is True
    assert v_uncond.care_badge == ri.CareBadge.ACTION

    # 3. Default evaluation (allow_preview_sandbox=False) also refuses Preview for production
    v_default = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="oracle",
        target_engine="snowflake",
        version="19.0",
        require_unconditional_support=False,
        allow_preview_sandbox=False,
    )
    assert v_default.standing == ri.SupportStanding.REFUSED_UNCONDITIONAL_ON_PREVIEW
    assert v_default.is_unconditionally_supported is False
    assert v_default.is_conditionally_supported is False

    # 4. Even in guarded preview sandbox mode, is_unconditionally_supported and is_conditionally_supported stay False
    v_sandbox = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="oracle",
        target_engine="snowflake",
        version="19.0",
        require_unconditional_support=False,
        allow_preview_sandbox=True,
    )
    assert v_sandbox.standing == ri.SupportStanding.GUARDED_PREVIEW_ONLY
    assert v_sandbox.is_unconditionally_supported is False
    assert v_sandbox.is_conditionally_supported is False
    assert v_sandbox.is_preview_only is True
    assert v_sandbox.care_badge == ri.CareBadge.REVIEW


def test_strict_separation_of_ga_preview_roadmap_marketing_and_inference() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    tiers_and_caps = [
        (ri.KnowledgeTier.GA, "bulk-move", True),
        (ri.KnowledgeTier.PREVIEW, "delta", True),
        (ri.KnowledgeTier.ROADMAP, "fence", True),
        (ri.KnowledgeTier.MARKETING, "users", True),
        (ri.KnowledgeTier.INFERENCE, "sequences", True),
    ]

    for tier, cap, verified in tiers_and_caps:
        pred = ri.CapabilityPredicate(
            capability=cap,
            source_engine="postgres",
            target_engine="AlloyDB",
            min_version="15.0",
            max_version="16.0",
            falsifier_probe_id=f"probe_{cap}",
        )
        snap = ri.ReleaseNoteSnapshot(
            snapshot_id=f"snap-{tier.value}",
            vendor="gcp",
            product="cloud-dms",
            official_url=f"https://docs.cloud.google.com/database-migration/docs/release-notes#{tier.value}",
            version="2026.10",
            release_date=datetime.date(2026, 10, 1),
            tier=tier,
            status=(
                ri.ReleaseDocStatus.ACTIVE_GA
                if tier == ri.KnowledgeTier.GA
                else ri.ReleaseDocStatus.ACTIVE_PREVIEW
            ),
            predicates=(pred,),
            probe_verified=verified,
        )
        registry.ingest_documentation_update(snap)

    summary = registry.summarize_by_knowledge_tier(vendor="gcp", product="cloud-dms")
    assert summary["ga"] == ["snap-ga"]
    assert summary["preview"] == ["snap-preview"]
    assert summary["roadmap"] == ["snap-roadmap"]
    assert summary["marketing"] == ["snap-marketing"]
    assert summary["inference"] == ["snap-inference"]

    # Roadmap, marketing, and inference are strictly refused even if probe_verified=True was passed
    for non_falsifiable_tier, cap in [
        (ri.KnowledgeTier.ROADMAP, "fence"),
        (ri.KnowledgeTier.MARKETING, "users"),
        (ri.KnowledgeTier.INFERENCE, "sequences"),
    ]:
        verdict = registry.evaluate_capability(
            vendor="gcp",
            product="cloud-dms",
            capability=cap,
            source_engine="postgres",
            target_engine="AlloyDB",
            version="15.2",
        )
        assert verdict.standing == ri.SupportStanding.REFUSED_NON_FALSIFIABLE_TIER
        assert verdict.effective_tier == non_falsifiable_tier
        assert verdict.is_unconditionally_supported is False
        assert verdict.is_conditionally_supported is False
        assert verdict.care_badge == ri.CareBadge.ACTION

    # Only GA with valid version & probe gets conditional GA support
    v_ga = registry.evaluate_capability(
        vendor="gcp",
        product="cloud-dms",
        capability="bulk-move",
        source_engine="postgres",
        target_engine="AlloyDB",
        version="15.2",
    )
    assert v_ga.standing == ri.SupportStanding.VERIFIED_CONDITIONAL_GA
    assert v_ga.is_conditionally_supported is True
    assert v_ga.is_unconditionally_supported is False
    assert v_ga.care_badge == ri.CareBadge.PASS


# -----------------------------------------------------------------------------
# 5. Proof That Vendor Rename or Redirected Docs Never Create Fabricated Coverage
# -----------------------------------------------------------------------------

def test_redirected_documentation_does_not_create_fabricated_coverage() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="db2",
        target_engine="postgres",
        min_version="11.5",
        max_version="11.5",
        falsifier_probe_id="probe_db2_pg",
    )
    # Specific feature URL redirected (301) to a generic /release-notes landing page
    redir = ri.RedirectObservation(
        original_url="https://help.qlik.com/en-US/replicate/May2025/Content/Replicate/Main/DB2/db2_cdc_limitations.htm#lob",
        final_url="https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes",
        http_status=301,
        preserves_predicate_anchor=False,
    )
    assert redir.is_redirected is True
    assert redir.is_suspicious_or_generic is True

    snap_redirected = ri.ReleaseNoteSnapshot(
        snapshot_id="qlik-db2-redirected-2026",
        vendor="qlik",
        product="replicate",
        official_url=redir.final_url,
        version="2026.5",
        release_date=datetime.date(2026, 5, 15),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
        predicates=(pred,),
        redirect_observation=redir,
        probe_verified=True,
    )
    tasks = registry.ingest_documentation_update(snap_redirected)
    assert any(t.task_kind == ri.TaskKind.REDIRECT_VERIFICATION for t in tasks)

    # Stored snapshot status was automatically downgraded to REDIRECTED_UNVERIFIED
    stored = registry.snapshots["qlik-db2-redirected-2026"]
    assert stored.status == ri.ReleaseDocStatus.REDIRECTED_UNVERIFIED
    assert stored.probe_verified is False

    verdict = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="db2",
        target_engine="postgres",
        version="11.5",
    )
    assert verdict.standing == ri.SupportStanding.REFUSED_REDIRECT_OR_RENAME
    assert verdict.is_unconditionally_supported is False
    assert verdict.is_conditionally_supported is False
    assert verdict.care_badge == ri.CareBadge.ACTION


def test_vendor_rename_does_not_create_fabricated_coverage() -> None:
    registry = ri.ReleaseIntelligenceRegistry()

    # Former product (Attunity Replicate 6.5) had verified support for Oracle -> Kafka
    former_pred = ri.CapabilityPredicate(
        capability="stream",
        source_engine="oracle",
        target_engine="kafka",
        min_version="12.2",
        max_version="19.0",
        falsifier_probe_id="probe_attunity_ora_kafka_65",
    )
    snap_former = ri.ReleaseNoteSnapshot(
        snapshot_id="attunity-replicate-6.5",
        vendor="attunity",
        product="attunity-replicate",
        official_url="https://docs.attunity.com/replicate/6.5/release-notes#ora-kafka",
        version="6.5",
        release_date=datetime.date(2020, 4, 1),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.LEGACY_RETAINED,
        predicates=(former_pred,),
        probe_verified=True,
    )
    registry.ingest_documentation_update(snap_former)

    # Register vendor rename: Attunity Replicate -> Qlik Replicate
    # Only version 2023.11 was shared/verified for bulk-move, NOT 2026.5 stream
    rename_lineage = ri.VendorRenameLineage(
        former_vendor="attunity",
        former_product="attunity-replicate",
        new_vendor="qlik",
        new_product="replicate",
        rename_date=datetime.date(2021, 1, 15),
        official_announcement_url="https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm",
        verified_shared_versions=("2023.11",),
        re_probed_capabilities=("bulk-move",),
    )
    rename_task = registry.register_vendor_rename(rename_lineage)
    assert rename_task.task_kind == ri.TaskKind.VENDOR_RENAME_LINEAGE_CHECK
    assert rename_task.auto_install_permitted is False

    # Case A: Querying the new vendor/product for a capability that only existed on the old brand
    verdict_untransferred = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="oracle",
        target_engine="kafka",
        version="2026.5",
    )
    assert verdict_untransferred.standing == ri.SupportStanding.REFUSED_REDIRECT_OR_RENAME
    assert verdict_untransferred.is_conditionally_supported is False
    assert verdict_untransferred.is_unconditionally_supported is False
    assert verdict_untransferred.care_badge == ri.CareBadge.ACTION
    assert "cannot fabricate coverage" in verdict_untransferred.explanation

    # Case B: Ingesting a snapshot under the new vendor name claiming lineage from the rename
    # for an unverified version ("2026.5" not in verified_shared_versions)
    new_snap_unproved = ri.ReleaseNoteSnapshot(
        snapshot_id="qlik-replicate-2026.5-ora-kafka",
        vendor="qlik",
        product="replicate",
        official_url="https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Release_Notes/features.htm#kafka",
        version="2026.5",
        release_date=datetime.date(2026, 5, 1),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
        predicates=(former_pred,),
        rename_lineage=rename_lineage,
        probe_verified=True,
    )
    registry.ingest_documentation_update(new_snap_unproved)
    verdict_unproved_ver = registry.evaluate_capability(
        vendor="qlik",
        product="replicate",
        capability="stream",
        source_engine="oracle",
        target_engine="kafka",
        version="19.0",
    )
    assert verdict_unproved_ver.standing == ri.SupportStanding.REFUSED_REDIRECT_OR_RENAME
    assert verdict_unproved_ver.is_conditionally_supported is False


# -----------------------------------------------------------------------------
# 6. Resilience Primitives: O(1) Typed Checkpoint, Lease Lock, AIMD, Merkle, Tool Wrapper
# -----------------------------------------------------------------------------

def test_o1_typed_durable_checkpoint_and_merkle_catalog_diff(tmp_path: Path) -> None:
    ckpt_path = tmp_path / "release_intel_ckpt.json"
    store = ri.ReleaseIntelligenceCheckpointStore(ckpt_path)

    composite_unit = ("aws", "dms", "3.5.4", datetime.date(2026, 6, 20))
    cursor_ts = datetime.datetime(2026, 10, 17, 12, 30, 0)
    run_uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    cost_weight = decimal.Decimal("19.875")

    assert store.is_completed(composite_unit) is False
    store.mark_completed(
        composite_unit,
        cursor=cursor_ts,
        meta={"run_id": run_uid, "cost_weight": cost_weight},
    )
    assert store.is_completed(composite_unit) is True

    # Reload from disk and verify O(1) lookup with preserved native Python types
    reloaded = ri.ReleaseIntelligenceCheckpointStore(ckpt_path)
    assert reloaded.is_completed(composite_unit) is True
    assert isinstance(reloaded.last_cursor, datetime.datetime)
    assert reloaded.last_cursor == cursor_ts
    assert isinstance(reloaded.metadata["run_id"], uuid.UUID)
    assert reloaded.metadata["run_id"] == run_uid
    assert isinstance(reloaded.metadata["cost_weight"], decimal.Decimal)
    assert reloaded.metadata["cost_weight"] == cost_weight

    # Merkle bucket catalog diffing
    reg_a = ri.ReleaseIntelligenceRegistry()
    reg_b = ri.ReleaseIntelligenceRegistry()
    snap = ri.ReleaseNoteSnapshot(
        snapshot_id="s1",
        vendor="aws",
        product="dms",
        official_url="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html#s1",
        version="3.5.4",
        release_date=datetime.date(2026, 6, 1),
        tier=ri.KnowledgeTier.GA,
        status=ri.ReleaseDocStatus.ACTIVE_GA,
    )
    reg_a.ingest_documentation_update(snap)
    reg_b.ingest_documentation_update(snap)
    assert ri.ReleaseIntelligenceRegistry.diff_merkle_catalogs(
        reg_a.compute_merkle_catalog_buckets(4),
        reg_b.compute_merkle_catalog_buckets(4),
    ) == []

    snap2 = ri.ReleaseNoteSnapshot(
        snapshot_id="s2",
        vendor="aws",
        product="dms",
        official_url="https://docs.aws.amazon.com/dms/latest/userguide/CHAP_ReleaseNotes.html#s2",
        version="3.5.5",
        release_date=datetime.date(2026, 7, 1),
        tier=ri.KnowledgeTier.PREVIEW,
        status=ri.ReleaseDocStatus.ACTIVE_PREVIEW,
    )
    reg_b.ingest_documentation_update(snap2)
    diff_buckets = ri.ReleaseIntelligenceRegistry.diff_merkle_catalogs(
        reg_a.compute_merkle_catalog_buckets(4),
        reg_b.compute_merkle_catalog_buckets(4),
    )
    assert len(diff_buckets) == 1


def test_active_lease_lock_and_aimd_controller_and_tool_wrapper(tmp_path: Path) -> None:
    # 1. Mutual exclusion lease (.run.lock)
    with ri.ReleaseIntelligenceLease(tmp_path, owner_id="worker-1", ttl_seconds=60.0) as lease1:
        lease1.heartbeat()
        with pytest.raises(ri.LeaseLockError, match="worker-1"):
            ri.ReleaseIntelligenceLease(tmp_path, owner_id="worker-2", ttl_seconds=60.0).acquire()

    # After context exit, worker-2 can acquire
    with ri.ReleaseIntelligenceLease(tmp_path, owner_id="worker-2", ttl_seconds=60.0) as lease2:
        assert lease2.acquired is True

    # 2. AIMD batch controller and byte-weighted cold-start ETA
    ctrl = ri.AdaptiveProbeBatchController(current_batch_size=50, additive_increase=25, target_latency_ms=200.0)
    cold_eta = ctrl.estimate_eta_seconds(10 * 1024 * 1024)
    assert cold_eta > 0.0
    assert ctrl.record_batch(50, 2 * 1024 * 1024, latency_ms=100.0) == 75
    assert ctrl.record_batch(75, 2 * 1024 * 1024, latency_ms=450.0) == 37
    assert ctrl.estimate_eta_seconds(0) == 0.0

    # 3. External tool probe wrapper with safe endpoint check and zero internal tool leaks
    with pytest.raises(ri.UnsafeEndpointError):
        ri.ExternalReleaseProbeWrapper.run_falsifier_probe("debezium", probe_host="192.168.1.99")

    ok_res = ri.ExternalReleaseProbeWrapper.run_falsifier_probe(
        "debezium",
        probe_host="10.0.0.15",
        runner=lambda: {"passed": True, "version": "2.7.3", "summary": "logical slot probe verified"},
    )
    assert ok_res.care_badge == ri.CareBadge.PASS
    assert ok_res.standing == ri.SupportStanding.VERIFIED_CONDITIONAL_GA
    assert "debezium" not in ok_res.adapter_role.lower()

    err_res = ri.ExternalReleaseProbeWrapper.run_falsifier_probe(
        "datafold",
        probe_host="127.0.0.1",
        runner=lambda: (_ for _ in ()).throw(RuntimeError("internal vendor stacktrace token=secret")),
    )
    assert err_res.care_badge == ri.CareBadge.ACTION
    assert err_res.status == "normalized_probe_error"
    assert "secret" not in err_res.summary


def test_pure_ascii_and_zero_emoji_compliance() -> None:
    module_path = Path(ri.__file__).resolve()
    test_path = Path(__file__).resolve()
    for path in (module_path, test_path):
        content = path.read_text(encoding="utf-8")
        assert content.isascii(), f"non-ASCII characters found in {path}"
