"""Tests for Row 635 (RCH27.stream-metadata): Move event consumers and schemas with stream bytes."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pytest

from migkit.stream_metadata import (
    AclBinding,
    AclOperation,
    AclPatternType,
    AclPermissionType,
    AclPolicyViolationError,
    AclResourceType,
    ActiveConsumerFenceError,
    CapabilityMismatchError,
    CareBadge,
    CompatibilityLevel,
    ConsumerGroupSpec,
    ConsumerGroupState,
    ConsumerPartitionOffset,
    ConsumerRestartDiscontinuityError,
    LeaseLockError,
    OffsetCapability,
    OffsetOutOfRetentionBoundsError,
    PartitionIdentityMismatchError,
    PartitionOffsetMapping,
    PartitionRetainedRange,
    PromotionFenceError,
    PromotionPhase,
    ReplicationLinkSpec,
    SchemaCompatibilityError,
    SchemaFieldSpec,
    SchemaFormat,
    SchemaSubjectState,
    SchemaVersionSpec,
    StreamMetadataCheckpointStore,
    StreamMigrationLeaseLock,
    StreamPromotionCoordinator,
    TopicConfigSpec,
    UnsafeTestHostError,
    check_structural_schema_compatibility,
    evaluate_stream_migration_proof,
    translate_and_validate_consumer_offsets,
    validate_test_host,
    verify_acl_policy,
    verify_consumer_restart,
    verify_schema_registry_state,
    verify_topic_and_retained_ranges,
)


# -----------------------------------------------------------------------------
# Fixtures and Builders
# -----------------------------------------------------------------------------

def _make_partitions(
    offsets: List[Tuple[int, int, int]],
    digest_prefix: str = "sha256-orders-p",
    schema_ids: Tuple[int, ...] = (101, 102),
) -> Tuple[PartitionRetainedRange, ...]:
    return tuple(
        PartitionRetainedRange(
            partition_id=pid,
            log_start_offset=start,
            high_watermark=hwm,
            last_stable_offset=hwm,
            record_count=hwm - start,
            byte_count=(hwm - start) * 256,
            payload_sha256=f"{digest_prefix}{pid}",
            embedded_schema_ids=schema_ids,
        )
        for pid, start, hwm in offsets
    )


def _make_baseline_topics() -> Tuple[TopicConfigSpec, TopicConfigSpec]:
    parts = _make_partitions([(0, 100, 500), (1, 200, 800)])
    source = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=2,
        replication_factor=3,
        cleanup_policy="delete",
        retention_ms=604800000,
        min_insync_replicas=2,
        max_message_bytes=1048588,
        partitions=parts,
        is_mirror_topic=False,
        writable=True,
        broker_host="127.0.0.1",
    )
    target = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=2,
        replication_factor=3,
        cleanup_policy="delete",
        retention_ms=604800000,
        min_insync_replicas=2,
        max_message_bytes=1048588,
        partitions=parts,
        is_mirror_topic=True,
        writable=False,
        broker_host="10.0.0.10",
    )
    return source, target


def _make_baseline_schemas() -> Dict[str, SchemaSubjectState]:
    v1 = SchemaVersionSpec(
        subject="orders.events.v1-value",
        version=1,
        global_schema_id=101,
        schema_format=SchemaFormat.AVRO,
        canonical_text='{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}',
        fields=(
            SchemaFieldSpec(name="id", field_type="string", required=True, has_default=False),
        ),
    )
    v2 = SchemaVersionSpec(
        subject="orders.events.v1-value",
        version=2,
        global_schema_id=102,
        schema_format=SchemaFormat.AVRO,
        canonical_text=(
            '{"type":"record","name":"Order","fields":['
            '{"name":"id","type":"string"},'
            '{"name":"region","type":"string","default":"ap-southeast-1"}]}'
        ),
        fields=(
            SchemaFieldSpec(name="id", field_type="string", required=True, has_default=False),
            SchemaFieldSpec(name="region", field_type="string", required=True, has_default=True),
        ),
    )
    return {
        "orders.events.v1-value": SchemaSubjectState(
            subject="orders.events.v1-value",
            compatibility=CompatibilityLevel.BACKWARD,
            versions=(v1, v2),
            registry_mode="IMPORT",
            registry_host="127.0.0.1",
        )
    }


def _make_baseline_acls() -> List[AclBinding]:
    return [
        AclBinding(
            principal="User:svc-order-producer",
            resource_type=AclResourceType.TOPIC,
            resource_name="orders.events.v1",
            operation=AclOperation.WRITE,
            permission_type=AclPermissionType.ALLOW,
            pattern_type=AclPatternType.LITERAL,
            host="10.0.0.11",
        ),
        AclBinding(
            principal="User:svc-order-consumer",
            resource_type=AclResourceType.TOPIC,
            resource_name="orders.events.v1",
            operation=AclOperation.READ,
            permission_type=AclPermissionType.ALLOW,
            pattern_type=AclPatternType.LITERAL,
            host="10.0.0.12",
        ),
        AclBinding(
            principal="User:svc-order-consumer",
            resource_type=AclResourceType.GROUP,
            resource_name="cg-order-fulfillment",
            operation=AclOperation.READ,
            permission_type=AclPermissionType.ALLOW,
            pattern_type=AclPatternType.LITERAL,
            host="10.0.0.12",
        ),
        AclBinding(
            principal="User:untrusted-analytics",
            resource_type=AclResourceType.TOPIC,
            resource_name="orders.events.v1",
            operation=AclOperation.WRITE,
            permission_type=AclPermissionType.DENY,
            pattern_type=AclPatternType.LITERAL,
            host="10.0.0.99",
        ),
    ]


def _make_baseline_consumer_group(
    state: ConsumerGroupState = ConsumerGroupState.QUIESCED,
    active_hosts: Tuple[str, ...] = (),
    fenced: bool = True,
    p0_offset: int = 350,
    p1_offset: int = 600,
) -> ConsumerGroupSpec:
    return ConsumerGroupSpec(
        group_id="cg-order-fulfillment",
        state=state,
        offsets={
            ("orders.events.v1", 0): ConsumerPartitionOffset(
                topic_name="orders.events.v1",
                partition_id=0,
                committed_offset=p0_offset,
            ),
            ("orders.events.v1", 1): ConsumerPartitionOffset(
                topic_name="orders.events.v1",
                partition_id=1,
                committed_offset=p1_offset,
            ),
        },
        active_member_hosts=list(active_hosts),
        fenced=fenced,
    )


# -----------------------------------------------------------------------------
# 1. Topic Configs, Partition Identity, and Retained Ranges
# -----------------------------------------------------------------------------

def test_topic_and_retained_ranges_pass_on_identical_cluster_link() -> None:
    source, target = _make_baseline_topics()
    proof = verify_topic_and_retained_ranges(
        source=source,
        target=target,
        capability=OffsetCapability.OFFSET_PRESERVING,
    )
    assert proof.certified is True
    assert proof.badge == CareBadge.PASS
    assert proof.byte_equal is True
    assert proof.partition_identity_equal is True
    assert proof.config_compatible is True
    assert proof.retained_ranges_valid is True


def test_topic_proof_detects_partition_identity_and_config_regressions() -> None:
    source, _ = _make_baseline_topics()
    bad_target = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=3,
        cleanup_policy="compact",
        min_insync_replicas=1,
        max_message_bytes=512000,
        partitioner_strategy="round_robin",
        partitions=_make_partitions([(0, 100, 500), (1, 200, 800), (2, 0, 10)]),
        broker_host="10.0.0.10",
    )
    proof = verify_topic_and_retained_ranges(
        source=source,
        target=bad_target,
        capability=OffsetCapability.OFFSET_PRESERVING,
    )
    assert proof.certified is False
    assert proof.badge == CareBadge.ACTION
    assert proof.partition_identity_equal is False
    assert proof.config_compatible is False
    codes = {f.code for f in proof.findings}
    assert "PARTITION_COUNT_MISMATCH" in codes
    assert "PARTITIONER_STRATEGY_MISMATCH" in codes
    assert "CLEANUP_POLICY_MISMATCH" in codes
    assert "MIN_ISR_DOWNGRADE" in codes
    assert "MAX_MESSAGE_BYTES_REDUCED" in codes


def test_topic_proof_detects_truncated_target_retention_and_hwm_divergence() -> None:
    source, _ = _make_baseline_topics()
    truncated_target = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=2,
        partitions=(
            PartitionRetainedRange(
                partition_id=0,
                log_start_offset=150,  # Truncated ahead of source 100
                high_watermark=500,
                record_count=400,
                byte_count=400 * 256,
                payload_sha256="sha256-orders-p0",
            ),
            PartitionRetainedRange(
                partition_id=1,
                log_start_offset=200,
                high_watermark=790,  # Lagging behind source 800
                record_count=600,
                byte_count=600 * 256,
                payload_sha256="sha256-orders-p1",
            ),
        ),
        broker_host="10.0.0.10",
    )
    proof = verify_topic_and_retained_ranges(
        source=source,
        target=truncated_target,
        capability=OffsetCapability.OFFSET_PRESERVING,
    )
    assert proof.certified is False
    assert proof.retained_ranges_valid is False
    codes = {f.code for f in proof.findings}
    assert "TARGET_RETENTION_TRUNCATED" in codes
    assert "HIGH_WATERMARK_DIVERGENCE" in codes


# -----------------------------------------------------------------------------
# 2. Offset-Preserving vs Offset-Remapped vs Byte-Copy Capabilities
# -----------------------------------------------------------------------------

def test_offset_preserving_vs_remapped_capabilities_are_distinct() -> None:
    source, _ = _make_baseline_topics()
    remapped_target = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=2,
        partitions=_make_partitions([(0, 1000, 1400), (1, 5000, 5600)]),
        broker_host="10.0.0.10",
    )
    group = _make_baseline_consumer_group(p0_offset=350, p1_offset=600)

    remapped_link = ReplicationLinkSpec(
        link_id="mm2-orders-link",
        capability=OffsetCapability.OFFSET_REMAPPED,
        source_cluster_id="src-kafka-1",
        target_cluster_id="tgt-kafka-2",
        partition_mappings={
            ("orders.events.v1", 0): PartitionOffsetMapping(
                partition_id=0,
                base_source_offset=100,
                base_target_offset=1000,
            ),
            ("orders.events.v1", 1): PartitionOffsetMapping(
                partition_id=1,
                base_source_offset=200,
                base_target_offset=5000,
            ),
        },
    )

    with pytest.raises(CapabilityMismatchError):
        translate_and_validate_consumer_offsets(
            group=group,
            source_topic=source,
            target_topic=remapped_target,
            link=remapped_link,
            allow_remapped=False,
        )

    translated = translate_and_validate_consumer_offsets(
        group=group,
        source_topic=source,
        target_topic=remapped_target,
        link=remapped_link,
        allow_remapped=True,
    )
    assert len(translated) == 2
    assert translated[0].source_committed_offset == 350
    assert translated[0].target_committed_offset == 1250
    assert translated[0].capability_used == OffsetCapability.OFFSET_REMAPPED
    assert translated[0].within_retention is True
    assert translated[0].badge == CareBadge.BENIGN

    assert translated[1].source_committed_offset == 600
    assert translated[1].target_committed_offset == 5400
    assert translated[1].within_retention is True


def test_byte_copy_only_link_refuses_consumer_offset_migration() -> None:
    source, target = _make_baseline_topics()
    group = _make_baseline_consumer_group()
    raw_link = ReplicationLinkSpec(
        link_id="raw-byte-pipe",
        capability=OffsetCapability.BYTE_COPY_ONLY,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )
    with pytest.raises(CapabilityMismatchError, match="BYTE_COPY_ONLY"):
        translate_and_validate_consumer_offsets(
            group=group,
            source_topic=source,
            target_topic=target,
            link=raw_link,
        )


# -----------------------------------------------------------------------------
# 3. Refuse Offsets Outside Retention Bounds
# -----------------------------------------------------------------------------

def test_offsets_outside_target_retention_bounds_are_refused() -> None:
    source, _ = _make_baseline_topics()
    narrow_target = TopicConfigSpec(
        topic_name="orders.events.v1",
        num_partitions=2,
        partitions=_make_partitions([(0, 400, 500), (1, 200, 550)]),
        broker_host="10.0.0.10",
    )
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )

    # Partition 0 offset 350 is below target log_start_offset 400 (expired on target)
    group_below = _make_baseline_consumer_group(p0_offset=350, p1_offset=500)
    with pytest.raises(OffsetOutOfRetentionBoundsError, match="outside target retention bounds"):
        translate_and_validate_consumer_offsets(
            group=group_below,
            source_topic=source,
            target_topic=narrow_target,
            link=link,
            raise_on_out_of_bounds=True,
        )

    # Partition 1 offset 600 is above target high_watermark 550 (ahead of target)
    group_above = _make_baseline_consumer_group(p0_offset=450, p1_offset=600)
    with pytest.raises(OffsetOutOfRetentionBoundsError, match="outside target retention bounds"):
        translate_and_validate_consumer_offsets(
            group=group_above,
            source_topic=source,
            target_topic=narrow_target,
            link=link,
            raise_on_out_of_bounds=True,
        )

    # Non-raising audit mode marks within_retention=False and CareBadge.ACTION
    audit_res = translate_and_validate_consumer_offsets(
        group=group_below,
        source_topic=source,
        target_topic=narrow_target,
        link=link,
        raise_on_out_of_bounds=False,
    )
    assert audit_res[0].within_retention is False
    assert audit_res[0].badge == CareBadge.ACTION


# -----------------------------------------------------------------------------
# 4. Coordinate Promotion with Fences and Active Consumers & Restart Continuity
# -----------------------------------------------------------------------------

def test_promotion_coordinator_enforces_fences_lag_drain_and_active_consumers(
    tmp_path: Path,
) -> None:
    source, target = _make_baseline_topics()
    schemas = _make_baseline_schemas()
    acls = _make_baseline_acls()

    lagging_link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
        mirror_lag_ByPartition={("orders.events.v1", 0): 15, ("orders.events.v1", 1): 0},
    )
    active_group = _make_baseline_consumer_group(
        state=ConsumerGroupState.STABLE,
        active_hosts=("10.0.0.21",),
        fenced=False,
    )

    coord = StreamPromotionCoordinator(
        source_topic=source,
        target_topic=target,
        link=lagging_link,
        consumer_groups=[active_group],
        source_subjects=schemas,
        target_subjects=schemas,
        source_acls=acls,
        target_acls=acls,
        checkpoint_store=StreamMetadataCheckpointStore(tmp_path / "ckpt.json"),
    )

    # Cannot drain mirror lag before fencing source producers
    with pytest.raises(PromotionFenceError, match="before fencing source producers"):
        coord.drain_mirror_lag()

    coord.fence_source_producers()
    assert coord.phase == PromotionPhase.FENCE_SOURCE_PRODUCERS
    assert coord.source_topic.writable is False

    # Cannot finish drain while mirror lag > 0
    with pytest.raises(PromotionFenceError, match="un-replicated messages"):
        coord.drain_mirror_lag()

    # Simulate mirror catching up to 0 lag
    coord.link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
        mirror_lag_ByPartition={("orders.events.v1", 0): 0, ("orders.events.v1", 1): 0},
    )
    assert coord.drain_mirror_lag() == 0
    assert coord.phase == PromotionPhase.DRAIN_MIRROR_LAG

    # Refuse consumer cutover while active consumer members are still polling
    with pytest.raises(ActiveConsumerFenceError, match="still active or unfenced"):
        coord.verify_consumers_quiesced()

    # Quiesce and fence consumer group
    coord.quiesce_consumer_group("cg-order-fulfillment")
    coord.verify_consumers_quiesced()
    assert coord.phase == PromotionPhase.QUIESCE_ACTIVE_CONSUMERS

    staged = coord.translate_and_stage_offsets()
    assert coord.phase == PromotionPhase.TRANSLATE_AND_VERIFY_OFFSETS
    assert staged["cg-order-fulfillment"][0].target_committed_offset == 350
    assert staged["cg-order-fulfillment"][1].target_committed_offset == 600

    # Promote target topic to writable primary
    pre_proof = coord.promote_target_topic()
    assert pre_proof.certified is True
    assert coord.phase == PromotionPhase.PROMOTE_TARGET_TOPIC
    assert coord.target_topic.is_mirror_topic is False
    assert coord.target_topic.writable is True

    # Resume consumers and verify zero unaccounted replay/gap
    final_proof = coord.resume_consumers_and_verify()
    assert final_proof.certified is True
    assert final_proof.badge == CareBadge.PASS
    assert coord.phase == PromotionPhase.COMPLETED
    assert len(final_proof.restart_verifications) == 2
    assert all(rv.unaccounted_gap == 0 and rv.unaccounted_replay == 0 for rv in final_proof.restart_verifications)


def test_consumer_restart_detects_unaccounted_gap_and_replay() -> None:
    _, target = _make_baseline_topics()
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )

    # Unaccounted gap: expected 350, restarted at 355 (5 messages skipped!)
    with pytest.raises(ConsumerRestartDiscontinuityError, match="unaccounted_gap=5"):
        verify_consumer_restart(
            group_id="cg-order-fulfillment",
            topic_name="orders.events.v1",
            target_topic=target,
            link=link,
            expected_source_offsets={0: 350, 1: 600},
            actual_target_restart_offsets={0: 355, 1: 600},
            approved_max_replay=0,
            raise_on_discontinuity=True,
        )

    # Unaccounted replay: expected 350, restarted at 340 with approved_max_replay=0
    with pytest.raises(ConsumerRestartDiscontinuityError, match="unaccounted_replay=10"):
        verify_consumer_restart(
            group_id="cg-order-fulfillment",
            topic_name="orders.events.v1",
            target_topic=target,
            link=link,
            expected_source_offsets={0: 350, 1: 600},
            actual_target_restart_offsets={0: 340, 1: 600},
            approved_max_replay=0,
            raise_on_discontinuity=True,
        )

    # Bounded replay within approved window is certified with CareBadge.BENIGN
    restarts = verify_consumer_restart(
        group_id="cg-order-fulfillment",
        topic_name="orders.events.v1",
        target_topic=target,
        link=link,
        expected_source_offsets={0: 350, 1: 600},
        actual_target_restart_offsets={0: 345, 1: 600},
        approved_max_replay=10,
        raise_on_discontinuity=True,
    )
    assert restarts[0].certified is True
    assert restarts[0].unaccounted_replay == 0
    assert restarts[0].badge == CareBadge.BENIGN
    assert restarts[1].badge == CareBadge.PASS


# -----------------------------------------------------------------------------
# 5. Topic Byte Equality Cannot Hide Incompatible Schema or Auth State
# -----------------------------------------------------------------------------

def test_byte_equality_cannot_hide_incompatible_schema_state() -> None:
    source, target = _make_baseline_topics()
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )
    group = _make_baseline_consumer_group()
    source_schemas = _make_baseline_schemas()
    acls = _make_baseline_acls()

    # Target schema registry has a colliding global schema ID 102 with an incompatible field
    # and downgraded compatibility mode NONE, even though topic partition bytes are 100% equal!
    bad_v1 = source_schemas["orders.events.v1-value"].versions[0]
    bad_v2 = SchemaVersionSpec(
        subject="orders.events.v1-value",
        version=2,
        global_schema_id=102,
        schema_format=SchemaFormat.AVRO,
        canonical_text=(
            '{"type":"record","name":"Order","fields":['
            '{"name":"id","type":"string"},'
            '{"name":"mandatory_tax_id","type":"string"}]}'
        ),
        fields=(
            SchemaFieldSpec(name="id", field_type="string", required=True, has_default=False),
            SchemaFieldSpec(
                name="mandatory_tax_id",
                field_type="string",
                required=True,
                has_default=False,
            ),
        ),
    )
    target_schemas = {
        "orders.events.v1-value": SchemaSubjectState(
            subject="orders.events.v1-value",
            compatibility=CompatibilityLevel.BACKWARD,
            versions=(bad_v1, bad_v2),
            registry_host="10.0.0.10",
        )
    }

    proof = evaluate_stream_migration_proof(
        source_topic=source,
        target_topic=target,
        link=link,
        consumer_groups=[group],
        source_subjects=source_schemas,
        target_subjects=target_schemas,
        source_acls=acls,
        target_acls=acls,
    )
    assert proof.byte_equal is True
    assert proof.topic_proof.certified is True
    assert proof.schema_proof.certified is False
    assert proof.certified is False
    assert proof.badge == CareBadge.ACTION
    codes = {f.code for f in proof.findings}
    assert "SCHEMA_VERSION_DRIFT" in codes
    assert "INCOMPATIBLE_SCHEMA_EVOLUTION" in codes
    assert "WIRE_SCHEMA_ID_COLLISION" in codes


def test_byte_equality_cannot_hide_missing_wire_schema_id_or_unrewritten_remap() -> None:
    source_schemas = _make_baseline_schemas()
    # Target is missing global schema ID 102
    partial_target_schemas = {
        "orders.events.v1-value": SchemaSubjectState(
            subject="orders.events.v1-value",
            compatibility=CompatibilityLevel.BACKWARD,
            versions=(source_schemas["orders.events.v1-value"].versions[0],),
            registry_host="10.0.0.10",
        )
    }
    s_proof = verify_schema_registry_state(
        source_subjects=source_schemas,
        target_subjects=partial_target_schemas,
        embedded_wire_schema_ids=(101, 102),
    )
    assert s_proof.certified is False
    assert s_proof.badge == CareBadge.ACTION
    codes = {f.code for f in s_proof.findings}
    assert "MISSING_SCHEMA_VERSION" in codes
    assert "MISSING_WIRE_SCHEMA_ID" in codes

    # Unrewritten wire schema ID when target registry assigned a different ID (e.g. 102 -> 502)
    remapped_proof = verify_schema_registry_state(
        source_subjects=source_schemas,
        target_subjects=source_schemas,
        embedded_wire_schema_ids=(101, 102),
        schema_id_remap={101: 101, 102: 502},
    )
    assert remapped_proof.certified is False
    assert any(f.code == "UNREWRITTEN_WIRE_SCHEMA_ID" for f in remapped_proof.findings)


def test_byte_equality_cannot_hide_acl_missing_deny_or_privilege_widening() -> None:
    source, target = _make_baseline_topics()
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )
    group = _make_baseline_consumer_group()
    schemas = _make_baseline_schemas()
    source_acls = _make_baseline_acls()

    # Target drops the DENY rule on untrusted-analytics and adds an unapproved wildcard ALLOW
    bad_target_acls = [
        acl for acl in source_acls if acl.permission_type != AclPermissionType.DENY
    ] + [
        AclBinding(
            principal="User:anonymous",
            resource_type=AclResourceType.TOPIC,
            resource_name="orders.events.v1",
            operation=AclOperation.ALL,
            permission_type=AclPermissionType.ALLOW,
            pattern_type=AclPatternType.PREFIXED,
            host="*",
        )
    ]

    proof = evaluate_stream_migration_proof(
        source_topic=source,
        target_topic=target,
        link=link,
        consumer_groups=[group],
        source_subjects=schemas,
        target_subjects=schemas,
        source_acls=source_acls,
        target_acls=bad_target_acls,
    )
    assert proof.byte_equal is True
    assert proof.topic_proof.certified is True
    assert proof.schema_proof.certified is True
    assert proof.acl_proof.certified is False
    assert proof.certified is False
    assert proof.badge == CareBadge.ACTION
    assert proof.acl_proof.missing_deny_count == 1
    assert proof.acl_proof.unapproved_widening_count == 2
    codes = {f.code for f in proof.findings}
    assert "MISSING_DENY_ACL" in codes
    assert "UNAPPROVED_PRIVILEGE_WIDENING" in codes


def test_promotion_coordinator_refuses_promotion_on_acl_or_schema_failure() -> None:
    source, target = _make_baseline_topics()
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )
    group = _make_baseline_consumer_group()
    schemas = _make_baseline_schemas()
    source_acls = _make_baseline_acls()

    # Missing consumer group READ ACL on target
    incomplete_target_acls = [source_acls[0]]

    coord = StreamPromotionCoordinator(
        source_topic=source,
        target_topic=target,
        link=link,
        consumer_groups=[group],
        source_subjects=schemas,
        target_subjects=schemas,
        source_acls=source_acls,
        target_acls=incomplete_target_acls,
    )
    coord.fence_source_producers()
    coord.drain_mirror_lag()
    coord.verify_consumers_quiesced()
    coord.translate_and_stage_offsets()

    with pytest.raises(AclPolicyViolationError, match="ACL policy proof failed"):
        coord.promote_target_topic()
    assert coord.phase == PromotionPhase.FAILED
    assert coord.target_promoted_writable is False


# -----------------------------------------------------------------------------
# 6. Lease Lock, Durable O(1) Checkpoint Resume, Host Safety, Pure ASCII
# -----------------------------------------------------------------------------

def test_lease_lock_mutual_exclusion_and_checkpoint_o1_resume(tmp_path: Path) -> None:
    lock_a = StreamMigrationLeaseLock(tmp_path, lease_ttl_sec=60.0)
    lock_b = StreamMigrationLeaseLock(tmp_path, lease_ttl_sec=60.0)

    with lock_a:
        lock_a.heartbeat()
        with pytest.raises(LeaseLockError, match="active lease lock already held"):
            lock_b.acquire()

    # After lock_a exits context, lock_b can acquire cleanly
    with lock_b:
        assert (tmp_path / ".run.lock").exists()

    ckpt_file = tmp_path / "stream_ckpt.json"
    store1 = StreamMetadataCheckpointStore(ckpt_file)
    key = store1.record_completed(
        "consumer_offset",
        ("cg-order-fulfillment", "orders.events.v1", 0),
        {"source_committed_offset": 350, "target_committed_offset": 1250},
    )
    assert key == "consumer_offset:cg-order-fulfillment:orders.events.v1:0"

    store2 = StreamMetadataCheckpointStore(ckpt_file)
    assert store2.is_completed("consumer_offset", "cg-order-fulfillment", "orders.events.v1", 0)
    saved = store2.get_cursor("consumer_offset", "cg-order-fulfillment", "orders.events.v1", 0)
    assert saved is not None
    assert saved["state"]["target_committed_offset"] == 1250


def test_safe_test_hosts_and_pure_ascii_report() -> None:
    assert validate_test_host("127.0.0.1") == "127.0.0.1"
    assert validate_test_host("10.0.0.42") == "10.0.0.42"
    assert validate_test_host("localhost") == "localhost"

    with pytest.raises(UnsafeTestHostError):
        validate_test_host("198.51.100.10")

    source, target = _make_baseline_topics()
    link = ReplicationLinkSpec(
        link_id="cl-orders",
        capability=OffsetCapability.OFFSET_PRESERVING,
        source_cluster_id="src-1",
        target_cluster_id="tgt-1",
    )
    schemas = _make_baseline_schemas()
    acls = _make_baseline_acls()
    group = _make_baseline_consumer_group()

    proof = evaluate_stream_migration_proof(
        source_topic=source,
        target_topic=target,
        link=link,
        consumer_groups=[group],
        source_subjects=schemas,
        target_subjects=schemas,
        source_acls=acls,
        target_acls=acls,
        restart_offsets_by_group={"cg-order-fulfillment": {0: 350, 1: 600}},
    )
    report = proof.format_report()
    assert report.isascii()
    assert "[PASS] STREAM_MIGRATION_PROOF" in report

    # Verify both implementation and test files are 100% pure ASCII
    mod_path = Path(__file__).resolve().parent.parent / "migkit" / "stream_metadata.py"
    test_path = Path(__file__).resolve()
    assert mod_path.read_text(encoding="utf-8").isascii()
    assert test_path.read_text(encoding="utf-8").isascii()
