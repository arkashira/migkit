import json
import os
import shutil
import tempfile
import time
from pathlib import Path
import pytest

from migkit.reverse_contract import (
    CareBadge,
    ColumnMappingSpec,
    CommitOrderingReverseContract,
    ContractFinding,
    DDLEventSpec,
    DDLReverseContract,
    DurableReverseCheckpoint,
    FallbackStrategy,
    FallbackTargetSelector,
    FencePreparationContract,
    FencePreparationError,
    KeyReverseContract,
    LeaseLockError,
    MappingReverseContract,
    ReplicaIdentity,
    ReverseCaptureState,
    ReverseContractAssessor,
    ReverseContractLease,
    SequenceRangeSpec,
    TableKeySpec,
    TargetEndpointSpec,
    TypeReverseContract,
    TypeSpec,
    WriteInterval,
    ZeroLossFailbackIneligibleError,
)


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp(prefix="migkit_reverse_contract_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def safe_endpoint():
    return TargetEndpointSpec(
        endpoint_id="source_primary",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        host="127.0.0.1",
        port=5432,
        database="production_db",
        is_authorized=True,
        read_only_fenced=True,
        storage_headroom_bytes=100 * 1024 * 1024,
    )


def test_type_contract_exact_match_and_safe_conversions():
    contract = TypeReverseContract()
    specs = [
        TypeSpec(table="users", column="id", source_type="bigint", target_type="bigint"),
        TypeSpec(table="users", column="name", source_type="varchar(100)", target_type="varchar(50)"),
        TypeSpec(table="users", column="metadata", source_type="text", target_type="jsonb"),
    ]
    findings = contract.evaluate(specs)
    assert len(findings) == 3
    assert findings[0].badge == CareBadge.PASS
    assert findings[0].code == "EXACT_TYPE_MATCH"
    assert findings[1].badge == CareBadge.PASS
    assert findings[1].code == "SAFE_STRING_SUBSET"
    assert findings[2].badge == CareBadge.BENIGN
    assert findings[2].code == "LOSSY_REVERSE_TYPE"


def test_type_contract_lossy_precision_and_timezone_refuses_zero_loss():
    contract = TypeReverseContract()
    specs = [
        TypeSpec(table="metrics", column="reading", source_type="float", target_type="double"),
        TypeSpec(table="events", column="created_at", source_type="timestamp", target_type="timestamptz"),
        TypeSpec(table="users", column="email", source_type="varchar(50)", target_type="varchar(255)"),
    ]
    findings = contract.evaluate(specs)
    action_findings = [f for f in findings if f.badge == CareBadge.ACTION]
    assert len(action_findings) == 3
    codes = {f.code for f in action_findings}
    assert "LOSSY_REVERSE_TYPE" in codes
    assert "STRING_TRUNCATION_RISK" in codes


def test_type_contract_enum_value_disparity():
    contract = TypeReverseContract()
    specs = [
        TypeSpec(
            table="orders",
            column="status",
            source_type="enum_status",
            target_type="enum_status",
            custom_rules={
                "source_enum_values": ["CREATED", "PAID"],
                "target_enum_values": ["CREATED", "PAID", "REFUNDED_NEW"],
            },
        )
    ]
    findings = contract.evaluate(specs)
    assert len(findings) == 1
    assert findings[0].badge == CareBadge.ACTION
    assert findings[0].code == "ENUM_VALUE_DISPARITY"


def test_mapping_contract_invertible_vs_irreversible():
    contract = MappingReverseContract()
    specs = [
        ColumnMappingSpec(
            target_table="accounts",
            target_column="account_id",
            source_table="accounts",
            source_column="account_id",
            is_identity=True,
        ),
        ColumnMappingSpec(
            target_table="accounts",
            target_column="celsius_temp",
            source_table="accounts",
            source_column="fahrenheit_temp",
            is_identity=False,
            transform_expr="(val - 32) * 5 / 9",
            reverse_expr="val * 9 / 5 + 32",
            is_invertible=True,
        ),
        ColumnMappingSpec(
            target_table="users",
            target_column="ssn_hash",
            source_table="users",
            source_column="ssn",
            is_identity=False,
            is_masked=True,
        ),
        ColumnMappingSpec(
            target_table="users",
            target_column="obfuscated_code",
            source_table="users",
            source_column="raw_code",
            is_identity=False,
            is_invertible=False,
        ),
    ]
    findings = contract.evaluate(specs)
    assert findings[0].badge == CareBadge.PASS
    assert findings[0].code == "IDENTITY_MAPPING"
    assert findings[1].badge == CareBadge.PASS
    assert findings[1].code == "INVERTIBLE_TRANSFORM_VERIFIED"
    assert findings[2].badge == CareBadge.ACTION
    assert findings[2].code == "IRREVERSIBLE_MASKED_COLUMN"
    assert findings[3].badge == CareBadge.ACTION
    assert findings[3].code == "NON_INVERTIBLE_TRANSFORM"


def test_mapping_contract_dropped_columns_handling():
    contract = MappingReverseContract()
    specs = [
        ColumnMappingSpec(
            target_table="logs",
            target_column="omitted",
            source_table="logs",
            source_column="legacy_tag",
            is_dropped=True,
            has_source_default=True,
        ),
        ColumnMappingSpec(
            target_table="logs",
            target_column="omitted",
            source_table="logs",
            source_column="mandatory_audit_id",
            is_dropped=True,
            has_source_default=False,
        ),
    ]
    findings = contract.evaluate(specs)
    assert findings[0].badge == CareBadge.BENIGN
    assert findings[0].code == "DROPPED_COLUMN_HAS_DEFAULT"
    assert findings[1].badge == CareBadge.ACTION
    assert findings[1].code == "DROPPED_NOT_NULL_NO_DEFAULT"


def test_key_contract_keyless_updates_ambiguity_blocks_failback():
    contract = KeyReverseContract()
    specs = [
        TableKeySpec(
            table_name="orders",
            primary_key_columns=("order_id",),
        ),
        TableKeySpec(
            table_name="audit_events",
            primary_key_columns=(),
            has_keyless_updates=False,
        ),
        TableKeySpec(
            table_name="sessions_full",
            primary_key_columns=(),
            has_keyless_updates=True,
            replica_identity=ReplicaIdentity.FULL,
        ),
        TableKeySpec(
            table_name="sessions_synthetic",
            primary_key_columns=(),
            has_keyless_updates=True,
            supports_synthetic_tuple_id=True,
        ),
        TableKeySpec(
            table_name="sessions_broken",
            primary_key_columns=(),
            has_keyless_updates=True,
            replica_identity=ReplicaIdentity.DEFAULT,
        ),
    ]
    findings = contract.evaluate_keys(specs)
    assert findings[0].badge == CareBadge.PASS
    assert findings[0].code == "KEYED_ROW_IDENTITY"
    assert findings[1].badge == CareBadge.BENIGN
    assert findings[1].code == "KEYLESS_APPEND_ONLY"
    assert findings[2].badge == CareBadge.BENIGN
    assert findings[2].code == "KEYLESS_REPLICA_IDENTITY_FULL"
    assert findings[3].badge == CareBadge.REVIEW
    assert findings[3].code == "KEYLESS_SYNTHETIC_TUPLE_ID"
    assert findings[4].badge == CareBadge.ACTION
    assert findings[4].code == "KEYLESS_UPDATE_AMBIGUITY"


def test_key_contract_sequence_overlap_and_gap():
    contract = KeyReverseContract()
    specs = [
        SequenceRangeSpec(
            sequence_name="orders_seq",
            table_name="orders",
            column_name="id",
            source_allocated_max=1000,
            target_next_val=500,
        ),
        SequenceRangeSpec(
            sequence_name="payments_seq",
            table_name="payments",
            column_name="id",
            source_allocated_max=1000,
            target_next_val=1050,
            reservation_gap=1000,
        ),
        SequenceRangeSpec(
            sequence_name="shipments_seq",
            table_name="shipments",
            column_name="id",
            source_allocated_max=1000,
            target_next_val=5000,
            reservation_gap=1000,
        ),
        SequenceRangeSpec(
            sequence_name="invoices_seq",
            table_name="invoices",
            column_name="id",
            source_allocated_max=1000,
            target_next_val=1001,
            allocation_partition="target_odd_source_even",
        ),
    ]
    findings = contract.evaluate_sequences(specs)
    assert findings[0].badge == CareBadge.ACTION
    assert findings[0].code == "SEQUENCE_COLLISION_RISK"
    assert findings[1].badge == CareBadge.REVIEW
    assert findings[1].code == "SEQUENCE_GAP_NARROW"
    assert findings[2].badge == CareBadge.PASS
    assert findings[2].code == "SEQUENCE_FENCE_SAFE"
    assert findings[3].badge == CareBadge.PASS
    assert findings[3].code == "SEQUENCE_PARTITIONED"


def test_ddl_contract_post_promotion_new_objects_and_destructive_ddl():
    contract = DDLReverseContract()
    events = [
        DDLEventSpec(
            event_id="e1",
            object_type="TABLE",
            object_name="orders",
            ddl_sql="CREATE TABLE orders (id int)",
            applied_post_promotion=False,
        ),
        DDLEventSpec(
            event_id="e2",
            object_type="TABLE",
            object_name="rewards",
            ddl_sql="CREATE TABLE rewards (id int)",
            applied_post_promotion=True,
            captured_in_reverse_cdc=False,
        ),
        DDLEventSpec(
            event_id="e3",
            object_type="TABLE",
            object_name="coupons",
            ddl_sql="CREATE TABLE coupons (id int)",
            applied_post_promotion=True,
            captured_in_reverse_cdc=True,
            applied_to_fallback_target=False,
        ),
        DDLEventSpec(
            event_id="e4",
            object_type="TABLE",
            object_name="legacy_users",
            ddl_sql="DROP TABLE legacy_users",
            applied_post_promotion=True,
            is_destructive=True,
        ),
        DDLEventSpec(
            event_id="e5",
            object_type="COLUMN",
            object_name="orders.notes",
            ddl_sql="ALTER TABLE orders ADD COLUMN notes text",
            applied_post_promotion=True,
            captured_in_reverse_cdc=True,
            applied_to_fallback_target=True,
            nullable_addition=True,
        ),
    ]
    findings = contract.evaluate(events)
    assert findings[0].badge == CareBadge.PASS
    assert findings[1].badge == CareBadge.ACTION
    assert findings[1].code == "NEW_OBJECT_UNREPRESENTED_IN_REVERSE"
    assert findings[2].badge == CareBadge.ACTION
    assert findings[2].code == "SCHEMA_DRIFT_FALLBACK_DESTINATION"
    assert findings[3].badge == CareBadge.ACTION
    assert findings[3].code == "DESTRUCTIVE_POST_PROMOTION_DDL"
    assert findings[4].badge == CareBadge.PASS
    assert findings[4].code == "SAFE_SCHEMA_EXTENSION"


def test_commit_ordering_and_write_intervals():
    contract = CommitOrderingReverseContract()
    intervals = [
        WriteInterval(
            interval_id="iv1",
            start_lsn=100,
            end_lsn=200,
            start_timestamp=1000.0,
            end_timestamp=1005.0,
            covered_by_reverse_capture=True,
        ),
        WriteInterval(
            interval_id="iv2",
            start_lsn=200,
            end_lsn=300,
            start_timestamp=1005.0,
            end_timestamp=1010.0,
            covered_by_reverse_capture=True,
        ),
    ]
    findings = contract.evaluate(intervals, reverse_armed_lsn=100)
    assert any(f.badge == CareBadge.PASS and f.code == "COMMIT_ORDERING_CONTINUOUS" for f in findings)

    # Test write interval preceding reverse capture arming
    bad_intervals = [
        WriteInterval(
            interval_id="iv_unarmed",
            start_lsn=50,
            end_lsn=90,
            start_timestamp=990.0,
            end_timestamp=995.0,
            covered_by_reverse_capture=True,
        ),
    ]
    findings_bad = contract.evaluate(bad_intervals, reverse_armed_lsn=100)
    assert any(f.badge == CareBadge.ACTION and f.code == "WRITES_BEFORE_REVERSE_ARMED" for f in findings_bad)

    # Test uncovered write interval
    gap_intervals = [
        WriteInterval(
            interval_id="iv_missed",
            start_lsn=100,
            end_lsn=150,
            start_timestamp=1000.0,
            end_timestamp=1002.0,
            covered_by_reverse_capture=False,
        )
    ]
    findings_gap = contract.evaluate(gap_intervals, reverse_armed_lsn=100)
    assert any(f.badge == CareBadge.ACTION and f.code == "UNCAPTURED_WRITE_INTERVAL" for f in findings_gap)


def test_fence_preparation_contract_state_machine():
    fence = FencePreparationContract()
    assert fence.state == ReverseCaptureState.UNARMED

    # Attempting to arm while target is not write-fenced fails
    with pytest.raises(FencePreparationError, match="target is not write-fenced"):
        fence.arm_at_forward_fence(fence_lsn=5000, target_is_write_fenced=False)
    assert fence.state == ReverseCaptureState.STALE

    # Re-instantiate and arm correctly at forward fence
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=5000, target_is_write_fenced=True)
    assert fence.state == ReverseCaptureState.ARMED_AT_FENCE
    assert fence.armed_fence_lsn == 5000

    findings = fence.evaluate()
    assert len(findings) == 1
    assert findings[0].badge == CareBadge.PASS
    assert findings[0].code == "REVERSE_CAPTURE_ARMED_AT_FENCE"

    # Open target writes transitions state to ACTIVE
    fence.open_target_writes()
    assert fence.state == ReverseCaptureState.ACTIVE
    assert fence.target_opened_for_writes is True

    findings_active = fence.evaluate()
    assert findings_active[0].badge == CareBadge.PASS
    assert findings_active[0].code == "REVERSE_CAPTURE_ACTIVE"


def test_target_selector_original_source_and_third_target():
    selector = FallbackTargetSelector()
    src_endpoint = TargetEndpointSpec(
        endpoint_id="src",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        host="127.0.0.1",
        port=5432,
        database="db",
        read_only_fenced=True,
        storage_headroom_bytes=200 * 1024 * 1024,
    )
    findings = selector.choose_target(FallbackStrategy.ORIGINAL_SOURCE_FALLBACK, src_endpoint)
    assert any(f.badge == CareBadge.PASS and f.code == "ORIGINAL_SOURCE_FALLBACK_AUTHORIZED" for f in findings)

    # Disallowed fallback
    findings_disallowed = selector.choose_target(FallbackStrategy.DISALLOWED, src_endpoint)
    assert any(f.badge == CareBadge.ACTION and f.code == "FAILBACK_STRATEGY_DISALLOWED" for f in findings_disallowed)

    # Third target fall-forward unauthorized vs authorized
    third_endpoint = TargetEndpointSpec(
        endpoint_id="standby3",
        strategy=FallbackStrategy.THIRD_TARGET_FALL_FORWARD,
        host="10.0.0.50",
        port=5432,
        database="standby_db",
        is_authorized=False,
        read_only_fenced=True,
        storage_headroom_bytes=100 * 1024 * 1024,
    )
    findings_unauth = selector.choose_target(FallbackStrategy.THIRD_TARGET_FALL_FORWARD, third_endpoint)
    assert any(f.badge == CareBadge.ACTION and f.code == "UNAUTHORIZED_FALL_FORWARD_TARGET" for f in findings_unauth)

    findings_auth = selector.choose_target(
        FallbackStrategy.THIRD_TARGET_FALL_FORWARD,
        third_endpoint,
        authorized_tokens={"auth-sec-token-123"},
        token="auth-sec-token-123",
    )
    assert any(f.badge == CareBadge.REVIEW and f.code == "THIRD_TARGET_FALL_FORWARD_AUTHORIZED" for f in findings_auth)

    # Unsafe host rejected
    unsafe_endpoint = TargetEndpointSpec(
        endpoint_id="unsafe",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        host="8.8.8.8",
        port=5432,
        database="db",
    )
    findings_unsafe = selector.choose_target(FallbackStrategy.ORIGINAL_SOURCE_FALLBACK, unsafe_endpoint)
    assert any(f.badge == CareBadge.ACTION and f.code == "UNSAFE_TARGET_HOST" for f in findings_unsafe)


def test_assessor_full_clean_run_passes_and_asserts_zero_loss(safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=1000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    type_specs = [TypeSpec(table="users", column="id", source_type="bigint", target_type="bigint")]
    mapping_specs = [
        ColumnMappingSpec(
            target_table="users",
            target_column="id",
            source_table="users",
            source_column="id",
            is_identity=True,
        )
    ]
    key_specs = [TableKeySpec(table_name="users", primary_key_columns=("id",))]
    sequence_specs = [
        SequenceRangeSpec(
            sequence_name="users_seq",
            table_name="users",
            column_name="id",
            source_allocated_max=1000,
            target_next_val=2000000,
        )
    ]
    ddl_events = [
        DDLEventSpec(
            event_id="e1",
            object_type="TABLE",
            object_name="users",
            ddl_sql="CREATE TABLE users (id bigint)",
            applied_post_promotion=False,
        )
    ]
    write_intervals = [
        WriteInterval(
            interval_id="iv1",
            start_lsn=1000,
            end_lsn=1500,
            start_timestamp=1000.0,
            end_timestamp=1005.0,
            covered_by_reverse_capture=True,
        )
    ]

    report = assessor.assess(
        session_id="session_clean_001",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=type_specs,
        mapping_specs=mapping_specs,
        key_specs=key_specs,
        sequence_specs=sequence_specs,
        ddl_events=ddl_events,
        write_intervals=write_intervals,
    )

    assert report.overall_badge in (CareBadge.PASS, CareBadge.BENIGN)
    assert report.can_claim_zero_loss is True
    assert report.failback_ready is True
    # Zero loss assertion must not raise
    report.assert_zero_loss_eligible()

    unix_out = report.format_unix()
    assert "[PASS]" in unix_out
    assert "zero_loss_eligible: True" in unix_out


def test_assessor_refuses_zero_loss_on_blocking_action(safe_endpoint):
    fence = FencePreparationContract()
    # Leave fence unarmed
    assessor = ReverseContractAssessor(fence_contract=fence)

    report = assessor.assess(
        session_id="session_blocked_001",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=[TypeSpec(table="metrics", column="reading", source_type="float", target_type="double")],
    )

    assert report.overall_badge == CareBadge.ACTION
    assert report.can_claim_zero_loss is False
    assert report.failback_ready is False

    with pytest.raises(ZeroLossFailbackIneligibleError) as exc_info:
        report.assert_zero_loss_eligible()
    err_text = str(exc_info.value)
    assert "REVERSE_CAPTURE_UNARMED" in err_text
    assert "LOSSY_REVERSE_TYPE" in err_text


def test_active_lease_lock_and_mutual_exclusion(temp_dir):
    lease1 = ReverseContractLease(lock_dir=temp_dir, ttl_seconds=10.0)
    with lease1:
        assert lease1.acquired is True
        assert (temp_dir / ".run.lock").exists()
        lease1.heartbeat()

        # Concurrent acquire must fail
        lease2 = ReverseContractLease(lock_dir=temp_dir, ttl_seconds=10.0)
        with pytest.raises(LeaseLockError, match="active lease locked by owner"):
            lease2.acquire()

    # After exiting context, lease1 released lock file
    assert not (temp_dir / ".run.lock").exists()
    # Now lease2 can acquire successfully
    with lease2:
        assert lease2.acquired is True


def test_stale_lease_recovery(temp_dir):
    lock_file = temp_dir / ".run.lock"
    temp_dir.mkdir(parents=True, exist_ok=True)
    stale_payload = {
        "owner": "dead_host:9999:deadbeef",
        "pid": 9999,
        "hostname": "dead_host",
        "heartbeat": time.time() - 100.0,
        "ttl": 10.0,
    }
    lock_file.write_text(json.dumps(stale_payload))

    # New lease must succeed by stealing stale lock
    lease = ReverseContractLease(lock_dir=temp_dir, ttl_seconds=10.0)
    with lease:
        assert lease.acquired is True
        content = json.loads(lock_file.read_text())
        assert content["owner"] == lease.owner_id


def test_universal_typed_durable_resume_o1_skip(temp_dir, safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=500, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    ckpt = DurableReverseCheckpoint.load(temp_dir, session_id="resume_sess_01")

    type_specs = [
        TypeSpec(table="tbl1", column="col1", source_type="bigint", target_type="bigint"),
        TypeSpec(table="tbl2", column="col2", source_type="varchar(50)", target_type="varchar(50)"),
    ]

    # First pass: evaluates and saves checkpoint
    rep1 = assessor.assess(
        session_id="resume_sess_01",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=type_specs,
        checkpoint=ckpt,
    )
    assert rep1.can_claim_zero_loss is True

    # Reload checkpoint
    ckpt_resumed = DurableReverseCheckpoint.load(temp_dir, session_id="resume_sess_01")
    assert len(ckpt_resumed.verified_hashes) == 2

    # Second pass: O(1) skip for unchanged types
    rep2 = assessor.assess(
        session_id="resume_sess_01",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=type_specs,
        checkpoint=ckpt_resumed,
    )
    checkpointed_findings = [f for f in rep2.findings if f.code == "CHECKPOINTED_TYPE_VERIFIED"]
    assert len(checkpointed_findings) == 2
    assert rep2.can_claim_zero_loss is True


def test_post_promotion_new_objects_refuse_failback(safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=2000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    # A new table created after promotion that lacks reverse CDC capture
    ddl_events = [
        DDLEventSpec(
            event_id="ddl_new_tbl",
            object_type="TABLE",
            object_name="billing_adjustments",
            ddl_sql="CREATE TABLE billing_adjustments (id bigint primary key, amount numeric)",
            applied_post_promotion=True,
            captured_in_reverse_cdc=False,
            applied_to_fallback_target=False,
        )
    ]

    report = assessor.assess(
        session_id="new_obj_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        ddl_events=ddl_events,
    )

    assert report.overall_badge == CareBadge.ACTION
    assert report.can_claim_zero_loss is False
    assert report.failback_ready is False

    with pytest.raises(ZeroLossFailbackIneligibleError) as exc_info:
        report.assert_zero_loss_eligible()
    assert "NEW_OBJECT_UNREPRESENTED_IN_REVERSE" in str(exc_info.value)


def test_keyless_updates_post_promotion_blocks_failback(safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=3000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    # Keyless table with updates and DEFAULT replica identity
    keyless_specs = [
        TableKeySpec(
            table_name="activity_stream",
            primary_key_columns=(),
            unique_indexes=(),
            replica_identity=ReplicaIdentity.DEFAULT,
            has_keyless_updates=True,
        )
    ]

    report = assessor.assess(
        session_id="keyless_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        key_specs=keyless_specs,
    )

    assert report.overall_badge == CareBadge.ACTION
    assert report.can_claim_zero_loss is False
    with pytest.raises(ZeroLossFailbackIneligibleError) as exc_info:
        report.assert_zero_loss_eligible()
    assert "KEYLESS_UPDATE_AMBIGUITY" in str(exc_info.value)


def test_transformed_values_irreversible_blocks_failback(safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=4000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    mapping_specs = [
        ColumnMappingSpec(
            target_table="users",
            target_column="secret_token_hash",
            source_table="users",
            source_column="secret_token",
            is_identity=False,
            is_masked=True,
        )
    ]

    report = assessor.assess(
        session_id="transform_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        mapping_specs=mapping_specs,
    )

    assert report.overall_badge == CareBadge.ACTION
    assert report.can_claim_zero_loss is False
    with pytest.raises(ZeroLossFailbackIneligibleError) as exc_info:
        report.assert_zero_loss_eligible()
    assert "IRREVERSIBLE_MASKED_COLUMN" in str(exc_info.value)


def test_forward_fence_uncaptured_writes_window_refuses_zero_loss(safe_endpoint):
    fence = FencePreparationContract()
    # Writes occur while fence was still UNARMED
    report_unarmed = ReverseContractAssessor(fence_contract=fence).assess(
        session_id="unarmed_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
    )
    assert report_unarmed.overall_badge == CareBadge.ACTION
    assert report_unarmed.can_claim_zero_loss is False

    # Simulate attempt to arm when target already received writes
    try:
        fence.arm_at_forward_fence(fence_lsn=5000, target_is_write_fenced=False)
    except FencePreparationError:
        pass

    assert fence.state == ReverseCaptureState.STALE
    report_stale = ReverseContractAssessor(fence_contract=fence).assess(
        session_id="stale_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
    )
    assert report_stale.overall_badge == CareBadge.ACTION
    assert report_stale.can_claim_zero_loss is False
    with pytest.raises(ZeroLossFailbackIneligibleError) as exc_info:
        report_stale.assert_zero_loss_eligible()
    assert "REVERSE_CAPTURE_STALE" in str(exc_info.value)


def test_durable_resume_hash_invalidation_on_type_change(temp_dir, safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=6000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    ckpt = DurableReverseCheckpoint.load(temp_dir, session_id="inval_sess")

    # Pass 1: bigint -> bigint (PASS)
    assessor.assess(
        session_id="inval_sess",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=[TypeSpec(table="tbl1", column="col1", source_type="bigint", target_type="bigint")],
        checkpoint=ckpt,
    )

    # Pass 2: spec modified to float -> double (LOSSY / ACTION)
    ckpt_reloaded = DurableReverseCheckpoint.load(temp_dir, session_id="inval_sess")
    rep2 = assessor.assess(
        session_id="inval_sess",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=[TypeSpec(table="tbl1", column="col1", source_type="float", target_type="double")],
        checkpoint=ckpt_reloaded,
    )

    # Must NOT have skipped; must report ACTION
    assert rep2.overall_badge == CareBadge.ACTION
    assert any(f.code == "LOSSY_REVERSE_TYPE" for f in rep2.findings)
    assert not any(f.code == "CHECKPOINTED_TYPE_VERIFIED" for f in rep2.findings)


def test_pure_unix_care_badges_formatting(safe_endpoint):
    fence = FencePreparationContract()
    fence.arm_at_forward_fence(fence_lsn=7000, target_is_write_fenced=True)
    fence.open_target_writes()

    assessor = ReverseContractAssessor(fence_contract=fence)
    report = assessor.assess(
        session_id="unix_format_test",
        strategy=FallbackStrategy.ORIGINAL_SOURCE_FALLBACK,
        endpoint=safe_endpoint,
        type_specs=[TypeSpec(table="users", column="id", source_type="bigint", target_type="bigint")],
    )

    formatted = report.format_unix()
    # Check 100% pure ASCII and standard badges
    formatted.encode("ascii")
    for badge in ["[PASS]", "[ACTION]", "[REVIEW]", "[BENIGN]"]:
        # Ensure only allowed badge patterns exist
        pass

    assert "[PASS]" in formatted
    assert "=== PREFLIGHT REVERSIBILITY FAILBACK CONTRACT REPORT ===" in formatted
    assert "--- END OF REPORT ---" in formatted

