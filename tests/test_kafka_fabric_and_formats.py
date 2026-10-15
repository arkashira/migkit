import sqlite3
import struct
import pytest

from migkit.kafka_fabric import (
    AlignmentPlan,
    ClusterLinkController,
    ConfigReconciliationPlan,
    LinkState,
    OffsetAlignmentError,
    OutboxOffsetRecord,
    PartitionOffsetAligner,
    QuotaExceededError,
    RegistryFlavor,
    RegistryWireFormat,
    SchemaFormatValidator,
    SchemaRegistryClient,
    SchemaValidationError,
    ShadowTopicStatus,
    TopicConfigReconciler,
    TopicQuota,
    TopicQuotaEnforcer,
    TransactionalOutbox,
)


# ============================================================================
# Item 276 (LT.C2): Gap-Free Partition Offset Alignment Tests
# ============================================================================

def test_offset_aligner_already_aligned():
    aligner = PartitionOffsetAligner()
    plan = aligner.plan_alignment("orders", 0, current_target_offset=50, desired_start_offset=50)
    assert plan.is_aligned
    assert plan.padding_needed == 0

    result = aligner.align_partition(lambda *args: 0, "orders", 0, 50, 50)
    assert result["aligned"]
    assert result["padded_count"] == 0


def test_offset_aligner_padding_and_tombstones():
    aligner = PartitionOffsetAligner(pad_limit=1000)
    plan = aligner.plan_alignment("orders", 0, current_target_offset=10, desired_start_offset=15, use_tombstones=True)
    assert not plan.is_aligned
    assert plan.padding_needed == 5

    records = aligner.generate_padding_records(plan)
    assert len(records) == 5
    assert records[0].offset == 10
    assert records[0].is_tombstone
    assert records[0].value is None
    assert records[-1].offset == 14

    mock_log = []

    def mock_producer(topic, partition, key, value, headers):
        next_offset = len(mock_log) + 10
        mock_log.append((topic, partition, key, value, headers, next_offset))
        return next_offset

    res = aligner.align_partition(mock_producer, "orders", 0, 10, 15, use_tombstones=True)
    assert res["aligned"]
    assert res["padded_count"] == 5
    assert res["end_offset"] == 15
    assert len(mock_log) == 5


def test_offset_aligner_exceeds_pad_limit():
    aligner = PartitionOffsetAligner(pad_limit=100)
    with pytest.raises(OffsetAlignmentError, match="exceeds pad limit"):
        aligner.plan_alignment("orders", 0, current_target_offset=0, desired_start_offset=200)


def test_offset_aligner_target_ahead_rejected():
    aligner = PartitionOffsetAligner()
    with pytest.raises(OffsetAlignmentError, match="already ahead"):
        aligner.plan_alignment("orders", 0, current_target_offset=100, desired_start_offset=50)


def test_verify_gap_free_success_and_failure():
    aligner = PartitionOffsetAligner()
    consecutive = [{"offset": 10}, {"offset": 11}, {"offset": 12}, {"offset": 13}]
    assert aligner.verify_gap_free(consecutive, expected_start=10)

    with pytest.raises(OffsetAlignmentError, match="Expected start offset"):
        aligner.verify_gap_free(consecutive, expected_start=5)

    gapped = [{"offset": 10}, {"offset": 11}, {"offset": 13}]
    with pytest.raises(OffsetAlignmentError, match="Offset gap detected"):
        aligner.verify_gap_free(gapped)


# ============================================================================
# Item 277 (LT.C3): Transactional Outbox Offset Map Tests
# ============================================================================

def test_transactional_outbox_commit_and_rollback():
    conn = sqlite3.connect(":memory:")
    TransactionalOutbox.init_schema(conn)

    cur = conn.cursor()
    cur.execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);")
    conn.commit()

    def successful_op(c):
        c.cursor().execute("INSERT INTO accounts VALUES (1, 100);")

    res, outbox_id = TransactionalOutbox.transactional_write(
        conn,
        business_fn=successful_op,
        topic="accounts",
        partition=0,
        source_offset=101,
        target_offset=501,
        payload=b"acc_1_created",
        headers={"env": "prod"},
    )
    assert outbox_id == 1

    cur.execute("SELECT balance FROM accounts WHERE id = 1;")
    assert cur.fetchone()[0] == 100

    offset_map = TransactionalOutbox.read_offset_map(conn, "accounts", 0)
    assert offset_map == {101: 501}

    def failing_op(c):
        c.cursor().execute("INSERT INTO accounts VALUES (2, 200);")
        raise RuntimeError("db connection failed mid-transaction")

    with pytest.raises(RuntimeError, match="db connection failed"):
        TransactionalOutbox.transactional_write(
            conn,
            business_fn=failing_op,
            topic="accounts",
            partition=0,
            source_offset=102,
            target_offset=502,
        )

    cur.execute("SELECT balance FROM accounts WHERE id = 2;")
    assert cur.fetchone() is None

    assert TransactionalOutbox.read_offset_map(conn, "accounts", 0) == {101: 501}


def test_transactional_outbox_pending_and_dispatch():
    conn = sqlite3.connect(":memory:")
    TransactionalOutbox.init_schema(conn)

    TransactionalOutbox.write_offset_map(conn, "orders", 0, 10, 100)
    TransactionalOutbox.write_offset_map(conn, "orders", 0, 11, 101)
    TransactionalOutbox.write_offset_map(conn, "orders", 0, 12, 102)
    conn.commit()

    pending = TransactionalOutbox.fetch_pending(conn, limit=2)
    assert len(pending) == 2
    assert pending[0].source_offset == 10
    assert pending[1].source_offset == 11

    updated = TransactionalOutbox.mark_dispatched(conn, [pending[0].id, pending[1].id])
    assert updated == 2

    remaining = TransactionalOutbox.fetch_pending(conn, limit=10)
    assert len(remaining) == 1
    assert remaining[0].source_offset == 12

    last_pos = TransactionalOutbox.get_last_mapped_position(conn, "orders", 0)
    assert last_pos == (12, 102)


# ============================================================================
# Item 314 (R4.formats): Schema Registry JSON Schema & Protobuf Tests
# ============================================================================

def test_registry_wire_format_confluent_and_apicurio():
    payload = b'{"msg": "hello"}'
    wire_conf = RegistryWireFormat.encode_confluent(42, payload)
    assert wire_conf[0:1] == b"\x00"
    assert struct.unpack(">I", wire_conf[1:5])[0] == 42
    assert wire_conf[5:] == payload

    sid, body, meta = RegistryWireFormat.decode_wire(wire_conf, flavor=RegistryFlavor.CONFLUENT)
    assert sid == 42
    assert body == payload
    assert meta["flavor"] == "confluent"

    wire_apic = RegistryWireFormat.encode_apicurio(1001, payload, as_global_id=True)
    assert wire_apic[0:1] == b"\x00"
    assert struct.unpack(">Q", wire_apic[1:9])[0] == 1001
    assert wire_apic[9:] == payload

    sid_a, body_a, meta_a = RegistryWireFormat.decode_wire(wire_apic, flavor=RegistryFlavor.APICURIO)
    assert sid_a == 1001
    assert body_a == payload


def test_schema_format_validator_json_schema():
    schema = {
        "type": "object",
        "required": ["id", "username"],
        "properties": {
            "id": {"type": "integer"},
            "username": {"type": "string"},
            "is_active": {"type": "boolean"},
        },
        "additionalProperties": False,
    }

    valid_payload = {"id": 1, "username": "alice", "is_active": True}
    res = SchemaFormatValidator.validate_json_schema(schema, valid_payload)
    assert res["username"] == "alice"

    with pytest.raises(SchemaValidationError, match="Missing required property"):
        SchemaFormatValidator.validate_json_schema(schema, {"id": 1})

    with pytest.raises(SchemaValidationError, match="invalid type"):
        SchemaFormatValidator.validate_json_schema(schema, {"id": "not_an_int", "username": "bob"})

    with pytest.raises(SchemaValidationError, match="Disallowed additional properties"):
        SchemaFormatValidator.validate_json_schema(schema, {"id": 2, "username": "charlie", "extra": 123})


def test_schema_format_validator_protobuf_wire():
    proto_schema = """
    syntax = "proto3";
    message UserRecord {
        int64 user_id = 1;
        string email = 2;
        bool verified = 3;
    }
    """
    fields = SchemaFormatValidator.parse_proto3_fields(proto_schema, "UserRecord")
    assert 1 in fields and fields[1][0] == "user_id"
    assert 2 in fields and fields[2][0] == "email"
    assert 3 in fields and fields[3][0] == "verified"

    wire = bytearray()
    wire.append((1 << 3) | 0)
    wire.append(42)
    wire.append((2 << 3) | 2)
    email_bytes = b"user@example.com"
    wire.append(len(email_bytes))
    wire.extend(email_bytes)
    wire.append((3 << 3) | 0)
    wire.append(1)

    parsed = SchemaFormatValidator.validate_protobuf_wire(proto_schema, "UserRecord", bytes(wire))
    assert parsed["user_id"] == 42
    assert parsed["email"] == "user@example.com"
    assert parsed["verified"] == 1

    corrupted_wire = RegistryWireFormat._encode_varint((99 << 3) | 0) + b"\x01"
    with pytest.raises(SchemaValidationError, match="unknown Protobuf field tag"):
        SchemaFormatValidator.validate_protobuf_wire(proto_schema, "UserRecord", corrupted_wire)


def test_schema_registry_client_roundtrip():
    client = SchemaRegistryClient()
    schema_json = {
        "type": "object",
        "required": ["item_id", "price"],
        "properties": {
            "item_id": {"type": "string"},
            "price": {"type": "number"},
        },
    }
    sid = client.register("items-value", schema_json, schema_type="JSON")
    assert sid == 1

    payload = {"item_id": "SKU-99", "price": 19.99}
    wire = client.serialize("items-value", sid, payload, flavor=RegistryFlavor.CONFLUENT)

    d_sid, d_data, meta = client.deserialize_and_validate(wire, flavor=RegistryFlavor.CONFLUENT)
    assert d_sid == sid
    assert d_data["item_id"] == "SKU-99"
    assert d_data["price"] == 19.99


# ============================================================================
# Item 355 (R13.kafka): Quota Enforcement & Config Reconciliation Tests
# ============================================================================

def test_topic_quota_enforcer():
    enforcer = TopicQuotaEnforcer(window_size_seconds=1.0)
    enforcer.set_quota("telemetry", TopicQuota(producer_byte_rate=1000.0, consumer_byte_rate=2000.0))

    t0 = 1000.0
    throttle = enforcer.check_and_record_produce("telemetry", 400, now=t0)
    assert throttle == 0.0

    throttle = enforcer.check_and_record_produce("telemetry", 400, now=t0 + 0.1)
    assert throttle == 0.0

    with pytest.raises(QuotaExceededError, match="Produce quota of 1000.0 B/s exceeded"):
        enforcer.check_and_record_produce("telemetry", 300, now=t0 + 0.2)

    throttle_no_raise = enforcer.check_and_record_produce(
        "telemetry", 300, now=t0 + 0.2, raise_on_exceed=False
    )
    assert throttle_no_raise > 0.0

    throttle_after_window = enforcer.check_and_record_produce("telemetry", 400, now=t0 + 1.5)
    assert throttle_after_window == 0.0


def test_topic_config_reconciliation():
    current = {
        "retention.ms": "86400000",
        "cleanup.policy": "delete",
        "partitions": "3",
    }
    desired = {
        "retention.ms": "604800000",
        "cleanup.policy": "compact",
        "min.insync.replicas": "2",
        "partitions": "3",
    }

    plan = TopicConfigReconciler.diff("orders", current, desired)
    assert not plan.is_noop
    assert "min.insync.replicas" in plan.to_add
    assert "retention.ms" in plan.to_update
    assert not plan.read_only_violations

    altered = {}
    def mock_alter(topic, changes):
        altered.update(changes)

    report = TopicConfigReconciler.reconcile("orders", current, desired, alter_callback=mock_alter)
    assert report["applied_count"] == 3
    assert current["retention.ms"] == "604800000"
    assert current["min.insync.replicas"] == "2"
    assert altered["cleanup.policy"] == "compact"

    desired_bad = {
        "partitions": "6",
    }
    with pytest.raises(ValueError, match="Cannot reconcile read-only configurations"):
        TopicConfigReconciler.reconcile("orders", current, desired_bad)


# ============================================================================
# Item 181 (S3.link): Cluster Linking & Topic Shadowing Controller Tests
# ============================================================================

def test_cluster_linking_and_topic_shadowing_lifecycle():
    controller = ClusterLinkController()
    link = controller.create_link("primary-dr", "source:9092", "target:9092")
    assert link.state == LinkState.ACTIVE

    shadow = controller.create_shadow_topic("primary-dr", "orders", shadow_topic_name="dr.orders", partitions=3)
    assert shadow.status == ShadowTopicStatus.ACTIVE
    assert shadow.shadow_topic == "dr.orders"

    controller.sync_record("primary-dr", "dr.orders", partition=0, source_offset=100, target_offset=95)
    assert shadow.offsets[0] == 95
    assert shadow.lag[0] == 5

    controller.sync_consumer_offset("primary-dr", "order-consumers", "orders", partition=0, source_offset=90)
    assert controller.links["primary-dr"].consumer_offsets["order-consumers"]["orders"][0] == 90

    controller.pause_link("primary-dr")
    assert link.state == LinkState.PAUSED
    assert shadow.status == ShadowTopicStatus.PAUSED

    with pytest.raises(RuntimeError, match="Cannot sync records while link primary-dr is in state LinkState.PAUSED"):
        controller.sync_record("primary-dr", "dr.orders", partition=0, source_offset=101, target_offset=96)

    controller.resume_link("primary-dr")
    assert link.state == LinkState.ACTIVE
    assert shadow.status == ShadowTopicStatus.ACTIVE

    promoted = controller.promote_shadow_topic("primary-dr", "dr.orders")
    assert promoted["promoted"]
    assert shadow.status == ShadowTopicStatus.PROMOTED

    with pytest.raises(RuntimeError, match="already PROMOTED"):
        controller.sync_record("primary-dr", "dr.orders", partition=0, source_offset=105, target_offset=100)

    status = controller.get_link_status("primary-dr")
    assert status["state"] == "ACTIVE"
    assert status["shadows"]["dr.orders"]["status"] == "PROMOTED"
    assert status["shadows"]["dr.orders"]["lag"][0] == 5
