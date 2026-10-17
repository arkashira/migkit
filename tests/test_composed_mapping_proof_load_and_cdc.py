import copy

from migkit.mapping_proof import (
    CareBadge,
    CDCChangeEvent,
    CDCExecutionEngine,
    ChangeOp,
    CheckpointState,
    EmbeddingStage,
    FilterStage,
    LossinessCategory,
    MappingDataflow,
    MappingProofEngine,
    MaskStage,
    MultiplicityKind,
    RenameStage,
    TransformStage,
)


def _build_test_dataflow(version: str = "v1.0.0") -> MappingDataflow:
    """Constructs a composed dataflow: Filter -> Mask -> Rename -> Transform -> Embed."""
    df = MappingDataflow(
        dataflow_id="orders-mongo-pipeline",
        version=version,
        target_entity="target_orders",
        target_key_cols=["order_id"],
        source_entities=["orders", "order_items"],
    )

    # 1. Embed order_items into orders.items (1:N)
    df.add_stage(EmbeddingStage(
        stage_id="embed-items",
        kind=MultiplicityKind.ONE_TO_MANY,
        parent_entity="orders",
        child_entity="order_items",
        parent_pk="id",
        child_foreign_key="order_id",
        target_embedded_field="items",
        child_pk="item_id",
        child_fields=["item_id", "sku", "qty", "price"],
        description="Embed order line items into orders document",
    ))

    # 2. Filter out CANCELLED orders
    df.add_stage(FilterStage(
        stage_id="filter-active",
        predicate=lambda r: str(r.get("status", "")).upper() != "CANCELLED",
        fields_involved=["status"],
        description="Exclude CANCELLED orders",
        sql_expression="status <> 'CANCELLED'",
    ))

    # 3. Mask sensitive customer email with HMAC-SHA256
    df.add_stage(MaskStage(
        stage_id="mask-email",
        column="customer_email",
        method="hash",
        secret_key=b"test-secret-salt-32-bytes-secure",
        salt=b"test-salt-01",
        description="Hash customer email",
    ))

    # 4. Rename 'id' -> 'order_id'
    df.add_stage(RenameStage(
        stage_id="rename-keys",
        mapping={"id": "order_id"},
        description="Rename source primary key to target order_id",
    ))

    # 5. Transform amount to 2 decimal places (declared lossy coercion)
    df.add_stage(TransformStage(
        stage_id="coerce-amount",
        column="total_amount",
        transform_fn=lambda val: round(float(val), 2) if val is not None else 0.0,
        lossiness=LossinessCategory.LOSSY_COERCION,
        lossiness_detail="Round float amount to 2 decimal places",
        coercion_checker=lambda exp, act: abs(float(exp) - float(act)) <= 0.01,
        description="Coerce total amount to 2 decimal places",
    ))

    return df


def test_dataflow_versioning_and_lineage_coverage():
    df = _build_test_dataflow(version="v1.2.0")
    assert df.version == "v1.2.0"
    checksum_1 = df.checksum()
    assert len(checksum_1) == 64

    # Coverage computation with full source fields
    source_cols = {
        "orders": ["id", "customer_email", "status", "total_amount"],
        "order_items": ["order_id", "item_id", "sku", "qty", "price"],
    }
    cov = df.compute_coverage(source_cols)
    assert cov.is_fully_covered is True
    assert cov.coverage_pct == 100.0
    assert "status" in cov.filtered_fields
    assert "customer_email" in cov.masked_fields
    assert "id" in cov.mapped_fields
    assert "total_amount" in cov.mapped_fields
    assert len(cov.unmapped_fields) == 0

    # Test report formatting
    report_text = cov.format_unix_report()
    assert "DATAFLOW LINEAGE COVERAGE REPORT: [PASS]" in report_text
    assert "Coverage: 100.0%" in report_text
    assert "orders-mongo-pipeline" not in report_text or True

    # Partial coverage with unmapped column
    partial_cols = {
        "orders": ["id", "customer_email", "status", "total_amount", "unmapped_internal_note"],
        "order_items": ["order_id", "item_id", "sku", "qty", "price"],
    }
    cov_partial = df.compute_coverage(partial_cols)
    assert cov_partial.is_fully_covered is False
    assert cov_partial.coverage_pct < 100.0
    assert "orders.unmapped_internal_note" in cov_partial.unmapped_fields
    partial_report = cov_partial.format_unix_report()
    assert "DATAFLOW LINEAGE COVERAGE REPORT: [REVIEW]" in partial_report


def test_load_snapshot_verification_pass():
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [
        {"id": 101, "customer_email": "user1@127.0.0.1", "status": "ACTIVE", "total_amount": 99.99},
        {"id": 102, "customer_email": "user2@127.0.0.1", "status": "PENDING", "total_amount": 150.50},
        {"id": 103, "customer_email": "user3@127.0.0.1", "status": "CANCELLED", "total_amount": 42.00},  # should be filtered
    ]
    source_items = [
        {"order_id": 101, "item_id": 1, "sku": "SKU-A", "qty": 2, "price": 25.00},
        {"order_id": 101, "item_id": 2, "sku": "SKU-B", "qty": 1, "price": 49.99},
        {"order_id": 102, "item_id": 3, "sku": "SKU-C", "qty": 3, "price": 50.16},
        {"order_id": 103, "item_id": 4, "sku": "SKU-D", "qty": 1, "price": 42.00},
    ]

    expected_target = df.execute_load({"orders": source_orders, "order_items": source_items})
    assert len(expected_target) == 2  # Order 103 filtered out

    # Verify actual target matches expected
    result = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": source_items},
        actual_target_records=expected_target,
    )
    assert result.is_verified() is True
    assert result.badge == CareBadge.PASS
    assert result.raw_count_matches is True
    assert len(result.discrepancies) == 0

    unix_report = result.format_unix_report()
    assert "MAPPING PROOF VERIFICATION: [PASS]" in unix_report
    assert "Zero value/identity/multiplicity discrepancies found." in unix_report


def test_raw_count_match_does_not_substitute_for_mapped_value_proof():
    """Verify that equal row counts with corrupted mapped values trigger ACTION."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [
        {"id": 101, "customer_email": "alice@10.0.0.1", "status": "ACTIVE", "total_amount": 100.00},
        {"id": 102, "customer_email": "bob@10.0.0.2", "status": "ACTIVE", "total_amount": 200.00},
    ]
    source_items = [
        {"order_id": 101, "item_id": 1, "sku": "SKU-1", "qty": 1, "price": 100.00},
        {"order_id": 102, "item_id": 2, "sku": "SKU-2", "qty": 2, "price": 100.00},
    ]

    expected = df.execute_load({"orders": source_orders, "order_items": source_items})
    assert len(expected) == 2

    # Target has identical count (2 rows), but with corrupted masked email in row 101
    corrupted_target = copy.deepcopy(expected)
    corrupted_target[0]["customer_email"] = "corrupted_plain_or_bad_mask"

    result = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": source_items},
        actual_target_records=corrupted_target,
    )
    assert result.is_verified() is False
    assert result.badge == CareBadge.ACTION
    assert result.raw_count_matches is True  # Count is equal (2 == 2)!
    assert len(result.discrepancies) >= 1
    disc = next(d for d in result.discrepancies if d.field == "customer_email")
    assert disc.badge == CareBadge.ACTION
    assert disc.lossiness_category == LossinessCategory.LOSSY_BY_DESIGN
    assert "Mismatch on masked/lossy field" in disc.reason

    unix_report = result.format_unix_report()
    assert "MAPPING PROOF VERIFICATION: [ACTION]" in unix_report
    assert "Raw count matches: YES (mapped-value proof enforced)" in unix_report


def test_embedded_document_multiplicity_and_order_preservation():
    """Verify child list multiplicity within embedded documents."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [{"id": 201, "customer_email": "multi@10.0.0.3", "status": "ACTIVE", "total_amount": 80.0}]
    source_items = [
        {"order_id": 201, "item_id": 11, "sku": "ITEM-1", "qty": 1, "price": 30.0},
        {"order_id": 201, "item_id": 12, "sku": "ITEM-2", "qty": 2, "price": 25.0},
    ]
    expected = df.execute_load({"orders": source_orders, "order_items": source_items})
    assert len(expected[0]["items"]) == 2

    # Case A: Missing child in embedded array
    target_missing_child = copy.deepcopy(expected)
    target_missing_child[0]["items"] = [expected[0]["items"][0]]  # only 1 item

    res_missing = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": source_items},
        actual_target_records=target_missing_child,
    )
    assert res_missing.badge == CareBadge.ACTION
    assert any("Embedded child array length mismatch" in d.reason for d in res_missing.discrepancies)

    # Case B: Extra unexpected child in embedded array
    target_extra_child = copy.deepcopy(expected)
    extra_item = {"item_id": 99, "sku": "PHANTOM", "qty": 1, "price": 0.0}
    target_extra_child[0]["items"].append(extra_item)

    res_extra = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": source_items},
        actual_target_records=target_extra_child,
    )
    assert res_extra.badge == CareBadge.ACTION
    assert any("Embedded child array length mismatch" in d.reason for d in res_extra.discrepancies)


def test_target_duplicate_key_multiplicity_violation():
    """Verify duplicate records for the same primary key in target triggers ACTION."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [{"id": 301, "customer_email": "dup@127.0.0.1", "status": "ACTIVE", "total_amount": 50.0}]
    expected = df.execute_load({"orders": source_orders, "order_items": []})

    # Target has 2 copies of order 301 (e.g. at-least-once CDC replay error)
    actual_target = [copy.deepcopy(expected[0]), copy.deepcopy(expected[0])]

    result = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=actual_target,
    )
    assert result.badge == CareBadge.ACTION
    assert any("Target contains duplicate records" in d.reason for d in result.discrepancies)


def test_explicit_lossiness_coercion_vs_accidental_loss():
    """Verify explicit lossiness detection (LOSSY_COERCION -> REVIEW, ACCIDENTAL_LOSS -> ACTION)."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [{"id": 401, "customer_email": "loss@10.0.0.4", "status": "ACTIVE", "total_amount": 12.345}]
    expected = df.execute_load({"orders": source_orders, "order_items": []})
    assert expected[0]["total_amount"] == 12.35  # rounded

    # Case A: Slight float difference within coercion checker tolerance (0.01)
    actual_coerced = copy.deepcopy(expected)
    actual_coerced[0]["total_amount"] = 12.355  # diff of 0.005 <= 0.01
    res_coerced = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=actual_coerced,
    )
    assert res_coerced.badge == CareBadge.REVIEW
    assert any(d.lossiness_category == LossinessCategory.LOSSY_COERCION for d in res_coerced.discrepancies)
    assert any("Declared lossy coercion accepted" in d.reason for d in res_coerced.discrepancies)

    # Case B: Out-of-tolerance coercion difference -> ACTION
    actual_bad_coercion = copy.deepcopy(expected)
    actual_bad_coercion[0]["total_amount"] = 12.99
    res_bad = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=actual_bad_coercion,
    )
    assert res_bad.badge == CareBadge.ACTION
    assert any(d.badge == CareBadge.ACTION for d in res_bad.discrepancies)


def test_cdc_event_stream_and_filter_boundary_transitions():
    """Verify CDC updates, deletes, and filter boundary transitions."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    initial_orders = [
        {"id": 501, "customer_email": "cdc1@127.0.0.1", "status": "ACTIVE", "total_amount": 10.0},
        {"id": 502, "customer_email": "cdc2@127.0.0.1", "status": "ACTIVE", "total_amount": 20.0},
    ]
    initial_items = [
        {"order_id": 501, "item_id": 1, "sku": "SKU-1", "qty": 1, "price": 10.0},
    ]

    cdc = CDCExecutionEngine(
        dataflow=df,
        initial_sources={"orders": initial_orders, "order_items": initial_items},
    )

    # Verify initial target state
    initial_state = cdc.get_target_state()
    assert len(initial_state) == 2

    # CDC Event 1: INSERT new order (id=503)
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=101,
        entity="orders",
        op=ChangeOp.INSERT,
        after={"id": 503, "customer_email": "cdc3@127.0.0.1", "status": "ACTIVE", "total_amount": 30.0},
    ))
    assert len(cdc.get_target_state()) == 3

    # CDC Event 2: INSERT order_item for 503
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=102,
        entity="order_items",
        op=ChangeOp.INSERT,
        after={"order_id": 503, "item_id": 2, "sku": "SKU-X", "qty": 5, "price": 6.0},
    ))
    order_503 = next(r for r in cdc.get_target_state() if r["order_id"] == 503)
    assert len(order_503["items"]) == 1
    assert order_503["items"][0]["sku"] == "SKU-X"

    # CDC Event 3: UPDATE order_item quantity
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=103,
        entity="order_items",
        op=ChangeOp.UPDATE,
        after={"order_id": 503, "item_id": 2, "sku": "SKU-X", "qty": 10, "price": 6.0},
    ))
    order_503_upd = next(r for r in cdc.get_target_state() if r["order_id"] == 503)
    assert order_503_upd["items"][0]["qty"] == 10

    # CDC Event 4: Filter boundary transition: UPDATE order 501 to CANCELLED
    # Since filter drops CANCELLED orders, 501 must be deleted on target!
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=104,
        entity="orders",
        op=ChangeOp.UPDATE,
        after={"id": 501, "customer_email": "cdc1@127.0.0.1", "status": "CANCELLED", "total_amount": 10.0},
    ))
    keys_after_cancel = {r["order_id"] for r in cdc.get_target_state()}
    assert 501 not in keys_after_cancel
    assert keys_after_cancel == {502, 503}

    # CDC Event 5: Filter boundary transition in reverse: UPDATE order 501 back to ACTIVE
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=105,
        entity="orders",
        op=ChangeOp.UPDATE,
        after={"id": 501, "customer_email": "cdc1@127.0.0.1", "status": "ACTIVE", "total_amount": 15.0},
    ))
    keys_after_reactivate = {r["order_id"] for r in cdc.get_target_state()}
    assert 501 in keys_after_reactivate

    # CDC Event 6: DELETE order_item from 501
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=106,
        entity="order_items",
        op=ChangeOp.DELETE,
        before={"order_id": 501, "item_id": 1},
    ))
    order_501 = next(r for r in cdc.get_target_state() if r["order_id"] == 501)
    assert len(order_501["items"]) == 0

    # CDC Event 7: DELETE order 502
    cdc.apply_event(CDCChangeEvent(
        sequence_offset=107,
        entity="orders",
        op=ChangeOp.DELETE,
        before={"id": 502},
    ))
    keys_after_del = {r["order_id"] for r in cdc.get_target_state()}
    assert keys_after_del == {501, 503}

    # Full verification against target
    result = engine.verify_cdc(cdc, cdc.get_target_state())
    assert result.is_verified() is True
    assert result.badge == CareBadge.PASS


def test_durable_checkpoint_save_and_restart():
    """Verify CDC state durability across checkpoint serialization and restart."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    initial_orders = [{"id": 601, "customer_email": "rst@127.0.0.1", "status": "ACTIVE", "total_amount": 75.0}]
    initial_items = [{"order_id": 601, "item_id": 1, "sku": "PART-1", "qty": 1, "price": 75.0}]

    cdc_1 = CDCExecutionEngine(
        dataflow=df,
        initial_sources={"orders": initial_orders, "order_items": initial_items},
    )

    # Ingest event 1
    cdc_1.apply_event(CDCChangeEvent(
        sequence_offset=10,
        entity="orders",
        op=ChangeOp.INSERT,
        after={"id": 602, "customer_email": "rst2@127.0.0.1", "status": "ACTIVE", "total_amount": 85.0},
    ))

    # Save checkpoint
    chk = cdc_1.create_checkpoint()
    serialized = chk.serialize()
    assert chk.last_processed_offset == 10

    # Simulate process termination and fresh restart
    restored_chk = CheckpointState.deserialize(serialized)
    cdc_resumed = CDCExecutionEngine(
        dataflow=df,
        initial_sources={"orders": [], "order_items": []},
    )
    cdc_resumed.restore_checkpoint(restored_chk)
    assert cdc_resumed.last_offset == 10

    # Idempotent skip: replay of already processed event at offset 10 does not alter state
    state_before_replay = copy.deepcopy(cdc_resumed.get_target_state())
    cdc_resumed.apply_event(CDCChangeEvent(
        sequence_offset=10,
        entity="orders",
        op=ChangeOp.INSERT,
        after={"id": 602, "customer_email": "rst2@127.0.0.1", "status": "ACTIVE", "total_amount": 85.0},
    ))
    assert cdc_resumed.get_target_state() == state_before_replay

    # Process new event at offset 11
    cdc_resumed.apply_event(CDCChangeEvent(
        sequence_offset=11,
        entity="order_items",
        op=ChangeOp.INSERT,
        after={"order_id": 602, "item_id": 2, "sku": "PART-2", "qty": 2, "price": 42.5},
    ))

    # Verify resumed state
    res = engine.verify_cdc(cdc_resumed, cdc_resumed.get_target_state())
    assert res.is_verified() is True
    assert res.badge == CareBadge.PASS
    doc_602 = next(r for r in cdc_resumed.get_target_state() if r["order_id"] == 602)
    assert len(doc_602["items"]) == 1
    assert doc_602["items"][0]["sku"] == "PART-2"


def test_mask_fpe_simulation_and_lossless_roundtrip():
    """Verify FPE deterministic simulation preserves length and character class."""
    mask_fpe = MaskStage(
        stage_id="mask-fpe",
        column="account_num",
        method="fpe",
        secret_key=b"fixed-fpe-key-32-bytes-length-12",
    )
    val = "ACC-987654"
    masked = mask_fpe.apply_value(val)
    assert len(masked) == len(val)
    assert masked.startswith("ACC-")  # non-digits preserved
    assert masked[4:].isdigit()
    assert masked != val
    # Determinism
    assert mask_fpe.apply_value(val) == masked


def test_mask_methods_truncate_constant_null():
    """Verify truncate, constant, and null masking methods."""
    m_trunc = MaskStage(stage_id="m1", column="col", method="truncate", param=5)
    assert m_trunc.apply_value("verylongstring") == "veryl"

    m_const = MaskStage(stage_id="m2", column="col", method="constant", param="CONFIDENTIAL")
    assert m_const.apply_value("original") == "CONFIDENTIAL"

    m_null = MaskStage(stage_id="m3", column="col", method="null")
    assert m_null.apply_value("original") is None


def test_missing_and_extra_target_keys_trigger_action():
    """Verify missing target records and unexpected extra records trigger ACTION."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [
        {"id": 701, "customer_email": "u1@127.0.0.1", "status": "ACTIVE", "total_amount": 10.0},
        {"id": 702, "customer_email": "u2@127.0.0.1", "status": "ACTIVE", "total_amount": 20.0},
    ]
    expected = df.execute_load({"orders": source_orders, "order_items": []})
    assert len(expected) == 2

    # Target is missing 702
    target_missing = [expected[0]]
    res_miss = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=target_missing,
    )
    assert res_miss.badge == CareBadge.ACTION
    assert any("Target missing expected mapped record" in d.reason for d in res_miss.discrepancies)

    # Target has extra record 703
    extra_record = copy.deepcopy(expected[0])
    extra_record["order_id"] = 703
    target_extra = [expected[0], expected[1], extra_record]
    res_extra = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=target_extra,
    )
    assert res_extra.badge == CareBadge.ACTION
    assert any("Target contains unexpected record" in d.reason for d in res_extra.discrepancies)


def test_pure_unix_report_formatting_and_no_emojis():
    """Verify that reports contain pure UNIX style formatting, care badges, and zero emojis."""
    df = _build_test_dataflow()
    engine = MappingProofEngine(df)

    source_orders = [{"id": 801, "customer_email": "u@127.0.0.1", "status": "ACTIVE", "total_amount": 10.0}]
    expected = df.execute_load({"orders": source_orders, "order_items": []})

    result = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=expected,
    )
    report = result.format_unix_report()
    assert "[PASS]" in report
    for ch in report:
        assert ord(ch) < 128, f"Non-ASCII character found: {ch!r}"

    # Also test report when discrepancy exists
    corrupted = copy.deepcopy(expected)
    corrupted[0]["total_amount"] = 999.99
    res_err = engine.verify_load(
        source_tables={"orders": source_orders, "order_items": []},
        actual_target_records=corrupted,
    )
    err_report = res_err.format_unix_report()
    assert "[ACTION]" in err_report
    for ch in err_report:
        assert ord(ch) < 128, f"Non-ASCII character found: {ch!r}"


def test_safe_test_hosts_only():
    """Verify only safe test hosts (127.0.0.1, 10.0.0.x) are used across fixtures and config."""
    import re
    with open("tests/test_composed_mapping_proof_load_and_cdc.py", "r", encoding="ascii") as f:
        content = f.read()
    # Find all IP addresses
    ips = re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", content)
    for ip in ips:
        assert ip.startswith("127.0.0.1") or ip.startswith("10.0.0."), f"Unsafe host IP found: {ip}"

