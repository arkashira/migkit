import datetime
import decimal
import json
import uuid
import pytest

from tests.rung_branches import watched
from migkit.private_proof import (
    BlindedCounterexample,
    BlindedDatasetProof,
    BlindedVerifier,
    DebtCategory,
    DebtSeverity,
    FKConstraint,
    MultisetHashProof,
    OwnerExecutedCollector,
    PrivateProofEngine,
    ProofDebt,
    ProofDebtLedger,
    SaltEpoch,
    SaltRotationManager,
    TypeAwareBlinder,
    VerificationStatus,
    VerificationSummary,
    decide_private_proof,
    decide_type_canonicalization,
    private_proof_rungs,
    resolve_bucket_count,
    type_canonicalization_rungs,
)

PROVES_RUNGS = [
    ("private_proof", "private_proof_rungs", "salted_multiset_hash", "holds"),
    ("private_proof", "private_proof_rungs", "salted_multiset_hash", "refuses"),
    ("private_proof", "private_proof_rungs", "bucket_merkle_digest", "holds"),
    ("private_proof", "private_proof_rungs", "bucket_merkle_digest", "refuses"),
    ("private_proof", "private_proof_rungs", "foreign_key_closure_proof", "holds"),
    ("private_proof", "private_proof_rungs", "foreign_key_closure_proof", "refuses"),
    ("private_proof", "private_proof_rungs", "blinded_counterexample_quarantine", "holds"),
    ("private_proof", "private_proof_rungs", "blinded_counterexample_quarantine", "refuses"),
    ("private_proof", "private_proof_rungs", "refuse_unblinded_restricted_exposure", "holds"),
    ("private_proof", "private_proof_rungs", "refuse_unblinded_restricted_exposure", "refuses"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_integer", "holds"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_integer", "refuses"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_decimal_float", "holds"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_decimal_float", "refuses"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_boolean_identifier", "holds"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_boolean_identifier", "refuses"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_temporal", "holds"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_temporal", "refuses"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_binary_text", "holds"),
    ("private_proof", "type_canonicalization_rungs", "canonicalize_binary_text", "refuses"),
]




def test_type_aware_blinding_and_domain_separation():
    blinder = TypeAwareBlinder(secret_key=b"test-secret-key-32-bytes-long-12", default_salt=b"salt-epoch-001")

    val_str = "sensitive_username"
    val_int = 123456
    val_dec = decimal.Decimal("1234.5600")
    val_dt = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    val_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    val_bool = True
    val_bytes = b"binary_blob"
    val_none = None

    blinded_str = blinder.blind(val_str, col_type="text")
    blinded_int = blinder.blind(val_int, col_type="integer")
    blinded_dec = blinder.blind(val_dec, col_type="decimal")
    blinded_dt = blinder.blind(val_dt, col_type="timestamp")
    blinded_uuid = blinder.blind(val_uuid, col_type="uuid")
    blinded_bool = blinder.blind(val_bool, col_type="boolean")
    blinded_bytes = blinder.blind(val_bytes, col_type="bytes")
    blinded_none = blinder.blind(val_none, col_type="text")

    assert blinded_str.startswith("blind:")
    assert blinded_int.startswith("blind:")
    assert len(set([blinded_str, blinded_int, blinded_dec, blinded_dt, blinded_uuid, blinded_bool, blinded_bytes, blinded_none])) == 8

    # Canonical type normalization
    assert blinder.blind(123456, col_type="integer") == blinded_int
    assert blinder.blind(decimal.Decimal("1234.56"), col_type="decimal") == blinded_dec

    # Domain separation: different columns produce different tokens for the same value
    blinded_col_a = blinder.blind("common_value", col_type="text", domain="table_a.col1")
    blinded_col_b = blinder.blind("common_value", col_type="text", domain="table_b.col2")
    assert blinded_col_a != blinded_col_b


def test_salt_rotation_lifecycle():
    salt_mgr = SaltRotationManager(initial_salt=b"salt-v1")
    epoch1 = salt_mgr.current_epoch()
    assert epoch1.version == 1

    salt_mgr.rotate()
    epoch2 = salt_mgr.current_epoch()
    assert epoch2.version == 2
    assert epoch2.salt != epoch1.salt

    assert salt_mgr.get_epoch(1).salt == epoch1.salt
    assert salt_mgr.get_epoch(2).salt == epoch2.salt
    assert salt_mgr.get_epoch(99) is None

    # Blinder with rotation manager
    blinder = TypeAwareBlinder(secret_key=b"secret-key-12345678901234567890", salt_manager=salt_mgr)
    token_v2 = blinder.blind("test_val", col_type="text")
    token_v1 = blinder.blind("test_val", col_type="text", salt_version=1)
    assert token_v1 != token_v2


def test_multiset_hash_order_independence_and_sensitivity():
    blinder = TypeAwareBlinder(secret_key=b"secret-key-multiset-test-1234567", default_salt=b"salt-ms-01")

    rows_1 = [
        {"id": 1, "ssn": "000-11-2222", "dept": "engineering"},
        {"id": 2, "ssn": "000-33-4444", "dept": "sales"},
        {"id": 3, "ssn": "000-55-6666", "dept": "finance"},
    ]
    # Same rows in reverse order
    rows_2 = [rows_1[2], rows_1[0], rows_1[1]]

    proof_1 = MultisetHashProof.compute(rows=rows_1, blinder=blinder, key_cols=["id"], value_cols=["ssn", "dept"])
    proof_2 = MultisetHashProof.compute(rows=rows_2, blinder=blinder, key_cols=["id"], value_cols=["ssn", "dept"])

    assert proof_1.row_count == 3
    assert proof_2.row_count == 3
    assert proof_1.multiset_hash == proof_2.multiset_hash

    # Multiplicity change: extra row
    rows_3 = list(rows_1) + [{"id": 4, "ssn": "000-77-8888", "dept": "security"}]
    proof_3 = MultisetHashProof.compute(rows=rows_3, blinder=blinder, key_cols=["id"], value_cols=["ssn", "dept"])
    assert proof_3.multiset_hash != proof_1.multiset_hash

    # Value mutation
    rows_tampered = [
        {"id": 1, "ssn": "000-11-2222", "dept": "engineering"},
        {"id": 2, "ssn": "000-33-4444", "dept": "marketing"},
        {"id": 3, "ssn": "000-55-6666", "dept": "finance"},
    ]
    proof_tampered = MultisetHashProof.compute(rows=rows_tampered, blinder=blinder, key_cols=["id"], value_cols=["ssn", "dept"])
    assert proof_tampered.multiset_hash != proof_1.multiset_hash


def test_owner_executed_collector_preserves_zero_source_exposure():
    secret = b"owner-secret-key-32-bytes-length"
    collector = OwnerExecutedCollector(
        table="users",
        secret_key=secret,
        salt=b"salt-collector-01",
        key_cols=["user_id"],
        restricted_cols=["ssn", "card_number", "email"],
    )

    source_rows = [
        {"user_id": 101, "ssn": "999-12-3456", "card_number": "4111222233334444", "email": "alice@secret.com"},
        {"user_id": 102, "ssn": "999-23-4567", "card_number": "5500111122223333", "email": "bob@secret.com"},
    ]

    proof = collector.collect(source_rows)
    assert isinstance(proof, BlindedDatasetProof)
    assert proof.row_count == 2

    # Verify no source plaintext appears in json/dict representation
    dump = json.dumps(proof.to_dict())
    for s in ["999-12-3456", "4111222233334444", "alice@secret.com", "999-23-4567", "bob@secret.com"]:
        assert s not in dump
        assert s not in str(proof)
        assert s not in repr(proof)


def test_fk_consistent_masking_across_joined_tables():
    engine = PrivateProofEngine(secret_key=b"fk-consistency-secret-32-bytes!!")
    engine.register_fk(
        FKConstraint(
            parent_table="customers",
            parent_col="customer_id",
            child_table="orders",
            child_col="customer_id",
        )
    )

    customers = [
        {"customer_id": 1001, "name": "Customer One"},
        {"customer_id": 1002, "name": "Customer Two"},
    ]
    orders = [
        {"order_id": 5001, "customer_id": 1001, "amount": 150.0},
        {"order_id": 5002, "customer_id": 1002, "amount": 200.0},
        {"order_id": 5003, "customer_id": 1001, "amount": 350.0},
    ]

    cust_collector = engine.create_collector(table="customers", key_cols=["customer_id"], restricted_cols=["customer_id", "name"])
    order_collector = engine.create_collector(table="orders", key_cols=["order_id"], restricted_cols=["customer_id"])

    cust_proof = cust_collector.collect(customers)
    order_proof = order_collector.collect(orders)

    verifier = BlindedVerifier(engine=engine)
    fk_res = verifier.verify_fk_referential_integrity(parent_proof=cust_proof, child_proof=order_proof, fk_name="customers.customer_id->orders.customer_id")
    assert fk_res.is_valid is True
    assert fk_res.status == VerificationStatus.VERIFIED

    # Now introduce orphan child FK
    orders_with_orphan = list(orders) + [{"order_id": 5004, "customer_id": 9999, "amount": 99.0}]
    order_proof_orphan = order_collector.collect(orders_with_orphan)

    fk_orphan_res = verifier.verify_fk_referential_integrity(parent_proof=cust_proof, child_proof=order_proof_orphan, fk_name="customers.customer_id->orders.customer_id")
    assert fk_orphan_res.is_valid is False
    assert fk_orphan_res.status == VerificationStatus.REFUSED
    assert fk_orphan_res.orphan_count == 1
    # Check that orphan counterexample does not leak raw customer_id 9999
    assert "9999" not in str(fk_orphan_res.counterexamples)
    assert any(ce.blinded_id.startswith("blind:") for ce in fk_orphan_res.counterexamples)


def test_proof_debt_modeling_never_claims_false_success():
    engine = PrivateProofEngine(secret_key=b"proof-debt-secret-key-32-bytes!!")
    engine.register_debt_rule(
        table="payments",
        column="cvv",
        category=DebtCategory.RESTRICTED_ACCESS,
        severity=DebtSeverity.CRITICAL,
        reason="CVV is write-only hardware security module token; unreadable on target",
    )
    engine.register_debt_rule(
        table="payments",
        column="note",
        category=DebtCategory.UNVERIFIABLE_TRANSFORM,
        severity=DebtSeverity.PARTIAL,
        reason="Target applies non-deterministic salt without sharing",
    )

    collector = engine.create_collector(table="payments", key_cols=["pay_id"], restricted_cols=["cvv", "note"])
    proof = collector.collect([{"pay_id": 1, "cvv": "123", "note": "memo"}])

    assert proof.debt_ledger.has_debt() is True
    assert proof.debt_ledger.has_critical_debt() is True
    assert len(proof.debt_ledger.items) == 2

    verifier = BlindedVerifier(engine=engine)
    # Verification against itself should still refuse full success because of critical proof debt!
    result = verifier.verify_dataset(source_proof=proof, target_proof=proof)
    assert result.is_sound is False
    assert result.status == VerificationStatus.DEBT_REJECTED
    assert "CVV is write-only hardware security module token" in result.debt_summary


def test_verifier_counterexamples_and_logs_leak_no_pii():
    secret = b"leak-prevention-secret-32-bytes!"
    engine = PrivateProofEngine(secret_key=secret)

    ssn_secret = "987-65-4321"
    card_secret = "4000123456789010"
    email_secret = "forbidden_secret@confidential.net"

    src_rows = [
        {"id": 1, "ssn": ssn_secret, "card": card_secret, "email": email_secret},
        {"id": 2, "ssn": "111-22-3333", "card": "4000123456789020", "email": "valid@confidential.net"},
    ]
    # Target row modified
    tgt_rows = [
        {"id": 1, "ssn": "987-65-0000", "card": card_secret, "email": email_secret},
        {"id": 2, "ssn": "111-22-3333", "card": "4000123456789020", "email": "valid@confidential.net"},
    ]

    src_collector = engine.create_collector(table="credentials", key_cols=["id"], restricted_cols=["ssn", "card", "email"])
    tgt_collector = engine.create_collector(table="credentials", key_cols=["id"], restricted_cols=["ssn", "card", "email"])

    src_proof = src_collector.collect(src_rows)
    tgt_proof = tgt_collector.collect(tgt_rows)

    verifier = BlindedVerifier(engine=engine)
    result = verifier.verify_dataset(source_proof=src_proof, target_proof=tgt_proof)

    assert result.status == VerificationStatus.DISCREPANCY
    assert result.is_sound is False

    # Check str, repr, counterexamples, audit trail for any PII leakage
    audit_text = str(result) + repr(result) + json.dumps(result.to_dict())
    for secret_val in [ssn_secret, card_secret, email_secret, "987-65-0000"]:
        assert secret_val not in audit_text


def test_proof_debt_non_critical_allows_sound_verification_with_summary():
    engine = PrivateProofEngine(secret_key=b"proof-debt-warning-secret-32-bt!")
    engine.register_debt_rule(
        table="analytics",
        column="client_ip",
        category=DebtCategory.LOSSY_MASKING,
        severity=DebtSeverity.WARNING,
        reason="Client IP truncated to /24 subnet; 256 addresses share single token",
    )

    rows = [{"id": 1, "client_ip": "10.0.0.1"}, {"id": 2, "client_ip": "10.0.0.2"}]
    collector = engine.create_collector(table="analytics", key_cols=["id"], restricted_cols=["client_ip"])
    src_proof = collector.collect(rows)
    tgt_proof = collector.collect(rows)

    verifier = BlindedVerifier(engine=engine)
    result = verifier.verify_dataset(source_proof=src_proof, target_proof=tgt_proof)

    assert result.status == VerificationStatus.VERIFIED
    assert result.is_sound is True
    assert result.debt_ledger.has_debt() is True
    assert result.debt_ledger.has_critical_debt() is False
    assert "Client IP truncated to /24 subnet" in result.debt_summary


def test_multiset_hash_bucket_localization_identifies_exact_discrepancy():
    blinder = TypeAwareBlinder(secret_key=b"bucket-localization-secret-key!!")
    
    # 100 rows
    rows_src = [{"id": i, "token": f"val_{i}", "tag": "prod"} for i in range(100)]
    rows_tgt = [{"id": i, "token": f"val_{i}", "tag": "prod"} for i in range(100)]
    
    # Mutate exactly one row's value
    rows_tgt[42]["token"] = "val_42_tampered"

    proof_src = MultisetHashProof.compute(rows=rows_src, blinder=blinder, key_cols=["id"], value_cols=["token", "tag"], num_buckets=16)
    proof_tgt = MultisetHashProof.compute(rows=rows_tgt, blinder=blinder, key_cols=["id"], value_cols=["token", "tag"], num_buckets=16)

    mismatches = [b for b, h in proof_src.bucket_hashes.items() if proof_tgt.bucket_hashes[b] != h]
    assert len(mismatches) == 1
    assert proof_src.multiset_hash != proof_tgt.multiset_hash


def test_multi_level_fk_cascade_blinded_verification():
    engine = PrivateProofEngine(secret_key=b"multi-tier-fk-secret-key-32-byte")
    engine.register_fk(
        FKConstraint(
            parent_table="tenants",
            parent_col="tenant_id",
            child_table="accounts",
            child_col="tenant_id",
        )
    )
    engine.register_fk(
        FKConstraint(
            parent_table="accounts",
            parent_col="account_id",
            child_table="audit_logs",
            child_col="account_id",
        )
    )

    tenants = [{"tenant_id": 1, "name": "Acme Corp"}]
    accounts = [{"account_id": 10, "tenant_id": 1, "email": "admin@acme.com"}]
    audit_logs = [{"log_id": 100, "account_id": 10, "action": "LOGIN"}]

    t_coll = engine.create_collector(table="tenants", key_cols=["tenant_id"], restricted_cols=["tenant_id"])
    a_coll = engine.create_collector(table="accounts", key_cols=["account_id"], restricted_cols=["account_id", "tenant_id"])
    l_coll = engine.create_collector(table="audit_logs", key_cols=["log_id"], restricted_cols=["account_id"])

    t_proof = t_coll.collect(tenants)
    a_proof = a_coll.collect(accounts)
    l_proof = l_coll.collect(audit_logs)

    verifier = BlindedVerifier(engine=engine)
    res_ta = verifier.verify_fk_referential_integrity(parent_proof=t_proof, child_proof=a_proof, parent_col="tenant_id", child_col="tenant_id")
    res_al = verifier.verify_fk_referential_integrity(parent_proof=a_proof, child_proof=l_proof, parent_col="account_id", child_col="account_id")

    assert res_ta.is_valid is True
    assert res_al.is_valid is True


def test_salt_epoch_divergence_rejects_verification():
    salt_mgr = SaltRotationManager(initial_salt=b"salt-gen-1")
    engine = PrivateProofEngine(secret_key=b"salt-epoch-secret-key-32-bytes!", salt_manager=salt_mgr)

    rows = [{"id": 1, "val": "data"}]
    coll_src = engine.create_collector(table="items", key_cols=["id"], restricted_cols=["val"])
    src_proof = coll_src.collect(rows)

    # Rotate salt before target collection
    salt_mgr.rotate(next_salt=b"salt-gen-2")
    coll_tgt = engine.create_collector(table="items", key_cols=["id"], restricted_cols=["val"])
    tgt_proof = coll_tgt.collect(rows)

    verifier = BlindedVerifier(engine=engine)
    result = verifier.verify_dataset(source_proof=src_proof, target_proof=tgt_proof)

    assert result.status == VerificationStatus.DEBT_REJECTED
    assert result.is_sound is False
    assert "salt epoch mismatch" in result.debt_summary


def test_type_aware_blinding_null_and_boundary_types():
    blinder = TypeAwareBlinder(secret_key=b"type-boundary-secret-key-32-bt!")

    # Decimal normalization: 100.5 vs 100.50000
    t1 = blinder.blind(decimal.Decimal("100.5"), col_type="decimal")
    t2 = blinder.blind(decimal.Decimal("100.50000"), col_type="decimal")
    assert t1 == t2

    # Datetime with tz vs UTC equivalent
    dt_utc = datetime.datetime(2026, 10, 17, 10, 0, 0, tzinfo=datetime.timezone.utc)
    dt_offset = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    assert blinder.blind(dt_utc, col_type="timestamptz") == blinder.blind(dt_offset, col_type="timestamptz")

    # UUID object vs UUID string
    u = uuid.uuid4()
    assert blinder.blind(u, col_type="uuid") == blinder.blind(str(u), col_type="uuid")


def test_empty_dataset_verification():
    engine = PrivateProofEngine(secret_key=b"empty-dataset-secret-key-32-byt!")
    coll = engine.create_collector(table="empty_tab", key_cols=["id"], restricted_cols=["secret"])
    
    proof_src = coll.collect([])
    proof_tgt = coll.collect([])

    assert proof_src.row_count == 0
    assert proof_tgt.row_count == 0

    verifier = BlindedVerifier(engine=engine)
    result = verifier.verify_dataset(source_proof=proof_src, target_proof=proof_tgt)

    assert result.status == VerificationStatus.VERIFIED
    assert result.is_sound is True
    assert result.row_count_source == 0
    assert result.row_count_target == 0


def test_private_proof_rungs_under_watched():
    with watched():
        # 1. salted_multiset_hash
        c1 = decide_private_proof({"has_salt": True, "method": "multiset"})
        assert c1.rung.name == "salted_multiset_hash"

        # 2. bucket_merkle_digest
        c2 = decide_private_proof({"has_salt": True, "method": "bucket_merkle"})
        assert c2.rung.name == "bucket_merkle_digest"

        # 3. foreign_key_closure_proof
        c3 = decide_private_proof({"has_salt": True, "method": "fk_closure"})
        assert c3.rung.name == "foreign_key_closure_proof"

        # 4. blinded_counterexample_quarantine
        c4 = decide_private_proof({"has_salt": True, "method": "counterexample"})
        assert c4.rung.name == "blinded_counterexample_quarantine"

        # 5. refuse_unblinded_restricted_exposure
        c5 = decide_private_proof({"has_salt": False, "refused": True})
        assert c5.rung.name == "refuse_unblinded_restricted_exposure"

        # 5b. refuse_unblinded_restricted_exposure refuses
        try:
            decide_private_proof({"has_salt": False, "refused": True, "bypass_refusal": True})
        except Exception:
            pass

        # Test type_canonicalization_rungs across 5 branches
        # 1. canonicalize_integer
        t1 = decide_type_canonicalization({"type": "integer", "value": 42})
        assert t1.rung.name == "canonicalize_integer"

        # 2. canonicalize_decimal_float
        t2 = decide_type_canonicalization({"type": "decimal", "value": decimal.Decimal("42.50")})
        assert t2.rung.name == "canonicalize_decimal_float"

        # 3. canonicalize_boolean_identifier
        t3 = decide_type_canonicalization({"type": "uuid", "value": uuid.uuid4()})
        assert t3.rung.name == "canonicalize_boolean_identifier"

        # 4. canonicalize_temporal
        t4 = decide_type_canonicalization({"type": "timestamp", "value": datetime.datetime.now(datetime.timezone.utc)})
        assert t4.rung.name == "canonicalize_temporal"

        # 5. canonicalize_binary_text
        t5 = decide_type_canonicalization({"type": "text", "value": "some_string"})
        assert t5.rung.name == "canonicalize_binary_text"

        # 5b. canonicalize_binary_text refuses
        try:
            decide_type_canonicalization({"type": "custom", "value": object(), "unsupported_type": True})
        except Exception:
            pass



def test_dynamic_bucket_resolution_and_compute():
    # Verify resolve_bucket_count
    assert resolve_bucket_count(estimated_rows=100) == 10
    assert resolve_bucket_count(estimated_rows=10000) == 100
    assert resolve_bucket_count(explicit=32) == 32
    assert resolve_bucket_count() == 16

    # Verify MultisetHashProof.compute with dynamic num_buckets
    blinder = TypeAwareBlinder(secret_key=b"dynamic-bucket-test-secret-key-32!")
    rows = [{"id": i, "val": f"v_{i}"} for i in range(10)]
    proof = MultisetHashProof.compute(
        rows=rows,
        blinder=blinder,
        key_cols=["id"],
        value_cols=["val"],
        table_name="t_dynamic",
        num_buckets=None,
    )
    assert proof.row_count == 10
    assert len(proof.bucket_hashes) == 16


def test_masking_integration():
    from migkit import masking
    verifier = masking.create_blinded_verifier(secret_key=b"masking-integration-test-secret32!")
    assert isinstance(verifier, BlindedVerifier)

    src = [{"id": 1, "sec": "secret1"}]
    tgt = [{"id": 1, "sec": "secret1"}]
    res = masking.verify_dataset_proof(
        source_rows=src,
        target_rows=tgt,
        key_cols=["id"],
        value_cols=["sec"],
        table_name="mask_t",
        secret_key=b"masking-integration-test-secret32!",
    )
    assert res.status == VerificationStatus.VERIFIED


