import unicodedata
import pytest

from migkit.identity_semantics import (
    CollationSemantics,
    IdentityContract,
    IdentityContractViolationError,
    IdentitySemanticsError,
    KeyStrategy,
    NonInjectiveKeyError,
    detect_non_injective_representation,
    encode_lossless_key,
    decode_lossless_key,
    verify_bulk_and_row_parity,
    decide_identity_strategy,
)
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("identity_semantics", "identity_strategy_rungs", "direct_identity_representation", "holds"),
    ("identity_semantics", "identity_strategy_rungs", "direct_identity_representation", "refuses"),
    ("identity_semantics", "identity_strategy_rungs", "binary_collation_adaptation", "holds"),
    ("identity_semantics", "identity_strategy_rungs", "binary_collation_adaptation", "refuses"),
    ("identity_semantics", "identity_strategy_rungs", "lossless_encoded_key", "holds"),
    ("identity_semantics", "identity_strategy_rungs", "lossless_encoded_key", "refuses"),
    ("identity_semantics", "identity_strategy_rungs", "surrogate_key_assignment", "holds"),
    ("identity_semantics", "identity_strategy_rungs", "surrogate_key_assignment", "refuses"),
    ("identity_semantics", "identity_strategy_rungs", "refuse_colliding_identity", "holds"),
]


def test_parse_collation_semantics():
    cs = CollationSemantics.from_name("utf8mb4_bin")
    assert cs.is_case_sensitive
    assert cs.is_binary

    ci = CollationSemantics.from_name("utf8mb4_0900_ai_ci")
    assert not ci.is_case_sensitive
    assert not ci.is_accent_sensitive
    assert not ci.is_binary

    pg_c = CollationSemantics.from_name("C")
    assert pg_c.is_case_sensitive
    assert pg_c.is_binary

    sql_ci = CollationSemantics.from_name("SQL_Latin1_General_CP1_CI_AS")
    assert not sql_ci.is_case_sensitive
    assert sql_ci.is_accent_sensitive


def test_detect_non_injective_case_collision():
    keys = ["Order_Alpha", "order_alpha", "Order_Beta"]
    collision = detect_non_injective_representation(
        keys,
        target_collation="utf8mb4_unicode_ci",
        source_collation="utf8mb4_bin",
    )
    assert collision is not None
    k1, k2, reason = collision
    assert {k1, k2} == {"Order_Alpha", "order_alpha"}
    assert reason == "case_sensitivity_collision"


def test_detect_non_injective_unicode_normalization_collision():
    nfc_key = "caf\u00e9"
    nfd_key = unicodedata.normalize("NFD", nfc_key)
    assert nfc_key != nfd_key

    keys = [nfc_key, nfd_key, "other"]
    collision = detect_non_injective_representation(
        keys,
        target_collation="utf8mb4_unicode_ci",
        unicode_normalization="NFC",
    )
    assert collision is not None
    k1, k2, reason = collision
    assert {k1, k2} == {nfc_key, nfd_key}
    assert reason == "unicode_normalization_collision"


def test_detect_no_collision_when_injective():
    keys = ["Order_Alpha", "Order_Beta", "Order_Gamma"]
    collision = detect_non_injective_representation(
        keys,
        target_collation="utf8mb4_unicode_ci",
        source_collation="utf8mb4_bin",
    )
    assert collision is None


def test_identity_contract_refuses_case_only_ids_under_ci():
    contract = IdentityContract(
        table="accounts",
        key_columns=("account_code",),
        target_collation="utf8mb4_0900_ai_ci",
        strategy=KeyStrategy.REFUSE,
    )

    rows = [
        {"account_code": "Acct_100", "balance": 500},
        {"account_code": "acct_100", "balance": 750},
    ]

    with pytest.raises(NonInjectiveKeyError) as exc_info:
        contract.validate_injectivity(rows)

    err = exc_info.value
    assert err.table == "accounts"
    assert err.columns == ("account_code",)
    assert {err.key1, err.key2} == {("Acct_100",), ("acct_100",)}
    assert err.reason == "case_sensitivity_collision"


def test_identity_contract_refuses_unicode_variants_under_normalization():
    contract = IdentityContract(
        table="tags",
        key_columns=("tag_name",),
        target_collation="utf8mb4_unicode_ci",
        unicode_normalization="NFC",
        strategy=KeyStrategy.REFUSE,
    )

    nfc_val = "r\u00e9sum\u00e9"
    nfd_val = unicodedata.normalize("NFD", nfc_val)
    rows = [
        {"tag_name": nfc_val, "category": "career"},
        {"tag_name": nfd_val, "category": "cv"},
    ]

    with pytest.raises(NonInjectiveKeyError) as exc_info:
        contract.validate_injectivity(rows)

    err = exc_info.value
    assert err.table == "tags"
    assert err.reason == "unicode_normalization_collision"


def test_lossless_key_encoding_strategy_preserves_case_and_unicode():
    k1 = "Identifier_A"
    k2 = "identifier_a"
    enc1 = encode_lossless_key(k1)
    enc2 = encode_lossless_key(k2)

    assert enc1 != enc2
    assert enc1.lower() != enc2.lower()
    assert decode_lossless_key(enc1) == k1
    assert decode_lossless_key(enc2) == k2

    nfc_val = "caf\u00e9"
    nfd_val = unicodedata.normalize("NFD", nfc_val)
    enc_nfc = encode_lossless_key(nfc_val)
    enc_nfd = encode_lossless_key(nfd_val)

    assert enc_nfc != enc_nfd
    assert enc_nfc.lower() != enc_nfd.lower()
    assert decode_lossless_key(enc_nfc) == nfc_val
    assert decode_lossless_key(enc_nfd) == nfd_val


def test_identity_contract_lossless_encoded_strategy_allows_case_only_ids():
    contract = IdentityContract(
        table="items",
        key_columns=("sku",),
        target_collation="utf8mb4_0900_ai_ci",
        strategy=KeyStrategy.ENCODED_KEY,
    )

    rows = [
        {"sku": "SKU_X", "qty": 10},
        {"sku": "sku_x", "qty": 20},
    ]

    # Should not raise NonInjectiveKeyError
    contract.validate_injectivity(rows)

    k1 = contract.target_identity_key(rows[0])
    k2 = contract.target_identity_key(rows[1])
    assert k1 != k2
    assert contract.target_equivalence_key(rows[0]) != contract.target_equivalence_key(rows[1])


def test_bulk_dedup_never_merges_distinct_case_only_or_unicode_entities():
    contract = IdentityContract(
        table="entities",
        key_columns=("entity_id",),
        target_collation="utf8mb4_bin",
        strategy=KeyStrategy.BINARY_COLLATION,
    )

    nfc_id = "na\u00efve"
    nfd_id = unicodedata.normalize("NFD", nfc_id)

    rows = [
        {"entity_id": "Node_1", "version": 1},
        {"entity_id": "node_1", "version": 1},
        {"entity_id": nfc_id, "version": 1},
        {"entity_id": nfd_id, "version": 1},
        {"entity_id": "Node_1", "version": 2},  # Duplicate source identity of row 0
    ]

    deduped = contract.dedup_batch(rows, keep="last")

    # The duplicate Node_1 is deduped to version 2, while node_1, nfc, and nfd remain distinct!
    assert len(deduped) == 4
    entity_map = {r["entity_id"]: r["version"] for r in deduped}
    assert entity_map["Node_1"] == 2
    assert entity_map["node_1"] == 1
    assert entity_map[nfc_id] == 1
    assert entity_map[nfd_id] == 1


def test_bulk_and_row_paths_share_identity_contract_and_yield_identical_state():
    contract = IdentityContract(
        table="users",
        key_columns=("username",),
        target_collation="utf8mb4_bin",
        strategy=KeyStrategy.BINARY_COLLATION,
    )

    rows = [
        {"username": "Admin", "score": 10},
        {"username": "admin", "score": 20},
        {"username": "ADMIN", "score": 30},
        {"username": "guest", "score": 5},
        {"username": "admin", "score": 25},  # update / replay on admin
    ]

    # Apply via bulk path
    bulk_state = {}
    bulk_result = contract.apply_bulk(bulk_state, rows)

    # Apply via row path
    row_state = {}
    for r in rows:
        contract.apply_row(row_state, r)

    assert bulk_state == row_state
    assert len(bulk_state) == 4
    assert bulk_state[("Admin",)]["score"] == 10
    assert bulk_state[("admin",)]["score"] == 25
    assert bulk_state[("ADMIN",)]["score"] == 30
    assert bulk_state[("guest",)]["score"] == 5

    assert verify_bulk_and_row_parity(rows, contract)


def test_replayed_bulk_batches_never_duplicate_effects():
    contract = IdentityContract(
        table="ledger",
        key_columns=("entry_id",),
        target_collation="utf8mb4_bin",
        strategy=KeyStrategy.BINARY_COLLATION,
    )

    batch_1 = [
        {"entry_id": "TxA", "amount": 100},
        {"entry_id": "txa", "amount": 200},
    ]

    state = {}
    res1 = contract.apply_bulk(state, batch_1)
    assert res1.inserted == 2
    assert res1.updated == 0

    # Replay identical batch
    res2 = contract.apply_bulk(state, batch_1)
    assert res2.inserted == 0
    assert res2.updated == 0
    assert res2.unchanged == 2
    assert len(state) == 2
    assert state[("TxA",)]["amount"] == 100
    assert state[("txa",)]["amount"] == 200


def test_composite_key_identity_semantics():
    contract = IdentityContract(
        table="workspace_members",
        key_columns=("workspace_id", "user_id"),
        target_collation="utf8mb4_bin",
        strategy=KeyStrategy.BINARY_COLLATION,
    )

    rows = [
        {"workspace_id": "WS_1", "user_id": "UserA", "role": "owner"},
        {"workspace_id": "WS_1", "user_id": "usera", "role": "viewer"},
        {"workspace_id": "ws_1", "user_id": "UserA", "role": "editor"},
    ]

    contract.validate_injectivity(rows)
    state = {}
    contract.apply_bulk(state, rows)
    assert len(state) == 3


def test_identity_contract_roundtrip_dict():
    contract = IdentityContract(
        table="orders",
        key_columns=("tenant_id", "order_id"),
        source_collation="utf8mb4_bin",
        target_collation="utf8mb4_0900_ai_ci",
        strategy=KeyStrategy.ENCODED_KEY,
        unicode_normalization="NFC",
    )

    d = contract.to_dict()
    restored = IdentityContract.from_dict(d)

    assert restored.table == contract.table
    assert restored.key_columns == contract.key_columns
    assert restored.source_collation == contract.source_collation
    assert restored.target_collation == contract.target_collation
    assert restored.strategy == contract.strategy
    assert restored.unicode_normalization == contract.unicode_normalization


def test_apply_row_refuses_colliding_key_when_existing_in_state():
    contract = IdentityContract(
        table="clients",
        key_columns=("client_id",),
        target_collation="utf8mb4_0900_ai_ci",
        strategy=KeyStrategy.REFUSE,
    )
    state = {}
    contract.apply_row(state, {"client_id": "CLIENT_ALPHA", "val": 10})
    assert len(state) == 1

    with pytest.raises(NonInjectiveKeyError) as exc_info:
        contract.apply_row(state, {"client_id": "client_alpha", "val": 20})

    err = exc_info.value
    assert err.table == "clients"
    assert err.reason == "case_sensitivity_collision"
    assert len(state) == 1


def test_surrogate_strategy_resolves_collisions():
    contract = IdentityContract(
        table="nodes",
        key_columns=("node_id",),
        target_collation="utf8mb4_0900_ai_ci",
        strategy=KeyStrategy.SURROGATE,
    )
    rows = [
        {"node_id": "NODE_A", "ip": "10.0.0.1"},
        {"node_id": "node_a", "ip": "10.0.0.2"},
    ]
    contract.validate_injectivity(rows)
    state = {}
    res = contract.apply_bulk(state, rows)
    assert res.inserted == 2
    assert len(state) == 2


def test_keyless_table_identity_contract():
    contract_allowed = IdentityContract(
        table="events_log",
        key_columns=(),
        allow_keyless=True,
        strategy=KeyStrategy.DIRECT,
    )
    row1 = {"event": "login", "user": "alice", "ts": 100}
    row2 = {"event": "login", "user": "alice", "ts": 100}
    row3 = {"event": "login", "user": "alice", "ts": 200}

    deduped = contract_allowed.dedup_batch([row1, row2, row3])
    assert len(deduped) == 2

    contract_disallowed = IdentityContract(
        table="events_log",
        key_columns=(),
        allow_keyless=False,
    )
    with pytest.raises(IdentityContractViolationError):
        contract_disallowed.source_identity(row1)


def test_pad_space_collation_collision_detected():
    keys = ["user", "user "]
    collision = detect_non_injective_representation(
        keys,
        target_collation="SQL_Latin1_General_CP1_CI_AS",
    )
    assert collision is not None
    k1, k2, reason = collision
    assert {k1, k2} == {"user", "user "}
    assert reason == "whitespace_padding_collision"


def test_compatibility_unicode_variants_detected():
    ligature = "\ufb01le"
    plain = "file"
    collision = detect_non_injective_representation(
        [ligature, plain],
        target_collation="utf8mb4_unicode_ci",
        unicode_normalization="NFKD",
    )
    assert collision is not None
    assert collision[2] == "unicode_normalization_collision"


def test_non_injective_key_error_properties_and_string():
    err = NonInjectiveKeyError(
        table="products",
        columns=("sku",),
        key1=("SKU-1",),
        key2=("sku-1",),
        target_collation="utf8mb4_general_ci",
        reason="case_sensitivity_collision",
    )
    msg = str(err)
    assert "products" in msg
    assert "sku" in msg
    assert "case_sensitivity_collision" in msg
    assert "utf8mb4_general_ci" in msg


def test_mixed_replayed_batch_updates_and_unchanged():
    contract = IdentityContract(
        table="inventory",
        key_columns=("item_code",),
        target_collation="utf8mb4_bin",
        strategy=KeyStrategy.BINARY_COLLATION,
    )

    batch_initial = [
        {"item_code": "A1", "count": 10},
        {"item_code": "a1", "count": 20},
        {"item_code": "B1", "count": 30},
    ]
    state = {}
    res1 = contract.apply_bulk(state, batch_initial)
    assert res1.inserted == 3
    assert res1.updated == 0
    assert res1.unchanged == 0

    batch_mixed = [
        {"item_code": "A1", "count": 10},  # unchanged
        {"item_code": "a1", "count": 25},  # updated
        {"item_code": "C1", "count": 40},  # new
    ]
    res2 = contract.apply_bulk(state, batch_mixed)
    assert res2.inserted == 1
    assert res2.updated == 1
    assert res2.unchanged == 1
    assert len(state) == 4
    assert state[("A1",)]["count"] == 10
    assert state[("a1",)]["count"] == 25
    assert state[("B1",)]["count"] == 30
    assert state[("C1",)]["count"] == 40


def test_identity_semantics_rungs_under_watched():
    with watched() as seen:
        # 1. direct_identity_representation holds
        f1 = {
            "injective_collation": True,
            "no_normalization_drift": True,
            "no_padding_collision": True,
            "target_supports_binary_collation": True,
            "can_alter_or_specify_collation": True,
            "schema_permits_encoded_key": True,
            "lossless_encoding_enabled": True,
            "surrogate_enabled": True,
        }
        c1 = decide_identity_strategy(f1)
        assert c1.rung.name == "direct_identity_representation"

        # 2. direct_identity_representation refuses (non-injective collation), binary_collation_adaptation holds
        f2 = {
            "injective_collation": False,
            "no_normalization_drift": True,
            "no_padding_collision": True,
            "target_supports_binary_collation": True,
            "can_alter_or_specify_collation": True,
            "schema_permits_encoded_key": True,
            "lossless_encoding_enabled": True,
            "surrogate_enabled": True,
        }
        c2 = decide_identity_strategy(f2)
        assert c2.rung.name == "binary_collation_adaptation"

        # 3. binary_collation_adaptation refuses, lossless_encoded_key holds
        f3 = {
            "injective_collation": False,
            "no_normalization_drift": True,
            "no_padding_collision": True,
            "target_supports_binary_collation": False,
            "can_alter_or_specify_collation": False,
            "schema_permits_encoded_key": True,
            "lossless_encoding_enabled": True,
            "surrogate_enabled": True,
        }
        c3 = decide_identity_strategy(f3)
        assert c3.rung.name == "lossless_encoded_key"

        # 4. lossless_encoded_key refuses, surrogate_key_assignment holds
        f4 = {
            "injective_collation": False,
            "no_normalization_drift": True,
            "no_padding_collision": True,
            "target_supports_binary_collation": False,
            "can_alter_or_specify_collation": False,
            "schema_permits_encoded_key": False,
            "lossless_encoding_enabled": False,
            "surrogate_enabled": True,
        }
        c4 = decide_identity_strategy(f4)
        assert c4.rung.name == "surrogate_key_assignment"

        # 5. surrogate_key_assignment refuses, refuse_colliding_identity holds
        f5 = {
            "injective_collation": False,
            "no_normalization_drift": True,
            "no_padding_collision": True,
            "target_supports_binary_collation": False,
            "can_alter_or_specify_collation": False,
            "schema_permits_encoded_key": False,
            "lossless_encoding_enabled": False,
            "surrogate_enabled": False,
        }
        c5 = decide_identity_strategy(f5)
        assert c5.rung.name == "refuse_colliding_identity"
