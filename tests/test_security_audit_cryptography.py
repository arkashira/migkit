"""Unit tests for security audit, cryptographic integrity, transparent encryption at rest,
scorecard hardening, format-preserving encryption, and immutable audit anchors
(backlog items 325/R17b.hmac, 326/R17b.prog, 223/F7.sec, 287/W4.b, 301/R5.anchor).
"""
import json
import os
from pathlib import Path
import stat
import pytest

from migkit import security_audit as sa


@pytest.fixture
def test_key():
    return b"0123456789abcdef0123456789abcdef"


# =====================================================================
# Item 325 (R17b.hmac): Keyed HMAC for shared reports
# =====================================================================

def test_sign_and_verify_report_data(test_key):
    report = {
        "hop": "prod-to-staging",
        "tables_inspected": 42,
        "summary": "all rows verified",
        "diffs": [],
    }
    envelope = sa.sign_report_data(report, test_key, metadata={"signer": "security-bot"})
    assert envelope["format"] == sa.REPORT_ENVELOPE_FORMAT
    assert envelope["algorithm"] == "HMAC-SHA256"
    assert "signature" in envelope
    assert envelope["payload"] == report

    ok, reason = sa.verify_report_data(envelope, test_key)
    assert ok is True
    assert reason == "OK"


def test_verify_report_data_detects_tampering(test_key):
    report = {"status": "clean", "count": 100}
    envelope = sa.sign_report_data(report, test_key)

    envelope["payload"]["count"] = 101
    ok, reason = sa.verify_report_data(envelope, test_key)
    assert ok is False
    assert "digest mismatch" in reason.lower()

    envelope["payload"]["count"] = 100
    envelope["payload_sha256"] = sa.hashlib.sha256(sa.canonical_json_bytes(envelope["payload"])).hexdigest()
    envelope["signature"] = "deadbeef" * 8
    ok, reason = sa.verify_report_data(envelope, test_key)
    assert ok is False
    assert "signature mismatch" in reason.lower()


def test_verify_report_data_wrong_key(test_key):
    report = {"status": "clean"}
    envelope = sa.sign_report_data(report, test_key)
    other_key = b"wrong-key-0000000000000000000000"
    ok, reason = sa.verify_report_data(envelope, other_key)
    assert ok is False
    assert "signature mismatch" in reason.lower()


def test_sign_and_verify_report_file(tmp_path, test_key):
    report_file = tmp_path / "findings.json"
    report_file.write_text('{"finding": "none", "score": 100}')
    sidecar_path = sa.sign_report_file(report_file, test_key)

    assert sidecar_path.is_file()
    assert stat.S_IMODE(sidecar_path.stat().st_mode) == 0o600

    ok, reason = sa.verify_report_file(report_file, test_key)
    assert ok is True
    assert reason == "OK"

    report_file.write_text('{"finding": "CRITICAL", "score": 0}')
    ok, reason = sa.verify_report_file(report_file, test_key)
    assert ok is False
    assert "digest mismatch" in reason.lower()


def test_derive_report_key():
    k1 = sa.derive_report_key("secret-seed-1", salt=b"salt-a")
    k2 = sa.derive_report_key("secret-seed-1", salt=b"salt-a")
    k3 = sa.derive_report_key("secret-seed-1", salt=b"salt-b")
    assert len(k1) == 32
    assert k1 == k2
    assert k1 != k3


# =====================================================================
# Item 326 (R17b.prog): Transparent encryption at rest for driven programs
# =====================================================================

def test_ephemeral_and_hop_keys():
    k1 = sa.generate_ephemeral_key()
    k2 = sa.generate_ephemeral_key()
    assert len(k1) == 32 and len(k2) == 32
    assert k1 != k2

    hk1 = sa.derive_hop_at_rest_key("orders-hop", master_secret=b"master-secret-123")
    hk2 = sa.derive_hop_at_rest_key("orders-hop", master_secret=b"master-secret-123")
    hk3 = sa.derive_hop_at_rest_key("users-hop", master_secret=b"master-secret-123")
    assert hk1 == hk2
    assert hk1 != hk3


def test_bytes_encryption_roundtrip(test_key):
    plain = b"pg_dump table orders COPY 12345 \n line 2"
    encrypted = sa.encrypt_bytes(plain, test_key)
    assert encrypted != plain
    assert encrypted.startswith(sa.ENC_MAGIC)

    decrypted = sa.decrypt_bytes(encrypted, test_key)
    assert decrypted == plain


def test_file_encryption_and_permissions(tmp_path, test_key):
    src = tmp_path / "dump.sql"
    src.write_text("CREATE TABLE test (id INT);")

    enc_path, used_key = sa.encrypt_file(src, key=test_key)
    assert enc_path.is_file()
    assert stat.S_IMODE(enc_path.stat().st_mode) == 0o600
    assert enc_path.read_bytes().startswith(sa.ENC_MAGIC)

    dec_path = sa.decrypt_file(enc_path, tmp_path / "restored.sql", key=used_key)
    assert dec_path.is_file()
    assert stat.S_IMODE(dec_path.stat().st_mode) == 0o600
    assert dec_path.read_text() == "CREATE TABLE test (id INT);"


def test_transparent_program_spool(tmp_path, test_key):
    spool_dir = tmp_path / "spool"
    with sa.TransparentProgramSpool(spool_dir, key=test_key, hop_name="test-hop") as spool:
        assert stat.S_IMODE(spool_dir.stat().st_mode) == 0o700

        dump_file = spool_dir / "table1.dat"
        dump_file.write_bytes(b"sensitive intermediate database dump contents")

        sub_dir = spool_dir / "sub"
        sub_dir.mkdir()
        (sub_dir / "table2.dat").write_bytes(b"another table chunk")

        sealed = spool.seal_directory()
        assert "table1.dat" in sealed
        assert "sub/table2.dat" in sealed

        assert not dump_file.exists()
        assert not (sub_dir / "table2.dat").exists()

        enc1 = spool_dir / "table1.dat.enc"
        assert enc1.is_file()
        assert stat.S_IMODE(enc1.stat().st_mode) == 0o600

        recovered_bytes = spool.read_sealed_bytes("table1.dat")
        assert recovered_bytes == b"sensitive intermediate database dump contents"

        unsealed_file = spool.unseal_file("sub/table2.dat")
        assert unsealed_file.is_file()
        assert stat.S_IMODE(unsealed_file.stat().st_mode) == 0o600
        assert unsealed_file.read_bytes() == b"another table chunk"

    assert not (spool_dir / "sub" / "table2.dat").exists()


# =====================================================================
# Item 223 (F7.sec): Security scorecard hardening
# =====================================================================

def test_permissions_enforcement(tmp_path):
    f = tmp_path / "secret.key"
    f.write_text("private")
    sa.enforce_file_permissions(f, 0o600)
    assert stat.S_IMODE(f.stat().st_mode) == 0o600

    d = tmp_path / "private_dir"
    d.mkdir()
    sa.enforce_directory_permissions(d, 0o700)
    assert stat.S_IMODE(d.stat().st_mode) == 0o700


def test_audit_and_harden_directory_tree(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    os.chmod(tree, 0o755)

    sub = tree / "subdir"
    sub.mkdir()
    os.chmod(sub, 0o777)

    f1 = sub / "loose.txt"
    f1.write_text("hello")
    os.chmod(f1, 0o644)

    violations = sa.audit_permissions(tree)
    assert len(violations) >= 2

    hardened_count = sa.harden_directory_tree(tree)
    assert hardened_count >= 3

    assert stat.S_IMODE(tree.stat().st_mode) == 0o700
    assert stat.S_IMODE(sub.stat().st_mode) == 0o700
    assert stat.S_IMODE(f1.stat().st_mode) == 0o600

    assert len(sa.audit_permissions(tree)) == 0


def test_token_redaction():
    text = "Authorization: Bearer eyJhbGciOi.eyJzdWIi.signature123 and token ghp_12345678901234567890"
    redacted = sa.redact_sensitive_tokens(text)
    assert "Bearer ***" in redacted
    assert "ghp_12345678901234567890" not in redacted

    url = "postgres://migrator:SuperSecret99@database.cloud.internal:5432/app"
    redacted_url = sa.redact_sensitive_tokens(url)
    assert "SuperSecret99" not in redacted_url
    assert "migrator:***@" in redacted_url

    cfg = {
        "endpoint": url,
        "api_secret": "sk-1234567890abcdef1234567890",
        "auth": {"jwt_token": "secret_jwt"},
        "nested": [{"password": "hidden_pass", "name": "app"}],
        "safe_param": "visible_value",
    }
    redacted_cfg = sa.redact_sensitive_tokens(cfg)
    assert redacted_cfg["api_secret"] == "***"
    assert redacted_cfg["auth"]["jwt_token"] == "***"
    assert redacted_cfg["nested"][0]["password"] == "***"
    assert redacted_cfg["nested"][0]["name"] == "app"
    assert redacted_cfg["safe_param"] == "visible_value"


def test_secure_wipe(tmp_path):
    f = tmp_path / "temp_sensitive.dat"
    f.write_bytes(b"A" * 1024)
    assert f.exists()
    sa.secure_wipe(f)
    assert not f.exists()


def test_secure_temp_contexts(tmp_path):
    with sa.SecureTempFile(dir=tmp_path) as p:
        assert p.is_file()
        assert stat.S_IMODE(p.stat().st_mode) == 0o600
        p.write_text("sensitive transient")
    assert not p.exists()

    with sa.SecureTempDir(dir=tmp_path) as d:
        assert d.is_dir()
        assert stat.S_IMODE(d.stat().st_mode) == 0o700
        (d / "temp.txt").write_text("scratch")
    assert not d.exists()


# =====================================================================
# Item 287 (W4.b): Format-preserving encryption and tokenization
# =====================================================================

def test_fpe_digits_cipher(test_key):
    cipher = sa.FormatPreservingCipher(alphabet=sa.FormatPreservingCipher.DIGITS)
    plaintext = "9876543210"
    encrypted = cipher.encrypt(plaintext, test_key, tweak=b"users.phone")
    assert len(encrypted) == len(plaintext)
    assert encrypted != plaintext
    assert encrypted.isdigit()

    decrypted = cipher.decrypt(encrypted, test_key, tweak=b"users.phone")
    assert decrypted == plaintext


def test_fpe_formatting_masks(test_key):
    cipher = sa.FormatPreservingCipher(alphabet=sa.FormatPreservingCipher.DIGITS)
    ssn = "123-45-6789"
    enc_ssn = cipher.encrypt(ssn, test_key, tweak=b"users.ssn")
    assert len(enc_ssn) == len(ssn)
    assert enc_ssn[3] == "-" and enc_ssn[6] == "-"
    assert enc_ssn != ssn

    dec_ssn = cipher.decrypt(enc_ssn, test_key, tweak=b"users.ssn")
    assert dec_ssn == ssn

    cc = "4111-2222-3333-4444"
    enc_cc = cipher.encrypt(cc, test_key, tweak=b"orders.cc", preserve_prefix=4, preserve_suffix=4)
    assert enc_cc.startswith("4111-")
    assert enc_cc.endswith("-4444")
    assert enc_cc != cc

    dec_cc = cipher.decrypt(enc_cc, test_key, tweak=b"orders.cc", preserve_prefix=4, preserve_suffix=4)
    assert dec_cc == cc


def test_fpe_alphanumeric_cipher(test_key):
    cipher = sa.FormatPreservingCipher(alphabet=sa.FormatPreservingCipher.ALPHANUMERIC)
    code = "PromoCode2026X"
    encrypted = cipher.encrypt(code, test_key)
    assert len(encrypted) == len(code)
    assert encrypted != code
    assert cipher.decrypt(encrypted, test_key) == code


def test_fpe_edge_cases(test_key):
    cipher = sa.FormatPreservingCipher(alphabet=sa.FormatPreservingCipher.DIGITS)
    assert cipher.encrypt("", test_key) == ""
    assert cipher.decrypt("", test_key) == ""
    assert cipher.encrypt("5", test_key).isdigit()
    assert cipher.decrypt(cipher.encrypt("5", test_key), test_key) == "5"


def test_in_flight_column_tokenizer(test_key):
    tokenizer = sa.InFlightColumnTokenizer(master_key=test_key)
    tokenizer.register_rule("users.ssn", mode="fpe", alphabet=sa.FormatPreservingCipher.DIGITS)
    tokenizer.register_rule("users.cc", mode="fpe", alphabet=sa.FormatPreservingCipher.DIGITS, preserve_prefix=4, preserve_suffix=4)
    tokenizer.register_rule("users.api_token", mode="hash_token")

    row = {
        "id": 101,
        "username": "alice",
        "ssn": "987-65-4321",
        "cc": "4000-1111-2222-9999",
        "api_token": "raw_secret_token_value",
    }
    tokenized = tokenizer.tokenize_row("users", row)
    assert tokenized["id"] == 101
    assert tokenized["username"] == "alice"
    assert tokenized["ssn"] != row["ssn"]
    assert tokenized["ssn"][3] == "-" and tokenized["ssn"][6] == "-"
    assert tokenized["cc"].startswith("4000-")
    assert tokenized["cc"].endswith("-9999")
    assert tokenized["api_token"].startswith("tok_")

    restored_ssn = tokenizer.detokenize_value("users", "ssn", tokenized["ssn"])
    assert restored_ssn == row["ssn"]

    restored_cc = tokenizer.detokenize_value("users", "cc", tokenized["cc"])
    assert restored_cc == row["cc"]


# =====================================================================
# Item 301 (R5.anchor): Signed audit checkpoints in immutable envelopes
# =====================================================================

def test_create_and_verify_checkpoint_envelope(test_key):
    envelope = sa.create_immutable_checkpoint_envelope(
        seq=150,
        head="a1b2c3d4e5f600112233445566778899aabbccddeeff00112233445566778899",
        signing_key=test_key,
        hop_name="hop-europe",
        entries_count=150,
        retention_days=180,
        mode="COMPLIANCE",
    )
    assert envelope["format"] == sa.CHECKPOINT_ENVELOPE_FORMAT
    assert envelope["immutable"] is True
    assert envelope["checkpoint"]["retention"]["mode"] == "COMPLIANCE"
    assert "signature" in envelope

    ok, reason = sa.verify_immutable_checkpoint_envelope(envelope, test_key)
    assert ok is True
    assert reason == "OK"


def test_checkpoint_envelope_tampering_detected(test_key):
    envelope = sa.create_immutable_checkpoint_envelope(
        seq=1,
        head="hash001",
        signing_key=test_key,
        hop_name="h1",
    )
    envelope["checkpoint"]["seq"] = 2
    ok, reason = sa.verify_immutable_checkpoint_envelope(envelope, test_key)
    assert ok is False
    assert "digest mismatch" in reason.lower()

    envelope["checkpoint"]["seq"] = 1
    envelope["signature"] = "deadbeef"
    ok, reason = sa.verify_immutable_checkpoint_envelope(envelope, test_key)
    assert ok is False
    assert "signature mismatch" in reason.lower()


def test_immutable_storage_vault(tmp_path, test_key):
    vault_dir = tmp_path / "vault"
    vault = sa.ImmutableStorageVault(vault_dir)

    env1 = sa.create_immutable_checkpoint_envelope(
        seq=1,
        head="hash_head_1",
        signing_key=test_key,
        hop_name="main-hop",
    )
    stored_path = vault.store_checkpoint(env1)
    assert stored_path.is_file()
    assert stat.S_IMODE(stored_path.stat().st_mode) == 0o400

    # Overwrite guard prevents modifying immutable checkpoint
    with pytest.raises(PermissionError, match="already anchored"):
        vault.store_checkpoint(env1, overwrite_guard=True)

    read_back = vault.read_checkpoint("main-hop", 1, "hash_head_1")
    assert read_back["checkpoint"]["seq"] == 1
    assert read_back["signature"] == env1["signature"]

    audit_res = vault.audit_vault_integrity(test_key)
    assert len(audit_res) == 1
    rel_path, ok, reason = audit_res[0]
    assert ok is True
    assert reason == "OK"

    # Test detection of unauthorized write permission change
    os.chmod(stored_path, 0o644)
    audit_res = vault.audit_vault_integrity(test_key)
    assert len(audit_res) == 1
    _, ok, reason = audit_res[0]
    assert ok is False
    assert "writable" in reason.lower()
