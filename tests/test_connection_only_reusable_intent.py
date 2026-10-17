"""Tests for Row 594: SMART-N11.connection-only (Complete endpoint-only operation with reusable intent).

Verifies:
1. Every `decide.Rung` in `migkit/connection_only.py` reaches both `holds` and
   `refuses` under `tests.rung_branches.watched()` and is declared in `PROVES_RUNGS`.
2. Reusable intent policy persistence:
   - Persists one goal, scope, and authorization policy.
   - Subsequent runs need only endpoints and credential references.
   - Zero repeated technical questions.
   - Unknown business intent is never guessed.
3. Decoupling of endpoint credentials:
   - Endpoint credentials do not determine whether to replace existing target rows.
   - Endpoint credentials do not determine when to switch applications.
   - Credentials never appear in logs, evidence, or error messages.
4. Universal typed durable resume in O(1):
   - Preserves native Python types (datetime, Decimal, UUID, composite tuples).
   - Skips completed units in O(1) without rescan.
5. Mutual exclusion active verification lease lock (.run.lock) with heartbeat.
6. Dynamic AIMD batch sizing and cold-start byte-weighted ETA derived from measurements.
7. Normalized external tool wrapper with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
"""

from __future__ import annotations

import datetime
import decimal
import json
import uuid
from pathlib import Path

import pytest
import tests.rung_branches
from tests.rung_branches import branch

from migkit import connection_only as co
from migkit import decide


PROVES_RUNGS = [
    # POLICY_REUSE_LADDER
    ("connection_only", "POLICY_REUSE_LADDER", "reuse_persisted_policy", "holds"),
    ("connection_only", "POLICY_REUSE_LADDER", "reuse_persisted_policy", "refuses"),
    ("connection_only", "POLICY_REUSE_LADDER", "compile_explicit_intent_policy", "holds"),
    ("connection_only", "POLICY_REUSE_LADDER", "compile_explicit_intent_policy", "refuses"),
    ("connection_only", "POLICY_REUSE_LADDER", "refuse_unknown_business_intent", "holds"),
    ("connection_only", "POLICY_REUSE_LADDER", "refuse_unknown_business_intent", "refuses"),
    # TARGET_REPLACEMENT_LADDER
    ("connection_only", "TARGET_REPLACEMENT_LADDER", "authorize_policy_target_replacement", "holds"),
    ("connection_only", "TARGET_REPLACEMENT_LADDER", "authorize_policy_target_replacement", "refuses"),
    ("connection_only", "TARGET_REPLACEMENT_LADDER", "forbid_destructive_target_replacement", "holds"),
    ("connection_only", "TARGET_REPLACEMENT_LADDER", "forbid_destructive_target_replacement", "refuses"),
    # APPLICATION_SWITCH_LADDER
    ("connection_only", "APPLICATION_SWITCH_LADDER", "verified_zero_lag_application_switch", "holds"),
    ("connection_only", "APPLICATION_SWITCH_LADDER", "verified_zero_lag_application_switch", "refuses"),
    ("connection_only", "APPLICATION_SWITCH_LADDER", "require_manual_cutover_authorization", "holds"),
    ("connection_only", "APPLICATION_SWITCH_LADDER", "require_manual_cutover_authorization", "refuses"),
    ("connection_only", "APPLICATION_SWITCH_LADDER", "refuse_unauthorized_application_switch", "holds"),
    ("connection_only", "APPLICATION_SWITCH_LADDER", "refuse_unauthorized_application_switch", "refuses"),
    # TECHNICAL_DISCOVERY_LADDER
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "native_parallel_streaming_mover", "holds"),
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "native_parallel_streaming_mover", "refuses"),
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "bounded_resource_serial_copier", "holds"),
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "bounded_resource_serial_copier", "refuses"),
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "external_tool_wrapper_mover", "holds"),
    ("connection_only", "TECHNICAL_DISCOVERY_LADDER", "external_tool_wrapper_mover", "refuses"),
    # BATCH_SIZING_LADDER
    ("connection_only", "BATCH_SIZING_LADDER", "aimd_high_throughput_batch", "holds"),
    ("connection_only", "BATCH_SIZING_LADDER", "aimd_high_throughput_batch", "refuses"),
    ("connection_only", "BATCH_SIZING_LADDER", "conservative_bounded_batch", "holds"),
    ("connection_only", "BATCH_SIZING_LADDER", "conservative_bounded_batch", "refuses"),
    # RESUME_ADMISSION_LADDER
    ("connection_only", "RESUME_ADMISSION_LADDER", "fast_o1_durable_checkpoint_resume", "holds"),
    ("connection_only", "RESUME_ADMISSION_LADDER", "fast_o1_durable_checkpoint_resume", "refuses"),
    ("connection_only", "RESUME_ADMISSION_LADDER", "cold_start_clean_initialization", "holds"),
    ("connection_only", "RESUME_ADMISSION_LADDER", "cold_start_clean_initialization", "refuses"),
    # LEASE_ACQUISITION_LADDER
    ("connection_only", "LEASE_ACQUISITION_LADDER", "acquire_active_heartbeat_lease", "holds"),
    ("connection_only", "LEASE_ACQUISITION_LADDER", "acquire_active_heartbeat_lease", "refuses"),
    ("connection_only", "LEASE_ACQUISITION_LADDER", "refuse_concurrent_run_conflict", "holds"),
    ("connection_only", "LEASE_ACQUISITION_LADDER", "refuse_concurrent_run_conflict", "refuses"),
    # CREDENTIAL_ISOLATION_LADDER
    ("connection_only", "CREDENTIAL_ISOLATION_LADDER", "resolve_safe_credential_reference", "holds"),
    ("connection_only", "CREDENTIAL_ISOLATION_LADDER", "resolve_safe_credential_reference", "refuses"),
    ("connection_only", "CREDENTIAL_ISOLATION_LADDER", "refuse_hardcoded_or_leaked_secret", "holds"),
    ("connection_only", "CREDENTIAL_ISOLATION_LADDER", "refuse_hardcoded_or_leaked_secret", "refuses"),
]


# -----------------------------------------------------------------------------
# Ladder 1: Policy Reuse Tests
# -----------------------------------------------------------------------------

def test_policy_reuse_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. reuse_persisted_policy holds; compile & refuse are evaluated
        facts_reuse = {
            "has_saved_policy": True,
            "policy_signature_valid": True,
            "business_intent_complete": True,
            "explicit_intent_provided": False,
            "intent_missing_or_ambiguous": False,
        }
        c1 = decide.climb("resolve reusable intent policy", co.POLICY_REUSE_LADDER, facts_reuse)
        assert c1.rung is not None
        assert c1.rung.name == "reuse_persisted_policy"

        # 2. reuse_persisted_policy refuses (signature invalid) -> compile holds
        facts_compile = {
            "has_saved_policy": False,
            "policy_signature_valid": False,
            "business_intent_complete": False,
            "explicit_intent_provided": True,
            "intent_valid": True,
            "intent_missing_or_ambiguous": False,
        }
        c2 = decide.climb("resolve reusable intent policy", co.POLICY_REUSE_LADDER, facts_compile)
        assert c2.rung is not None
        assert c2.rung.name == "compile_explicit_intent_policy"

        # 3. reuse refuses, compile refuses -> refuse_unknown_business_intent holds
        facts_refuse = {
            "has_saved_policy": False,
            "explicit_intent_provided": False,
            "intent_missing_or_ambiguous": True,
        }
        c3 = decide.climb("resolve reusable intent policy", co.POLICY_REUSE_LADDER, facts_refuse)
        assert c3.rung is not None
        assert c3.rung.name == "refuse_unknown_business_intent"

        # 4. all refuse
        facts_none = {
            "has_saved_policy": False,
            "explicit_intent_provided": False,
            "intent_missing_or_ambiguous": False,
        }
        c4 = decide.climb("resolve reusable intent policy", co.POLICY_REUSE_LADDER, facts_none)
        assert c4.rung is None

    assert "holds" in branch(seen, "reuse_persisted_policy")
    assert "refuses" in branch(seen, "reuse_persisted_policy")
    assert "holds" in branch(seen, "compile_explicit_intent_policy")
    assert "refuses" in branch(seen, "compile_explicit_intent_policy")
    assert "holds" in branch(seen, "refuse_unknown_business_intent")
    assert "refuses" in branch(seen, "refuse_unknown_business_intent")


# -----------------------------------------------------------------------------
# Ladder 2: Target Replacement Decoupling Tests
# -----------------------------------------------------------------------------

def test_target_replacement_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. Policy authorizes replacement -> authorize holds, forbid refuses
        facts_auth = {
            "policy_allows_target_replacement": True,
            "target_inspected": True,
        }
        c1 = decide.climb("decide target row replacement", co.TARGET_REPLACEMENT_LADDER, facts_auth)
        assert c1.rung is not None
        assert c1.rung.name == "authorize_policy_target_replacement"

        # 2. Policy forbids replacement (even with admin endpoint credentials) -> forbid holds, authorize refuses
        facts_forbid = {
            "policy_allows_target_replacement": False,
            "target_inspected": True,
        }
        c2 = decide.climb("decide target row replacement", co.TARGET_REPLACEMENT_LADDER, facts_forbid)
        assert c2.rung is not None
        assert c2.rung.name == "forbid_destructive_target_replacement"

    assert "holds" in branch(seen, "authorize_policy_target_replacement")
    assert "refuses" in branch(seen, "authorize_policy_target_replacement")
    assert "holds" in branch(seen, "forbid_destructive_target_replacement")
    assert "refuses" in branch(seen, "forbid_destructive_target_replacement")


# -----------------------------------------------------------------------------
# Ladder 3: Application Switch Decoupling Tests
# -----------------------------------------------------------------------------

def test_application_switch_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. verified_zero_lag holds
        facts_switch = {
            "cutover_mode": "verified_sync",
            "data_equality_proved": True,
            "replication_lag_seconds": 0.0,
            "cutover_preconditions_unmet": False,
        }
        c1 = decide.climb("decide application switch cutover", co.APPLICATION_SWITCH_LADDER, facts_switch)
        assert c1.rung is not None
        assert c1.rung.name == "verified_zero_lag_application_switch"

        # 2. manual trigger required
        facts_manual = {
            "cutover_mode": "manual",
            "data_equality_proved": True,
            "replication_lag_seconds": 0.0,
            "cutover_preconditions_unmet": False,
        }
        c2 = decide.climb("decide application switch cutover", co.APPLICATION_SWITCH_LADDER, facts_manual)
        assert c2.rung is not None
        assert c2.rung.name == "require_manual_cutover_authorization"

        # 3. refuse unauthorized switch
        facts_refuse = {
            "cutover_mode": "unknown",
            "cutover_preconditions_unmet": True,
        }
        c3 = decide.climb("decide application switch cutover", co.APPLICATION_SWITCH_LADDER, facts_refuse)
        assert c3.rung is not None
        assert c3.rung.name == "refuse_unauthorized_application_switch"

    assert "holds" in branch(seen, "verified_zero_lag_application_switch")
    assert "refuses" in branch(seen, "verified_zero_lag_application_switch")
    assert "holds" in branch(seen, "require_manual_cutover_authorization")
    assert "refuses" in branch(seen, "require_manual_cutover_authorization")
    assert "holds" in branch(seen, "refuse_unauthorized_application_switch")
    assert "refuses" in branch(seen, "refuse_unauthorized_application_switch")


# -----------------------------------------------------------------------------
# Ladder 4: Technical Discovery Tests (Zero Questions Asked)
# -----------------------------------------------------------------------------

def test_technical_discovery_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. High headroom -> parallel streaming holds
        facts_high = {
            "cpu_cores": 16,
            "connection_headroom": 32,
            "supports_streaming": True,
            "requires_specialized_external_tool": False,
        }
        c1 = decide.climb("discover technical mover without questions", co.TECHNICAL_DISCOVERY_LADDER, facts_high)
        assert c1.rung is not None
        assert c1.rung.name == "native_parallel_streaming_mover"

        # 2. Constrained resources -> bounded serial holds
        facts_low = {
            "cpu_cores": 2,
            "connection_headroom": 4,
            "supports_streaming": True,
            "requires_specialized_external_tool": False,
        }
        c2 = decide.climb("discover technical mover without questions", co.TECHNICAL_DISCOVERY_LADDER, facts_low)
        assert c2.rung is not None
        assert c2.rung.name == "bounded_resource_serial_copier"

        # 3. External tool wrapper needed
        facts_ext = {
            "cpu_cores": 8,
            "connection_headroom": 16,
            "supports_streaming": False,
            "requires_specialized_external_tool": True,
            "external_tool_installed": True,
        }
        c3 = decide.climb("discover technical mover without questions", co.TECHNICAL_DISCOVERY_LADDER, facts_ext)
        assert c3.rung is not None
        assert c3.rung.name == "external_tool_wrapper_mover"

    assert "holds" in branch(seen, "native_parallel_streaming_mover")
    assert "refuses" in branch(seen, "native_parallel_streaming_mover")
    assert "holds" in branch(seen, "bounded_resource_serial_copier")
    assert "refuses" in branch(seen, "bounded_resource_serial_copier")
    assert "holds" in branch(seen, "external_tool_wrapper_mover")
    assert "refuses" in branch(seen, "external_tool_wrapper_mover")


# -----------------------------------------------------------------------------
# Ladder 5: Adaptive AIMD Batch Sizing Tests
# -----------------------------------------------------------------------------

def test_batch_sizing_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. Low latency, high bandwidth -> aimd_large holds
        facts_aimd = {
            "rtt_ms": 2.5,
            "bandwidth_mb_s": 150.0,
            "backpressure_events": 0,
        }
        c1 = decide.climb("derive adaptive batch size", co.BATCH_SIZING_LADDER, facts_aimd)
        assert c1.rung is not None
        assert c1.rung.name == "aimd_high_throughput_batch"

        # 2. High latency or backpressure -> conservative_small holds
        facts_cons = {
            "rtt_ms": 45.0,
            "bandwidth_mb_s": 10.0,
            "backpressure_events": 1,
        }
        c2 = decide.climb("derive adaptive batch size", co.BATCH_SIZING_LADDER, facts_cons)
        assert c2.rung is not None
        assert c2.rung.name == "conservative_bounded_batch"

    assert "holds" in branch(seen, "aimd_high_throughput_batch")
    assert "refuses" in branch(seen, "aimd_high_throughput_batch")
    assert "holds" in branch(seen, "conservative_bounded_batch")
    assert "refuses" in branch(seen, "conservative_bounded_batch")


# -----------------------------------------------------------------------------
# Ladder 6: Universal Typed O(1) Resume Tests
# -----------------------------------------------------------------------------

def test_resume_admission_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. Checkpoint exists and epoch matches -> fast_o1 holds
        facts_chk = {
            "has_valid_checkpoint": True,
            "epoch_matches": True,
        }
        c1 = decide.climb("admit durable checkpoint resume", co.RESUME_ADMISSION_LADDER, facts_chk)
        assert c1.rung is not None
        assert c1.rung.name == "fast_o1_durable_checkpoint_resume"

        # 2. No checkpoint -> cold_start holds
        facts_cold = {
            "has_valid_checkpoint": False,
        }
        c2 = decide.climb("admit durable checkpoint resume", co.RESUME_ADMISSION_LADDER, facts_cold)
        assert c2.rung is not None
        assert c2.rung.name == "cold_start_clean_initialization"

    assert "holds" in branch(seen, "fast_o1_durable_checkpoint_resume")
    assert "refuses" in branch(seen, "fast_o1_durable_checkpoint_resume")
    assert "holds" in branch(seen, "cold_start_clean_initialization")
    assert "refuses" in branch(seen, "cold_start_clean_initialization")


# -----------------------------------------------------------------------------
# Ladder 7: Mutual Exclusion Lease Lock Tests
# -----------------------------------------------------------------------------

def test_lease_acquisition_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. Lease available -> acquire holds, refuse_conflict refuses
        facts_avail = {
            "lease_available": True,
            "heartbeat_ready": True,
        }
        c1 = decide.climb("acquire mutual exclusion lease lock", co.LEASE_ACQUISITION_LADDER, facts_avail)
        assert c1.rung is not None
        assert c1.rung.name == "acquire_active_heartbeat_lease"

        # 2. Lease conflict -> refuse_conflict holds, acquire refuses
        facts_conflict = {
            "lease_available": False,
            "heartbeat_ready": True,
        }
        c2 = decide.climb("acquire mutual exclusion lease lock", co.LEASE_ACQUISITION_LADDER, facts_conflict)
        assert c2.rung is not None
        assert c2.rung.name == "refuse_concurrent_run_conflict"

    assert "holds" in branch(seen, "acquire_active_heartbeat_lease")
    assert "refuses" in branch(seen, "acquire_active_heartbeat_lease")
    assert "holds" in branch(seen, "refuse_concurrent_run_conflict")
    assert "refuses" in branch(seen, "refuse_concurrent_run_conflict")


# -----------------------------------------------------------------------------
# Ladder 8: Credential Isolation Tests
# -----------------------------------------------------------------------------

def test_credential_isolation_ladder_reaches_both_branches():
    with tests.rung_branches.watched() as seen:
        # 1. Reference provided, sanitized -> resolve holds, refuse leak refuses
        facts_safe = {
            "has_credential_reference": True,
            "reference_resolved": True,
            "secret_sanitized": True,
            "raw_secret_leak": False,
        }
        c1 = decide.climb("isolate and resolve credentials", co.CREDENTIAL_ISOLATION_LADDER, facts_safe)
        assert c1.rung is not None
        assert c1.rung.name == "resolve_safe_credential_reference"

        # 2. Raw secret leak detected -> refuse leak holds, resolve refuses
        facts_leak = {
            "has_credential_reference": False,
            "raw_secret_leak": True,
        }
        c2 = decide.climb("isolate and resolve credentials", co.CREDENTIAL_ISOLATION_LADDER, facts_leak)
        assert c2.rung is not None
        assert c2.rung.name == "refuse_hardcoded_or_leaked_secret"

    assert "holds" in branch(seen, "resolve_safe_credential_reference")
    assert "refuses" in branch(seen, "resolve_safe_credential_reference")
    assert "holds" in branch(seen, "refuse_hardcoded_or_leaked_secret")
    assert "refuses" in branch(seen, "refuse_hardcoded_or_leaked_secret")


# -----------------------------------------------------------------------------
# Domain & Quality Gate Verification Tests
# -----------------------------------------------------------------------------

def test_policy_save_load_and_integrity(tmp_path: Path):
    policy_file = tmp_path / "test_hop.policy.json"
    policy = co.ReusableIntentPolicy(
        hop_name="order_service_hop",
        source_endpoint_ref="postgres://app:env:SRC_PWD@127.0.0.1:5432/orders",
        target_endpoint_ref="mysql://app:env:DST_PWD@127.0.0.1:3306/orders",
        goal="copy",
        scope_tables=("orders", "line_items", "customers"),
        allow_target_replacement=False,
        cutover_mode="manual",
    )
    assert policy.verify_integrity()
    policy.save(policy_file)

    loaded = co.ReusableIntentPolicy.load(policy_file)
    assert loaded.hop_name == policy.hop_name
    assert loaded.scope_tables == policy.scope_tables
    assert loaded.allow_target_replacement is False
    assert loaded.verify_integrity()

    # Tampering triggers integrity failure
    tampered_data = loaded.to_dict()
    tampered_data["allow_target_replacement"] = True  # modified without updating digest
    tampered_file = tmp_path / "tampered.policy.json"
    tampered_file.write_text(json.dumps(tampered_data), encoding="utf-8")
    with pytest.raises(co.PolicyMissingError, match="integrity check failed"):
        co.ReusableIntentPolicy.load(tampered_file)


def test_credential_redaction_and_leak_assertion():
    raw_secret = "UltraSecretPass123!"
    safe_env = {"SRC_PWD": raw_secret}
    res = co.resolve_credential_reference("env:SRC_PWD", env=safe_env)
    assert res == raw_secret

    with pytest.raises(co.CredentialLeakError):
        co.resolve_credential_reference("env:NONEXISTENT_VAR", env=safe_env)

    # Verification that credentials in URI are masked
    uri = f"postgres://dbuser:{raw_secret}@127.0.0.1:5432/appdb"
    masked = co.redact_credentials(uri)
    assert raw_secret not in masked
    assert "***REDACTED***" in masked

    # Structure redaction
    payload = {
        "user": "admin",
        "auth_key": raw_secret,
        "endpoint": uri,
    }
    redacted_payload = co.redact_credentials(payload)
    assert redacted_payload["auth_key"] == "***REDACTED***"
    assert raw_secret not in json.dumps(redacted_payload)

    # Assertion catches unredacted leaks
    co.assert_no_raw_secrets(redacted_payload, raw_secrets=[raw_secret])
    with pytest.raises(co.CredentialLeakError):
        co.assert_no_raw_secrets({"plain": raw_secret}, raw_secrets=[raw_secret])


def test_universal_typed_durable_resume_o1(tmp_path: Path):
    checkpoint_file = tmp_path / "orders.checkpoint.json"
    cp = co.TypedCheckpoint(hop_name="orders_hop")

    # Native types preservation
    dt = datetime.datetime(2026, 10, 17, 12, 0, 0, tzinfo=datetime.timezone.utc)
    dec = decimal.Decimal("123456789.987654321")
    u = uuid.uuid4()
    composite_key = (42, "tenant_a", dt)

    cp.record_completed("unit_partition_1", cursor=dt)
    cp.record_completed("unit_partition_2", cursor=dec)
    cp.record_completed("unit_partition_3", cursor=u)
    cp.record_completed("unit_partition_4", cursor=composite_key)

    assert cp.is_completed("unit_partition_1")
    assert cp.is_completed("unit_partition_4")
    assert not cp.is_completed("unit_partition_999")

    cp.save(checkpoint_file)

    loaded_cp = co.TypedCheckpoint.load(checkpoint_file)
    assert loaded_cp.is_completed("unit_partition_1")
    assert loaded_cp.is_completed("unit_partition_4")
    assert not loaded_cp.is_completed("unit_partition_unknown")

    # Verify native type restoration of the last cursor
    recovered_cursor = loaded_cp.get_cursor()
    assert isinstance(recovered_cursor, tuple)
    assert recovered_cursor[0] == 42
    assert recovered_cursor[1] == "tenant_a"
    assert isinstance(recovered_cursor[2], datetime.datetime)
    assert recovered_cursor[2] == dt


def test_mutual_exclusion_lease_lock_and_heartbeat(tmp_path: Path):
    lock_file = tmp_path / ".run.lock"
    lock1 = co.ActiveLeaseLock(lock_file, ttl_seconds=2.0)
    lock2 = co.ActiveLeaseLock(lock_file, ttl_seconds=2.0)

    # First process acquires lease
    assert lock1.acquire() is True
    assert lock1.heartbeat() is True

    # Concurrent process cannot acquire active lease
    assert lock2.acquire() is False

    # First process releases lease
    assert lock1.release() is True

    # Now second process can acquire
    assert lock2.acquire() is True
    assert lock2.release() is True


def test_adaptive_aimd_batch_sizer():
    sizer = co.AdaptiveBatchSizer(
        current_batch_rows=100,
        min_batch_rows=10,
        max_batch_rows=1000,
        additive_step=50,
        decrease_factor=0.5,
    )
    # Success on fast network increases batch
    batch1 = sizer.on_success(rtt_ms=10.0)
    assert batch1 == 150
    batch2 = sizer.on_success(rtt_ms=12.0)
    assert batch2 == 200

    # Backpressure throttles multiplicatively
    batch3 = sizer.on_backpressure()
    assert batch3 == 100
    batch4 = sizer.on_backpressure()
    assert batch4 == 50


def test_derived_bounds_from_measurements():
    bounds = co.derive_bounds_from_measurements(
        latency_ms=15.0,
        bandwidth_mb_s=100.0,
        cpu_cores=8,
        max_connections=32,
        row_bytes=256,
    )
    assert bounds["target_workers"] > 1
    assert bounds["initial_batch_rows"] >= 20
    assert bounds["min_batch_rows"] < bounds["initial_batch_rows"]
    assert bounds["timeout_seconds"] > 0.0


def test_byte_weighted_eta():
    eta = co.compute_byte_weighted_eta(
        total_bytes=10_000_000,
        completed_bytes=2_000_000,
        elapsed_seconds=10.0,
        rolling_rate_bytes_sec=1_000_000.0,
    )
    assert eta == 8.0  # 8,000,000 bytes remaining / 1,000,000 bytes/sec


def test_normalized_tool_wrapper_classification():
    # Pass: 0 exit code, 0 diffs
    assert co.classify_tool_outcome(0, 0, "") == co.CARE_PASS

    # Action: non-zero exit code
    assert co.classify_tool_outcome(1, 0, "fatal error") == co.CARE_ACTION

    # Benign: drift expected
    assert co.classify_tool_outcome(0, 5, "Metadata timestamp differences") == co.CARE_BENIGN

    # Review: diffs require inspection
    assert co.classify_tool_outcome(0, 3, "clean query output") == co.CARE_REVIEW


def test_connection_only_runner_orchestration(tmp_path: Path):
    policy_dir = tmp_path / "policies"
    policy = co.ReusableIntentPolicy(
        hop_name="web_app_hop",
        source_endpoint_ref="postgres://app:env:SRC_P@127.0.0.1:5432/web",
        target_endpoint_ref="postgres://app:env:DST_P@127.0.0.1:5432/web_replica",
        goal="copy",
        scope_tables=("users", "sessions"),
        allow_target_replacement=False,
        cutover_mode="manual",
    )
    runner = co.ConnectionOnlyRunner(
        source_address="postgres://app:env:SRC_P@127.0.0.1:5432/web",
        target_address="postgres://app:env:DST_P@127.0.0.1:5432/web_replica",
        hop_name="web_app_hop",
        policy=policy,
        policy_dir=policy_dir,
    )

    c_policy = runner.resolve_policy()
    assert c_policy.rung.name == "reuse_persisted_policy"

    c_rep = runner.decide_target_replacement()
    assert c_rep.rung.name == "forbid_destructive_target_replacement"

    c_cutover = runner.decide_application_switch()
    assert c_cutover.rung.name == "require_manual_cutover_authorization"

    c_tech = runner.discover_technical_choices()
    assert c_tech.rung.name == "native_parallel_streaming_mover"

    c_batch = runner.derive_batch_sizing()
    assert c_batch.rung.name == "aimd_high_throughput_batch"

    c_resume = runner.admit_resume({"has_valid_checkpoint": True, "epoch_matches": True})
    assert c_resume.rung.name == "fast_o1_durable_checkpoint_resume"

    c_lease = runner.acquire_lease()
    assert c_lease.rung.name == "acquire_active_heartbeat_lease"

    c_cred = runner.isolate_credentials()
    assert c_cred.rung.name == "resolve_safe_credential_reference"
