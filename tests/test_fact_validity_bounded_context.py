"""Tests for Row 585: SMART-N2.fact-validity (Make facts and decisions valid for a bounded context).

Verifies:
1. Every decide.Rung in `migkit/fact_validity.py` across all 4 ladders
   (`FACT_VALIDITY_LADDER`, `DEPENDENCY_INVALIDATION_LADDER`,
   `CRITICAL_STEP_VERIFICATION_LADDER`, `PROBE_DEDUPLICATION_LADDER`) reaches
   both `holds` and `refuses` under `tests.rung_branches.watched()`.
2. Facts/factbook extended with provenance, observation time, server/config/schema
   epoch vector, confidence, and validity scope.
3. Stale facts cannot count as current proof; attempting to read an expired fact
   without a refresh reader raises `StaleFactError`.
4. Dependency map selectively invalidates only decisions whose inputs changed
   after failover, DDL, permission, packet, quota, or network changes, while
   unrelated work is not restarted.
5. Re-read critical inputs before irreversible steps: cutover forces fresh re-reading,
   blocks on detected drift (`ACTION`), and proceeds on concordant state (`PASS`).
6. Concurrent probe deduplication ensures identical in-flight probes do not hammer
   the source database.
7. Probe concurrency and backoff intervals are dynamically derived from measured
   source load and query latency rather than arbitrary hardcoded magic numbers.
8. Secret values, credentials, tokens, and passwords are redacted across all
   account summaries and exported dictionaries.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import concurrent.futures
import time
from typing import Any, Dict, List, Set, Tuple

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

from migkit import decide
from migkit import fact_validity as fv


PROVES_RUNGS = [
    # FACT_VALIDITY_LADDER
    ("fact_validity", "FACT_VALIDITY_LADDER", "trusted_in_bounded_context", "holds"),
    ("fact_validity", "FACT_VALIDITY_LADDER", "trusted_in_bounded_context", "refuses"),
    ("fact_validity", "FACT_VALIDITY_LADDER", "refresh_stale_fact", "holds"),
    ("fact_validity", "FACT_VALIDITY_LADDER", "refresh_stale_fact", "refuses"),
    ("fact_validity", "FACT_VALIDITY_LADDER", "refuse_cross_context_leak", "holds"),
    ("fact_validity", "FACT_VALIDITY_LADDER", "refuse_cross_context_leak", "refuses"),

    # DEPENDENCY_INVALIDATION_LADDER
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "selective_invalidation_only", "holds"),
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "selective_invalidation_only", "refuses"),
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "context_wide_invalidation", "holds"),
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "context_wide_invalidation", "refuses"),
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "refuse_untracked_epoch_mutation", "holds"),
    ("fact_validity", "DEPENDENCY_INVALIDATION_LADDER", "refuse_untracked_epoch_mutation", "refuses"),

    # CRITICAL_STEP_VERIFICATION_LADDER
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "proceed_irreversible_step", "holds"),
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "proceed_irreversible_step", "refuses"),
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "force_reread_critical_inputs", "holds"),
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "force_reread_critical_inputs", "refuses"),
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "refuse_irreversible_on_drift", "holds"),
    ("fact_validity", "CRITICAL_STEP_VERIFICATION_LADDER", "refuse_irreversible_on_drift", "refuses"),

    # PROBE_DEDUPLICATION_LADDER
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "deduplicate_in_flight_probe", "holds"),
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "deduplicate_in_flight_probe", "refuses"),
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "execute_bounded_probe", "holds"),
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "execute_bounded_probe", "refuses"),
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "throttle_probe_budget_exceeded", "holds"),
    ("fact_validity", "PROBE_DEDUPLICATION_LADDER", "throttle_probe_budget_exceeded", "refuses"),
]


# -----------------------------------------------------------------------------
# Test 1: Exercise Every Rung on Holds and Refuses under watched()
# -----------------------------------------------------------------------------

def test_all_fact_validity_rungs_hold_and_refuse_under_watched() -> None:
    """Explicitly verify and record both holds and refuses for all 12 rungs."""

    cases = [
        # FACT_VALIDITY_LADDER: trusted_in_bounded_context
        (
            fv.FACT_VALIDITY_LADDER[0],
            {"scope_matches": True, "epoch_compatible": True, "ttl_fresh": True, "confidence": 1.0, "min_confidence": 0.5},
            {"scope_matches": True, "epoch_compatible": False, "ttl_fresh": True, "confidence": 1.0, "min_confidence": 0.5},
        ),
        # FACT_VALIDITY_LADDER: refresh_stale_fact
        (
            fv.FACT_VALIDITY_LADDER[1],
            {"is_stale": True, "can_refresh": True},
            {"is_stale": False, "can_refresh": True},
        ),
        # FACT_VALIDITY_LADDER: refuse_cross_context_leak
        (
            fv.FACT_VALIDITY_LADDER[2],
            {"cross_context_leak": True},
            {"cross_context_leak": False},
        ),

        # DEPENDENCY_INVALIDATION_LADDER: selective_invalidation_only
        (
            fv.DEPENDENCY_INVALIDATION_LADDER[0],
            {"valid_epoch_transition": True, "catastrophic_failover": False, "dependency_map_ready": True},
            {"valid_epoch_transition": True, "catastrophic_failover": True, "dependency_map_ready": True},
        ),
        # DEPENDENCY_INVALIDATION_LADDER: context_wide_invalidation
        (
            fv.DEPENDENCY_INVALIDATION_LADDER[1],
            {"catastrophic_failover": True},
            {"catastrophic_failover": False},
        ),
        # DEPENDENCY_INVALIDATION_LADDER: refuse_untracked_epoch_mutation
        (
            fv.DEPENDENCY_INVALIDATION_LADDER[2],
            {"untracked_mutation": True},
            {"untracked_mutation": False},
        ),

        # CRITICAL_STEP_VERIFICATION_LADDER: proceed_irreversible_step
        (
            fv.CRITICAL_STEP_VERIFICATION_LADDER[0],
            {"critical_inputs_fresh": True, "critical_confidence": 1.0, "safety_drift_detected": False, "probes_in_flight": 0},
            {"critical_inputs_fresh": True, "critical_confidence": 1.0, "safety_drift_detected": True, "probes_in_flight": 0},
        ),
        # CRITICAL_STEP_VERIFICATION_LADDER: force_reread_critical_inputs
        (
            fv.CRITICAL_STEP_VERIFICATION_LADDER[1],
            {"irreversible_step": True, "has_stale_critical_inputs": True},
            {"irreversible_step": True, "has_stale_critical_inputs": False},
        ),
        # CRITICAL_STEP_VERIFICATION_LADDER: refuse_irreversible_on_drift
        (
            fv.CRITICAL_STEP_VERIFICATION_LADDER[2],
            {"safety_drift_detected": True},
            {"safety_drift_detected": False},
        ),

        # PROBE_DEDUPLICATION_LADDER: deduplicate_in_flight_probe
        (
            fv.PROBE_DEDUPLICATION_LADDER[0],
            {"matching_probe_in_flight": True},
            {"matching_probe_in_flight": False},
        ),
        # PROBE_DEDUPLICATION_LADDER: execute_bounded_probe
        (
            fv.PROBE_DEDUPLICATION_LADDER[1],
            {"matching_probe_in_flight": False, "source_load_safe": True},
            {"matching_probe_in_flight": True, "source_load_safe": True},
        ),
        # PROBE_DEDUPLICATION_LADDER: throttle_probe_budget_exceeded
        (
            fv.PROBE_DEDUPLICATION_LADDER[2],
            {"source_load_safe": False},
            {"source_load_safe": True},
        ),
    ]

    with tests.rung_branches.watched() as seen:
        for rung, ok_facts, bad_facts in cases:
            c_ok = decide.climb(f"test_holds_{rung.name}", [rung], decide.Facts(ok_facts))
            assert c_ok.rung is rung, f"Expected {rung.name} to hold on ok_facts"

            c_bad = decide.climb(f"test_refuses_{rung.name}", [rung], decide.Facts(bad_facts))
            assert c_bad.rung is None, f"Expected {rung.name} to refuse on bad_facts"

        # Verify seen records both HOLDS and REFUSES for every rung
        for rung, _, _ in cases:
            branches = branch(seen, rung.name)
            assert HOLDS in branches, f"Missing HOLDS for {rung.name}"
            assert REFUSES in branches, f"Missing REFUSES for {rung.name}"


# -----------------------------------------------------------------------------
# Test 2: Epoch Vectors and Compatibility
# -----------------------------------------------------------------------------

def test_epoch_vector_bump_and_diff() -> None:
    ev1 = fv.EpochVector(context_id="tenant-pg-01")
    assert ev1.get_epoch(fv.EpochKind.DDL) == 0
    assert ev1.generation == 0

    ev2 = ev1.bump(fv.EpochKind.DDL, delta=1)
    assert ev2.get_epoch(fv.EpochKind.DDL) == 1
    assert ev2.generation == 1
    assert not ev1.is_compatible(ev2)
    assert ev1.diff(ev2) == {fv.EpochKind.DDL}

    ev3 = ev2.bump(fv.EpochKind.PERMISSION, delta=2)
    assert ev3.get_epoch(fv.EpochKind.PERMISSION) == 2
    assert ev3.generation == 2
    assert ev1.diff(ev3) == {fv.EpochKind.DDL, fv.EpochKind.PERMISSION}

    with pytest.raises(fv.InvalidEpochMutationError):
        ev1.bump(fv.EpochKind.DDL, delta=0)

    with pytest.raises(fv.InvalidEpochMutationError):
        ev1.get_epoch("nonexistent_epoch")


# -----------------------------------------------------------------------------
# Test 3: Contextual Facts, Stale Facts Cannot Count as Proof
# -----------------------------------------------------------------------------

def test_stale_fact_cannot_count_as_proof() -> None:
    epochs = fv.EpochVector(context_id="tenant-db")
    facts = fv.ContextualFacts(context_id="tenant-db", epoch_vector=epochs, default_ttl_s=1.0)

    # Put a fact with 0.1s TTL
    prov = fv.FactProvenance(
        source="test_probe",
        how="read",
        observed_at_s=time.time() - 2.0,  # Observed 2 seconds ago
        ttl_s=0.5,
        epoch_vector=epochs,
        confidence=1.0,
        validity_scope="server",
    )
    facts.put_fact("table_row_count", 45000, provenance=prov)

    # Fact is stale: accessing it without a refresh reader must raise StaleFactError
    with pytest.raises(fv.StaleFactError):
        _ = facts["table_row_count"]


def test_stale_fact_refreshes_automatically_with_reader() -> None:
    epochs = fv.EpochVector(context_id="tenant-db")
    probe_counter = 0

    def read_row_count() -> int:
        nonlocal probe_counter
        probe_counter += 1
        return 50000 + probe_counter

    facts = fv.ContextualFacts(
        context_id="tenant-db",
        epoch_vector=epochs,
        default_ttl_s=0.1,
        table_row_count=read_row_count,
    )

    # Initial lazy read
    val1 = facts["table_row_count"]
    assert val1 == 50001
    assert probe_counter == 1

    # Fast read: fresh within TTL, no new probe
    val2 = facts["table_row_count"]
    assert val2 == 50001
    assert probe_counter == 1

    # Expire TTL
    time.sleep(0.15)

    # Accessing stale fact invokes reader and updates cache
    val3 = facts["table_row_count"]
    assert val3 == 50002
    assert probe_counter == 2


# -----------------------------------------------------------------------------
# Test 4: Selective Invalidation Preserves Unrelated Work
# -----------------------------------------------------------------------------

def test_selective_invalidation_preserves_unrelated_decisions() -> None:
    epochs = fv.EpochVector(context_id="tenant-orders")
    facts = fv.ContextualFacts(
        context_id="tenant-orders",
        epoch_vector=epochs,
        known={
            "orders_table_schema": "CREATE TABLE orders (id INT, total NUMERIC)",
            "network_rtt_ms": 2.5,
            "max_allowed_packet": 67108864,
            "quota_rate_limit": 5000,
            "db_grants": "SELECT, INSERT",
        },
    )

    dep_map = fv.DecisionDependencyMap()

    # Decision 1: Table schema transform decision (depends on orders_table_schema)
    cp1 = facts.checkpoint_asked()
    choice1 = decide.climb(
        "plan_orders_table_transform",
        [decide.Rung("use_binary_copy", "use binary copy", needs=(decide.Need("schema present", lambda f: bool(f.get("orders_table_schema"))),))],
        facts,
    )
    dep_map.record_decision("work_orders_table", choice1, epochs, asked_facts=facts.asked_since(cp1))

    # Decision 2: Network stream pipeline (depends on network_rtt_ms and max_allowed_packet)
    cp2 = facts.checkpoint_asked()
    choice2 = decide.climb(
        "plan_network_stream",
        [decide.Rung("use_framed_socket", "use framed socket", needs=(
            decide.Need("packet ok", lambda f: int(f.get("max_allowed_packet", 0)) > 1024),
            decide.Need("rtt ok", lambda f: float(f.get("network_rtt_ms", 999)) < 10.0),
        ))],
        facts,
    )
    dep_map.record_decision("work_network_stream", choice2, epochs, asked_facts=facts.asked_since(cp2))

    # Decision 3: Rate limit throttler (depends on quota_rate_limit)
    cp3 = facts.checkpoint_asked()
    choice3 = decide.climb(
        "plan_throttler",
        [decide.Rung("use_token_bucket", "use token bucket", needs=(decide.Need("quota ok", lambda f: int(f.get("quota_rate_limit", 0)) > 0),))],
        facts,
    )
    dep_map.record_decision("work_throttler", choice3, epochs, asked_facts=facts.asked_since(cp3))

    # Mutate DDL epoch only (e.g. column added to orders table)
    new_epochs = epochs.bump(fv.EpochKind.DDL)
    facts.epoch_vector = new_epochs

    report = dep_map.selective_invalidate(
        context_id="tenant-orders",
        changed_epochs={fv.EpochKind.DDL},
        current_epochs=new_epochs,
        facts=facts,
    )

    # Verify selective invalidation:
    # 1. orders_table_schema was invalidated
    assert "orders_table_schema" in report.invalidated_facts
    # 2. Network and quota facts were retained
    assert "network_rtt_ms" in report.retained_facts
    assert "max_allowed_packet" in report.retained_facts
    assert "quota_rate_limit" in report.retained_facts
    # 3. Decision for orders table was invalidated
    assert "work_orders_table" in report.invalidated_decisions
    # 4. CRITICAL PROOF: Unrelated work (network stream & throttler) was NOT restarted
    assert "work_network_stream" in report.retained_decisions
    assert "work_throttler" in report.retained_decisions
    assert report.care_badge == fv.CARE_REVIEW


def test_catastrophic_failover_triggers_context_wide_invalidation() -> None:
    epochs = fv.EpochVector(context_id="tenant-01")
    facts = fv.ContextualFacts(
        context_id="tenant-01",
        epoch_vector=epochs,
        known={"server_primary_uuid": "srv-101", "replica_lag": 0.2},
    )
    dep_map = fv.DecisionDependencyMap()
    choice = decide.climb(
        "work_primary_election",
        [decide.Rung("primary_ok", "primary ok", needs=(decide.Need("srv", lambda f: bool(f.get("server_primary_uuid"))),))],
        facts,
    )
    dep_map.record_decision("work_primary", choice, epochs)

    new_epochs = epochs.bump(fv.EpochKind.FAILOVER)
    report = dep_map.selective_invalidate(
        context_id="tenant-01",
        changed_epochs={fv.EpochKind.FAILOVER},
        current_epochs=new_epochs,
        facts=facts,
        catastrophic_failover=True,
    )

    assert "work_primary" in report.invalidated_decisions
    assert len(report.retained_decisions) == 0
    assert report.care_badge == fv.CARE_ACTION


# -----------------------------------------------------------------------------
# Test 5: Re-Read Critical Inputs Before Irreversible Steps
# -----------------------------------------------------------------------------

def test_irreversible_step_forces_reread_and_approves_when_concordant() -> None:
    epochs = fv.EpochVector(context_id="cutover-cluster")
    read_counts = {"replica_lag": 0, "primary_uuid": 0}

    def probe_replica_lag() -> float:
        read_counts["replica_lag"] += 1
        return 0.05

    def probe_primary_uuid() -> str:
        read_counts["primary_uuid"] += 1
        return "srv-active-01"

    facts = fv.ContextualFacts(
        context_id="cutover-cluster",
        epoch_vector=epochs,
        default_ttl_s=10.0,
        replica_lag=probe_replica_lag,
        primary_uuid=probe_primary_uuid,
    )

    verifier = fv.CriticalStepVerifier()

    # Pre-populate with slightly stale facts
    old_epochs = fv.EpochVector(context_id="cutover-cluster", generation=-1)
    facts.put_fact(
        "replica_lag",
        0.1,
        fv.FactProvenance("old_probe", "read", time.time() - 20.0, 5.0, old_epochs),
    )
    facts.put_fact(
        "primary_uuid",
        "srv-active-01",
        fv.FactProvenance("old_probe", "read", time.time() - 20.0, 5.0, old_epochs),
    )

    # Verification must force fresh re-read of critical inputs
    result = verifier.verify_before_irreversible_step(
        step_name="cutover_switch_traffic",
        critical_keys=["replica_lag", "primary_uuid"],
        facts=facts,
        current_epochs=epochs,
        expected_preconditions={"primary_uuid": "srv-active-01"},
    )

    assert result.approved is True
    assert result.care_badge == fv.CARE_PASS
    assert result.fresh_critical_facts["replica_lag"] == 0.05
    assert result.fresh_critical_facts["primary_uuid"] == "srv-active-01"
    assert read_counts["replica_lag"] >= 1
    assert read_counts["primary_uuid"] >= 1


def test_irreversible_step_blocks_on_detected_drift() -> None:
    epochs = fv.EpochVector(context_id="cutover-cluster")

    def probe_primary_uuid() -> str:
        # Surprise failover occurred during migration!
        return "srv-different-02"

    facts = fv.ContextualFacts(
        context_id="cutover-cluster",
        epoch_vector=epochs,
        primary_uuid=probe_primary_uuid,
    )

    verifier = fv.CriticalStepVerifier()

    with pytest.raises(fv.IrreversibleStepBlockedError) as exc_info:
        verifier.verify_before_irreversible_step(
            step_name="cutover_promote_target",
            critical_keys=["primary_uuid"],
            facts=facts,
            current_epochs=epochs,
            expected_preconditions={"primary_uuid": "srv-expected-01"},
        )

    assert "drifted from expected 'srv-expected-01'" in str(exc_info.value)


# -----------------------------------------------------------------------------
# Test 6: Concurrent Probe Deduplication
# -----------------------------------------------------------------------------

def test_concurrent_probes_are_deduplicated() -> None:
    load_tracker = fv.SourceLoadTracker(
        measured_cpu_utilization=0.20,
        free_connections=100,
        measured_latency_ms=5.0,
    )
    dedup = fv.ProbeDeduplicator(load_tracker=load_tracker)
    execution_counter = 0

    def slow_database_probe() -> str:
        nonlocal execution_counter
        execution_counter += 1
        time.sleep(0.08)  # Simulate network / engine probe latency
        return "val_5042"

    num_threads = 5
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as pool:
        futures = [
            pool.submit(
                dedup.execute_or_deduplicate,
                "tenant-dedup",
                "table_checksum",
                slow_database_probe,
            )
            for _ in range(num_threads)
        ]
        results = [f.result() for f in futures]

    # All threads got the correct result
    for r in results:
        assert r == "val_5042"

    # CRITICAL PROOF: Probe was executed exactly ONCE, remaining 4 were deduplicated
    assert execution_counter == 1
    assert dedup.probes_executed == 1
    assert dedup.probes_deduplicated == 4


# -----------------------------------------------------------------------------
# Test 7: Source Load Budgeting and Throttling
# -----------------------------------------------------------------------------

def test_source_load_tracker_derives_dynamic_bounds() -> None:
    # High CPU, low free connections
    overloaded = fv.SourceLoadTracker(
        measured_cpu_utilization=0.92,
        free_connections=5,
        measured_latency_ms=25.0,
        cpu_safe_headroom=0.85,
    )
    assert overloaded.is_safe_for_probe() is False
    assert overloaded.calculate_backoff_delay_s() > 0.1

    # Healthy server with ample headroom
    healthy = fv.SourceLoadTracker(
        measured_cpu_utilization=0.15,
        free_connections=200,
        measured_latency_ms=2.0,
        cpu_safe_headroom=0.85,
    )
    assert healthy.is_safe_for_probe() is True
    limit = healthy.derive_probe_concurrency_limit()
    assert limit >= 10
    assert healthy.calculate_backoff_delay_s() < 0.05


def test_probe_deduplicator_throttles_when_budget_exceeded() -> None:
    overloaded_tracker = fv.SourceLoadTracker(
        measured_cpu_utilization=0.95,
        free_connections=10,
        cpu_safe_headroom=0.85,
    )
    dedup = fv.ProbeDeduplicator(load_tracker=overloaded_tracker)

    with pytest.raises(fv.ProbeExecutionError) as exc_info:
        dedup.execute_or_deduplicate(
            "tenant-overloaded",
            "heavy_scan",
            lambda: "result",
        )

    assert "Source probe budget exceeded" in str(exc_info.value)
    assert dedup.probes_throttled == 1


# -----------------------------------------------------------------------------
# Test 8: Secret Redaction
# -----------------------------------------------------------------------------

def test_secret_facts_are_strictly_redacted() -> None:
    epochs = fv.EpochVector(context_id="secure-vault")
    facts = fv.ContextualFacts(
        context_id="secure-vault",
        epoch_vector=epochs,
        known={
            "db_host": "127.0.0.1",
            "db_port": 5432,
            "master_password": "super_secret_plain_text_password",
            "auth_token": "bearer_987654321",
        },
    )

    # Explicit secret fact
    prov = fv.FactProvenance(
        source="vault",
        how="given",
        epoch_vector=epochs,
        is_secret=True,
    )
    facts.put_fact("custom_credential_blob", {"private_key": "-----BEGIN RSA PRIVATE KEY-----"}, provenance=prov)

    # 1. Plain dictionary access preserves value in-memory for internal logic
    assert facts["master_password"] == "super_secret_plain_text_password"

    # 2. Exported dictionary redacts sensitive keys and secret facts
    exported = facts.to_dict(redact=True)
    assert exported["db_host"] == "127.0.0.1"
    assert exported["db_port"] == 5432
    assert exported["master_password"] == fv.REDACTED_TEXT
    assert exported["auth_token"] == fv.REDACTED_TEXT
    assert exported["custom_credential_blob"] == fv.REDACTED_TEXT

    # 3. Account summary redacts secret values
    acc = facts.account(redact=True)
    assert acc["context_id"] == "secure-vault"
    assert "master_password" in acc["known"]
    # Check that cleartext does not leak in string representations
    acc_str = str(acc)
    assert "super_secret_plain_text_password" not in acc_str
    assert "bearer_987654321" not in acc_str
    assert "-----BEGIN RSA PRIVATE KEY-----" not in acc_str
