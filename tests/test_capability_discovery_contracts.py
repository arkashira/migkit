"""Tests for Capability Discovery and Proved Engine Contracts (Row 592: SMART-N9.capability-discovery).

Verifies that:
1. Engine pair capability and depth discovery is governed by `decide.Rung` and `decide.climb(work, rungs, facts)`.
2. Every rung in `migkit.capability_discovery` reaches both `holds` and `refuses` (or `holds` for fallback rungs)
   under `tests.rung_branches.watched()` and is declared in `PROVES_RUNGS`.
3. Capabilities are earned through bounded sandbox rehearsal tests, grants checks, and version checks
   instead of assuming brand labels.
4. Bounded probe budgets (row, statement, time limits) and safe test hosts are strictly enforced;
   unsafe hosts raise `UnsafeProbeHostError`, unauthorized mutation is rejected.
5. Tool inventory composes only registered trusted tools with verified recovery semantics.
6. Universal typed durable resume in O(1) preserves native types (datetime with timezone, Decimal, UUID, tuples)
   and `.run.lock` active verification lease enforces mutual exclusion.
7. Adaptive AIMD batch sizing and byte-weighted cold-start ETA forecasting behave predictably.
8. Output adheres to pure UNIX terse formatting with 4-tier care badges (ACTION, REVIEW, BENIGN, PASS).
"""

from __future__ import annotations

import datetime
import decimal
import time
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
import tests.rung_branches

from migkit.capability_discovery import (
    AdaptiveAimdBatchController,
    CapabilityDiscoveryCheckpoint,
    CapabilityDiscoveryCoordinator,
    CapabilityDiscoveryError,
    CapabilityDiscoveryLease,
    CapabilityKind,
    CapabilityLeaseError,
    CapabilityLimits,
    CapabilityStatus,
    CareBadge,
    DepthLevel,
    EngineCoordinate,
    InstalledToolInventory,
    OwnedCapabilityGap,
    PairCapabilityContract,
    PairCoordinate,
    ProbeBudget,
    ProbeKind,
    ProbeResult,
    RecoverySemantics,
    ToolMetadata,
    UnauthorizedMutationError,
    UnsafeProbeHostError,
    calculate_byte_weighted_eta,
    decide_pair_capability,
    decide_probe_authorization,
    decide_tool_composition,
    decode_typed_value,
    encode_typed_value,
    execute_grants_probe,
    execute_sandbox_rehearsal,
    execute_service_topology_probe,
    execute_version_probe,
    is_safe_test_host,
    load_checkpoint,
    pair_capability_rungs,
    parse_version_tuple,
    probe_authorization_rungs,
    resume_units_in_o1,
    save_checkpoint,
    tool_composition_rungs,
    version_satisfies,
)

PROVES = [
    ("capability-discovery.engine-breadth", "postgres"),
    ("capability-discovery.engine-breadth", "mysql"),
    ("capability-discovery.engine-breadth", "mssql"),
    ("capability-discovery.engine-breadth", "tidb"),
]

PROVES_RUNGS = [
    # pair_capability_rungs
    ("capability_discovery", "pair_capability_rungs", "native_proven_fastpath", "holds"),
    ("capability_discovery", "pair_capability_rungs", "native_proven_fastpath", "refuses"),
    ("capability_discovery", "pair_capability_rungs", "rehearsal_verified_composite", "holds"),
    ("capability_discovery", "pair_capability_rungs", "rehearsal_verified_composite", "refuses"),
    ("capability_discovery", "pair_capability_rungs", "degraded_fallback_path", "holds"),
    ("capability_discovery", "pair_capability_rungs", "degraded_fallback_path", "refuses"),
    ("capability_discovery", "pair_capability_rungs", "quarantine_owned_gap", "holds"),

    # probe_authorization_rungs
    ("capability_discovery", "probe_authorization_rungs", "sandbox_read_rehearsal", "holds"),
    ("capability_discovery", "probe_authorization_rungs", "sandbox_read_rehearsal", "refuses"),
    ("capability_discovery", "probe_authorization_rungs", "authorized_bounded_mutation", "holds"),
    ("capability_discovery", "probe_authorization_rungs", "authorized_bounded_mutation", "refuses"),
    ("capability_discovery", "probe_authorization_rungs", "reject_probe_violation", "holds"),

    # tool_composition_rungs
    ("capability_discovery", "tool_composition_rungs", "trusted_tool_chain", "holds"),
    ("capability_discovery", "tool_composition_rungs", "trusted_tool_chain", "refuses"),
    ("capability_discovery", "tool_composition_rungs", "builtin_engine_methods", "holds"),
    ("capability_discovery", "tool_composition_rungs", "builtin_engine_methods", "refuses"),
    ("capability_discovery", "tool_composition_rungs", "refuse_unregistered_composition", "holds"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def pg_coord() -> EngineCoordinate:
    return EngineCoordinate(
        engine="postgres",
        service="rds",
        version="16.1",
        edition="enterprise",
    )


@pytest.fixture
def mysql_coord() -> EngineCoordinate:
    return EngineCoordinate(
        engine="mysql",
        service="cloudsql",
        version="8.0.35",
        edition="standard",
    )


@pytest.fixture
def pg_to_mysql_pair(pg_coord: EngineCoordinate, mysql_coord: EngineCoordinate) -> PairCoordinate:
    return PairCoordinate(source=pg_coord, target=mysql_coord)


# -----------------------------------------------------------------------------
# 1. Decision Rung Branch Coverage (PROVES_RUNGS)
# -----------------------------------------------------------------------------

def test_pair_capability_rungs_all_branches(pg_to_mysql_pair: PairCoordinate):
    """Exercise both holds and refuses on every rung in pair_capability_rungs."""
    with tests.rung_branches.watched() as seen:
        # Branch 1: native_proven_fastpath HOLDS
        f_native = {
            "engine_version_supported": True,
            "required_grants_verified": True,
            "service_topology_compatible": True,
            "registered_tools_available": True,
            "no_owned_capability_gaps": True,
        }
        choice1, contract1 = decide_pair_capability(
            pg_to_mysql_pair,
            CapabilityKind.STREAM,
            f_native,
        )
        assert choice1.rung.name == "native_proven_fastpath"
        assert contract1.status == CapabilityStatus.PROVED
        assert contract1.care_badge == CareBadge.PASS
        assert contract1.owned_gap is None

        # Branch 2: native_proven_fastpath REFUSES, rehearsal_verified_composite HOLDS
        f_rehearsal = {
            "engine_version_supported": False,  # fails native
            "required_grants_verified": True,
            "service_topology_compatible": True,
            "registered_tools_available": True,
            "no_owned_capability_gaps": True,
            "rehearsal_probe_passed": True,
            "source_mutation_authorized_or_read_only": True,
            "composite_tools_registered": True,
        }
        choice2, contract2 = decide_pair_capability(
            pg_to_mysql_pair,
            CapabilityKind.STREAM,
            f_rehearsal,
        )
        assert choice2.rung.name == "rehearsal_verified_composite"
        assert contract2.status == CapabilityStatus.REHEARSED
        assert contract2.care_badge == CareBadge.REVIEW
        assert contract2.owned_gap is None

        # Branch 3: rehearsal_verified_composite REFUSES, degraded_fallback_path HOLDS
        f_degraded = {
            "engine_version_supported": False,
            "rehearsal_probe_passed": False,  # fails rehearsal
            "fallback_mode_admissible": True,
            "recovery_semantics_declared": True,
        }
        choice3, contract3 = decide_pair_capability(
            pg_to_mysql_pair,
            CapabilityKind.STREAM,
            f_degraded,
        )
        assert choice3.rung.name == "degraded_fallback_path"
        assert contract3.status == CapabilityStatus.DEGRADED
        assert contract3.care_badge == CareBadge.REVIEW
        assert contract3.owned_gap is not None

        # Branch 4: degraded_fallback_path REFUSES, quarantine_owned_gap HOLDS
        f_gap = {
            "engine_version_supported": False,
            "rehearsal_probe_passed": False,
            "fallback_mode_admissible": False,  # fails degraded
            "recovery_semantics_declared": False,
            "gap_category": "unsupported_version",
            "gap_remediation": "Upgrade engine version or install CDC adapter",
        }
        choice4, contract4 = decide_pair_capability(
            pg_to_mysql_pair,
            CapabilityKind.STREAM,
            f_gap,
        )
        assert choice4.rung.name == "quarantine_owned_gap"
        assert contract4.status == CapabilityStatus.OWNED_GAP
        assert contract4.care_badge == CareBadge.ACTION
        assert contract4.owned_gap is not None
        assert contract4.owned_gap.category == "unsupported_version"

    # Verify observed branches in seen
    branches_native = tests.rung_branches.branch(seen, "native_proven_fastpath")
    assert "holds" in branches_native and "refuses" in branches_native

    branches_rehearsal = tests.rung_branches.branch(seen, "rehearsal_verified_composite")
    assert "holds" in branches_rehearsal and "refuses" in branches_rehearsal

    branches_degraded = tests.rung_branches.branch(seen, "degraded_fallback_path")
    assert "holds" in branches_degraded and "refuses" in branches_degraded

    branches_quarantine = tests.rung_branches.branch(seen, "quarantine_owned_gap")
    assert "holds" in branches_quarantine


def test_probe_authorization_rungs_all_branches():
    """Exercise both holds and refuses on every rung in probe_authorization_rungs."""
    safe_budget = ProbeBudget(target_host="127.0.0.1", source_write_authorized=False)
    mutating_budget = ProbeBudget(target_host="10.0.0.42", source_write_authorized=True)

    with tests.rung_branches.watched() as seen:
        # Branch 1: sandbox_read_rehearsal HOLDS
        f_read = {
            "safe_test_endpoint": True,
            "non_mutating_probe_scope": True,
            "within_finite_budget": True,
        }
        choice1, auth1, _ = decide_probe_authorization(safe_budget, f_read)
        assert choice1.rung.name == "sandbox_read_rehearsal"
        assert auth1 is True

        # Branch 2: sandbox_read_rehearsal REFUSES, authorized_bounded_mutation HOLDS
        f_mutating = {
            "safe_test_endpoint": True,
            "non_mutating_probe_scope": False,  # fails read-only
            "explicit_source_write_authorized": True,
            "within_finite_budget": True,
        }
        choice2, auth2, _ = decide_probe_authorization(mutating_budget, f_mutating)
        assert choice2.rung.name == "authorized_bounded_mutation"
        assert auth2 is True

        # Branch 3: authorized_bounded_mutation REFUSES, reject_probe_violation HOLDS
        f_reject = {
            "safe_test_endpoint": False,  # fails both
            "non_mutating_probe_scope": False,
            "explicit_source_write_authorized": False,
            "within_finite_budget": False,
        }
        choice3, auth3, _ = decide_probe_authorization(safe_budget, f_reject)
        assert choice3.rung.name == "reject_probe_violation"
        assert auth3 is False

    branches_read = tests.rung_branches.branch(seen, "sandbox_read_rehearsal")
    assert "holds" in branches_read and "refuses" in branches_read

    branches_mutation = tests.rung_branches.branch(seen, "authorized_bounded_mutation")
    assert "holds" in branches_mutation and "refuses" in branches_mutation

    branches_reject = tests.rung_branches.branch(seen, "reject_probe_violation")
    assert "holds" in branches_reject


def test_tool_composition_rungs_all_branches(pg_to_mysql_pair: PairCoordinate):
    """Exercise both holds and refuses on every rung in tool_composition_rungs."""
    with tests.rung_branches.watched() as seen:
        # Branch 1: trusted_tool_chain HOLDS
        f_trusted = {
            "tools_registered_and_verified": True,
            "known_recovery_semantics": True,
            "throughput_limits_respected": True,
        }
        choice1, ok1, _ = decide_tool_composition(pg_to_mysql_pair, ["debezium"], f_trusted)
        assert choice1.rung.name == "trusted_tool_chain"
        assert ok1 is True

        # Branch 2: trusted_tool_chain REFUSES, builtin_engine_methods HOLDS
        f_builtin = {
            "tools_registered_and_verified": False,  # fails external tool
            "known_recovery_semantics": True,
            "throughput_limits_respected": True,
            "builtin_support_present": True,
            "workload_within_builtin_scale": True,
        }
        choice2, ok2, _ = decide_tool_composition(pg_to_mysql_pair, [], f_builtin)
        assert choice2.rung.name == "builtin_engine_methods"
        assert ok2 is True

        # Branch 3: builtin_engine_methods REFUSES, refuse_unregistered_composition HOLDS
        f_refuse = {
            "tools_registered_and_verified": False,
            "builtin_support_present": False,
            "workload_within_builtin_scale": False,
        }
        choice3, ok3, _ = decide_tool_composition(pg_to_mysql_pair, ["untrusted_llm_tool"], f_refuse)
        assert choice3.rung.name == "refuse_unregistered_composition"
        assert ok3 is False

    branches_trusted = tests.rung_branches.branch(seen, "trusted_tool_chain")
    assert "holds" in branches_trusted and "refuses" in branches_trusted

    branches_builtin = tests.rung_branches.branch(seen, "builtin_engine_methods")
    assert "holds" in branches_builtin and "refuses" in branches_builtin

    branches_refuse = tests.rung_branches.branch(seen, "refuse_unregistered_composition")
    assert "holds" in branches_refuse


# -----------------------------------------------------------------------------
# 2. Version, Grants, Service Topology & Type Probes
# -----------------------------------------------------------------------------

def test_version_parsing_and_satisfaction():
    """Verify exact semver parsing and range checks."""
    assert parse_version_tuple("16.1.2") == (16, 1, 2)
    assert parse_version_tuple("v8.0.35-cloudsql") == (8, 0, 35)
    assert parse_version_tuple("invalid") == (0,)

    assert version_satisfies("16.1", "10.0") is True
    assert version_satisfies("9.6.2", "10.0") is False
    assert version_satisfies("8.0.35", "5.7", "9.0") is True
    assert version_satisfies("9.1.0", "5.7", "9.0") is False


def test_empirical_probes(pg_coord: EngineCoordinate):
    """Verify empirical version, grants, and topology probes."""
    # Version probe
    v_res = execute_version_probe(pg_coord, min_version="14.0")
    assert v_res.passed is True
    assert v_res.failure_reason is None

    v_fail = execute_version_probe(pg_coord, min_version="17.0")
    assert v_fail.passed is False
    assert "fails contract" in v_fail.failure_reason

    # Grants probe
    g_res = execute_grants_probe(
        pg_coord,
        required_grants=["REPLICATION", "SELECT"],
        held_grants=["SELECT", "REPLICATION", "CONNECT"],
    )
    assert g_res.passed is True

    g_fail = execute_grants_probe(
        pg_coord,
        required_grants=["REPLICATION", "SUPER"],
        held_grants=["SELECT", "CONNECT"],
    )
    assert g_fail.passed is False
    assert "missing required database grants: REPLICATION, SUPER" in g_fail.failure_reason

    # Service topology probe
    top_res = execute_service_topology_probe(
        pg_coord,
        topology_flags={"read_only": False, "wal_level": "logical"},
        disallowed_flags=["read_only"],
    )
    assert top_res.passed is True

    top_fail = execute_service_topology_probe(
        pg_coord,
        topology_flags={"read_only": True, "wal_level": "minimal"},
        disallowed_flags=["read_only"],
    )
    assert top_fail.passed is False
    assert "disallowed flag 'read_only' is active" in top_fail.failure_reason


# -----------------------------------------------------------------------------
# 3. Bounded Sandbox Rehearsal & Safe Host Enforcement
# -----------------------------------------------------------------------------

def test_safe_host_validation():
    """Verify safe test host filter allows loopback/subnets and rejects production hosts."""
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("localhost") is True
    assert is_safe_test_host("::1") is True
    assert is_safe_test_host("10.0.0.15") is True
    assert is_safe_test_host("10.0.0.2") is True
    assert is_safe_test_host("staging-db.test") is True

    assert is_safe_test_host("db.production.internal") is False
    assert is_safe_test_host("54.210.12.8") is False
    assert is_safe_test_host("prod-postgres.company.com") is False


def test_rehearsal_probe_budget_and_mutation_enforcement(pg_to_mysql_pair: PairCoordinate):
    """Verify bounded probe rejects unsafe hosts, unauthorized mutation, and budget breach."""
    # Unsafe host raises immediately
    unsafe_budget = ProbeBudget(target_host="db.production.internal")
    with pytest.raises(UnsafeProbeHostError, match="not an authorized test host"):
        execute_sandbox_rehearsal(pg_to_mysql_pair, unsafe_budget)

    # Unauthorized source mutation
    unauth_budget = ProbeBudget(target_host="127.0.0.1", source_write_authorized=False)
    res_mut = execute_sandbox_rehearsal(pg_to_mysql_pair, unauth_budget, mutates_source=True)
    assert res_mut.passed is False
    assert "source mutation without explicit authorization" in res_mut.failure_reason

    # Bounded budget enforcement
    strict_budget = ProbeBudget(
        target_host="127.0.0.1",
        max_probe_rows=50,
        max_statements=5,
        max_probe_seconds=0.5,
    )

    # Runner exceeding rows
    def runner_rows(pair, b):
        return {"passed": True, "rows_probed": 100, "statements_executed": 1}

    res_rows = execute_sandbox_rehearsal(pg_to_mysql_pair, strict_budget, runner_rows)
    assert res_rows.passed is False
    assert "exceeded probe row budget" in res_rows.failure_reason

    # Runner exceeding statements
    def runner_stmts(pair, b):
        return {"passed": True, "rows_probed": 10, "statements_executed": 20}

    res_stmts = execute_sandbox_rehearsal(pg_to_mysql_pair, strict_budget, runner_stmts)
    assert res_stmts.passed is False
    assert "exceeded probe statement budget" in res_stmts.failure_reason


# -----------------------------------------------------------------------------
# 4. Installed Tool Inventory & Composition Semantics
# -----------------------------------------------------------------------------

def test_tool_inventory_registration_and_verification(pg_to_mysql_pair: PairCoordinate):
    """Verify tool inventory registers trusted tools and rejects unautomated or unregistered tools."""
    inv = InstalledToolInventory.default_inventory()

    # Valid chain
    ok, err = inv.verify_tool_chain(["debezium", "psql"], pg_to_mysql_pair)
    assert ok is True
    assert err is None

    # Unregistered tool
    ok_unreg, err_unreg = inv.verify_tool_chain(["unknown_custom_script"], pg_to_mysql_pair)
    assert ok_unreg is False
    assert "not registered" in err_unreg

    # Tool with manual repair semantics
    manual_tool = ToolMetadata(
        name="manual_csv_copier",
        path="/usr/bin/csvcopy",
        version="1.0",
        checksum_sha256="abc12345",
        is_available=True,
        limits=CapabilityLimits(),
        recovery_semantics=RecoverySemantics.MANUAL_REPAIR,
    )
    inv.register(manual_tool)
    ok_manual, err_manual = inv.verify_tool_chain(["manual_csv_copier"], pg_to_mysql_pair)
    assert ok_manual is False
    assert "unautomated recovery semantics (MANUAL_REPAIR)" in err_manual


# -----------------------------------------------------------------------------
# 5. O(1) Typed Durable Resume & Mutual Exclusion Lease
# -----------------------------------------------------------------------------

def test_typed_durable_checkpoint_preserves_native_types(tmp_path: Path):
    """Prove checkpoint preserves datetime with tz, Decimal, UUID, and tuple in O(1)."""
    now = datetime.datetime(2026, 10, 17, 14, 0, 0, tzinfo=datetime.timezone.utc)
    complex_cursor = (
        now,
        decimal.Decimal("1234567.8901"),
        uuid.UUID("a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"),
        ("source_col", 42),
    )

    cp = CapabilityDiscoveryCheckpoint(
        checkpoint_id="cp-test-01",
        pair_ident="postgres:16.1->mysql:8.0.35",
        cursor=complex_cursor,
        contracts={"stream": {"status": "proved"}},
        gaps=[],
        created_at=now,
        updated_at=now,
    )

    cp_file = tmp_path / "checkpoint.json"
    save_checkpoint(cp_file, cp)

    loaded = load_checkpoint(cp_file)
    assert loaded.checkpoint_id == "cp-test-01"
    assert loaded.cursor[0] == now
    assert loaded.cursor[1] == decimal.Decimal("1234567.8901")
    assert loaded.cursor[2] == uuid.UUID("a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d")
    assert loaded.cursor[3] == ("source_col", 42)

    # Test O(1) unit resume
    all_caps = [
        CapabilityKind.SCHEMA,
        CapabilityKind.COUNTS,
        CapabilityKind.DATA,
        CapabilityKind.STREAM,
        CapabilityKind.FENCE,
    ]
    # Checkpoint stopped at COUNTS
    cp_counts = CapabilityDiscoveryCheckpoint(
        checkpoint_id="cp-counts",
        pair_ident="test",
        cursor=(CapabilityKind.COUNTS.value,),
        contracts={},
        gaps=[],
        created_at=now,
        updated_at=now,
    )
    resumed = resume_units_in_o1(all_caps, cp_counts, key_fn=lambda c: c.value)
    assert resumed == [CapabilityKind.DATA, CapabilityKind.STREAM, CapabilityKind.FENCE]


def test_mutual_exclusion_lease_lock(tmp_path: Path):
    """Prove active lease lock prevents overlapping concurrent executions."""
    lock_file = tmp_path / ".run.lock"

    with CapabilityDiscoveryLease(lock_file, term_seconds=30.0) as lease1:
        assert lock_file.exists()
        # Attempt concurrent acquire with second lease
        lease2 = CapabilityDiscoveryLease(lock_file, term_seconds=30.0)
        with pytest.raises(CapabilityLeaseError, match="locked by another process"):
            lease2.acquire()

        # Heartbeat succeeds
        lease1.heartbeat()

    # Once released, lease2 can acquire
    with CapabilityDiscoveryLease(lock_file, term_seconds=30.0) as lease3:
        assert lock_file.exists()
        lease3.heartbeat()


# -----------------------------------------------------------------------------
# 6. Adaptive AIMD Batch Sizing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------

def test_adaptive_aimd_controller():
    """Verify AIMD additive increase on fast latency and multiplicative decrease on backpressure."""
    aimd = AdaptiveAimdBatchController(
        initial_batch=1000,
        min_batch=100,
        max_batch=5000,
        additive_step=200,
        multiplicative_factor=0.5,
        target_latency_seconds=0.2,
    )
    assert aimd.current_batch == 1000

    # Low latency increases batch size additively
    b1 = aimd.on_success(0.1)
    assert b1 == 1200
    b2 = aimd.on_success(0.15)
    assert b2 == 1400

    # Backpressure triggers multiplicative reduction
    b3 = aimd.on_backpressure("latency_spike")
    assert b3 == 700

    # High latency near threshold steps down
    b4 = aimd.on_success(0.3)
    assert b4 < 700


def test_cold_start_byte_weighted_eta():
    """Verify instant cold-start ETA calculation with p50 and p90 spread."""
    # Cold start
    eta_cold = calculate_byte_weighted_eta(
        total_bytes=100000000,  # 100 MB
        processed_bytes=0,
        elapsed_seconds=0.0,
        baseline_bytes_per_sec=10000000.0,  # 10 MB/s
    )
    assert eta_cold["p50_seconds"] == 10.0
    assert eta_cold["p90_seconds"] == 14.0

    # Active measured throughput (processed 50 MB in 2.5s -> 20 MB/s)
    eta_active = calculate_byte_weighted_eta(
        total_bytes=100000000,
        processed_bytes=50000000,
        elapsed_seconds=2.5,
    )
    assert eta_active["p50_seconds"] == 2.5
    assert eta_active["p90_seconds"] == 3.125
    assert eta_active["effective_bps"] == 20000000.0


# -----------------------------------------------------------------------------
# 7. End-to-End Capability Discovery Coordinator Flow
# -----------------------------------------------------------------------------

def test_coordinator_end_to_end(pg_to_mysql_pair: PairCoordinate, tmp_path: Path):
    """Run full coordinator execution discovering multiple capabilities and logging pure UNIX lines."""
    coord = CapabilityDiscoveryCoordinator(state_dir=tmp_path)

    facts = {
        CapabilityKind.SCHEMA: {
            "engine_version_supported": True,
            "required_grants_verified": True,
            "service_topology_compatible": True,
            "registered_tools_available": True,
            "no_owned_capability_gaps": True,
        },
        CapabilityKind.STREAM: {
            "engine_version_supported": False,
            "rehearsal_probe_passed": True,
            "source_mutation_authorized_or_read_only": True,
            "composite_tools_registered": True,
        },
        CapabilityKind.FENCE: {
            "engine_version_supported": False,
            "rehearsal_probe_passed": False,
            "fallback_mode_admissible": False,
            "recovery_semantics_declared": False,
            "gap_category": "missing_atomic_fence_token",
            "gap_remediation": "Requires transaction epoch fence grant on MySQL target",
        },
    }

    contracts, gaps = coord.discover_pair_capabilities(
        pair=pg_to_mysql_pair,
        capabilities=[CapabilityKind.SCHEMA, CapabilityKind.STREAM, CapabilityKind.FENCE],
        facts_map=facts,
    )

    assert len(contracts) == 3
    assert contracts[0].care_badge == CareBadge.PASS
    assert contracts[1].care_badge == CareBadge.REVIEW
    assert contracts[2].care_badge == CareBadge.ACTION

    assert len(gaps) == 1
    assert gaps[0].category == "missing_atomic_fence_token"

    # Verify UNIX format lines
    for c in contracts:
        line = c.format_unix_line()
        assert not any(ord(ch) > 127 for ch in line)  # 100% pure ASCII
        assert line.startswith(("[PASS]", "[REVIEW]", "[ACTION]"))

    # Verify checkpoint was written
    assert coord.checkpoint_path.exists()
    cp = load_checkpoint(coord.checkpoint_path)
    assert cp.pair_ident == pg_to_mysql_pair.ident()
    assert cp.cursor[0] == CapabilityKind.FENCE.value
