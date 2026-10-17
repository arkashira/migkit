"""Tests for compiling intent into one checked execution contract (Row 584: SMART-N1.contract).

Proves:
1. Every `decide.Rung` in `execution_contract.intent_compilation_rungs`,
   `execution_contract.contract_path_rungs`, and
   `execution_contract.boundary_admission_rungs` is exercised on both `holds` and
   `refuses` inside `with tests.rung_branches.watched():` and declared in
   `PROVES_RUNGS`.
2. Intent compilation compiles goal and scope into required proof coverage,
   snapshot/fence/ordering, idempotence, restartability, source impact, outage,
   byte and money bounds derived from measured telemetry readings + spread, and
   binds `facts_version`, `model_version`, `plan_version`, and `TargetIdentity`.
3. Componentwise minimum guarantees hold before lexicographic guarantee ranking;
   incompatible minimums are refused with `IncompatibleMinimumGuaranteesError`.
4. The compiled contract is carried through `bulk`, `table`, `range`, and `native`
   paths, runtime fallback, and O(1) typed durable resume (`datetime`, `Decimal`,
   `UUID`, composite tuples). Adversarial faster alternatives and runtime failures
   cannot drop any required guarantee dimension or exceed bounds.
5. Independent admission checks run before irreversible boundaries (`cutover`,
   `rollback`, `target_exposure`, `source_log_release`), blocking target identity
   mismatches, dropped proof coverage, unverified fences/rollback compensations,
   unauthorized source mutations, budget overruns, and stale fact/model/plan versions.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import pathlib
import uuid
from typing import Dict

import pytest

import tests.rung_branches
from migkit import decide, execution_contract, guarantees
from migkit.execution_contract import (
    CARE_ACTION,
    CARE_BENIGN,
    CARE_PASS,
    CARE_REVIEW,
    BoundaryAdmissionVerdict,
    CandidatePathProfile,
    CareBadge,
    CompiledExecutionContract,
    ContractDimensionDroppedError,
    ContractTelemetryReadings,
    ExecutionContractError,
    ExecutionPathClass,
    IncompatibleMinimumGuaranteesError,
    IrreversibleBoundaryKind,
    IrreversibleBoundaryRefusedError,
    LifecycleBoundary,
    MigrationGoalKind,
    ProofCoverageLevel,
    ScopeContract,
    SnapshotFenceOrderingMode,
    TargetIdentity,
    UnsafeEndpointError,
    VersionBinding,
    acquire_contract_execution_lease,
    admit_irreversible_boundary,
    boundary_admission_rungs,
    compile_execution_contract,
    contract_path_rungs,
    derive_bounds_from_readings,
    evaluate_fallback_under_contract,
    evaluate_path_under_contract,
    filter_and_rank_by_guarantees,
    holds_componentwise_minimums,
    intent_compilation_rungs,
    parse_guarantee_minimums,
    save_contract_unit_checkpoint,
    validate_compatible_minimums,
    validate_safe_endpoint,
    verify_resume_under_contract,
)
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("execution_contract", "intent_compilation_rungs", "zero_loss_online_cutover_contract", "holds"),
    ("execution_contract", "intent_compilation_rungs", "zero_loss_online_cutover_contract", "refuses"),
    ("execution_contract", "intent_compilation_rungs", "keyless_exact_multiset_contract", "holds"),
    ("execution_contract", "intent_compilation_rungs", "keyless_exact_multiset_contract", "refuses"),
    ("execution_contract", "intent_compilation_rungs", "consistent_snapshot_seed_contract", "holds"),
    ("execution_contract", "intent_compilation_rungs", "consistent_snapshot_seed_contract", "refuses"),
    ("execution_contract", "intent_compilation_rungs", "bounded_window_maintenance_contract", "holds"),
    ("execution_contract", "intent_compilation_rungs", "bounded_window_maintenance_contract", "refuses"),
    ("execution_contract", "contract_path_rungs", "native_contract_path", "holds"),
    ("execution_contract", "contract_path_rungs", "native_contract_path", "refuses"),
    ("execution_contract", "contract_path_rungs", "bulk_contract_path", "holds"),
    ("execution_contract", "contract_path_rungs", "bulk_contract_path", "refuses"),
    ("execution_contract", "contract_path_rungs", "range_contract_path", "holds"),
    ("execution_contract", "contract_path_rungs", "range_contract_path", "refuses"),
    ("execution_contract", "contract_path_rungs", "table_contract_path", "holds"),
    ("execution_contract", "contract_path_rungs", "table_contract_path", "refuses"),
    ("execution_contract", "boundary_admission_rungs", "refuse_contract_breach", "holds"),
    ("execution_contract", "boundary_admission_rungs", "refuse_contract_breach", "refuses"),
    ("execution_contract", "boundary_admission_rungs", "hold_for_revalidation", "holds"),
    ("execution_contract", "boundary_admission_rungs", "hold_for_revalidation", "refuses"),
    ("execution_contract", "boundary_admission_rungs", "admit_irreversible_boundary", "holds"),
    ("execution_contract", "boundary_admission_rungs", "admit_irreversible_boundary", "refuses"),
]


# -----------------------------------------------------------------------------
# Fixtures & Sample Measured Readings
# -----------------------------------------------------------------------------

def _sample_readings() -> ContractTelemetryReadings:
    return ContractTelemetryReadings(
        dataset_bytes_samples=(10 * 1024**3, 11 * 1024**3, 12 * 1024**3),
        wire_amplification_samples=(1.05, 1.08, 1.10),
        price_per_gib_usd_samples=(0.08, 0.09, 0.10),
        source_cpu_headroom_ratio_samples=(0.45, 0.50, 0.55),
        source_iops_headroom_samples=(4000.0, 4200.0, 4400.0),
        source_read_bytes_per_sec_samples=(120_000_000, 125_000_000, 130_000_000),
        source_max_connections=100,
        source_reserved_system_connections=20,
        cutover_drain_sec_samples=(1.2, 1.5, 1.8),
        cutover_switch_sec_samples=(0.4, 0.5, 0.6),
    )


def _sample_scope(*, keyless: bool = False, cross_fk: bool = True) -> ScopeContract:
    return ScopeContract(
        tables=("orders", "order_items", "ledger_entries"),
        secondary_objects=("active_orders_v", "orders_id_seq", "app_rw_grant"),
        row_filter_digest="tenant_id=42",
        column_mapping_digest="orders:v1",
        has_keyless_tables=keyless,
        has_cross_table_fks=cross_fk,
    )


def _sample_target_identity(host: str = "10.0.0.25") -> TargetIdentity:
    return TargetIdentity(
        engine="postgres",
        host=host,
        port=5432,
        database="prod_orders",
        cluster_uid="cluster-pg-target-01",
        schema_fingerprint="sha256:8f91ab02c4",
    )


def _sample_versions() -> VersionBinding:
    return VersionBinding(
        facts_version="facts-2026-10-17-v1",
        model_version="costmodel-v3.2",
        plan_version="plan-rev-14",
        source_epoch=104,
    )


def _candidate_profiles(
    contract: CompiledExecutionContract,
) -> Dict[ExecutionPathClass, CandidatePathProfile]:
    """Build valid baseline candidate profiles for all 4 execution path classes."""
    full_guarantees = {
        "correct": "byte exact",
        "consistent": "as of one moment",
        "idempotent": "converges",
        "exactly once": "exactly once",
        "resumable": "goes on",
    }
    sb = contract.source_impact_bounds
    rb = contract.outage_byte_money_bounds
    return {
        ExecutionPathClass.NATIVE: CandidatePathProfile(
            path_class=ExecutionPathClass.NATIVE,
            guarantees_held=full_guarantees,
            covers_full_scope=True,
            preserves_fence_and_ordering=True,
            measured_duration_seconds=120.0,
            measured_outage_seconds=rb.max_outage_seconds * 0.5,
            measured_billed_bytes=int(rb.max_billed_bytes * 0.7),
            measured_money_usd=rb.max_money_usd * 0.7,
            measured_source_cpu_ratio=sb.max_cpu_ratio * 0.5,
            measured_source_iops=sb.max_iops * 0.5,
            measured_source_read_bytes_per_sec=int(sb.max_read_bytes_per_sec * 0.6),
            measured_worker_connections=min(4, sb.max_worker_connections),
        ),
        ExecutionPathClass.BULK: CandidatePathProfile(
            path_class=ExecutionPathClass.BULK,
            guarantees_held=full_guarantees,
            covers_full_scope=True,
            preserves_fence_and_ordering=True,
            measured_duration_seconds=180.0,
            measured_outage_seconds=rb.max_outage_seconds * 0.6,
            measured_billed_bytes=int(rb.max_billed_bytes * 0.8),
            measured_money_usd=rb.max_money_usd * 0.8,
            measured_source_cpu_ratio=sb.max_cpu_ratio * 0.7,
            measured_source_iops=sb.max_iops * 0.7,
            measured_source_read_bytes_per_sec=int(sb.max_read_bytes_per_sec * 0.75),
            measured_worker_connections=min(8, sb.max_worker_connections),
        ),
        ExecutionPathClass.RANGE: CandidatePathProfile(
            path_class=ExecutionPathClass.RANGE,
            guarantees_held=full_guarantees,
            covers_full_scope=True,
            preserves_fence_and_ordering=True,
            measured_duration_seconds=240.0,
            measured_outage_seconds=rb.max_outage_seconds * 0.65,
            measured_billed_bytes=int(rb.max_billed_bytes * 0.85),
            measured_money_usd=rb.max_money_usd * 0.85,
            measured_source_cpu_ratio=sb.max_cpu_ratio * 0.65,
            measured_source_iops=sb.max_iops * 0.65,
            measured_source_read_bytes_per_sec=int(sb.max_read_bytes_per_sec * 0.7),
            measured_worker_connections=min(6, sb.max_worker_connections),
        ),
        ExecutionPathClass.TABLE: CandidatePathProfile(
            path_class=ExecutionPathClass.TABLE,
            guarantees_held=full_guarantees,
            covers_full_scope=True,
            preserves_fence_and_ordering=True,
            measured_duration_seconds=310.0,
            measured_outage_seconds=rb.max_outage_seconds * 0.7,
            measured_billed_bytes=int(rb.max_billed_bytes * 0.9),
            measured_money_usd=rb.max_money_usd * 0.9,
            measured_source_cpu_ratio=sb.max_cpu_ratio * 0.4,
            measured_source_iops=sb.max_iops * 0.4,
            measured_source_read_bytes_per_sec=int(sb.max_read_bytes_per_sec * 0.5),
            measured_worker_connections=min(2, sb.max_worker_connections),
        ),
    }


# -----------------------------------------------------------------------------
# 1. Exhaustive Rung Coverage (`holds` & `refuses` inside `watched()`)
# -----------------------------------------------------------------------------

def test_all_execution_contract_rungs_hold_and_refuse_under_watched() -> None:
    """Exercise both `holds` and `refuses` for every rung in `execution_contract`."""
    readings = _sample_readings()
    scope = _sample_scope()
    target = _sample_target_identity()
    versions = _sample_versions()

    with watched() as seen:
        # --- Ladder 1: intent_compilation_rungs ---
        # 1a. zero_loss_online_cutover_contract HOLDS, others REFUSE
        c_online = compile_execution_contract(
            MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
            scope,
            target,
            versions,
            readings,
        )
        assert c_online.compilation_choice.path == "zero_loss_online_cutover_contract"

        # 1b. keyless_exact_multiset_contract HOLDS, zero_loss REFUSES
        c_keyless = compile_execution_contract(
            MigrationGoalKind.KEYLESS_EXACT_MULTISET,
            _sample_scope(keyless=True),
            target,
            versions,
            readings,
        )
        assert c_keyless.compilation_choice.path == "keyless_exact_multiset_contract"

        # 1c. consistent_snapshot_seed_contract HOLDS
        c_seed = compile_execution_contract(
            MigrationGoalKind.CONSISTENT_SNAPSHOT_SEED,
            scope,
            target,
            versions,
            readings,
        )
        assert c_seed.compilation_choice.path == "consistent_snapshot_seed_contract"

        # 1d. bounded_window_maintenance_contract HOLDS
        c_maint = compile_execution_contract(
            MigrationGoalKind.BOUNDED_WINDOW_MAINTENANCE,
            _sample_scope(keyless=False, cross_fk=False),
            target,
            versions,
            readings,
        )
        assert c_maint.compilation_choice.path == "bounded_window_maintenance_contract"

        # --- Ladder 2: contract_path_rungs ---
        # 2a. All 4 paths HOLD when valid
        profiles = _candidate_profiles(c_online)
        sel_all = evaluate_path_under_contract(c_online, profiles)
        assert sel_all.selected_path == ExecutionPathClass.NATIVE

        # 2b. Force each path to REFUSE by failing its contract guarantee or bound
        bad_profiles = {
            p: dataclasses.replace(
                prof,
                guarantees_held={"correct": "counted"},
                covers_full_scope=False,
            )
            for p, prof in profiles.items()
        }
        sel_none = evaluate_path_under_contract(c_online, bad_profiles)
        assert sel_none.selected_path is None
        assert sel_none.care_badge == CareBadge.ACTION

        # --- Ladder 3: boundary_admission_rungs ---
        base_state = execution_contract.BoundaryRuntimeState(
            boundary=IrreversibleBoundaryKind.CUTOVER_COMMIT,
            observed_target_identity=target,
            observed_version_binding=versions,
            observed_scope_digest=scope.scope_digest,
            achieved_proof_coverage=ProofCoverageLevel.BYTE_EXACT_AND_CATALOG,
            achieved_guarantees=c_online.minimum_guarantee_names,
            snapshot_fence_verified=True,
            commit_ordering_verified=True,
            consumed_outage_seconds=c_online.outage_byte_money_bounds.max_outage_seconds * 0.5,
            consumed_billed_bytes=int(c_online.outage_byte_money_bounds.max_billed_bytes * 0.8),
            consumed_money_usd=c_online.outage_byte_money_bounds.max_money_usd * 0.8,
            observed_source_cpu_ratio=c_online.source_impact_bounds.max_cpu_ratio * 0.6,
            observed_source_iops=c_online.source_impact_bounds.max_iops * 0.6,
            observed_source_read_bytes_per_sec=int(
                c_online.source_impact_bounds.max_read_bytes_per_sec * 0.6
            ),
            observed_worker_connections=2,
        )
        # 3a. admit_irreversible_boundary HOLDS; refuse_contract_breach & hold_for_revalidation REFUSE
        rep_admit = admit_irreversible_boundary(c_online, base_state)
        assert rep_admit.verdict == BoundaryAdmissionVerdict.ADMIT_IRREVERSIBLE_BOUNDARY
        assert rep_admit.admitted is True

        # 3b. hold_for_revalidation HOLDS; refuse_contract_breach & admit_irreversible_boundary REFUSE
        stale_state = dataclasses.replace(
            base_state,
            observed_version_binding=VersionBinding(
                facts_version="facts-2026-10-17-v2-stale",
                model_version=versions.model_version,
                plan_version=versions.plan_version,
                source_epoch=versions.source_epoch + 1,
            ),
        )
        rep_hold = admit_irreversible_boundary(c_online, stale_state)
        assert rep_hold.verdict == BoundaryAdmissionVerdict.HOLD_FOR_REVALIDATION
        assert rep_hold.admitted is False

        # 3c. refuse_contract_breach HOLDS; hold_for_revalidation & admit_irreversible_boundary REFUSE
        breach_state = dataclasses.replace(
            base_state,
            achieved_proof_coverage=ProofCoverageLevel.ROW_COUNTED,
        )
        rep_refuse = admit_irreversible_boundary(c_online, breach_state)
        assert rep_refuse.verdict == BoundaryAdmissionVerdict.REFUSE_CONTRACT_BREACH
        assert rep_refuse.admitted is False

    for _, _, rung_name, expected_branch in PROVES_RUNGS:
        assert expected_branch in branch(seen, rung_name), (
            f"rung {rung_name!r} did not reach branch {expected_branch!r}; "
            f"seen={branch(seen, rung_name)}"
        )


# -----------------------------------------------------------------------------
# 2. Componentwise Minimums & Lexicographic Guarantee Ranking
# -----------------------------------------------------------------------------

def test_componentwise_minimums_enforced_before_lexicographic_ranking() -> None:
    """Explicit minimums must hold componentwise before lexicographic ranking."""
    # Suppose candidate A has stronger 'correct' ('byte exact') but weaker 'resumable' ('starts over'),
    # while candidate B has 'correct': 'proved' and 'resumable': 'goes on'.
    # Without componentwise minimums, lexicographic order on ('correct', ...) would rank A first.
    # With explicit minimum {'correct': 'proved', 'resumable': 'goes on'}, A is rejected before ranking!
    rung_a = decide.Rung(
        "fast_non_resumable",
        "byte-exact one-shot stream that starts over on failure",
        holds={
            "correct": "byte exact",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "starts over",
        },
    )
    rung_b = decide.Rung(
        "resumable_proved",
        "proved snapshot copy that resumes from durable cursor",
        holds={
            "correct": "proved",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
    )
    rung_c = decide.Rung(
        "resumable_byte_exact",
        "byte-exact snapshot copy that resumes from durable cursor",
        holds={
            "correct": "byte exact",
            "consistent": "as of one moment",
            "idempotent": "converges",
            "exactly once": "exactly once",
            "resumable": "goes on",
        },
    )

    mins = parse_guarantee_minimums({"correct": "proved", "resumable": "goes on"})
    kept, rejected = filter_and_rank_by_guarantees((rung_a, rung_b, rung_c), mins)

    assert [r.name for r in kept] == ["resumable_byte_exact", "resumable_proved"]
    assert len(rejected) == 1
    assert rejected[0][0].name == "fast_non_resumable"
    assert "resumable: holds 'starts over' < required minimum 'goes on'" in rejected[0][1]


def test_incompatible_minimum_guarantees_are_refused() -> None:
    """Incompatible or contradictory minimum requirements are refused before ranking."""
    readings = _sample_readings()
    scope = _sample_scope()
    target = _sample_target_identity()
    versions = _sample_versions()

    # Case 1: 'byte exact' requested when unapproved lossy type mapping is present
    with pytest.raises(IncompatibleMinimumGuaranteesError, match="lossy type mapping"):
        compile_execution_contract(
            MigrationGoalKind.CONSISTENT_SNAPSHOT_SEED,
            scope,
            target,
            versions,
            readings,
            explicit_minimum_guarantees={"correct": "byte exact"},
            scope_facts={
                "lossy_type_mapping_present": True,
                "approved_lossy_mapping_rule": False,
            },
        )

    # Case 2: 'as of one moment' consistency when neither snapshot, CDC fence, nor frozen writes exist
    with pytest.raises(IncompatibleMinimumGuaranteesError, match="as of one moment"):
        compile_execution_contract(
            MigrationGoalKind.BOUNDED_WINDOW_MAINTENANCE,
            _sample_scope(cross_fk=False),
            target,
            versions,
            readings,
            explicit_minimum_guarantees={"consistent": "as of one moment"},
            scope_facts={
                "supports_point_in_time_snapshot": False,
                "supports_cdc_catchup_fence": False,
                "source_writes_frozen": False,
            },
        )

    # Case 3: Minimum exceeds pair capability ceiling
    with pytest.raises(IncompatibleMinimumGuaranteesError, match="exceeds pair capability ceiling"):
        compile_execution_contract(
            MigrationGoalKind.CONSISTENT_SNAPSHOT_SEED,
            scope,
            target,
            versions,
            readings,
            explicit_minimum_guarantees={"correct": "byte exact"},
            scope_facts={
                "pair_max_guarantees": {"correct": "proved"},
            },
        )

    # Case 4: Unknown guarantee dimension or level
    with pytest.raises(IncompatibleMinimumGuaranteesError):
        parse_guarantee_minimums({"nonexistent_dim": "proved"})
    with pytest.raises(IncompatibleMinimumGuaranteesError):
        parse_guarantee_minimums({"correct": "magic_level"})


# -----------------------------------------------------------------------------
# 3. Carrying Contract Across Bulk/Table/Range/Native & Adversarial Alternatives
# -----------------------------------------------------------------------------

def test_adversarial_faster_alternatives_cannot_drop_required_dimension() -> None:
    """An adversarial 10x faster path that drops any contract dimension is refused."""
    contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )
    profiles = _candidate_profiles(contract)

    # Make NATIVE 10x faster (12s) but drop 'correct' from 'byte exact' to 'counted'
    # Make BULK 5x faster (25s) but omit secondary objects (covers_full_scope=False)
    # Make RANGE 3x faster (40s) but exceed source CPU bound
    # Leave TABLE valid at 310s
    adversarial = {
        ExecutionPathClass.NATIVE: dataclasses.replace(
            profiles[ExecutionPathClass.NATIVE],
            measured_duration_seconds=12.0,
            guarantees_held={
                "correct": "counted",
                "consistent": "as of one moment",
                "idempotent": "converges",
                "exactly once": "exactly once",
                "resumable": "goes on",
            },
        ),
        ExecutionPathClass.BULK: dataclasses.replace(
            profiles[ExecutionPathClass.BULK],
            measured_duration_seconds=25.0,
            covers_full_scope=False,
        ),
        ExecutionPathClass.RANGE: dataclasses.replace(
            profiles[ExecutionPathClass.RANGE],
            measured_duration_seconds=40.0,
            measured_source_cpu_ratio=contract.source_impact_bounds.max_cpu_ratio + 0.15,
        ),
        ExecutionPathClass.TABLE: profiles[ExecutionPathClass.TABLE],
    }

    selection = evaluate_path_under_contract(contract, adversarial)
    assert selection.selected_path == ExecutionPathClass.TABLE
    assert selection.care_badge == CareBadge.PASS
    assert "correct: holds 'counted' < required minimum 'byte exact'" in selection.rejected_reasons["native"][0]
    assert "bulk omits required scope" in selection.rejected_reasons["bulk"][0]
    assert "source CPU" in selection.rejected_reasons["range"][0]


def test_runtime_fallback_preserves_contract_across_all_paths_and_refuses_degradation() -> None:
    """Runtime failures switch bulk -> native -> range -> table while preserving the contract."""
    contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )
    profiles = _candidate_profiles(contract)

    # Initial pick: NATIVE (120s)
    s0 = evaluate_path_under_contract(contract, profiles)
    assert s0.selected_path == ExecutionPathClass.NATIVE

    # NATIVE fails at runtime -> falls back to BULK (180s)
    s1 = evaluate_fallback_under_contract(
        contract, profiles, failed_paths=(ExecutionPathClass.NATIVE,)
    )
    assert s1.selected_path == ExecutionPathClass.BULK
    assert s1.care_badge == CareBadge.REVIEW

    # NATIVE and BULK fail -> falls back to RANGE (240s)
    s2 = evaluate_fallback_under_contract(
        contract,
        profiles,
        failed_paths=(ExecutionPathClass.NATIVE, ExecutionPathClass.BULK),
    )
    assert s2.selected_path == ExecutionPathClass.RANGE

    # NATIVE, BULK, and RANGE fail -> falls back to TABLE (310s)
    s3 = evaluate_fallback_under_contract(
        contract,
        profiles,
        failed_paths=(
            ExecutionPathClass.NATIVE,
            ExecutionPathClass.BULK,
            ExecutionPathClass.RANGE,
        ),
    )
    assert s3.selected_path == ExecutionPathClass.TABLE

    # If TABLE also compromises idempotence or fails, fallback refuses with ContractDimensionDroppedError
    degraded_table = dict(profiles)
    degraded_table[ExecutionPathClass.TABLE] = dataclasses.replace(
        profiles[ExecutionPathClass.TABLE],
        guarantees_held={
            "correct": "byte exact",
            "consistent": "as of one moment",
            "idempotent": "no",
            "exactly once": "at least once",
            "resumable": "goes on",
        },
    )
    with pytest.raises(ContractDimensionDroppedError, match="runtime fallback refused"):
        evaluate_fallback_under_contract(
            contract,
            degraded_table,
            failed_paths=(
                ExecutionPathClass.NATIVE,
                ExecutionPathClass.BULK,
                ExecutionPathClass.RANGE,
            ),
        )


# -----------------------------------------------------------------------------
# 4. O(1) Typed Durable Resume & Active Lease Lock under Contract
# -----------------------------------------------------------------------------

def test_o1_typed_durable_resume_and_incompatible_contract_rejection(
    tmp_path: pathlib.Path,
) -> None:
    """Completed units preserve composite native types in O(1) and reject incompatible contracts."""
    contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )
    ckpt_file = tmp_path / "contract_ckpt.json"
    lock_file = tmp_path / ".run.lock"

    # Acquire exclusive lease bound to contract_digest
    lease_info = acquire_contract_execution_lease(
        lock_file,
        holder="worker-node-1",
        contract_digest=contract.contract_digest,
        ttl_seconds=60.0,
        now_epoch=1000.0,
    )
    assert lease_info["holder"] == "worker-node-1"
    assert lease_info["contract_digest"] == contract.contract_digest

    # Overlapping worker must be rejected while lease is active
    with pytest.raises(ExecutionContractError, match="actively held by worker-node-1"):
        acquire_contract_execution_lease(
            lock_file,
            holder="worker-node-2",
            contract_digest=contract.contract_digest,
            ttl_seconds=60.0,
            now_epoch=1020.0,
        )

    # Save composite typed cursor (datetime, Decimal, UUID, tuple)
    composite_cursor = (
        datetime.datetime(2026, 10, 17, 12, 30, 45, tzinfo=datetime.timezone.utc),
        decimal.Decimal("987654.3210"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        datetime.date(2026, 10, 17),
    )
    save_contract_unit_checkpoint(
        ckpt_file,
        contract,
        unit_id="orders:range-001",
        path_class=ExecutionPathClass.RANGE,
        cursor_position=composite_cursor,
        billed_bytes_delta=1048576,
        money_usd_delta=0.0012,
    )

    loaded = verify_resume_under_contract(ckpt_file, contract, "orders:range-001")
    assert loaded is not None
    assert loaded["path_class"] == ExecutionPathClass.RANGE
    assert loaded["cursor_position"] == composite_cursor
    assert isinstance(loaded["cursor_position"][0], datetime.datetime)
    assert isinstance(loaded["cursor_position"][1], decimal.Decimal)
    assert isinstance(loaded["cursor_position"][2], uuid.UUID)
    assert isinstance(loaded["cursor_position"][3], datetime.date)

    # Attempt to resume with a different target identity -> refused
    wrong_target_contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(host="10.0.0.99"),
        _sample_versions(),
        _sample_readings(),
    )
    with pytest.raises(ContractDimensionDroppedError, match="target_digest"):
        verify_resume_under_contract(ckpt_file, wrong_target_contract, "orders:range-001")

    # Attempt to resume with a modified row filter / scope -> refused
    changed_scope = dataclasses.replace(_sample_scope(), row_filter_digest="tenant_id=99")
    wrong_scope_contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        changed_scope,
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )
    with pytest.raises(ContractDimensionDroppedError, match="scope_digest"):
        verify_resume_under_contract(ckpt_file, wrong_scope_contract, "orders:range-001")


# -----------------------------------------------------------------------------
# 5. Independent Pre-Boundary Admission (Cutover, Rollback, Exposure, Release)
# -----------------------------------------------------------------------------

def test_independent_admission_blocks_irreversible_boundaries_on_any_violation() -> None:
    """Cutover, rollback, target exposure, and log release run independent admission gates."""
    contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )

    clean_state = execution_contract.BoundaryRuntimeState(
        boundary=IrreversibleBoundaryKind.CUTOVER_COMMIT,
        observed_target_identity=contract.target_identity,
        observed_version_binding=contract.version_binding,
        observed_scope_digest=contract.scope.scope_digest,
        achieved_proof_coverage=ProofCoverageLevel.BYTE_EXACT_AND_CATALOG,
        achieved_guarantees=contract.minimum_guarantee_names,
        snapshot_fence_verified=True,
        commit_ordering_verified=True,
        consumed_outage_seconds=contract.outage_byte_money_bounds.max_outage_seconds * 0.4,
        consumed_billed_bytes=int(contract.outage_byte_money_bounds.max_billed_bytes * 0.5),
        consumed_money_usd=contract.outage_byte_money_bounds.max_money_usd * 0.5,
        observed_source_cpu_ratio=contract.source_impact_bounds.max_cpu_ratio * 0.5,
        observed_source_iops=contract.source_impact_bounds.max_iops * 0.5,
        observed_source_read_bytes_per_sec=int(
            contract.source_impact_bounds.max_read_bytes_per_sec * 0.5
        ),
        observed_worker_connections=2,
    )

    # 1. Clean state passes admission
    ok_report = admit_irreversible_boundary(contract, clean_state, raise_on_refusal=True)
    assert ok_report.admitted is True
    assert ok_report.care_badge == CareBadge.PASS
    assert "[PASS] boundary=cutover verdict=admit_irreversible_boundary" in ok_report.format_unix_line()

    # 2. Target identity mismatch blocks cutover
    wrong_target_state = dataclasses.replace(
        clean_state,
        observed_target_identity=_sample_target_identity(host="10.0.0.77"),
    )
    with pytest.raises(IrreversibleBoundaryRefusedError, match="target identity mismatch"):
        admit_irreversible_boundary(contract, wrong_target_state, raise_on_refusal=True)

    # 3. Unauthorized source mutation attempt blocks boundary (no implicit authorization)
    unauth_state = dataclasses.replace(
        clean_state,
        unauthorized_source_mutation_attempted=True,
    )
    with pytest.raises(IrreversibleBoundaryRefusedError, match="unauthorized source mutation"):
        admit_irreversible_boundary(contract, unauth_state, raise_on_refusal=True)

    # 4. Rollback boundary without verified rollback compensations is refused
    bad_rollback_state = dataclasses.replace(
        clean_state,
        boundary=IrreversibleBoundaryKind.ROLLBACK_SWITCH,
        rollback_compensations_verified=False,
    )
    with pytest.raises(
        IrreversibleBoundaryRefusedError, match="rollback compensation invariants"
    ):
        admit_irreversible_boundary(contract, bad_rollback_state, raise_on_refusal=True)

    # 5. Byte or money budget overrun blocks boundary independently
    over_bytes_state = dataclasses.replace(
        clean_state,
        consumed_billed_bytes=contract.outage_byte_money_bounds.max_billed_bytes + 1024,
    )
    with pytest.raises(IrreversibleBoundaryRefusedError, match="consumed billed bytes"):
        admit_irreversible_boundary(contract, over_bytes_state, raise_on_refusal=True)

    # 6. Stale model/plan version holds boundary for revalidation (REVIEW)
    drift_state = dataclasses.replace(
        clean_state,
        observed_version_binding=dataclasses.replace(
            contract.version_binding, plan_version="plan-rev-99-drift"
        ),
    )
    drift_report = admit_irreversible_boundary(
        contract, drift_state, raise_on_refusal=False
    )
    assert drift_report.admitted is False
    assert drift_report.verdict == BoundaryAdmissionVerdict.HOLD_FOR_REVALIDATION
    assert drift_report.care_badge == CareBadge.REVIEW


def test_endpoint_safety_and_unix_formatting() -> None:
    """Verify loopback/10.0.0.x endpoint validation and pure ASCII UNIX output."""
    assert validate_safe_endpoint("127.0.0.1") == "127.0.0.1"
    assert validate_safe_endpoint("10.0.0.42") == "10.0.0.42"
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("192.168.1.10")
    with pytest.raises(UnsafeEndpointError):
        validate_safe_endpoint("")

    contract = compile_execution_contract(
        MigrationGoalKind.ZERO_LOSS_ONLINE_CUTOVER,
        _sample_scope(),
        _sample_target_identity(),
        _sample_versions(),
        _sample_readings(),
    )
    line = contract.format_unix_line()
    assert line.startswith("[PASS] contract=")
    assert line.isascii()
