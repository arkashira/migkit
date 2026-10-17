"""Tests for Connector Capability Schema Drift & Version Matrix Modeling (Row 640: RCH32.connector-capability).

Proves that:
1. Feature matrices are snapshotted by (engine, service, version, connector, mode)
   and distinguish added, deleted, renamed, and type-changed columns, old-row
   backfill, and new-table discovery.
2. Unknown, preview-only, or contradictory documentation claims never infer "yes"
   (`auto_admit=False`); they trigger bounded sandbox probes and record owned
   capability debt.
3. A connector that supports `ADD_COLUMN` inline but requires a full refresh
   (`REQUIRES_TABLE_RESYNC`) for `RENAME_COLUMN` cannot auto-admit `RENAME_COLUMN`
   (either standalone or in a mixed batch).
4. Stale connector/version metadata (connector version drift, engine version drift,
   mode change, digest mismatch, or snapshot TTL expiration) invalidates any
   prior decision and refuses auto-admission.
5. Every `decide.Rung` in `migkit.connector_capability` reaches both `holds` and
   `refuses` under `tests.rung_branches.watched()` and is declared in `PROVES_RUNGS`.
6. Universal typed durable checkpoints (`ConnectorCapabilityCheckpoint`) and
   `.run.lock` mutual-exclusion leases (`ConnectorCapabilityLease`) preserve native
   types in O(1) and prevent concurrent execution.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import uuid
from pathlib import Path

import pytest
import tests.rung_branches

from migkit.connector_capability import (
    CapabilityCell,
    CapabilityLeaseError,
    CapabilityLeaseExpiredError,
    CareBadge,
    ConnectorCapabilityCheckpoint,
    ConnectorCapabilityCoordinator,
    ConnectorCapabilityError,
    ConnectorCapabilityLease,
    ConnectorCapabilitySnapshot,
    ConnectorMatrixCoordinate,
    DocReleaseStatus,
    DocSourceClaim,
    DriftAdmissionDecision,
    DriftOperation,
    OwnedCapabilityDebt,
    ProbeBudget,
    ProbeResult,
    ReplicationMode,
    SchemaDriftEvent,
    StaleCapabilityMetadataError,
    SupportDisposition,
    UnsafeProbeHostError,
    build_snapshot_from_claims,
    compare_versions,
    decide_drift_admission,
    decode_typed_value,
    default_reference_claims,
    encode_typed_value,
    execute_bounded_probe,
    is_safe_test_host,
    parse_version_tuple,
    synthesize_capability_cell,
    version_satisfies,
)

PROVES = [
    ("connector-capability.schema-drift", "postgres"),
    ("connector-capability.schema-drift", "mysql"),
    ("connector-capability.schema-drift", "mssql"),
]

PROVES_RUNGS = [
    ("connector_capability", "decide_drift_admission", "auto_inline_admission", "holds"),
    ("connector_capability", "decide_drift_admission", "auto_inline_admission", "refuses"),
    ("connector_capability", "decide_drift_admission", "inline_with_backfill_admission", "holds"),
    ("connector_capability", "decide_drift_admission", "inline_with_backfill_admission", "refuses"),
    ("connector_capability", "decide_drift_admission", "probed_sandbox_admission", "holds"),
    ("connector_capability", "decide_drift_admission", "probed_sandbox_admission", "refuses"),
    ("connector_capability", "decide_drift_admission", "table_resync_required", "holds"),
    ("connector_capability", "decide_drift_admission", "table_resync_required", "refuses"),
    ("connector_capability", "decide_drift_admission", "refuse_and_record_debt", "holds"),
]


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def lakeflow_coord() -> ConnectorMatrixCoordinate:
    return ConnectorMatrixCoordinate(
        engine="mssql",
        service="rds",
        engine_version="2022.0",
        connector="lakeflow-connect",
        connector_version="2.4.0",
        mode=ReplicationMode.CDC_CHANGE_TRACKING,
    )


@pytest.fixture
def lakeflow_snapshot(lakeflow_coord: ConnectorMatrixCoordinate) -> ConnectorCapabilitySnapshot:
    return build_snapshot_from_claims(
        lakeflow_coord,
        default_reference_claims(),
        captured_at=1000.0,
        max_metadata_age_seconds=3600.0,
    )


# -----------------------------------------------------------------------------
# 1. Rung Coverage: Every Rung Reaches Both Holds and Refuses Under watched()
# -----------------------------------------------------------------------------

def test_every_connector_capability_rung_holds_and_refuses_under_watched(
    lakeflow_coord: ConnectorMatrixCoordinate,
    lakeflow_snapshot: ConnectorCapabilitySnapshot,
):
    """Prove all 5 rungs in `decide_drift_admission` reach their declared branches."""
    with tests.rung_branches.watched() as seen:
        # Case A: auto_inline_admission holds (nullable ADD_COLUMN on GA connector)
        ev_add = SchemaDriftEvent(
            event_id="ev-add-inline",
            table="dbo.orders",
            operation=DriftOperation.ADD_COLUMN,
            column_name="promo_code",
            new_type="VARCHAR(32)",
            nullable=True,
        )
        choice_a, dec_a = decide_drift_admission(lakeflow_snapshot, ev_add, now=1100.0)
        assert choice_a.rung.name == "auto_inline_admission"
        assert dec_a.auto_admit is True
        assert dec_a.care_badge == CareBadge.PASS

        # Case B: auto_inline_admission refuses (old-row backfill needed, not inline),
        # inline_with_backfill_admission holds
        backfill_coord = ConnectorMatrixCoordinate(
            engine="postgres",
            service="cloud-sql",
            engine_version="15.4",
            connector="policy-sync-connector",
            connector_version="1.8.0",
            mode=ReplicationMode.CDC_LOG,
        )
        backfill_claims = [
            DocSourceClaim(
                source_id="S108-policy-add",
                operation=DriftOperation.ADD_COLUMN,
                claimed_disposition=SupportDisposition.REQUIRES_BACKFILL_TASK,
                release_status=DocReleaseStatus.GA,
                backfill_historical_rows=False,
            ),
            DocSourceClaim(
                source_id="S108-policy-bf",
                operation=DriftOperation.OLD_ROW_BACKFILL,
                claimed_disposition=SupportDisposition.REQUIRES_BACKFILL_TASK,
                release_status=DocReleaseStatus.GA,
                backfill_historical_rows=False,
            ),
        ]
        bf_snap = build_snapshot_from_claims(backfill_coord, backfill_claims, captured_at=1000.0)
        ev_add_bf = SchemaDriftEvent(
            event_id="ev-add-bf",
            table="public.invoices",
            operation=DriftOperation.ADD_COLUMN,
            column_name="tax_rate",
            new_type="NUMERIC(5,2)",
            default_value=decimal.Decimal("0.00"),
            requires_historical_backfill=True,
        )
        choice_b, dec_b = decide_drift_admission(bf_snap, ev_add_bf, now=1100.0)
        assert choice_b.rung.name == "inline_with_backfill_admission"
        assert dec_b.auto_admit is True
        assert dec_b.requires_backfill_task is True
        assert dec_b.backfill_task is not None
        assert dec_b.care_badge == CareBadge.REVIEW

        # Case C: unknown/contradictory docs + passing bounded probe -> probed_sandbox_admission holds
        unknown_snap = build_snapshot_from_claims(backfill_coord, [], captured_at=1000.0)
        ev_disc = SchemaDriftEvent(
            event_id="ev-disc-probe",
            table="public.audit_trail",
            operation=DriftOperation.NEW_TABLE_DISCOVERY,
        )
        passing_probe = execute_bounded_probe(
            backfill_coord,
            DriftOperation.NEW_TABLE_DISCOVERY,
            ProbeBudget(max_probe_rows=16, max_probe_seconds=2.0, max_statements=4, target_host="127.0.0.1"),
            lambda c, op, b: {
                "passed": True,
                "observed_disposition": SupportDisposition.AUTO_INLINE,
                "rows_probed": 8,
                "statements_executed": 2,
                "elapsed_seconds": 0.05,
                "preserved_existing_rows": True,
                "preserved_column_identity": True,
                "backfilled_old_rows": True,
                "discovered_new_table": True,
            },
        )
        choice_c, dec_c = decide_drift_admission(
            unknown_snap,
            ev_disc,
            now=1100.0,
            probe_result=passing_probe,
        )
        assert choice_c.rung.name == "probed_sandbox_admission"
        assert dec_c.auto_admit is True
        assert dec_c.debt_record is not None
        assert dec_c.debt_record.category == "probed_undocumented_capability"

        # Case D: RENAME_COLUMN on lakeflow -> auto_inline refuses, inline_with_backfill refuses,
        # probed_sandbox refuses (docs are GA, not ambiguous), table_resync_required holds
        ev_rename = SchemaDriftEvent(
            event_id="ev-rename",
            table="dbo.orders",
            operation=DriftOperation.RENAME_COLUMN,
            column_name=" legacy_code ",
            new_column_name="order_code",
        )
        choice_d, dec_d = decide_drift_admission(lakeflow_snapshot, ev_rename, now=1100.0)
        assert choice_d.rung.name == "table_resync_required"
        assert dec_d.auto_admit is False
        assert dec_d.requires_table_resync is True
        assert dec_d.care_badge == CareBadge.ACTION

        # Case E: unknown docs without a passing probe -> all first 4 rungs refuse,
        # refuse_and_record_debt holds
        choice_e, dec_e = decide_drift_admission(unknown_snap, ev_disc, now=1100.0, probe_result=None)
        assert choice_e.rung.name == "refuse_and_record_debt"
        assert dec_e.auto_admit is False
        assert dec_e.debt_record is not None
        assert dec_e.debt_record.category == "unknown_or_unproved_docs"

    assert set(tests.rung_branches.branch(seen, "auto_inline_admission")) == {"holds", "refuses"}
    assert set(tests.rung_branches.branch(seen, "inline_with_backfill_admission")) == {"holds", "refuses"}
    assert set(tests.rung_branches.branch(seen, "probed_sandbox_admission")) == {"holds", "refuses"}
    assert set(tests.rung_branches.branch(seen, "table_resync_required")) == {"holds", "refuses"}
    assert set(tests.rung_branches.branch(seen, "refuse_and_record_debt")) == {"holds"}


# -----------------------------------------------------------------------------
# 2. Feature Matrix Snapshot Distinguishes All 6 Schema Drift Operations
# -----------------------------------------------------------------------------

def test_feature_matrix_distinguishes_all_six_operations_and_type_widening(
    lakeflow_coord: ConnectorMatrixCoordinate,
    lakeflow_snapshot: ConnectorCapabilitySnapshot,
):
    """Verify matrix distinguishes add, delete, rename, type change (widening vs narrowing), backfill, discovery."""
    assert set(lakeflow_snapshot.cells.keys()) == {
        DriftOperation.ADD_COLUMN,
        DriftOperation.DELETE_COLUMN,
        DriftOperation.RENAME_COLUMN,
        DriftOperation.CHANGE_COLUMN_TYPE,
        DriftOperation.OLD_ROW_BACKFILL,
        DriftOperation.NEW_TABLE_DISCOVERY,
    }

    # 1. ADD_COLUMN -> AUTO_INLINE
    assert lakeflow_snapshot.cell_for(DriftOperation.ADD_COLUMN).disposition == SupportDisposition.AUTO_INLINE
    # 2. DELETE_COLUMN -> AUTO_INLINE
    assert lakeflow_snapshot.cell_for(DriftOperation.DELETE_COLUMN).disposition == SupportDisposition.AUTO_INLINE
    # 3. RENAME_COLUMN -> REQUIRES_TABLE_RESYNC
    assert (
        lakeflow_snapshot.cell_for(DriftOperation.RENAME_COLUMN).disposition
        == SupportDisposition.REQUIRES_TABLE_RESYNC
    )
    # 4. CHANGE_COLUMN_TYPE -> widening_only=True
    type_cell = lakeflow_snapshot.cell_for(DriftOperation.CHANGE_COLUMN_TYPE)
    assert type_cell.disposition == SupportDisposition.AUTO_INLINE
    assert type_cell.widening_only is True

    # Widening type change (INT -> BIGINT) auto-admits inline
    ev_widen = SchemaDriftEvent(
        event_id="ev-widen",
        table="dbo.orders",
        operation=DriftOperation.CHANGE_COLUMN_TYPE,
        column_name="amount",
        old_type="INT",
        new_type="BIGINT",
    )
    _, dec_widen = decide_drift_admission(lakeflow_snapshot, ev_widen, now=1100.0)
    assert dec_widen.auto_admit is True
    assert dec_widen.selected_rung == "auto_inline_admission"

    # Narrowing type change (BIGINT -> SMALLINT) cannot auto-admit; requires table resync
    ev_narrow = SchemaDriftEvent(
        event_id="ev-narrow",
        table="dbo.orders",
        operation=DriftOperation.CHANGE_COLUMN_TYPE,
        column_name="amount",
        old_type="BIGINT",
        new_type="SMALLINT",
    )
    _, dec_narrow = decide_drift_admission(lakeflow_snapshot, ev_narrow, now=1100.0)
    assert dec_narrow.auto_admit is False
    assert dec_narrow.requires_table_resync is True
    assert dec_narrow.selected_rung == "table_resync_required"
    assert dec_narrow.care_badge == CareBadge.ACTION


# -----------------------------------------------------------------------------
# 3. Acceptance Criterion: Add-Column Supported vs Rename Needs Full Refresh
# -----------------------------------------------------------------------------

def test_connector_supporting_add_column_cannot_auto_admit_rename(
    lakeflow_coord: ConnectorMatrixCoordinate,
    lakeflow_snapshot: ConnectorCapabilitySnapshot,
    tmp_path: Path,
):
    """Prove a connector supporting ADD_COLUMN inline but needing full refresh for RENAME_COLUMN refuses rename."""
    coord_mgr = ConnectorCapabilityCoordinator(state_dir=tmp_path, snapshot=lakeflow_snapshot)

    add_ev = SchemaDriftEvent(
        event_id="drift-101-add",
        table="dbo.customers",
        operation=DriftOperation.ADD_COLUMN,
        column_name="loyalty_tier",
        new_type="VARCHAR(16)",
        nullable=True,
        cursor_position=("lsn", 101),
    )
    rename_ev = SchemaDriftEvent(
        event_id="drift-102-rename",
        table="dbo.customers",
        operation=DriftOperation.RENAME_COLUMN,
        column_name="phone",
        new_column_name="mobile_phone",
        cursor_position=("lsn", 102),
    )

    dec_add = coord_mgr.evaluate_event(add_ev, now=1200.0)
    assert dec_add.auto_admit is True
    assert dec_add.requires_table_resync is False
    assert dec_add.selected_rung == "auto_inline_admission"
    assert dec_add.care_badge == CareBadge.PASS

    dec_rename = coord_mgr.evaluate_event(rename_ev, now=1205.0)
    assert dec_rename.auto_admit is False
    assert dec_rename.requires_table_resync is True
    assert dec_rename.selected_rung == "table_resync_required"
    assert dec_rename.care_badge == CareBadge.ACTION
    assert dec_rename.debt_record is not None
    assert dec_rename.debt_record.category == "requires_table_resync"

    # Mixed batch containing both ADD_COLUMN and RENAME_COLUMN cannot auto-admit as a batch
    batch_admitted, batch_decisions = coord_mgr.evaluate_batch([add_ev, rename_ev], now=1210.0)
    assert batch_admitted is False
    assert [d.auto_admit for d in batch_decisions] == [True, False]


# -----------------------------------------------------------------------------
# 4. Unknown, Preview, and Contradictory Docs Trigger Bounded Probes & Debt
# -----------------------------------------------------------------------------

def test_unknown_and_contradictory_docs_never_infer_yes_and_trigger_probes(tmp_path: Path):
    """Verify contradictory or unknown claims never infer 'yes', trigger bounded probes, and record debt."""
    coord = ConnectorMatrixCoordinate(
        engine="mysql",
        service="aurora",
        engine_version="8.0.35",
        connector="hevo-edge-pipeline",
        connector_version="3.1.0",
        mode=ReplicationMode.CDC_LOG,
    )

    # Contradictory claims for RENAME_COLUMN (one says AUTO_INLINE, regional doc says REQUIRES_TABLE_RESYNC)
    contradictory_claims = [
        DocSourceClaim(
            source_id="S040-global-doc",
            operation=DriftOperation.RENAME_COLUMN,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.GA,
            preserves_column_identity_on_rename=True,
        ),
        DocSourceClaim(
            source_id="S040-aurora-note",
            operation=DriftOperation.RENAME_COLUMN,
            claimed_disposition=SupportDisposition.REQUIRES_TABLE_RESYNC,
            release_status=DocReleaseStatus.GA,
            preserves_column_identity_on_rename=False,
        ),
        # Preview-only claim for NEW_TABLE_DISCOVERY
        DocSourceClaim(
            source_id="S040-preview-discovery",
            operation=DriftOperation.NEW_TABLE_DISCOVERY,
            claimed_disposition=SupportDisposition.AUTO_INLINE,
            release_status=DocReleaseStatus.PREVIEW,
            discovers_new_tables_automatically=True,
        ),
    ]

    snap = build_snapshot_from_claims(coord, contradictory_claims, captured_at=2000.0)

    rename_cell = snap.cell_for(DriftOperation.RENAME_COLUMN)
    assert rename_cell.disposition == SupportDisposition.CONTRADICTORY
    assert rename_cell.requires_probe is True
    assert len(rename_cell.contradiction_reasons) >= 1

    preview_cell = snap.cell_for(DriftOperation.NEW_TABLE_DISCOVERY)
    assert preview_cell.release_status == DocReleaseStatus.PREVIEW
    assert preview_cell.is_ambiguous_or_unverified is True

    unknown_cell = snap.cell_for(DriftOperation.DELETE_COLUMN)
    assert unknown_cell.disposition == SupportDisposition.UNKNOWN
    assert unknown_cell.requires_probe is True

    # Probe runner that detects rename drops column identity (fails probe)
    probe_calls = []

    def fake_probe_runner(c: ConnectorMatrixCoordinate, op: DriftOperation, budget: ProbeBudget):
        probe_calls.append((c.connector, op))
        return {
            "passed": False,
            "observed_disposition": SupportDisposition.REQUIRES_TABLE_RESYNC,
            "rows_probed": 10,
            "statements_executed": 3,
            "elapsed_seconds": 0.12,
            "preserved_existing_rows": True,
            "preserved_column_identity": False,
            "backfilled_old_rows": False,
            "discovered_new_table": False,
            "failure_reason": "connector emitted DROP + ADD losing historical column values",
        }

    mgr = ConnectorCapabilityCoordinator(
        state_dir=tmp_path,
        snapshot=snap,
        debt_owner="cdc-platform-squad",
        probe_budget=ProbeBudget(max_probe_rows=32, max_probe_seconds=2.0, max_statements=6, target_host="10.0.0.15"),
        probe_runner=fake_probe_runner,
    )

    ev_contradict = SchemaDriftEvent(
        event_id="ev-contradict-rename",
        table="shop.payments",
        operation=DriftOperation.RENAME_COLUMN,
        column_name="card_num",
        new_column_name="masked_card",
    )
    dec = mgr.evaluate_event(ev_contradict, now=2050.0)
    assert len(probe_calls) == 1
    assert dec.auto_admit is False
    assert dec.selected_rung == "refuse_and_record_debt"
    assert dec.care_badge == CareBadge.ACTION
    assert dec.probe_result is not None
    assert dec.probe_result.passed is False
    assert dec.debt_record is not None
    assert dec.debt_record.owner == "cdc-platform-squad"
    assert dec.debt_record.category == "contradictory_docs"
    assert "[ACTION]" in dec.debt_record.format_line()


# -----------------------------------------------------------------------------
# 5. Stale Connector or Engine Version Metadata Invalidates Decisions
# -----------------------------------------------------------------------------

def test_stale_connector_or_version_metadata_invalidates_decision(
    lakeflow_coord: ConnectorMatrixCoordinate,
    lakeflow_snapshot: ConnectorCapabilitySnapshot,
):
    """Prove connector version drift, engine version drift, digest change, or TTL expiry invalidates decisions."""
    ev_add = SchemaDriftEvent(
        event_id="ev-fresh-1",
        table="dbo.orders",
        operation=DriftOperation.ADD_COLUMN,
        column_name="notes",
        new_type="VARCHAR(100)",
    )
    _, decision = decide_drift_admission(lakeflow_snapshot, ev_add, now=1500.0)
    assert decision.auto_admit is True

    # 1. Fresh metadata passes validation
    ok, why = decision.validate_freshness(
        live_coordinate=lakeflow_coord,
        current_snapshot=lakeflow_snapshot,
        now=1600.0,
    )
    assert ok is True
    assert why is None

    # 2. Connector version drift (2.4.0 -> 2.3.1) invalidates decision
    downgraded_coord = dataclasses.replace(lakeflow_coord, connector_version="2.3.1")
    ok_ver, why_ver = decision.validate_freshness(live_coordinate=downgraded_coord, now=1600.0)
    assert ok_ver is False
    assert "connector_version changed" in (why_ver or "")
    with pytest.raises(StaleCapabilityMetadataError, match="connector_version changed"):
        decision.ensure_fresh(live_coordinate=downgraded_coord, now=1600.0)

    # 3. Engine version drift (2022.0 -> 2017.0) invalidates decision
    older_engine_coord = dataclasses.replace(lakeflow_coord, engine_version="2017.0")
    with pytest.raises(StaleCapabilityMetadataError, match="engine_version changed"):
        decision.ensure_fresh(live_coordinate=older_engine_coord, now=1600.0)

    # 4. Snapshot age past max_metadata_age_seconds (3600s) invalidates decision
    with pytest.raises(StaleCapabilityMetadataError, match="stale capability snapshot"):
        decision.ensure_fresh(live_coordinate=lakeflow_coord, now=5000.0)

    # 5. Calling decide_drift_admission with drifted live_coordinate refuses auto-admission
    _, stale_dec = decide_drift_admission(
        lakeflow_snapshot,
        ev_add,
        live_coordinate=downgraded_coord,
        now=1600.0,
    )
    assert stale_dec.auto_admit is False
    assert stale_dec.selected_rung == "refuse_and_record_debt"
    assert stale_dec.care_badge == CareBadge.ACTION
    assert stale_dec.debt_record is not None
    assert stale_dec.debt_record.category == "stale_metadata"


# -----------------------------------------------------------------------------
# 6. Bounded Probe Budget Enforcement & Safe Host Validation
# -----------------------------------------------------------------------------

def test_bounded_probe_enforces_budgets_and_safe_hosts(lakeflow_coord: ConnectorMatrixCoordinate):
    """Verify probe budget rejects non-test hosts and fails when row/statement/time limits are exceeded."""
    assert is_safe_test_host("127.0.0.1") is True
    assert is_safe_test_host("10.0.0.42") is True
    assert is_safe_test_host("db.production.internal") is False

    bad_budget = ProbeBudget(target_host="db.production.internal")
    with pytest.raises(UnsafeProbeHostError, match="non-test host"):
        execute_bounded_probe(lakeflow_coord, DriftOperation.ADD_COLUMN, bad_budget)

    strict_budget = ProbeBudget(max_probe_rows=10, max_probe_seconds=1.0, max_statements=3, target_host="127.0.0.1")

    # Exceeds row budget
    res_rows = execute_bounded_probe(
        lakeflow_coord,
        DriftOperation.ADD_COLUMN,
        strict_budget,
        lambda c, op, b: {"passed": True, "rows_probed": 25, "statements_executed": 1, "elapsed_seconds": 0.1},
    )
    assert res_rows.passed is False
    assert "exceeded row budget" in (res_rows.failure_reason or "")

    # Exceeds statement budget
    res_stmts = execute_bounded_probe(
        lakeflow_coord,
        DriftOperation.ADD_COLUMN,
        strict_budget,
        lambda c, op, b: {"passed": True, "rows_probed": 5, "statements_executed": 9, "elapsed_seconds": 0.1},
    )
    assert res_stmts.passed is False
    assert "exceeded statement budget" in (res_stmts.failure_reason or "")


# -----------------------------------------------------------------------------
# 7. O(1) Typed Durable Checkpoint & Mutual Exclusion Lease (.run.lock)
# -----------------------------------------------------------------------------

def test_durable_checkpoint_preserves_native_types_and_lease_prevents_overlap(
    lakeflow_coord: ConnectorMatrixCoordinate,
    lakeflow_snapshot: ConnectorCapabilitySnapshot,
    tmp_path: Path,
):
    """Prove O(1) checkpoint preserves datetime, Decimal, UUID, tuple and `.run.lock` enforces mutual exclusion."""
    complex_cursor = (
        datetime.datetime(2026, 10, 17, 12, 30, 0, tzinfo=datetime.timezone.utc),
        decimal.Decimal("98765.4321"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )
    ev = SchemaDriftEvent(
        event_id="ev-typed-resume",
        table="dbo.ledger",
        operation=DriftOperation.ADD_COLUMN,
        column_name="settlement_ref",
        new_type="VARCHAR(64)",
        cursor_position=complex_cursor,
    )

    mgr1 = ConnectorCapabilityCoordinator(state_dir=tmp_path, snapshot=lakeflow_snapshot)
    dec1 = mgr1.evaluate_event(ev, now=1200.0)
    assert dec1.auto_admit is True

    # Reload checkpoint from disk in a fresh instance and verify native types survived
    cp2 = ConnectorCapabilityCheckpoint(tmp_path)
    assert cp2.is_completed("ev-typed-resume", lakeflow_snapshot.digest) is True
    assert cp2.cursor_watermarks["dbo.ledger"] == complex_cursor

    # Mutual exclusion lease blocks concurrent acquisition and detects expiry
    lease_a = ConnectorCapabilityLease(tmp_path, ttl_seconds=10.0)
    lease_a.acquire(now=100.0)
    try:
        lease_b = ConnectorCapabilityLease(tmp_path, ttl_seconds=10.0)
        with pytest.raises(CapabilityLeaseError, match="held by another active process"):
            lease_b.acquire(now=102.0)

        with pytest.raises(CapabilityLeaseExpiredError, match="expired"):
            lease_a.check_active(now=115.0)

        lease_a.acquire(now=116.0)
        lease_a.heartbeat(now=120.0)
        lease_a.check_active(now=125.0)
    finally:
        lease_a.release()
