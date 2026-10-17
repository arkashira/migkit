"""Tests for Row 590: SMART-N7.proof-planning (Plan the deepest required proof with explicit coverage).

Verifies:
1. Every `decide.Rung` in `migkit/proof_planning.py` (`PROOF_DEPTH_LADDER`,
   `EVIDENCE_REUSE_LADDER`, `READER_CORRELATION_LADDER`, and
   `PROOF_VERDICT_GATE_LADDER`) reaches both `holds` and `refuses` under
   `tests.rung_branches.watched()`.
2. Proof dependency/coverage DAG covers values, schema, sequences, grants,
   constraints, stored code, and application-visible semantics under explicit
   epochs/fences, detects cycles, and computes transitive affected closures.
3. Prior evidence is reused only when backed by a valid dependency certificate;
   changed ranges and transitively affected downstream objects are rechecked,
   and source reads + execution cost are measured.
4. Independent readers require compatible canonicalization and documented
   non-overlapping blind spots before cross-check trust is granted.
5. Sampling-only evidence stays `incomplete` (`REVIEW`), never `same`, and
   keyless multisets preserve exact duplicate multiplicity.
6. Injected omissions, RLS mismatches, DDL drift, canonicalization defects,
   and concurrent writes cannot produce a false `same` verdict.
7. O(1) typed durable checkpoint resume (`datetime`, `Decimal`, `UUID`,
   composite tuple cursors), active verification lease lock (`.run.lock`) with
   heartbeat, adaptive AIMD batch sizing with instant cold-start byte-weighted
   ETA, and external tool normalization with zero internal tool leaks.
"""

from __future__ import annotations

import datetime
import decimal
import uuid
from pathlib import Path
from typing import Dict, Tuple

import pytest
import tests.rung_branches
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

from migkit import proof_planning as pp

PROVES_RUNGS = [
    # PROOF_DEPTH_LADDER
    ("proof_planning", "PROOF_DEPTH_LADDER", "keyless_multiset_exact", "holds"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "keyless_multiset_exact", "refuses"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "rls_epoch_fenced_full_scan", "holds"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "rls_epoch_fenced_full_scan", "refuses"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "merkle_range_fused_exact", "holds"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "merkle_range_fused_exact", "refuses"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "full_canonical_row_proof", "holds"),
    ("proof_planning", "PROOF_DEPTH_LADDER", "full_canonical_row_proof", "refuses"),
    # EVIDENCE_REUSE_LADDER
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "reuse_certified_evidence", "holds"),
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "reuse_certified_evidence", "refuses"),
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "recheck_changed_ranges_only", "holds"),
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "recheck_changed_ranges_only", "refuses"),
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "recheck_full_node_and_dependents", "holds"),
    ("proof_planning", "EVIDENCE_REUSE_LADDER", "recheck_full_node_and_dependents", "refuses"),
    # READER_CORRELATION_LADDER
    ("proof_planning", "READER_CORRELATION_LADDER", "independent_compatible_readers", "holds"),
    ("proof_planning", "READER_CORRELATION_LADDER", "independent_compatible_readers", "refuses"),
    ("proof_planning", "READER_CORRELATION_LADDER", "single_reader_escalated_exact", "holds"),
    ("proof_planning", "READER_CORRELATION_LADDER", "single_reader_escalated_exact", "refuses"),
    # PROOF_VERDICT_GATE_LADDER
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "refuse_false_same_on_defect", "holds"),
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "refuse_false_same_on_defect", "refuses"),
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "incomplete_sampling_or_unfenced", "holds"),
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "incomplete_sampling_or_unfenced", "refuses"),
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "certified_complete_same", "holds"),
    ("proof_planning", "PROOF_VERDICT_GATE_LADDER", "certified_complete_same", "refuses"),
]


def _make_fence(
    *,
    schema_ddl_epoch: int = 10,
    rls_policy_epoch: int = 3,
    canonical_contract_version: str = "canon-v2",
    captured_at_s: float = 1000.0,
) -> pp.ProofEpochFence:
    return pp.ProofEpochFence(
        source_host="127.0.0.1",
        target_host="10.0.0.21",
        schema_ddl_epoch=schema_ddl_epoch,
        rls_policy_epoch=rls_policy_epoch,
        canonical_contract_version=canonical_contract_version,
        source_watermark_pos="0/16B3748",
        target_watermark_pos="0/16B3748",
        captured_at_s=captured_at_s,
        measured_scan_window_s=2.0,
        measured_apply_lag_s=0.5,
        measured_retention_horizon_s=300.0,
    )


def _make_full_seven_domain_dag(
    *,
    r1_marker: str = "m1",
    r2_marker: str = "m1",
    r2_target_rows: Tuple[Tuple[object, ...], ...] = ((3, "c"), (4, "d")),
    keyless_target_rows: Tuple[Tuple[object, ...], ...] = (
        ("dup", decimal.Decimal("10.00")),
        ("dup", decimal.Decimal("10.00")),
        ("unique", decimal.Decimal("5.50")),
    ),
    schema_target_fp: str = "ddl-orders-v1",
    rls_target_fp: str = "rls-tenant-visible-100",
    seq_target_val: int = 505,
    stored_code_target_fp: str = "fn-calc-tax-v1",
    concurrent_write: bool = False,
    sample_only: bool = False,
) -> pp.ProofCoverageDAG:
    node_schema = pp.ProofCoverageNode(
        node_id="schema:orders",
        domain=pp.ProofDomain.SCHEMA,
        object_name="public.orders",
        depends_on=(),
        source_definition_fingerprint="ddl-orders-v1",
        target_definition_fingerprint=schema_target_fp,
        estimated_rows=1,
        estimated_bytes=512,
    )
    node_constraints = pp.ProofCoverageNode(
        node_id="constraints:orders_pk_fk",
        domain=pp.ProofDomain.CONSTRAINTS,
        object_name="public.orders_constraints",
        depends_on=("schema:orders",),
        source_definition_fingerprint="pk-fk-valid-v1",
        target_definition_fingerprint="pk-fk-valid-v1",
        estimated_rows=1,
        estimated_bytes=256,
    )
    node_sequences = pp.ProofCoverageNode(
        node_id="sequences:orders_id_seq",
        domain=pp.ProofDomain.SEQUENCES,
        object_name="public.orders_id_seq",
        depends_on=("schema:orders",),
        source_definition_fingerprint="seq-meta-v1",
        target_definition_fingerprint="seq-meta-v1",
        source_sequence_value=500,
        target_sequence_value=seq_target_val,
        estimated_rows=1,
        estimated_bytes=128,
    )
    node_grants = pp.ProofCoverageNode(
        node_id="grants:orders_rls",
        domain=pp.ProofDomain.GRANTS,
        object_name="public.orders_rls_policy",
        depends_on=("schema:orders",),
        has_rls=True,
        source_definition_fingerprint="grant-app-role-v1",
        target_definition_fingerprint="grant-app-role-v1",
        source_rls_visible_fingerprint="rls-tenant-visible-100",
        target_rls_visible_fingerprint=rls_target_fp,
        estimated_rows=100,
        estimated_bytes=4096,
    )
    node_values_keyed = pp.ProofCoverageNode(
        node_id="values:orders",
        domain=pp.ProofDomain.VALUES,
        object_name="public.orders",
        depends_on=("schema:orders", "constraints:orders_pk_fk"),
        is_keyless=False,
        has_rls=False,
        supports_fused_merkle=True,
        change_marker=f"{r1_marker}:{r2_marker}",
        concurrent_write_after_fence=concurrent_write,
        is_sample_only=sample_only,
        ranges=(
            pp.KeyRangeSpec(
                range_id="r1",
                cursor_start=1,
                cursor_end=2,
                estimated_rows=2,
                estimated_bytes=2048,
                change_marker=r1_marker,
                source_rows=((1, "a"), (2, "b")),
                target_rows=((1, "a"), (2, "b")),
            ),
            pp.KeyRangeSpec(
                range_id="r2",
                cursor_start=3,
                cursor_end=4,
                estimated_rows=2,
                estimated_bytes=2048,
                change_marker=r2_marker,
                source_rows=((3, "c"), (4, "d")),
                target_rows=r2_target_rows,
            ),
        ),
    )
    node_values_keyless = pp.ProofCoverageNode(
        node_id="values:audit_events_keyless",
        domain=pp.ProofDomain.VALUES,
        object_name="public.audit_events",
        depends_on=("schema:orders",),
        is_keyless=True,
        ranges=(
            pp.KeyRangeSpec(
                range_id="b0",
                cursor_start=None,
                cursor_end=None,
                estimated_rows=3,
                estimated_bytes=1536,
                change_marker="km1",
                source_rows=(
                    ("dup", decimal.Decimal("10.00")),
                    ("dup", decimal.Decimal("10.00")),
                    ("unique", decimal.Decimal("5.50")),
                ),
                target_rows=keyless_target_rows,
            ),
        ),
    )
    node_stored_code = pp.ProofCoverageNode(
        node_id="stored_code:calc_tax",
        domain=pp.ProofDomain.STORED_CODE,
        object_name="public.calc_tax()",
        depends_on=("schema:orders",),
        source_definition_fingerprint="fn-calc-tax-v1",
        target_definition_fingerprint=stored_code_target_fp,
        estimated_rows=1,
        estimated_bytes=1024,
    )
    node_app_semantics = pp.ProofCoverageNode(
        node_id="application_semantics:checkout_readiness",
        domain=pp.ProofDomain.APPLICATION_SEMANTICS,
        object_name="app.checkout_flow",
        depends_on=(
            "values:orders",
            "grants:orders_rls",
            "sequences:orders_id_seq",
            "stored_code:calc_tax",
        ),
        source_definition_fingerprint="sem-checkout-v1",
        target_definition_fingerprint="sem-checkout-v1",
        source_rls_visible_fingerprint="app-role-visible-ok",
        target_rls_visible_fingerprint="app-role-visible-ok",
        estimated_rows=10,
        estimated_bytes=2048,
    )
    return pp.ProofCoverageDAG(
        (
            node_schema,
            node_constraints,
            node_sequences,
            node_grants,
            node_values_keyed,
            node_values_keyless,
            node_stored_code,
            node_app_semantics,
        )
    )


# -----------------------------------------------------------------------------
# 1. Decision Engine Rung Branch Verification (watched)
# -----------------------------------------------------------------------------

def test_all_proof_planning_rungs_hold_and_refuse_under_watched() -> None:
    fence = _make_fence()
    dag = _make_full_seven_domain_dag()

    with watched() as seen:
        # A. PROOF_DEPTH_LADDER:
        # 1) keyless_multiset_exact holds
        c_keyless = pp.select_proof_depth(
            "depth:keyless",
            dag.nodes["values:audit_events_keyless"],
        )
        assert c_keyless.path == "keyless_multiset_exact"

        # 2) keyless_multiset_exact refuses, rls_epoch_fenced_full_scan holds
        c_rls = pp.select_proof_depth(
            "depth:rls",
            dag.nodes["grants:orders_rls"],
        )
        assert c_rls.path == "rls_epoch_fenced_full_scan"

        # 3) merkle_range_fused_exact holds (keyed value node with ranges)
        c_merkle = pp.select_proof_depth(
            "depth:merkle",
            dag.nodes["values:orders"],
        )
        assert c_merkle.path == "merkle_range_fused_exact"

        # 4) merkle_range_fused_exact refuses -> full_canonical_row_proof holds
        c_full = pp.select_proof_depth(
            "depth:full",
            dag.nodes["schema:orders"],
        )
        assert c_full.path == "full_canonical_row_proof"

        # 5) full_canonical_row_proof refuses when scope_valid=False
        c_depth_refused = pp.select_proof_depth(
            "depth:refused",
            dag.nodes["schema:orders"],
            scope_valid=False,
        )
        assert c_depth_refused.rung is None
        assert c_depth_refused.path is None

        # B. EVIDENCE_REUSE_LADDER:
        val_node = dag.nodes["values:orders"]
        valid_cert = pp.DependencyCertificate.issue(
            node=val_node,
            fence=fence,
            upstream_certificate_digests={"schema:orders": "d1", "constraints:orders_pk_fk": "d2"},
            evidence_digest="ev-123",
            verified_at_s=1000.0,
        )

        # 1) reuse_certified_evidence holds
        c_reuse, ch_ranges = pp.select_evidence_reuse(
            "reuse:certified",
            val_node,
            fence=fence,
            prior_certificate=valid_cert,
            current_upstream_digests={"schema:orders": "d1", "constraints:orders_pk_fk": "d2"},
            now_s=1005.0,
        )
        assert c_reuse.path == "reuse_certified_evidence"
        assert ch_ranges == ()

        # 2) reuse_certified_evidence refuses, recheck_changed_ranges_only holds
        dag_one_range_changed = _make_full_seven_domain_dag(r2_marker="m2")
        val_node_moved = dag_one_range_changed.nodes["values:orders"]
        c_partial, ch_ranges_partial = pp.select_evidence_reuse(
            "reuse:partial",
            val_node_moved,
            fence=fence,
            prior_certificate=valid_cert,
            current_upstream_digests={"schema:orders": "d1", "constraints:orders_pk_fk": "d2"},
            now_s=1005.0,
        )
        assert c_partial.path == "recheck_changed_ranges_only"
        assert ch_ranges_partial == ("r2",)

        # 3) recheck_changed_ranges_only refuses -> recheck_full_node_and_dependents holds
        c_recheck_full, _ = pp.select_evidence_reuse(
            "reuse:full",
            val_node_moved,
            fence=fence,
            prior_certificate=None,
            current_upstream_digests={},
            now_s=1005.0,
        )
        assert c_recheck_full.path == "recheck_full_node_and_dependents"

        # 4) recheck_full_node_and_dependents refuses when scope_valid=False
        c_reuse_refused, _ = pp.select_evidence_reuse(
            "reuse:refused",
            val_node_moved,
            fence=fence,
            prior_certificate=None,
            current_upstream_digests={},
            now_s=1005.0,
            scope_valid=False,
        )
        assert c_reuse_refused.rung is None
        assert c_reuse_refused.path is None

        # C. READER_CORRELATION_LADDER:
        r_primary = pp.IndependentReaderSpec(
            reader_id="wire_cursor_reader",
            canonical_contract_version="canon-v2",
            documented_blind_spots=("toast_compression_header",),
        )
        r_secondary_ok = pp.IndependentReaderSpec(
            reader_id="snapshot_export_reader",
            canonical_contract_version="canon-v2",
            documented_blind_spots=("physical_ctid_order",),
        )
        r_secondary_bad = pp.IndependentReaderSpec(
            reader_id="correlated_reader",
            canonical_contract_version="canon-v1",
            documented_blind_spots=("toast_compression_header",),
            undocumented_blind_spots=("locale_collation_fold",),
        )

        # 1) independent_compatible_readers holds
        c_readers_ok, issues_ok = pp.evaluate_reader_correlation(
            "readers:compatible",
            (r_primary, r_secondary_ok),
            fence,
        )
        assert c_readers_ok.path == "independent_compatible_readers"
        assert issues_ok == ()

        # 2) independent_compatible_readers refuses -> single_reader_escalated_exact holds
        c_readers_esc, issues_bad = pp.evaluate_reader_correlation(
            "readers:escalated",
            (r_primary, r_secondary_bad),
            fence,
        )
        assert c_readers_esc.path == "single_reader_escalated_exact"
        assert "toast_compression_header" in issues_bad
        assert "locale_collation_fold" in issues_bad

        # 3) single_reader_escalated_exact refuses when scope_valid=False
        c_readers_refused, _ = pp.evaluate_reader_correlation(
            "readers:refused",
            (r_primary,),
            fence,
            scope_valid=False,
        )
        assert c_readers_refused.rung is None
        assert c_readers_refused.path is None

        # D. PROOF_VERDICT_GATE_LADDER:
        # 1) refuse_false_same_on_defect holds
        c_gate_defect = pp.select_proof_verdict_gate(
            "gate:defect",
            defect_detected=True,
            incomplete_coverage=False,
            all_nodes_verified=False,
        )
        assert c_gate_defect.path == "refuse_false_same_on_defect"

        # 2) refuse_false_same_on_defect refuses -> incomplete_sampling_or_unfenced holds
        c_gate_inc = pp.select_proof_verdict_gate(
            "gate:incomplete",
            defect_detected=False,
            incomplete_coverage=True,
            all_nodes_verified=False,
        )
        assert c_gate_inc.path == "incomplete_sampling_or_unfenced"

        # 3) certified_complete_same holds
        c_gate_same = pp.select_proof_verdict_gate(
            "gate:same",
            defect_detected=False,
            incomplete_coverage=False,
            all_nodes_verified=True,
        )
        assert c_gate_same.path == "certified_complete_same"

        # 4) certified_complete_same refuses when all_nodes_verified=False (and scope_valid=False)
        c_gate_refused = pp.select_proof_verdict_gate(
            "gate:refused",
            defect_detected=False,
            incomplete_coverage=False,
            all_nodes_verified=False,
            scope_valid=False,
        )
        assert c_gate_refused.rung is None
        assert c_gate_refused.path is None

    for _, ladder_name, rung_name, expected_branch in PROVES_RUNGS:
        observed = branch(seen, rung_name)
        assert expected_branch in observed, (
            f"expected branch {expected_branch!r} for {ladder_name}.{rung_name}, got {observed}"
        )
        if expected_branch == REFUSES:
            assert why(seen, rung_name) is not None


# -----------------------------------------------------------------------------
# 2. Coverage DAG Topology, Cycle Detection, and Transitive Invalidation
# -----------------------------------------------------------------------------

def test_coverage_dag_topological_order_and_affected_closure() -> None:
    dag = _make_full_seven_domain_dag()
    topo = dag.topological_order()
    assert topo.index("schema:orders") < topo.index("constraints:orders_pk_fk")
    assert topo.index("constraints:orders_pk_fk") < topo.index("values:orders")
    assert topo.index("values:orders") < topo.index("application_semantics:checkout_readiness")
    assert dag.covered_domains() == frozenset(pp.ProofDomain)

    # Changing grants:orders_rls invalidates grants:orders_rls and application_semantics:checkout_readiness
    affected_rls = dag.affected_closure(["grants:orders_rls"])
    assert affected_rls == (
        "grants:orders_rls",
        "application_semantics:checkout_readiness",
    )

    # Changing schema:orders invalidates the entire dependent subgraph
    affected_schema = dag.affected_closure(["schema:orders"])
    assert set(affected_schema) == set(dag.nodes.keys())


def test_coverage_dag_rejects_cycles_and_missing_dependencies() -> None:
    n1 = pp.ProofCoverageNode(
        node_id="a",
        domain=pp.ProofDomain.SCHEMA,
        object_name="a",
        depends_on=("b",),
    )
    n2 = pp.ProofCoverageNode(
        node_id="b",
        domain=pp.ProofDomain.VALUES,
        object_name="b",
        depends_on=("a",),
    )
    with pytest.raises(pp.ProofDAGCycleError):
        pp.ProofCoverageDAG((n1, n2))

    n_missing = pp.ProofCoverageNode(
        node_id="orphan",
        domain=pp.ProofDomain.VALUES,
        object_name="orphan",
        depends_on=("nonexistent_node",),
    )
    with pytest.raises(pp.ProofDAGDependencyError):
        pp.ProofCoverageDAG((n_missing,))


# -----------------------------------------------------------------------------
# 3. Certificate Reuse, Partial Range Recheck & Source Read Cost Reduction
# -----------------------------------------------------------------------------

def test_dependency_certificate_reuse_and_changed_range_recheck_save_source_reads() -> None:
    fence = _make_fence()
    dag_initial = _make_full_seven_domain_dag()

    # Pass 1: Cold run with no prior certificates reads all nodes and ranges
    report_cold = pp.execute_proof_plan(dag_initial, fence, now_s=1001.0)
    assert report_cold.verdict_word == "same"
    assert report_cold.care_badge == pp.CareBadge.PASS
    assert report_cold.total_reused_nodes == 0
    assert report_cold.total_source_rows_read > 0

    certs_pass1: Dict[str, pp.DependencyCertificate] = {
        nid: outcome.issued_certificate
        for nid, outcome in report_cold.node_outcomes.items()
        if outcome.issued_certificate is not None
    }
    assert len(certs_pass1) == len(dag_initial.nodes)

    # Pass 2: Nothing changed and certificates are valid -> 100% reuse, 0 source rows read
    report_warm = pp.execute_proof_plan(
        dag_initial,
        fence,
        prior_certificates=certs_pass1,
        now_s=1005.0,
    )
    assert report_warm.verdict_word == "same"
    assert report_warm.total_reused_nodes == len(dag_initial.nodes)
    assert report_warm.total_source_rows_read == 0
    assert report_warm.total_measured_cost_s == 0.0

    # Pass 3: Only range r2 on values:orders moved (from m1 to m2), while r1 held still.
    # Schema, sequences, grants, constraints, keyless table, and stored_code all reuse certificates;
    # values:orders rechecks ONLY range r2 (reading 2 rows instead of 4), and downstream
    # application_semantics:checkout_readiness rechecks because its upstream values:orders digest moved!
    dag_r2_moved = _make_full_seven_domain_dag(r2_marker="m2")
    report_incremental = pp.execute_proof_plan(
        dag_r2_moved,
        fence,
        prior_certificates=certs_pass1,
        now_s=1010.0,
    )
    assert report_incremental.verdict_word == "same"
    val_outcome = report_incremental.node_outcomes["values:orders"]
    assert val_outcome.reuse_kind == pp.EvidenceReuseKind.RECHECK_CHANGED_RANGES_ONLY
    assert val_outcome.rechecked_range_ids == ("r2",)
    assert val_outcome.source_rows_read == 2
    assert report_incremental.total_source_rows_read < report_cold.total_source_rows_read
    assert report_incremental.total_measured_cost_s < report_cold.total_measured_cost_s


# -----------------------------------------------------------------------------
# 4. Soundness: Omissions, RLS, DDL, Canonicalization, Concurrent Writes,
#    Sampling-Only Incompleteness, and Keyless Multiplicity
# -----------------------------------------------------------------------------

def test_injected_omissions_rls_ddl_canonicalization_and_concurrent_writes_never_produce_false_same() -> None:
    fence = _make_fence()

    # 1. Injected row omission in range r2 ((3, 'c') only, missing (4, 'd'))
    dag_omission = _make_full_seven_domain_dag(r2_target_rows=((3, "c"),))
    rep_omission = pp.execute_proof_plan(dag_omission, fence)
    assert rep_omission.verdict_word == "different"
    assert rep_omission.care_badge == pp.CareBadge.ACTION
    assert any("row_count_or_omission" in d for d in rep_omission.defect_summary)

    # 2. Keyless multiset duplicate multiplicity defect:
    # Same distinct values ("dup", 10.00) and ("unique", 5.50) and same total row count (3),
    # but multiplicity shifted from (2x "dup", 1x "unique") to (1x "dup", 2x "unique").
    dag_multiplicity = _make_full_seven_domain_dag(
        keyless_target_rows=(
            ("dup", decimal.Decimal("10.00")),
            ("unique", decimal.Decimal("5.50")),
            ("unique", decimal.Decimal("5.50")),
        )
    )
    rep_mult = pp.execute_proof_plan(dag_multiplicity, fence)
    assert rep_mult.verdict_word == "different"
    assert rep_mult.care_badge == pp.CareBadge.ACTION
    assert any("multiset_or_value_mismatch" in d for d in rep_mult.defect_summary)

    # 3. RLS visibility drift under application role
    dag_rls = _make_full_seven_domain_dag(rls_target_fp="rls-tenant-visible-LEAKED-200")
    rep_rls = pp.execute_proof_plan(dag_rls, fence)
    assert rep_rls.verdict_word == "different"
    assert rep_rls.care_badge == pp.CareBadge.ACTION
    assert any("rls_or_role_visibility_mismatch" in d for d in rep_rls.defect_summary)

    # 4. DDL / schema definition drift
    dag_ddl = _make_full_seven_domain_dag(schema_target_fp="ddl-orders-v2-dropped-not-null")
    rep_ddl = pp.execute_proof_plan(dag_ddl, fence)
    assert rep_ddl.verdict_word == "different"
    assert rep_ddl.care_badge == pp.CareBadge.ACTION
    assert any("definition_or_ddl_mismatch" in d for d in rep_ddl.defect_summary)

    # 5. Canonicalization contract defect across readers
    bad_readers = (
        pp.IndependentReaderSpec(reader_id="r1", canonical_contract_version="canon-v2"),
        pp.IndependentReaderSpec(reader_id="r2", canonical_contract_version="canon-BROKEN-tz"),
    )
    dag_clean = _make_full_seven_domain_dag()
    rep_canon = pp.execute_proof_plan(dag_clean, fence, readers=bad_readers)
    assert rep_canon.verdict_word == "different"
    assert rep_canon.care_badge == pp.CareBadge.ACTION
    assert any("incompatible_canonical_contract" in d for d in rep_canon.defect_summary)

    # 6. Concurrent write after fence watermark
    dag_concurrent = _make_full_seven_domain_dag(concurrent_write=True)
    rep_concurrent = pp.execute_proof_plan(dag_concurrent, fence)
    assert rep_concurrent.verdict_word == "different"
    assert rep_concurrent.care_badge == pp.CareBadge.ACTION
    assert any("concurrent_write_after_fence" in d for d in rep_concurrent.defect_summary)

    # 7. Sampling alone stays incomplete (REVIEW), never same
    dag_sample = _make_full_seven_domain_dag(sample_only=True)
    rep_sample = pp.execute_proof_plan(dag_sample, fence)
    assert rep_sample.verdict_word == "incomplete"
    assert rep_sample.care_badge == pp.CareBadge.REVIEW
    assert rep_sample.verdict_kind == pp.ProofVerdictKind.INCOMPLETE_SAMPLING_OR_UNFENCED


# -----------------------------------------------------------------------------
# 5. O(1) Typed Durable Checkpoint, Lease Lock, AIMD & External Tool Wrapper
# -----------------------------------------------------------------------------

def test_o1_typed_durable_checkpoint_lease_lock_aimd_and_tool_wrapper(tmp_path: Path) -> None:
    fence = _make_fence()
    ckpt_path = tmp_path / "proof_ckpt.json"
    store = pp.ProofPlanningCheckpointStore(ckpt_path)

    dt_cursor = datetime.datetime(2026, 10, 17, 12, 0, tzinfo=datetime.timezone.utc)
    dec_cursor = decimal.Decimal("99999.1234")
    uid_cursor = uuid.UUID("12345678-1234-5678-1234-567812345678")

    rec = pp.ProofRangeCheckpointRecord(
        unit_key="values:orders:r1",
        node_id="values:orders",
        range_id="r1",
        cursor_start=(dt_cursor, dec_cursor),
        cursor_end=(dt_cursor, uid_cursor),
        epoch_signature=fence.epoch_signature(),
        change_marker="m1",
        evidence_digest="digest-r1",
        verified_at_s=1000.0,
    )
    store.record_verified_range(rec)

    # Reload from disk and verify native types and O(1) skip
    reloaded = pp.ProofPlanningCheckpointStore(ckpt_path)
    loaded_rec = reloaded.get_record("values:orders:r1")
    assert loaded_rec is not None
    assert isinstance(loaded_rec.cursor_start[0], datetime.datetime)
    assert isinstance(loaded_rec.cursor_start[1], decimal.Decimal)
    assert isinstance(loaded_rec.cursor_end[1], uuid.UUID)
    assert reloaded.should_skip_range(
        "values:orders:r1",
        epoch_signature=fence.epoch_signature(),
        change_marker="m1",
    )
    assert not reloaded.should_skip_range(
        "values:orders:r1",
        epoch_signature=fence.epoch_signature(),
        change_marker="m2",
    )

    # Mutual exclusion lease lock (.run.lock) with heartbeat
    lock1 = pp.ProofPlanningLeaseLock(tmp_path, lease_ttl_s=10.0)
    lock2 = pp.ProofPlanningLeaseLock(tmp_path, lease_ttl_s=10.0)
    lock1.acquire(now_s=100.0)
    try:
        with pytest.raises(pp.VerificationLeaseError):
            lock2.acquire(now_s=101.0)
        lock1.heartbeat(now_s=105.0)
        with pytest.raises(pp.VerificationLeaseError):
            lock1.heartbeat(now_s=120.0)
    finally:
        lock1.release()

    # Adaptive AIMD batch controller and cold-start byte-weighted ETA
    aimd = pp.AdaptiveProofBatchController(
        initial_batch_rows=1000,
        min_batch_rows=250,
        max_batch_rows=5000,
        target_latency_s=0.5,
        additive_increase_rows=500,
        multiplicative_decrease_factor=0.5,
    )
    assert aimd.estimate_remaining_eta_s(
        remaining_bytes=10_000_000,
        cold_start_probe_bytes_per_sec=2_000_000.0,
    ) == pytest.approx(5.0)
    assert aimd.observe_batch(rows=1000, byte_weight=1_000_000, elapsed_s=0.2) == 1500
    assert aimd.observe_batch(rows=1500, byte_weight=1_500_000, elapsed_s=1.2) == 750

    # External tool wrapper sanitizes internal tool names
    dag = _make_full_seven_domain_dag()
    rep = pp.execute_proof_plan(dag, fence)
    env = pp.normalize_external_proof_tool_output(
        "datafold",
        object_name="public.orders",
        raw_output="Datafold and reladiff via dvt/pgcopydb/debezium/dms/dts checked public.orders",
        report=rep,
    )
    assert env.care_badge == pp.CareBadge.PASS
    for forbidden in ("datafold", "reladiff", "dvt", "pgcopydb", "debezium", "dms", "dts"):
        assert forbidden not in env.sanitized_detail.lower()
        assert forbidden not in env.normalized_summary.lower()
