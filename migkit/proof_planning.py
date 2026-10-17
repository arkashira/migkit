"""Plan the deepest required proof with explicit coverage (Row 590: SMART-N7.proof-planning).

Builds a proof dependency and coverage DAG across values, schema, sequences,
grants, constraints, stored code, and application-visible semantics under
explicit epochs and watermark fences:
- Routes every planning, reuse, reader-compatibility, and verdict-gate choice
  through `decide.Rung` and `decide.climb(work, rungs, facts)`.
- Reuses prior evidence only when backed by a valid cryptographic dependency
  certificate; rechecks changed key ranges and all transitively affected
  downstream DAG objects.
- Requires compatible canonicalization and documented non-overlapping blind
  spots before trusting independent readers.
- Treats sampling-only evidence as incomplete (`REVIEW` / `incomplete`), never
  `same`, and preserves exact row multiplicity on keyless multisets.
- Prevents injected omissions, RLS policy drift, DDL epoch changes,
  canonicalization defects, and concurrent writes from producing a false `same`
  verdict, while measuring source reads and execution cost to complete the
  proof contract.
- Includes O(1) typed durable checkpoint resume, active mutual-exclusion lease
  lock (`.run.lock`) with heartbeat, adaptive AIMD batch sizing with instant
  cold-start byte-weighted ETA, Merkle bucket folding, and external tool
  normalization with zero internal tool name leaks.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
"""

from __future__ import annotations

import collections
import datetime
import decimal
import enum
import fcntl
import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from . import decide, durable


# -----------------------------------------------------------------------------
# Care Badges, Safe Host Validation, and Core Enums
# -----------------------------------------------------------------------------

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


SAFE_TEST_HOSTS: FrozenSet[str] = frozenset({"127.0.0.1", "localhost", "::1"})


class ProofPlanningError(RuntimeError):
    """Base error for proof planning and dependency coverage DAG operations."""


class UnsafeEndpointHostError(ProofPlanningError, ValueError):
    """Raised when an endpoint host is outside approved loopback or 10.0.0.x ranges."""


class ProofDAGCycleError(ProofPlanningError, ValueError):
    """Raised when the proof dependency DAG contains a dependency cycle."""


class ProofDAGDependencyError(ProofPlanningError, KeyError):
    """Raised when a proof node references an unknown upstream dependency node."""


class InvalidDependencyCertificateError(ProofPlanningError, ValueError):
    """Raised when a caller attempts to force evidence reuse with an invalid certificate."""


class VerificationLeaseError(ProofPlanningError):
    """Raised when an active verification lease lock (.run.lock) cannot be acquired or renewed."""


def validate_safe_endpoint(host: str) -> str:
    """Validate that an endpoint host uses approved loopback or 10.0.0.x test addresses."""
    cleaned = str(host or "").strip().lower()
    if not cleaned:
        raise UnsafeEndpointHostError("endpoint host cannot be empty")
    if cleaned in SAFE_TEST_HOSTS:
        return cleaned
    if cleaned.startswith("10.0.0."):
        parts = cleaned.split(".")
        if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            return cleaned
    raise UnsafeEndpointHostError(
        f"unsafe endpoint '{cleaned}': only 127.0.0.1, localhost, ::1, or 10.0.0.x allowed"
    )


class ProofDomain(str, enum.Enum):
    """The seven explicit coverage domains in a migkit proof contract."""

    VALUES = "values"
    SCHEMA = "schema"
    SEQUENCES = "sequences"
    GRANTS = "grants"
    CONSTRAINTS = "constraints"
    STORED_CODE = "stored_code"
    APPLICATION_SEMANTICS = "application_semantics"


class ProofDepthKind(str, enum.Enum):
    """Planned proof mechanism depth for a coverage node."""

    KEYLESS_MULTISET_EXACT = "keyless_multiset_exact"
    RLS_EPOCH_FENCED_FULL_SCAN = "rls_epoch_fenced_full_scan"
    MERKLE_RANGE_FUSED_EXACT = "merkle_range_fused_exact"
    FULL_CANONICAL_ROW_PROOF = "full_canonical_row_proof"


class EvidenceReuseKind(str, enum.Enum):
    """Disposition for reusing prior proof evidence versus re-reading."""

    REUSE_CERTIFIED_EVIDENCE = "reuse_certified_evidence"
    RECHECK_CHANGED_RANGES_ONLY = "recheck_changed_ranges_only"
    RECHECK_FULL_NODE_AND_DEPENDENTS = "recheck_full_node_and_dependents"


class ReaderCorrelationKind(str, enum.Enum):
    """Disposition for independent reader canonicalization and blind-spot correlation."""

    INDEPENDENT_COMPATIBLE_READERS = "independent_compatible_readers"
    SINGLE_READER_ESCALATED_EXACT = "single_reader_escalated_exact"


class ProofVerdictKind(str, enum.Enum):
    """Overall proof contract verdict gate outcome."""

    REFUSE_FALSE_SAME_ON_DEFECT = "refuse_false_same_on_defect"
    INCOMPLETE_SAMPLING_OR_UNFENCED = "incomplete_sampling_or_unfenced"
    CERTIFIED_COMPLETE_SAME = "certified_complete_same"


# -----------------------------------------------------------------------------
# Decision Engine Ladders (`decide.Rung` + `decide.climb`)
# -----------------------------------------------------------------------------

PROOF_DEPTH_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "keyless_multiset_exact",
        "multiplicity-preserving canonical multiset digest with exact spill verification",
        needs=(
            decide.Need(
                "node represents a keyless table or multiset value domain",
                lambda f: bool(f.get("scope_valid", True))
                and bool(f.get("is_keyless"))
                and f.get("domain") == ProofDomain.VALUES.value,
                "{rung} applies only to keyless multiset value nodes in a valid scope",
            ),
        ),
        because="keyless tables lack unique row keys and must preserve exact duplicate multiplicity without XOR cancellation",
    ),
    decide.Rung(
        "rls_epoch_fenced_full_scan",
        "epoch-fenced full scan under both owner and application RLS roles",
        needs=(
            decide.Need(
                "node carries row-level security policies or application role visibility rules",
                lambda f: bool(f.get("scope_valid", True))
                and (bool(f.get("has_rls")) or f.get("domain") in (
                    ProofDomain.GRANTS.value,
                    ProofDomain.APPLICATION_SEMANTICS.value,
                )),
                "{rung} requires RLS policies or role-scoped semantic obligations",
            ),
        ),
        because="RLS and role-scoped semantics can hide or filter rows differently under application principals",
    ),
    decide.Rung(
        "merkle_range_fused_exact",
        "fused pushdown Merkle range digest with exact leaf escalation on divergent ranges",
        needs=(
            decide.Need(
                "keyed value node with covering key ranges and compatible canonical pushdown",
                lambda f: bool(f.get("scope_valid", True))
                and f.get("domain") == ProofDomain.VALUES.value
                and not bool(f.get("is_keyless"))
                and not bool(f.get("has_rls"))
                and int(f.get("range_count", 0)) >= 1
                and bool(f.get("supports_fused_merkle", True)),
                "{rung} requires a keyed value node without RLS and with covering Merkle ranges",
            ),
        ),
        because="fused range digests localize changed ranges in O(log N) while escalating divergent buckets to exact rows",
    ),
    decide.Rung(
        "full_canonical_row_proof",
        "full canonical metadata and row-level verification under explicit epoch fence",
        needs=(
            decide.Need(
                "node belongs to an authorized proof scope",
                lambda f: bool(f.get("scope_valid", True)),
                "{rung} requires a valid authorized proof scope",
            ),
        ),
        because="provides complete exact verification for schema, sequences, constraints, stored code, and unfused tables",
    ),
)


EVIDENCE_REUSE_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "reuse_certified_evidence",
        "reuse prior proof evidence verified by a matching dependency certificate",
        needs=(
            decide.Need(
                "dependency certificate matches current schema, RLS, canonical contract, and upstream digests",
                lambda f: bool(f.get("scope_valid", True))
                and bool(f.get("certificate_valid"))
                and not bool(f.get("upstream_changed"))
                and not bool(f.get("node_markers_changed"))
                and not bool(f.get("is_sample_only"))
                and bool(f.get("allow_certified_reuse", True))
                and bool(f.get("within_retention_window", True)),
                "{rung} requires a valid certificate, unchanged upstream/range markers, non-sample evidence, and fresh window",
            ),
        ),
        because="avoids redundant source reads when cryptographic dependency certificates prove neither object nor upstream moved",
    ),
    decide.Rung(
        "recheck_changed_ranges_only",
        "recheck only mutated key ranges while reusing certified unchanged sibling ranges",
        needs=(
            decide.Need(
                "schema, RLS, and canonicalization epochs held still and changed key ranges are isolated",
                lambda f: bool(f.get("scope_valid", True))
                and bool(f.get("epochs_compatible"))
                and not bool(f.get("upstream_changed"))
                and not bool(f.get("is_keyless"))
                and not bool(f.get("is_sample_only"))
                and int(f.get("changed_range_count", 0)) > 0
                and int(f.get("unchanged_certified_range_count", 0)) > 0
                and bool(f.get("within_retention_window", True)),
                "{rung} requires intact schema/RLS/canonical epochs and isolated changed key ranges",
            ),
        ),
        because="re-reads only the key ranges whose change markers moved while preserving full-table proof completeness",
    ),
    decide.Rung(
        "recheck_full_node_and_dependents",
        "re-verify the full object and all downstream dependent nodes in the coverage DAG",
        needs=(
            decide.Need(
                "node belongs to an authorized proof scope",
                lambda f: bool(f.get("scope_valid", True)),
                "{rung} requires a valid authorized proof scope",
            ),
        ),
        because="any broken certificate, DDL/RLS/canonical shift, or upstream change invalidates prior evidence",
    ),
)


READER_CORRELATION_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "independent_compatible_readers",
        "cross-check with independent readers sharing canonical framing and disjoint blind spots",
        needs=(
            decide.Need(
                "independent readers have compatible canonicalization and zero overlapping or undocumented blind spots",
                lambda f: bool(f.get("scope_valid", True))
                and int(f.get("reader_count", 0)) >= 2
                and bool(f.get("canonicalization_compatible"))
                and int(f.get("overlapping_blind_spots", 0)) == 0
                and int(f.get("undocumented_blind_spots", 0)) == 0,
                "{rung} requires >= 2 independent readers with compatible canonicalization and disjoint documented blind spots",
            ),
        ),
        because="independent readers strengthen proof only when canonical framing matches and blind spots cannot mask the same defect",
    ),
    decide.Rung(
        "single_reader_escalated_exact",
        "escalate to direct canonical byte-and-value verification without trusting correlated secondary reader",
        needs=(
            decide.Need(
                "node belongs to an authorized proof scope",
                lambda f: bool(f.get("scope_valid", True)),
                "{rung} requires a valid authorized proof scope",
            ),
        ),
        because="refuses to trust a secondary reader whose canonicalization differs or whose blind spots overlap",
    ),
)


PROOF_VERDICT_GATE_LADDER: Tuple[decide.Rung, ...] = (
    decide.Rung(
        "refuse_false_same_on_defect",
        "refuse 'same' verdict on omission, RLS drift, DDL drift, canonicalization defect, or concurrent write",
        needs=(
            decide.Need(
                "one or more value, multiplicity, RLS, DDL, canonicalization, or concurrent-write defects detected",
                lambda f: bool(f.get("scope_valid", True)) and bool(f.get("defect_detected")),
                "no value, RLS, DDL, canonicalization, or concurrent-write defect was detected",
            ),
        ),
        because="injected omissions, RLS mismatches, DDL drift, canonicalization errors, or concurrent writes must never pass as same",
    ),
    decide.Rung(
        "incomplete_sampling_or_unfenced",
        "mark proof 'incomplete' when sampling-only, unfenced, or missing required domain coverage",
        needs=(
            decide.Need(
                "no hard defect found yet, but evidence includes sampling-only or incomplete DAG coverage",
                lambda f: bool(f.get("scope_valid", True))
                and not bool(f.get("defect_detected"))
                and bool(f.get("incomplete_coverage")),
                "coverage is neither incomplete nor sample-only, or a hard defect already triggered refusal",
            ),
        ),
        because="sampling alone or unverified DAG nodes cannot certify full data and semantic equivalence",
    ),
    decide.Rung(
        "certified_complete_same",
        "certify 'same' after all DAG domains and ranges verify under a consistent epoch fence",
        needs=(
            decide.Need(
                "all required DAG nodes verified with zero defects and zero sample-only shortcuts",
                lambda f: bool(f.get("scope_valid", True))
                and not bool(f.get("defect_detected"))
                and not bool(f.get("incomplete_coverage"))
                and bool(f.get("all_nodes_verified")),
                "{rung} requires all DAG nodes verified with zero defects and complete non-sampled coverage",
            ),
        ),
        because="every value, schema, sequence, grant, constraint, stored-code, and semantic obligation held under the fence",
    ),
)


# -----------------------------------------------------------------------------
# Epoch Fences, Reader Specifications, and Dependency Certificates
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ProofEpochFence:
    """Explicit epoch and watermark fence governing a proof run."""

    source_host: str
    target_host: str
    schema_ddl_epoch: int
    rls_policy_epoch: int
    canonical_contract_version: str
    source_watermark_pos: str
    target_watermark_pos: str
    captured_at_s: float
    measured_scan_window_s: float
    measured_apply_lag_s: float
    measured_retention_horizon_s: float

    def __post_init__(self) -> None:
        validate_safe_endpoint(self.source_host)
        validate_safe_endpoint(self.target_host)
        if self.schema_ddl_epoch < 0 or self.rls_policy_epoch < 0:
            raise ValueError("schema_ddl_epoch and rls_policy_epoch must be non-negative")
        if not str(self.canonical_contract_version or "").strip():
            raise ValueError("canonical_contract_version cannot be empty")
        if self.measured_scan_window_s <= 0 or self.measured_retention_horizon_s <= 0:
            raise ValueError("measured scan window and retention horizon must be positive")
        if self.measured_apply_lag_s < 0:
            raise ValueError("measured_apply_lag_s must be non-negative")

    @property
    def max_evidence_age_s(self) -> float:
        """Derive maximum valid evidence age from measured retention, scan window, and apply lag."""
        usable_retention = max(
            0.0,
            self.measured_retention_horizon_s - (self.measured_scan_window_s + self.measured_apply_lag_s),
        )
        return max(self.measured_scan_window_s, usable_retention)

    def epoch_signature(self) -> str:
        """Deterministic digest of the structural and canonical fence parameters."""
        payload = {
            "schema_ddl_epoch": self.schema_ddl_epoch,
            "rls_policy_epoch": self.rls_policy_epoch,
            "canonical_contract_version": self.canonical_contract_version,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class IndependentReaderSpec:
    """Metadata describing a proof reader's canonicalization contract and blind spots."""

    reader_id: str
    canonical_contract_version: str
    null_framing_mode: str = "length_prefixed_injective"
    decimal_normalization: str = "exact_scale_preserved"
    timezone_normalization: str = "utc_epoch_micros"
    documented_blind_spots: Tuple[str, ...] = ()
    undocumented_blind_spots: Tuple[str, ...] = ()

    def is_canonical_compatible_with(self, other: IndependentReaderSpec, fence: ProofEpochFence) -> bool:
        return (
            self.canonical_contract_version == fence.canonical_contract_version
            and other.canonical_contract_version == fence.canonical_contract_version
            and self.null_framing_mode == other.null_framing_mode
            and self.decimal_normalization == other.decimal_normalization
            and self.timezone_normalization == other.timezone_normalization
        )


@dataclass(frozen=True)
class KeyRangeSpec:
    """A covering key range within a value node, with measured row/byte weight and change marker."""

    range_id: str
    cursor_start: Any
    cursor_end: Any
    estimated_rows: int
    estimated_bytes: int
    change_marker: str
    source_rows: Tuple[Tuple[Any, ...], ...] = ()
    target_rows: Tuple[Tuple[Any, ...], ...] = ()

    def __post_init__(self) -> None:
        if self.estimated_rows < 0 or self.estimated_bytes < 0:
            raise ValueError("estimated_rows and estimated_bytes must be non-negative")


@dataclass(frozen=True)
class ProofCoverageNode:
    """A single node in the proof dependency/coverage DAG."""

    node_id: str
    domain: ProofDomain
    object_name: str
    depends_on: Tuple[str, ...] = ()
    is_keyless: bool = False
    has_rls: bool = False
    supports_fused_merkle: bool = True
    estimated_rows: int = 1
    estimated_bytes: int = 256
    change_marker: str = "v1"
    ranges: Tuple[KeyRangeSpec, ...] = ()
    source_definition_fingerprint: str = ""
    target_definition_fingerprint: str = ""
    source_rls_visible_fingerprint: str = ""
    target_rls_visible_fingerprint: str = ""
    source_sequence_value: Optional[int] = None
    target_sequence_value: Optional[int] = None
    concurrent_write_after_fence: bool = False
    is_sample_only: bool = False

    def __post_init__(self) -> None:
        if not self.node_id or not self.object_name:
            raise ValueError("node_id and object_name cannot be empty")
        if self.estimated_rows < 0 or self.estimated_bytes < 0:
            raise ValueError("estimated_rows and estimated_bytes must be non-negative")


@dataclass(frozen=True)
class DependencyCertificate:
    """Cryptographic certificate binding prior proof evidence to exact epochs and upstream digests."""

    node_id: str
    epoch_signature: str
    node_change_marker: str
    range_change_markers: Mapping[str, str]
    upstream_certificate_digests: Mapping[str, str]
    verified_at_s: float
    is_sample_only: bool
    evidence_digest: str
    certificate_digest: str

    @classmethod
    def issue(
        cls,
        *,
        node: ProofCoverageNode,
        fence: ProofEpochFence,
        upstream_certificate_digests: Mapping[str, str],
        evidence_digest: str,
        verified_at_s: float,
    ) -> DependencyCertificate:
        range_markers = {r.range_id: r.change_marker for r in node.ranges}
        body = {
            "node_id": node.node_id,
            "epoch_signature": fence.epoch_signature(),
            "node_change_marker": node.change_marker,
            "range_change_markers": dict(sorted(range_markers.items())),
            "upstream_certificate_digests": dict(sorted(upstream_certificate_digests.items())),
            "is_sample_only": bool(node.is_sample_only),
            "evidence_digest": str(evidence_digest),
        }
        cert_digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()
        return cls(
            node_id=node.node_id,
            epoch_signature=fence.epoch_signature(),
            node_change_marker=node.change_marker,
            range_change_markers=range_markers,
            upstream_certificate_digests=dict(upstream_certificate_digests),
            verified_at_s=float(verified_at_s),
            is_sample_only=bool(node.is_sample_only),
            evidence_digest=str(evidence_digest),
            certificate_digest=cert_digest,
        )

    def verify_integrity(self) -> bool:
        """Recompute certificate digest to detect tampering or forged certificates."""
        body = {
            "node_id": self.node_id,
            "epoch_signature": self.epoch_signature,
            "node_change_marker": self.node_change_marker,
            "range_change_markers": dict(sorted(self.range_change_markers.items())),
            "upstream_certificate_digests": dict(sorted(self.upstream_certificate_digests.items())),
            "is_sample_only": bool(self.is_sample_only),
            "evidence_digest": str(self.evidence_digest),
        }
        expected = hashlib.sha256(json.dumps(body, sort_keys=True).encode("utf-8")).hexdigest()
        return self.certificate_digest == expected


# -----------------------------------------------------------------------------
# Proof Coverage DAG Construction, Topological Sort & Transitive Invalidation
# -----------------------------------------------------------------------------

class ProofCoverageDAG:
    """Directed Acyclic Graph of proof obligations across all 7 migration domains."""

    def __init__(self, nodes: Iterable[ProofCoverageNode]) -> None:
        self._nodes: Dict[str, ProofCoverageNode] = {}
        for node in nodes:
            if node.node_id in self._nodes:
                raise ValueError(f"duplicate proof DAG node_id: {node.node_id!r}")
            self._nodes[node.node_id] = node

        self._dependents: Dict[str, List[str]] = {nid: [] for nid in self._nodes}
        for nid, node in self._nodes.items():
            for dep_id in node.depends_on:
                if dep_id not in self._nodes:
                    raise ProofDAGDependencyError(
                        f"proof node {nid!r} depends on missing node {dep_id!r}"
                    )
                self._dependents[dep_id].append(nid)

        self._topo_order: Tuple[str, ...] = self._compute_topological_order()

    def _compute_topological_order(self) -> Tuple[str, ...]:
        in_degree: Dict[str, int] = {
            nid: len(set(node.depends_on)) for nid, node in self._nodes.items()
        }
        ready = sorted(nid for nid, deg in in_degree.items() if deg == 0)
        order: List[str] = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for child in sorted(set(self._dependents[current])):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    ready.append(child)
                    ready.sort()
        if len(order) != len(self._nodes):
            cycling = sorted(set(self._nodes.keys()) - set(order))
            raise ProofDAGCycleError(f"cycle detected in proof coverage DAG among nodes: {cycling}")
        return tuple(order)

    @property
    def nodes(self) -> Mapping[str, ProofCoverageNode]:
        return self._nodes

    def topological_order(self) -> Tuple[str, ...]:
        return self._topo_order

    def affected_closure(self, changed_node_ids: Iterable[str]) -> Tuple[str, ...]:
        """Return topological order of all nodes transitively depending on `changed_node_ids`."""
        visited: Set[str] = set()
        queue = collections.deque()
        for nid in changed_node_ids:
            if nid not in self._nodes:
                raise ProofDAGDependencyError(f"unknown node_id {nid!r} in affected_closure")
            if nid not in visited:
                visited.add(nid)
                queue.append(nid)
        while queue:
            cur = queue.popleft()
            for dep_child in self._dependents.get(cur, ()):
                if dep_child not in visited:
                    visited.add(dep_child)
                    queue.append(dep_child)
        return tuple(nid for nid in self._topo_order if nid in visited)

    def covered_domains(self) -> FrozenSet[ProofDomain]:
        return frozenset(node.domain for node in self._nodes.values())


# -----------------------------------------------------------------------------
# Decision Helper Functions (Always Climbing Rungs via `decide.climb`)
# -----------------------------------------------------------------------------

def select_proof_depth(
    work: str,
    node: ProofCoverageNode,
    *,
    scope_valid: bool = True,
) -> decide.Choice:
    """Select the required proof mechanism depth for `node` using `PROOF_DEPTH_LADDER`."""
    facts = {
        "scope_valid": bool(scope_valid),
        "domain": node.domain.value,
        "is_keyless": bool(node.is_keyless),
        "has_rls": bool(node.has_rls),
        "range_count": len(node.ranges),
        "supports_fused_merkle": bool(node.supports_fused_merkle),
    }
    return decide.climb(work, PROOF_DEPTH_LADDER, facts)


def select_evidence_reuse(
    work: str,
    node: ProofCoverageNode,
    *,
    fence: ProofEpochFence,
    prior_certificate: Optional[DependencyCertificate],
    current_upstream_digests: Mapping[str, str],
    now_s: float,
    allow_certified_reuse: bool = True,
    scope_valid: bool = True,
) -> Tuple[decide.Choice, Tuple[str, ...]]:
    """Decide whether `node` can reuse prior evidence, recheck changed ranges, or recheck fully."""
    epochs_compatible = False
    certificate_integrity = False
    upstream_changed = True
    node_markers_changed = True
    within_retention_window = False
    changed_range_ids: List[str] = []
    unchanged_range_count = 0

    if prior_certificate is not None and prior_certificate.node_id == node.node_id:
        certificate_integrity = prior_certificate.verify_integrity()
        epochs_compatible = (
            certificate_integrity
            and prior_certificate.epoch_signature == fence.epoch_signature()
        )
        age_s = max(0.0, float(now_s) - float(prior_certificate.verified_at_s))
        within_retention_window = age_s <= fence.max_evidence_age_s
        upstream_changed = dict(prior_certificate.upstream_certificate_digests) != dict(
            current_upstream_digests
        )
        if node.ranges:
            for r in node.ranges:
                old_marker = prior_certificate.range_change_markers.get(r.range_id)
                if old_marker != r.change_marker:
                    changed_range_ids.append(r.range_id)
                else:
                    unchanged_range_count += 1
            node_markers_changed = (
                prior_certificate.node_change_marker != node.change_marker
                or len(changed_range_ids) > 0
            )
        else:
            node_markers_changed = prior_certificate.node_change_marker != node.change_marker
    else:
        changed_range_ids = [r.range_id for r in node.ranges]

    certificate_valid = (
        certificate_integrity
        and epochs_compatible
        and prior_certificate is not None
        and not prior_certificate.is_sample_only
    )

    facts = {
        "scope_valid": bool(scope_valid),
        "certificate_valid": bool(certificate_valid),
        "epochs_compatible": bool(epochs_compatible),
        "upstream_changed": bool(upstream_changed),
        "node_markers_changed": bool(node_markers_changed),
        "is_keyless": bool(node.is_keyless),
        "is_sample_only": bool(
            node.is_sample_only or (prior_certificate is not None and prior_certificate.is_sample_only)
        ),
        "allow_certified_reuse": bool(allow_certified_reuse),
        "within_retention_window": bool(within_retention_window),
        "changed_range_count": len(changed_range_ids),
        "unchanged_certified_range_count": unchanged_range_count,
    }
    choice = decide.climb(work, EVIDENCE_REUSE_LADDER, facts)
    return choice, tuple(changed_range_ids)


def evaluate_reader_correlation(
    work: str,
    readers: Sequence[IndependentReaderSpec],
    fence: ProofEpochFence,
    *,
    scope_valid: bool = True,
) -> Tuple[decide.Choice, Tuple[str, ...]]:
    """Evaluate whether independent readers share canonicalization and disjoint blind spots."""
    reader_count = len(readers)
    canonicalization_compatible = False
    overlapping: Set[str] = set()
    undocumented: Set[str] = set()

    for r in readers:
        undocumented.update(r.undocumented_blind_spots)

    if reader_count >= 2:
        primary = readers[0]
        canonicalization_compatible = all(
            primary.is_canonical_compatible_with(other, fence) for other in readers[1:]
        )
        for i in range(reader_count):
            for j in range(i + 1, reader_count):
                overlap = set(readers[i].documented_blind_spots) & set(
                    readers[j].documented_blind_spots
                )
                overlapping.update(overlap)

    facts = {
        "scope_valid": bool(scope_valid),
        "reader_count": reader_count,
        "canonicalization_compatible": bool(canonicalization_compatible),
        "overlapping_blind_spots": len(overlapping),
        "undocumented_blind_spots": len(undocumented),
    }
    choice = decide.climb(work, READER_CORRELATION_LADDER, facts)
    issues = tuple(sorted(overlapping | undocumented))
    return choice, issues


def select_proof_verdict_gate(
    work: str,
    *,
    defect_detected: bool,
    incomplete_coverage: bool,
    all_nodes_verified: bool,
    scope_valid: bool = True,
) -> decide.Choice:
    """Select the final proof verdict disposition using `PROOF_VERDICT_GATE_LADDER`."""
    facts = {
        "scope_valid": bool(scope_valid),
        "defect_detected": bool(defect_detected),
        "incomplete_coverage": bool(incomplete_coverage),
        "all_nodes_verified": bool(all_nodes_verified),
    }
    return decide.climb(work, PROOF_VERDICT_GATE_LADDER, facts)


# -----------------------------------------------------------------------------
# Multiplicity-Preserving Canonical Row & Multiset Verification
# -----------------------------------------------------------------------------

def canonical_row_frame(row: Sequence[Any]) -> str:
    """Length-prefixed injective canonical framing for a row tuple (distinguishes NULL vs empty string)."""
    parts: List[str] = []
    for val in row:
        if val is None:
            parts.append("N:")
        elif isinstance(val, bool):
            token = "true" if val else "false"
            parts.append(f"B:{len(token)}:{token}")
        elif isinstance(val, int):
            token = str(val)
            parts.append(f"I:{len(token)}:{token}")
        elif isinstance(val, decimal.Decimal):
            token = format(val.normalize(), "f")
            parts.append(f"D:{len(token)}:{token}")
        elif isinstance(val, float):
            if not math.isfinite(val):
                raise ValueError(f"non-finite float in canonical row frame: {val!r}")
            token = repr(val)
            parts.append(f"F:{len(token)}:{token}")
        elif isinstance(val, datetime.datetime):
            utc_val = (
                val.astimezone(datetime.timezone.utc)
                if val.tzinfo is not None
                else val.replace(tzinfo=datetime.timezone.utc)
            )
            token = utc_val.isoformat()
            parts.append(f"T:{len(token)}:{token}")
        else:
            token = str(val)
            parts.append(f"S:{len(token)}:{token}")
    return "|".join(parts)


def compute_multiset_digest(rows: Iterable[Sequence[Any]]) -> Tuple[str, int, Dict[str, int]]:
    """Compute a multiplicity-preserving canonical digest over a multiset of rows.

    Unlike XOR digests, duplicate rows (multiplicity > 1) are counted explicitly
    as `(canonical_frame, multiplicity)` pairs in sorted order, so `[A, A]` never
    cancels out and `[A, A, B]` never collides with `[A, B, B]`.
    """
    counts: Dict[str, int] = collections.Counter(canonical_row_frame(r) for r in rows)
    total_rows = sum(counts.values())
    h = hashlib.sha256()
    for framed in sorted(counts.keys()):
        mult = counts[framed]
        h.update(f"{mult}*{len(framed)}:{framed}\n".encode("utf-8"))
    return h.hexdigest(), total_rows, dict(counts)


def merkle_fold_range_buckets(
    ranges: Sequence[KeyRangeSpec],
) -> Tuple[str, Dict[str, Tuple[str, str]], Tuple[str, ...]]:
    """Fold key ranges into a root Merkle digest and identify any divergent range IDs."""
    range_digests: Dict[str, Tuple[str, str]] = {}
    divergent_range_ids: List[str] = []
    root_hasher = hashlib.sha256()

    for r in ranges:
        src_digest, _, _ = compute_multiset_digest(r.source_rows)
        tgt_digest, _, _ = compute_multiset_digest(r.target_rows)
        range_digests[r.range_id] = (src_digest, tgt_digest)
        root_hasher.update(f"{r.range_id}:{src_digest}\n".encode("utf-8"))
        if src_digest != tgt_digest:
            divergent_range_ids.append(r.range_id)

    return root_hasher.hexdigest(), range_digests, tuple(divergent_range_ids)


# -----------------------------------------------------------------------------
# End-to-End Proof Execution, Defect Detection & Cost/Read Accounting
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class NodeProofOutcome:
    """Verification outcome and measured read cost for a single DAG node."""

    node_id: str
    domain: ProofDomain
    depth_kind: ProofDepthKind
    reuse_kind: EvidenceReuseKind
    care_badge: CareBadge
    verified_same: bool
    is_incomplete: bool
    defect_reasons: Tuple[str, ...]
    rechecked_range_ids: Tuple[str, ...]
    source_rows_read: int
    source_bytes_read: int
    source_queries_issued: int
    measured_cost_s: float
    issued_certificate: Optional[DependencyCertificate]


@dataclass(frozen=True)
class ProofContractReport:
    """Complete proof execution report with verdict, coverage, defects, and read cost."""

    verdict_word: str
    verdict_kind: ProofVerdictKind
    care_badge: CareBadge
    reader_correlation_kind: ReaderCorrelationKind
    reader_blind_spot_issues: Tuple[str, ...]
    node_outcomes: Mapping[str, NodeProofOutcome]
    covered_domains: FrozenSet[ProofDomain]
    missing_domains: FrozenSet[ProofDomain]
    total_source_rows_read: int
    total_source_bytes_read: int
    total_source_queries_issued: int
    total_reused_nodes: int
    total_rechecked_ranges: int
    total_measured_cost_s: float
    defect_summary: Tuple[str, ...]


def execute_proof_plan(
    dag: ProofCoverageDAG,
    fence: ProofEpochFence,
    *,
    readers: Sequence[IndependentReaderSpec] = (),
    prior_certificates: Optional[Mapping[str, DependencyCertificate]] = None,
    required_domains: Optional[Iterable[ProofDomain]] = None,
    allow_certified_reuse: bool = True,
    now_s: Optional[float] = None,
    measured_bytes_per_sec: float = 50_000_000.0,
    per_query_overhead_s: float = 0.002,
) -> ProofContractReport:
    """Execute the planned proof across `dag` under `fence` and return `ProofContractReport`."""
    if measured_bytes_per_sec <= 0 or per_query_overhead_s < 0:
        raise ValueError("measured_bytes_per_sec must be positive and per_query_overhead_s non-negative")

    eval_now_s = float(now_s if now_s is not None else fence.captured_at_s)
    prior_certs = dict(prior_certificates or {})
    req_domains = (
        frozenset(required_domains)
        if required_domains is not None
        else frozenset(ProofDomain)
    )

    reader_choice, reader_issues = evaluate_reader_correlation(
        "proof:readers",
        readers,
        fence,
    )
    reader_kind = ReaderCorrelationKind(reader_choice.path)

    node_outcomes: Dict[str, NodeProofOutcome] = {}
    issued_certs: Dict[str, DependencyCertificate] = {}
    all_defects: List[str] = []
    any_incomplete = False

    # If a caller supplied multiple readers that have incompatible canonicalization
    # or overlapping/undocumented blind spots, record whether canonicalization mismatch
    # was injected as a defect when secondary reader was required.
    if len(readers) >= 2 and reader_kind == ReaderCorrelationKind.SINGLE_READER_ESCALATED_EXACT:
        for r in readers:
            if r.canonical_contract_version != fence.canonical_contract_version:
                all_defects.append(
                    f"reader:{r.reader_id}:incompatible_canonical_contract:{r.canonical_contract_version}!={fence.canonical_contract_version}"
                )

    for node_id in dag.topological_order():
        node = dag.nodes[node_id]
        depth_choice = select_proof_depth(f"proof:depth:{node_id}", node)
        depth_kind = ProofDepthKind(depth_choice.path)

        upstream_digests: Dict[str, str] = {}
        upstream_failed_or_missing = False
        for dep_id in node.depends_on:
            dep_cert = issued_certs.get(dep_id)
            dep_outcome = node_outcomes.get(dep_id)
            if dep_cert is None or dep_outcome is None or not dep_outcome.verified_same:
                upstream_failed_or_missing = True
            else:
                upstream_digests[dep_id] = dep_cert.certificate_digest

        prior_cert = prior_certs.get(node_id)
        if upstream_failed_or_missing:
            # Force full recheck when an upstream dependency failed or lacked a certificate
            reuse_choice, changed_ranges = select_evidence_reuse(
                f"proof:reuse:{node_id}",
                node,
                fence=fence,
                prior_certificate=None,
                current_upstream_digests=upstream_digests,
                now_s=eval_now_s,
                allow_certified_reuse=allow_certified_reuse,
            )
        else:
            reuse_choice, changed_ranges = select_evidence_reuse(
                f"proof:reuse:{node_id}",
                node,
                fence=fence,
                prior_certificate=prior_cert,
                current_upstream_digests=upstream_digests,
                now_s=eval_now_s,
                allow_certified_reuse=allow_certified_reuse,
            )
        reuse_kind = EvidenceReuseKind(reuse_choice.path)

        node_defects: List[str] = []
        node_incomplete = False
        rows_read = 0
        bytes_read = 0
        queries_issued = 0
        rechecked_range_ids: Tuple[str, ...] = ()
        evidence_digest = ""

        # Check concurrent write after fence
        if node.concurrent_write_after_fence:
            node_defects.append(f"{node_id}:concurrent_write_after_fence")

        # Check sampling-only incompleteness
        if node.is_sample_only:
            node_incomplete = True

        if (
            reuse_kind == EvidenceReuseKind.REUSE_CERTIFIED_EVIDENCE
            and prior_cert is not None
            and not node_defects
            and not node_incomplete
        ):
            # Certified O(1) evidence reuse: 0 source rows read
            evidence_digest = prior_cert.evidence_digest
            new_cert = prior_cert
        else:
            # Inspect definitions, RLS visibility, sequences, and value ranges
            if node.source_definition_fingerprint != node.target_definition_fingerprint:
                node_defects.append(f"{node_id}:definition_or_ddl_mismatch")

            if node.has_rls or node.domain in (ProofDomain.GRANTS, ProofDomain.APPLICATION_SEMANTICS):
                if node.source_rls_visible_fingerprint != node.target_rls_visible_fingerprint:
                    node_defects.append(f"{node_id}:rls_or_role_visibility_mismatch")

            if node.domain == ProofDomain.SEQUENCES:
                if (
                    node.source_sequence_value is not None
                    and node.target_sequence_value is not None
                    and node.target_sequence_value < node.source_sequence_value
                ):
                    node_defects.append(
                        f"{node_id}:target_sequence_behind:{node.target_sequence_value}<{node.source_sequence_value}"
                    )

            if node.ranges:
                if reuse_kind == EvidenceReuseKind.RECHECK_CHANGED_RANGES_ONLY:
                    ranges_to_read = [r for r in node.ranges if r.range_id in set(changed_ranges)]
                else:
                    ranges_to_read = list(node.ranges)

                rechecked_range_ids = tuple(r.range_id for r in ranges_to_read)
                for r in ranges_to_read:
                    rows_read += r.estimated_rows
                    bytes_read += r.estimated_bytes
                    queries_issued += 1
                    src_d, src_n, src_counts = compute_multiset_digest(r.source_rows)
                    tgt_d, tgt_n, tgt_counts = compute_multiset_digest(r.target_rows)
                    if src_d != tgt_d:
                        if src_n != tgt_n:
                            node_defects.append(
                                f"{node_id}:range:{r.range_id}:row_count_or_omission:{src_n}!={tgt_n}"
                            )
                        elif src_counts != tgt_counts:
                            node_defects.append(
                                f"{node_id}:range:{r.range_id}:multiset_or_value_mismatch"
                            )
                root_digest, _, _ = merkle_fold_range_buckets(node.ranges)
                evidence_digest = root_digest
            else:
                rows_read = node.estimated_rows
                bytes_read = node.estimated_bytes
                queries_issued = 1
                h = hashlib.sha256()
                h.update(
                    f"{node.source_definition_fingerprint}|{node.source_rls_visible_fingerprint}|{node.source_sequence_value}".encode(
                        "utf-8"
                    )
                )
                evidence_digest = h.hexdigest()

            new_cert = (
                DependencyCertificate.issue(
                    node=node,
                    fence=fence,
                    upstream_certificate_digests=upstream_digests,
                    evidence_digest=evidence_digest,
                    verified_at_s=eval_now_s,
                )
                if not node_defects and not node_incomplete
                else None
            )

        cost_s = (
            float(bytes_read) / measured_bytes_per_sec
            + float(queries_issued) * per_query_overhead_s
        )

        if node_defects:
            badge = CareBadge.ACTION
            verified_same = False
            all_defects.extend(node_defects)
        elif node_incomplete:
            badge = CareBadge.REVIEW
            verified_same = False
            any_incomplete = True
        else:
            badge = CareBadge.PASS
            verified_same = True
            if new_cert is not None:
                issued_certs[node_id] = new_cert

        node_outcomes[node_id] = NodeProofOutcome(
            node_id=node_id,
            domain=node.domain,
            depth_kind=depth_kind,
            reuse_kind=reuse_kind,
            care_badge=badge,
            verified_same=verified_same,
            is_incomplete=node_incomplete,
            defect_reasons=tuple(node_defects),
            rechecked_range_ids=rechecked_range_ids,
            source_rows_read=rows_read,
            source_bytes_read=bytes_read,
            source_queries_issued=queries_issued,
            measured_cost_s=cost_s,
            issued_certificate=new_cert,
        )

    covered = dag.covered_domains()
    missing = req_domains - covered
    if missing:
        any_incomplete = True

    all_nodes_verified = (
        len(dag.nodes) > 0
        and not missing
        and all(o.verified_same and not o.is_incomplete for o in node_outcomes.values())
    )

    gate_choice = select_proof_verdict_gate(
        "proof:verdict_gate",
        defect_detected=bool(all_defects),
        incomplete_coverage=bool(any_incomplete),
        all_nodes_verified=bool(all_nodes_verified),
    )
    verdict_kind = ProofVerdictKind(gate_choice.path)

    if verdict_kind == ProofVerdictKind.REFUSE_FALSE_SAME_ON_DEFECT:
        verdict_word = "different"
        overall_badge = CareBadge.ACTION
    elif verdict_kind == ProofVerdictKind.INCOMPLETE_SAMPLING_OR_UNFENCED:
        verdict_word = "incomplete"
        overall_badge = CareBadge.REVIEW
    else:
        verdict_word = "same"
        overall_badge = CareBadge.PASS

    return ProofContractReport(
        verdict_word=verdict_word,
        verdict_kind=verdict_kind,
        care_badge=overall_badge,
        reader_correlation_kind=reader_kind,
        reader_blind_spot_issues=reader_issues,
        node_outcomes=node_outcomes,
        covered_domains=covered,
        missing_domains=missing,
        total_source_rows_read=sum(o.source_rows_read for o in node_outcomes.values()),
        total_source_bytes_read=sum(o.source_bytes_read for o in node_outcomes.values()),
        total_source_queries_issued=sum(o.source_queries_issued for o in node_outcomes.values()),
        total_reused_nodes=sum(
            1
            for o in node_outcomes.values()
            if o.reuse_kind == EvidenceReuseKind.REUSE_CERTIFIED_EVIDENCE
        ),
        total_rechecked_ranges=sum(len(o.rechecked_range_ids) for o in node_outcomes.values()),
        total_measured_cost_s=sum(o.measured_cost_s for o in node_outcomes.values()),
        defect_summary=tuple(all_defects),
    )


# -----------------------------------------------------------------------------
# Universal Typed Durable Checkpoint Resume in O(1)
# -----------------------------------------------------------------------------

def encode_typed_cursor(value: Any) -> Any:
    """Recursively encode native Python types (datetime, Decimal, UUID, tuple) for JSON."""
    if isinstance(value, datetime.datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__type__": "uuid", "value": str(value)}
    if isinstance(value, tuple):
        return {"__type__": "tuple", "items": [encode_typed_cursor(v) for v in value]}
    if isinstance(value, list):
        return [encode_typed_cursor(v) for v in value]
    if isinstance(value, dict):
        return {str(k): encode_typed_cursor(v) for k, v in value.items()}
    return value


def decode_typed_cursor(raw: Any) -> Any:
    """Restore native Python types (datetime, Decimal, UUID, tuple) from JSON payload."""
    if isinstance(raw, dict):
        tag = raw.get("__type__")
        if tag == "datetime":
            return datetime.datetime.fromisoformat(raw["value"])
        if tag == "decimal":
            return decimal.Decimal(raw["value"])
        if tag == "uuid":
            return uuid.UUID(raw["value"])
        if tag == "tuple":
            return tuple(decode_typed_cursor(v) for v in raw["items"])
        return {k: decode_typed_cursor(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [decode_typed_cursor(v) for v in raw]
    return raw


@dataclass(frozen=True)
class ProofRangeCheckpointRecord:
    """Durable checkpoint record for a verified proof node/range."""

    unit_key: str
    node_id: str
    range_id: str
    cursor_start: Any
    cursor_end: Any
    epoch_signature: str
    change_marker: str
    evidence_digest: str
    verified_at_s: float


class ProofPlanningCheckpointStore:
    """O(1) typed durable checkpoint store backed by `durable.write_json` and `durable.read_state`."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)
        self._records: Dict[str, ProofRangeCheckpointRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        raw = durable.read_state(self.path, "state")
        if not raw:
            return
        decoded = decode_typed_cursor(raw)
        for key, item in decoded.get("records", {}).items():
            self._records[key] = ProofRangeCheckpointRecord(
                unit_key=str(item["unit_key"]),
                node_id=str(item["node_id"]),
                range_id=str(item["range_id"]),
                cursor_start=item["cursor_start"],
                cursor_end=item["cursor_end"],
                epoch_signature=str(item["epoch_signature"]),
                change_marker=str(item["change_marker"]),
                evidence_digest=str(item["evidence_digest"]),
                verified_at_s=float(item["verified_at_s"]),
            )

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "records": {
                k: {
                    "unit_key": rec.unit_key,
                    "node_id": rec.node_id,
                    "range_id": rec.range_id,
                    "cursor_start": rec.cursor_start,
                    "cursor_end": rec.cursor_end,
                    "epoch_signature": rec.epoch_signature,
                    "change_marker": rec.change_marker,
                    "evidence_digest": rec.evidence_digest,
                    "verified_at_s": rec.verified_at_s,
                }
                for k, rec in self._records.items()
            },
        }
        durable.write_json(self.path, encode_typed_cursor(payload))

    def record_verified_range(self, record: ProofRangeCheckpointRecord) -> None:
        self._records[record.unit_key] = record
        self.save()

    def should_skip_range(
        self,
        unit_key: str,
        *,
        epoch_signature: str,
        change_marker: str,
    ) -> bool:
        """O(1) hash lookup verifying whether a checkpointed range matches epoch and change marker."""
        rec = self._records.get(unit_key)
        if rec is None:
            return False
        return (
            rec.epoch_signature == epoch_signature
            and rec.change_marker == change_marker
        )

    def get_record(self, unit_key: str) -> Optional[ProofRangeCheckpointRecord]:
        return self._records.get(unit_key)


# -----------------------------------------------------------------------------
# Mutual Exclusion & Active Lease Lock (.run.lock)
# -----------------------------------------------------------------------------

class ProofPlanningLeaseLock:
    """Mutual exclusion lock (.run.lock) with PID and monotonic heartbeat."""

    def __init__(
        self,
        lock_dir: Union[str, Path],
        *,
        lease_ttl_s: float,
        owner_id: str = "proof-planning",
    ) -> None:
        if lease_ttl_s <= 0:
            raise ValueError("lease_ttl_s must be positive")
        self.lock_dir = Path(lock_dir)
        self.lock_path = self.lock_dir / ".run.lock"
        self.lease_ttl_s = float(lease_ttl_s)
        self.owner_id = str(owner_id)
        self._fd: Optional[int] = None
        self.acquired_at_s: Optional[float] = None
        self.last_heartbeat_s: Optional[float] = None

    def acquire(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is not None:
            raise VerificationLeaseError("proof planning lease lock is already held by this instance")
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise VerificationLeaseError(
                f"active verification lease lock already held at {self.lock_path}"
            ) from exc
        ts = float(now_s if now_s is not None else time.monotonic())
        self._fd = fd
        self.acquired_at_s = ts
        self.last_heartbeat_s = ts
        self._write_metadata(ts)

    def _write_metadata(self, ts: float) -> None:
        if self._fd is None:
            raise VerificationLeaseError("lease lock is not open")
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "owner_id": self.owner_id,
                "acquired_at_s": self.acquired_at_s,
                "heartbeat_at_s": ts,
                "lease_ttl_s": self.lease_ttl_s,
            },
            sort_keys=True,
        ).encode("utf-8")
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def heartbeat(self, *, now_s: Optional[float] = None) -> None:
        if self._fd is None or self.last_heartbeat_s is None:
            raise VerificationLeaseError("cannot heartbeat an unacquired verification lease")
        ts = float(now_s if now_s is not None else time.monotonic())
        if ts - self.last_heartbeat_s > self.lease_ttl_s:
            raise VerificationLeaseError(
                f"verification lease expired before heartbeat ({ts - self.last_heartbeat_s:.2f}s > {self.lease_ttl_s:.2f}s)"
            )
        self.last_heartbeat_s = ts
        self._write_metadata(ts)

    def release(self) -> None:
        if self._fd is None:
            return
        fd = self._fd
        self._fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> ProofPlanningLeaseLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.release()


# -----------------------------------------------------------------------------
# Adaptive AIMD Batch Sizing & Cold-Start Byte-Weighted ETA
# -----------------------------------------------------------------------------

class AdaptiveProofBatchController:
    """Dynamic AIMD batch sizing and instant cold-start byte-weighted ETA."""

    def __init__(
        self,
        *,
        initial_batch_rows: int,
        min_batch_rows: int,
        max_batch_rows: int,
        target_latency_s: float,
        additive_increase_rows: int,
        multiplicative_decrease_factor: float = 0.5,
    ) -> None:
        if min_batch_rows <= 0 or max_batch_rows < min_batch_rows:
            raise ValueError("invalid min/max batch row bounds")
        if target_latency_s <= 0:
            raise ValueError("target_latency_s must be positive")
        if not (0.0 < multiplicative_decrease_factor < 1.0):
            raise ValueError("multiplicative_decrease_factor must be in (0, 1)")
        self.min_batch_rows = int(min_batch_rows)
        self.max_batch_rows = int(max_batch_rows)
        self.current_batch_rows = max(
            self.min_batch_rows, min(self.max_batch_rows, int(initial_batch_rows))
        )
        self.target_latency_s = float(target_latency_s)
        self.additive_increase_rows = max(1, int(additive_increase_rows))
        self.multiplicative_decrease_factor = float(multiplicative_decrease_factor)
        self._ewma_bytes_per_sec: Optional[float] = None

    def observe_batch(self, *, rows: int, byte_weight: int, elapsed_s: float) -> int:
        elapsed = max(1e-6, float(elapsed_s))
        rate = float(max(1, byte_weight)) / elapsed
        if self._ewma_bytes_per_sec is None:
            self._ewma_bytes_per_sec = rate
        else:
            self._ewma_bytes_per_sec = 0.3 * rate + 0.7 * self._ewma_bytes_per_sec

        if elapsed <= self.target_latency_s:
            self.current_batch_rows = min(
                self.max_batch_rows,
                self.current_batch_rows + self.additive_increase_rows,
            )
        else:
            reduced = int(self.current_batch_rows * self.multiplicative_decrease_factor)
            self.current_batch_rows = max(self.min_batch_rows, reduced)
        return self.current_batch_rows

    def estimate_remaining_eta_s(
        self,
        *,
        remaining_bytes: int,
        cold_start_probe_bytes_per_sec: float,
    ) -> float:
        if remaining_bytes <= 0:
            return 0.0
        effective_rate = (
            self._ewma_bytes_per_sec
            if self._ewma_bytes_per_sec is not None
            else float(cold_start_probe_bytes_per_sec)
        )
        if effective_rate <= 0:
            raise ValueError("effective byte rate must be positive")
        return float(remaining_bytes) / effective_rate


# -----------------------------------------------------------------------------
# Full-Capacity External Tool Wrapper (Zero Internal Tool Leaks)
# -----------------------------------------------------------------------------

_INTERNAL_TOOL_NAMES: FrozenSet[str] = frozenset(
    {"datafold", "dms", "dts", "reladiff", "dvt", "pgcopydb", "debezium"}
)


@dataclass(frozen=True)
class NormalizedProofToolEnvelope:
    """Normalized migkit proof envelope with zero internal tool name leaks."""

    object_name: str
    care_badge: CareBadge
    verdict_word: str
    verdict_kind: ProofVerdictKind
    sanitized_detail: str
    normalized_summary: str


def normalize_external_proof_tool_output(
    raw_tool_name: str,
    *,
    object_name: str,
    raw_output: str,
    report: ProofContractReport,
) -> NormalizedProofToolEnvelope:
    """Wrap external verifier output into migkit's normalized proof contract format."""
    sanitized = str(raw_output or "")
    for token in _INTERNAL_TOOL_NAMES:
        sanitized = sanitized.replace(token, "migkit-proof-engine")
        sanitized = sanitized.replace(token.upper(), "MIGKIT-PROOF-ENGINE")
        sanitized = sanitized.replace(token.capitalize(), "Migkit-Proof-Engine")

    summary = (
        f"[{report.care_badge.value}] {object_name}: verdict={report.verdict_word} "
        f"gate={report.verdict_kind.value} rows_read={report.total_source_rows_read} "
        f"reused_nodes={report.total_reused_nodes}"
    )
    return NormalizedProofToolEnvelope(
        object_name=object_name,
        care_badge=report.care_badge,
        verdict_word=report.verdict_word,
        verdict_kind=report.verdict_kind,
        sanitized_detail=sanitized,
        normalized_summary=summary,
    )
