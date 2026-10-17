"""Discover transitive dependencies within authorized scope (Row 595: SMART-N12.discovery-closure).

Extends guide.survey and dependency catalogues using `decide.Rung` and
`decide.climb(work, rungs, facts)`.

Inspects selected tables, partitions, foreign keys (FKs), views, routines,
extensions, sequences, jobs, principals, collation/timezone/encoding,
replication and target conflicts; distinguishing required, optional, absent,
unreadable, and out-of-scope objects.

Strict scope boundary invariant:
Does NOT expand network or data scope automatically.
Hidden or inaccessible objects cannot look complete.
Dependency cycles and external references receive explicit dispositions.
Metadata probes are bounded by explicit query budgets, and optional missing facts
do not force full scans.

Pure UNIX style only. Zero emojis or decorative glyphs.
Care badges: ACTION, REVIEW, BENIGN, PASS.
100% pure ASCII.
"""

from __future__ import annotations

import collections
import enum
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from . import decide

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


class CareBadge(str, enum.Enum):
    ACTION = "ACTION"
    REVIEW = "REVIEW"
    BENIGN = "BENIGN"
    PASS = "PASS"


class ObjectKind(str, enum.Enum):
    TABLE = "table"
    PARTITION = "partition"
    FOREIGN_KEY = "foreign_key"
    VIEW = "view"
    ROUTINE = "routine"
    EXTENSION = "extension"
    SEQUENCE = "sequence"
    JOB = "job"
    PRINCIPAL = "principal"
    COLLATION = "collation"
    TIMEZONE = "timezone"
    ENCODING = "encoding"
    REPLICATION = "replication"
    TARGET_CONFLICT = "target_conflict"


class ObjectStatus(str, enum.Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    ABSENT = "absent"
    UNREADABLE = "unreadable"
    OUT_OF_SCOPE = "out_of_scope"


class CycleDisposition(str, enum.Enum):
    DEFER_CONSTRAINTS = "defer_constraints"
    TWO_PASS_NULLABLE_FK = "two_pass_nullable_fk"
    OPERATOR_ACTION = "operator_action"


class ExternalRefDisposition(str, enum.Enum):
    ASSERT_PREEXISTING = "assert_preexisting"
    STUB_TARGET = "stub_target"
    BLOCK_UNAUTHORIZED = "block_unauthorized"


class TargetConflictDisposition(str, enum.Enum):
    REUSE_COMPATIBLE = "reuse_compatible"
    STAGE_SHADOW = "stage_shadow"
    BLOCK_COLLISION = "block_collision"


# =============================================================================
# 1. Decide Rung Ladders
# =============================================================================


def scope_boundary_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding disposition of external out-of-scope references."""
    return (
        decide.Rung(
            "target_preexisting_contract",
            "treat external dependency as an asserted preexisting target object without expanding crawl",
            needs=(
                decide.Need(
                    "target admits preexisting object contract",
                    lambda f: bool(f.get("target_allows_preexisting_contract", False)),
                    "{rung} requires target platform to allow preexisting object contract",
                ),
                decide.Need(
                    "external dependency verified preexisting on target",
                    lambda f: bool(f.get("external_object_registered_in_target", False)),
                    "{rung} requires external object to be already present on target",
                ),
            ),
            because="treats external dependency as an asserted preexisting target object without expanding source crawl",
            footprint=0,
        ),
        decide.Rung(
            "strict_enforce_authorized_boundary",
            "strictly enforce authorized scope boundary for non-blocking optional external reference",
            needs=(
                decide.Need(
                    "boundary strict enforcement enabled",
                    lambda f: bool(f.get("boundary_strict_enforced", True)),
                    "{rung} requires strict boundary enforcement",
                ),
                decide.Need(
                    "external dependency is optional or advisory",
                    lambda f: bool(f.get("external_dependency_is_optional", False)),
                    "{rung} requires external dependency to be optional",
                ),
            ),
            because="strictly excludes optional external reference at boundary without failing migration",
            footprint=1,
        ),
        decide.Rung(
            "block_unauthorized_scope_leak",
            "block migration when external dependency violates boundary without preexisting target contract",
            needs=(
                decide.Need(
                    "fail closed on unauthorized scope leak",
                    lambda f: bool(f.get("fail_closed_on_unauthorized_scope", True)),
                    "{rung} halts on unauthorized out-of-scope dependency",
                ),
            ),
            because="blocks migration when external dependency violates boundary without preexisting target contract",
            footprint=2,
        ),
    )


def cycle_disposition_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding resolution strategy for circular dependency cycles."""
    return (
        decide.Rung(
            "defer_deferrable_fk_constraints",
            "break cycle cleanly by deferring foreign key constraint checks until post-data load",
            needs=(
                decide.Need(
                    "all cyclic foreign keys are deferrable",
                    lambda f: bool(f.get("all_cycle_fks_deferrable", False)),
                    "{rung} requires all foreign keys in cycle to support DEFERRABLE",
                ),
                decide.Need(
                    "target supports deferred constraint validation",
                    lambda f: bool(f.get("target_supports_deferred_constraints", False)),
                    "{rung} requires target engine to support deferred constraints",
                ),
            ),
            because="breaks dependency cycle cleanly by deferring constraint checks to post-data load",
            footprint=0,
        ),
        decide.Rung(
            "staged_two_pass_nullable_fk",
            "break cycle by inserting rows with NULL foreign keys then backfilling in second pass",
            needs=(
                decide.Need(
                    "at least one foreign key in cycle is nullable",
                    lambda f: bool(f.get("at_least_one_fk_nullable", False)),
                    "{rung} requires at least one nullable column in circular foreign key set",
                ),
                decide.Need(
                    "staged two-pass update supported",
                    lambda f: bool(f.get("supports_two_pass_staged_update", False)),
                    "{rung} requires staged two-pass load capability",
                ),
            ),
            because="breaks cycle by inserting rows with NULL foreign key then updating FK references in second pass",
            footprint=1,
        ),
        decide.Rung(
            "halt_unresolvable_cycle_for_operator",
            "flag unresolvable circular dependency requiring explicit operator intervention",
            needs=(
                decide.Need(
                    "cycle cannot be resolved automatically",
                    lambda f: bool(f.get("cycle_cannot_be_broken_automatically", True)),
                    "{rung} flags unresolvable circular dependency",
                ),
            ),
            because="requires operator intervention for non-deferrable not-null circular dependency cycle",
            footprint=2,
        ),
    )


def unreadable_object_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for handling inaccessible or unreadable objects in discovery."""
    return (
        decide.Rung(
            "mark_incomplete_action_badge",
            "guarantee hidden or inaccessible objects cannot look complete and flag ACTION care badge",
            needs=(
                decide.Need(
                    "honest completeness guarantee enforced",
                    lambda f: bool(f.get("enforce_honest_completeness", True)),
                    "{rung} enforces honest completeness invariant",
                ),
                decide.Need(
                    "unreadable or hidden objects detected",
                    lambda f: bool(f.get("has_unreadable_objects", False)),
                    "{rung} requires presence of unreadable objects",
                ),
            ),
            because="guarantees hidden or inaccessible objects cannot look complete and flags ACTION care badge",
            footprint=0,
        ),
        decide.Rung(
            "bounded_schema_inferred_probe",
            "infer object interface from schema metadata without triggering full table scan",
            needs=(
                decide.Need(
                    "parent schema read privilege available",
                    lambda f: bool(f.get("schema_read_grant_available", False)),
                    "{rung} requires schema-level read privilege",
                ),
                decide.Need(
                    "remaining probe query budget permits inspection",
                    lambda f: int(f.get("probe_budget_remaining", 0)) > 0,
                    "{rung} exhausted probe budget",
                ),
            ),
            because="infers object interface from schema metadata without triggering full scan",
            footprint=1,
        ),
        decide.Rung(
            "fail_closed_security_violation",
            "fail closed when security boundaries conceal critical referential dependencies",
            needs=(
                decide.Need(
                    "strict security boundary failure required",
                    lambda f: bool(f.get("strict_security_failure_mode", True)),
                    "{rung} requires fail-closed mode",
                ),
            ),
            because="refuses to migrate when catalog permissions conceal critical referential dependencies",
            footprint=2,
        ),
    )


def metadata_probe_budget_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for budgeting metadata queries without forcing full scans."""
    return (
        decide.Rung(
            "cached_catalogue_facts",
            "use existing survey facts avoiding redundant network roundtrips",
            needs=(
                decide.Need(
                    "cached catalogue facts already complete",
                    lambda f: bool(f.get("cached_facts_sufficient", False)),
                    "{rung} requires complete cached catalogue facts",
                ),
                decide.Need(
                    "zero extra network queries required",
                    lambda f: int(f.get("probe_queries_executed", 0)) == 0,
                    "{rung} requires unspent probe queries",
                ),
            ),
            because="uses existing survey facts avoiding redundant network roundtrips",
            footprint=0,
        ),
        decide.Rung(
            "bounded_targeted_catalog_probe",
            "execute bounded single-object query without scanning unrelated catalog entries",
            needs=(
                decide.Need(
                    "probe budget permits targeted query",
                    lambda f: int(f.get("probe_budget_remaining", 0)) > 0,
                    "{rung} requires remaining probe budget",
                ),
                decide.Need(
                    "missing facts are strictly required",
                    lambda f: bool(f.get("missing_facts_required", False)),
                    "{rung} requires mandatory missing facts",
                ),
            ),
            because="executes bounded single-object query without scanning unrelated catalog entries",
            footprint=1,
        ),
        decide.Rung(
            "skip_optional_probe_graceful",
            "gracefully mark optional facts as unprobed rather than forcing expensive table scans",
            needs=(
                decide.Need(
                    "missing facts are optional or advisory",
                    lambda f: bool(f.get("missing_facts_are_optional", True)),
                    "{rung} requires optional missing facts",
                ),
                decide.Need(
                    "avoiding full catalog scan",
                    lambda f: bool(f.get("do_not_force_full_scan", True)),
                    "{rung} forbids triggering full scans for optional facts",
                ),
            ),
            because="gracefully marks optional facts as unprobed rather than forcing expensive table scans",
            footprint=2,
        ),
    )


def target_conflict_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for deciding target schema, collation, timezone, or encoding conflicts."""
    return (
        decide.Rung(
            "compatible_reuse_target_object",
            "reuse existing target object safely without destructive recreation",
            needs=(
                decide.Need(
                    "target object exists and matches schema",
                    lambda f: bool(f.get("target_object_exists", False)),
                    "{rung} requires existing target object",
                ),
                decide.Need(
                    "structural, collation, and encoding compatibility confirmed",
                    lambda f: bool(f.get("structural_and_encoding_compatible", False)),
                    "{rung} requires confirmed structural and character set compatibility",
                ),
            ),
            because="reuses existing target object safely without destructive recreation",
            footprint=0,
        ),
        decide.Rung(
            "stage_shadow_isolated_schema",
            "stage conflicting object in isolated shadow namespace for safe parallel verification",
            needs=(
                decide.Need(
                    "shadow schema isolation supported on target",
                    lambda f: bool(f.get("shadow_schema_supported", False)),
                    "{rung} requires shadow schema support",
                ),
                decide.Need(
                    "target conflict allows non-destructive shadowing",
                    lambda f: bool(f.get("conflict_tolerates_shadowing", False)),
                    "{rung} requires conflict to be isolatable",
                ),
            ),
            because="stages conflicting object in isolated shadow namespace for safe parallel verification",
            footprint=1,
        ),
        decide.Rung(
            "block_target_collision_action",
            "block cutover with ACTION care badge when target has unresolvable conflicting object or encoding",
            needs=(
                decide.Need(
                    "destructive target collision detected",
                    lambda f: bool(f.get("abort_on_incompatible_collision", True)),
                    "{rung} requires collision abort setting",
                ),
            ),
            because="blocks cutover with ACTION care badge when target has unresolvable conflicting object or encoding",
            footprint=2,
        ),
    )


def closure_evaluation_rungs() -> Tuple[decide.Rung, ...]:
    """Ladder for evaluating overall discovery closure completeness."""
    return (
        decide.Rung(
            "certify_complete_closure",
            "certify closure is complete, bounded, and safe for migration with PASS badge",
            needs=(
                decide.Need(
                    "all required objects present in scope or target contract",
                    lambda f: bool(f.get("all_required_objects_present", False)),
                    "{rung} requires all mandatory dependencies present",
                ),
                decide.Need(
                    "zero unreadable or inaccessible objects",
                    lambda f: bool(f.get("zero_unreadable_objects", False)),
                    "{rung} requires zero unreadable objects",
                ),
                decide.Need(
                    "all dependency cycles have verified dispositions",
                    lambda f: bool(f.get("all_cycles_disposed", False)),
                    "{rung} requires all cycles to have explicit safe dispositions",
                ),
                decide.Need(
                    "zero unauthorized out-of-scope references",
                    lambda f: bool(f.get("zero_unauthorized_external_refs", False)),
                    "{rung} requires zero unauthorized external references",
                ),
                decide.Need(
                    "all target schema and encoding conflicts resolved",
                    lambda f: bool(f.get("target_conflicts_resolved", False)),
                    "{rung} requires all target conflicts resolved",
                ),
            ),
            because="certifies closure is complete, bounded, and safe for migration with PASS badge",
            footprint=0,
        ),
        decide.Rung(
            "advisory_partial_closure_with_warnings",
            "admit partial closure with REVIEW care badge because missing items are non-blocking optional objects",
            needs=(
                decide.Need(
                    "all required core dependencies present",
                    lambda f: bool(f.get("all_required_objects_present", False)),
                    "{rung} requires required dependencies",
                ),
                decide.Need(
                    "zero unreadable objects",
                    lambda f: bool(f.get("zero_unreadable_objects", False)),
                    "{rung} forbids unreadable objects in advisory partial closure",
                ),
                decide.Need(
                    "zero unauthorized external references",
                    lambda f: bool(f.get("zero_unauthorized_external_refs", False)),
                    "{rung} forbids unauthorized external references in advisory partial closure",
                ),
                decide.Need(
                    "optional gaps annotated with care badges",
                    lambda f: bool(f.get("optional_gaps_annotated", False)),
                    "{rung} requires explicit care badges on optional gaps",
                ),
                decide.Need(
                    "cycles and external references disposed",
                    lambda f: bool(f.get("cycles_and_external_disposed", False)),
                    "{rung} requires disposed cycles and external references",
                ),
            ),
            because="admits partial closure with REVIEW care badge because missing items are non-blocking optional objects",
            footprint=1,
        ),
        decide.Rung(
            "reject_incomplete_closure",
            "honestly reject closure with ACTION care badge due to missing required objects or unreadable data",
            needs=(
                decide.Need(
                    "closure contains blocking gaps or security barriers",
                    lambda f: bool(f.get("closure_has_blocking_defects", True)),
                    "{rung} rejects invalid or incomplete closure",
                ),
            ),
            because="honestly rejects closure with ACTION care badge due to missing required objects or unreadable data",
            footprint=2,
        ),
    )


# =============================================================================
# 2. Domain Data Structures
# =============================================================================


@dataclass(frozen=True)
class DependencyEdge:
    """Directed dependency edge from source to target."""

    source_id: str
    target_id: str
    kind: ObjectKind
    is_deferrable: bool = False
    is_nullable: bool = False
    is_hard: bool = True
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveredObject:
    """Discovered database object within or adjacent to authorized scope."""

    object_id: str
    name: str
    kind: ObjectKind
    status: ObjectStatus
    in_authorized_scope: bool = True
    is_accessible: bool = True
    care_badge: str = CARE_PASS
    schema: str = ""
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    disposition: Optional[str] = None
    error_detail: Optional[str] = None


@dataclass
class MetadataProbeBudget:
    """Measured query budget for bounded discovery probes."""

    max_queries: int = 100
    queries_executed: int = 0
    max_depth: int = 10
    timeout_seconds: float = 30.0
    bytes_read: int = 0
    probe_log: List[Dict[str, Any]] = field(default_factory=list)

    def can_probe(self) -> bool:
        return self.queries_executed < self.max_queries

    def remaining_queries(self) -> int:
        return max(0, self.max_queries - self.queries_executed)

    def record_probe(self, object_id: str, probe_type: str, read_bytes: int = 0) -> None:
        if not self.can_probe():
            raise RuntimeError(
                f"Metadata probe budget exhausted: {self.queries_executed}/{self.max_queries} executed"
            )
        self.queries_executed += 1
        self.bytes_read += read_bytes
        self.probe_log.append(
            {
                "object_id": object_id,
                "probe_type": probe_type,
                "read_bytes": read_bytes,
                "query_number": self.queries_executed,
            }
        )


@dataclass
class TargetConflict:
    """Conflict observed against target database catalog or environment."""

    conflict_id: str
    object_id: str
    conflict_type: str
    source_value: Any
    target_value: Any
    care_badge: str = CARE_ACTION
    disposition: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryClosureResult:
    """The complete result of dependency closure discovery within authorized scope."""

    is_complete: bool
    care_badge: str
    objects: Dict[str, DiscoveredObject]
    edges: List[DependencyEdge]
    cycles: List[List[str]]
    cycle_dispositions: Dict[str, str]
    external_references: List[str]
    external_dispositions: Dict[str, str]
    target_conflicts: List[TargetConflict]
    target_dispositions: Dict[str, str]
    unreadable_objects: List[str]
    probe_budget: MetadataProbeBudget
    decisions_log: List[Dict[str, Any]]
    summary: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_complete": self.is_complete,
            "care_badge": self.care_badge,
            "objects_count": len(self.objects),
            "cycles_count": len(self.cycles),
            "cycles": self.cycles,
            "cycle_dispositions": self.cycle_dispositions,
            "external_references": self.external_references,
            "external_dispositions": self.external_dispositions,
            "target_conflicts": [
                {
                    "conflict_id": tc.conflict_id,
                    "object_id": tc.object_id,
                    "conflict_type": tc.conflict_type,
                    "source_value": str(tc.source_value),
                    "target_value": str(tc.target_value),
                    "care_badge": tc.care_badge,
                    "disposition": tc.disposition,
                }
                for tc in self.target_conflicts
            ],
            "unreadable_objects": self.unreadable_objects,
            "probes_executed": self.probe_budget.queries_executed,
            "summary": self.summary,
        }

    def render_report(self) -> str:
        """Render pure ASCII summary table."""
        lines = [
            "----------------------------------------------------------------------",
            "DISCOVERY CLOSURE REPORT (Row 595: SMART-N12)",
            "----------------------------------------------------------------------",
            f"Overall Status   : {'COMPLETE' if self.is_complete else 'INCOMPLETE'}",
            f"Care Badge       : [{self.care_badge}]",
            f"Total Objects    : {len(self.objects)}",
            f"Cycles Detected  : {len(self.cycles)}",
            f"External Refs    : {len(self.external_references)}",
            f"Target Conflicts : {len(self.target_conflicts)}",
            f"Unreadable Items : {len(self.unreadable_objects)}",
            f"Probes Executed  : {self.probe_budget.queries_executed}/{self.probe_budget.max_queries}",
            "----------------------------------------------------------------------",
            "OBJECT COUNTS BY KIND & STATUS:",
        ]
        counts_by_kind: Dict[str, int] = collections.defaultdict(int)
        for obj in self.objects.values():
            counts_by_kind[f"{obj.kind.value}:{obj.status.value}"] += 1
        for k, v in sorted(counts_by_kind.items()):
            lines.append(f"  {k:<32}: {v:>5}")

        if self.cycles:
            lines.append("----------------------------------------------------------------------")
            lines.append("DEPENDENCY CYCLES & DISPOSITIONS:")
            for c in self.cycles:
                ckey = " -> ".join(c)
                disp = self.cycle_dispositions.get(ckey, "unresolved")
                lines.append(f"  Cycle: {ckey} | Disposition: {disp}")

        if self.external_references:
            lines.append("----------------------------------------------------------------------")
            lines.append("EXTERNAL REFERENCES & DISPOSITIONS:")
            for ref in self.external_references:
                disp = self.external_dispositions.get(ref, "unresolved")
                lines.append(f"  External Ref: {ref} | Disposition: {disp}")

        if self.target_conflicts:
            lines.append("----------------------------------------------------------------------")
            lines.append("TARGET CONFLICTS:")
            for tc in self.target_conflicts:
                lines.append(
                    f"  [{tc.care_badge}] {tc.object_id} ({tc.conflict_type}): "
                    f"src='{tc.source_value}' vs dst='{tc.target_value}' -> {tc.disposition}"
                )

        if self.unreadable_objects:
            lines.append("----------------------------------------------------------------------")
            lines.append("UNREADABLE OBJECTS (ACTION REQUIRED):")
            for u in self.unreadable_objects:
                lines.append(f"  [ACTION] {u}")

        lines.append("----------------------------------------------------------------------")
        return "\n".join(lines)


# =============================================================================
# 3. Decision Climbing Wrappers
# =============================================================================


def climb_scope_boundary(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the scope boundary ladder for an external dependency."""
    return decide.climb(work, scope_boundary_rungs(), facts)


def climb_cycle_disposition(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the cycle disposition ladder for a dependency cycle."""
    return decide.climb(work, cycle_disposition_rungs(), facts)


def climb_unreadable_probe(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the unreadable object ladder for an inaccessible object."""
    return decide.climb(work, unreadable_object_rungs(), facts)


def climb_metadata_probe_budget(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the metadata probe budget ladder."""
    return decide.climb(work, metadata_probe_budget_rungs(), facts)


def climb_target_conflict(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the target conflict ladder."""
    return decide.climb(work, target_conflict_rungs(), facts)


def climb_closure_evaluation(work: str, facts: Mapping[str, Any]) -> decide.Choice:
    """Climb the closure evaluation ladder for the final completeness verdict."""
    return decide.climb(work, closure_evaluation_rungs(), facts)


# =============================================================================
# 4. Cycle Detection Algorithm
# =============================================================================


def detect_cycles(edges: Sequence[DependencyEdge]) -> List[List[str]]:
    """Detect simple cycles in directed dependency edges using Tarjan's algorithm."""
    adj: Dict[str, List[str]] = collections.defaultdict(list)
    nodes: Set[str] = set()
    for e in edges:
        adj[e.source_id].append(e.target_id)
        nodes.add(e.source_id)
        nodes.add(e.target_id)

    index = 0
    indices: Dict[str, int] = {}
    lowlink: Dict[str, int] = {}
    on_stack: Dict[str, bool] = {}
    stack: List[str] = []
    sccs: List[List[str]] = []

    def strongconnect(v: str) -> None:
        nonlocal index
        indices[v] = index
        lowlink[v] = index
        index += 1
        stack.append(v)
        on_stack[v] = True

        for w in adj[v]:
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif on_stack.get(w, False):
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            scc: List[str] = []
            while True:
                w = stack.pop()
                on_stack[w] = False
                scc.append(w)
                if w == v:
                    break
            if len(scc) > 1 or (len(scc) == 1 and v in adj[v]):
                sccs.append(list(reversed(scc)))

    for node in sorted(nodes):
        if node not in indices:
            strongconnect(node)

    return sccs


# =============================================================================
# 5. Core Discovery Engine
# =============================================================================


class DiscoveryClosureEngine:
    """Engine for discovering transitive dependency closures within authorized scope."""

    def __init__(
        self,
        authorized_tables: Iterable[str],
        authorized_schemas: Optional[Iterable[str]] = None,
        probe_budget: Optional[MetadataProbeBudget] = None,
        source_catalogue: Optional[Mapping[str, Any]] = None,
        target_catalogue: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.authorized_tables: Set[str] = set(authorized_tables)
        self.authorized_schemas: Set[str] = (
            set(authorized_schemas)
            if authorized_schemas is not None
            else {t.split(".", 1)[0] for t in self.authorized_tables if "." in t}
        )
        self.probe_budget: MetadataProbeBudget = (
            probe_budget if probe_budget is not None else MetadataProbeBudget()
        )
        self.source_catalogue: Dict[str, Any] = dict(source_catalogue or {})
        self.target_catalogue: Dict[str, Any] = dict(target_catalogue or {})

        self.objects: Dict[str, DiscoveredObject] = {}
        self.edges: List[DependencyEdge] = []
        self.target_conflicts: List[TargetConflict] = []
        self.decisions_log: List[Dict[str, Any]] = []

    def is_in_authorized_scope(self, object_id: str) -> bool:
        """Check if object_id belongs to authorized tables or schemas."""
        if object_id in self.authorized_tables:
            return True
        if "." in object_id:
            schema, name = object_id.split(".", 1)
            if schema in self.authorized_schemas:
                return True
        return False

    def discover(self) -> DiscoveryClosureResult:
        """Execute transitive dependency discovery within authorized scope."""
        # 1. Initialize root selected tables
        queue: collections.deque[str] = collections.deque(sorted(self.authorized_tables))
        visited: Set[str] = set()

        for t in self.authorized_tables:
            self._register_or_inspect_object(
                object_id=t,
                name=t.split(".", 1)[1] if "." in t else t,
                kind=ObjectKind.TABLE,
                is_hard=True,
            )

        # 2. Transitive exploration loop
        while queue:
            current_id = queue.popleft()
            if current_id in visited:
                continue
            visited.add(current_id)

            # Probe dependencies for current_id
            deps = self._probe_object_dependencies(current_id)
            for edge in deps:
                self.edges.append(edge)
                target_id = edge.target_id

                # Register discovered target object
                if target_id not in self.objects:
                    obj = self._register_or_inspect_object(
                        object_id=target_id,
                        name=target_id.split(".", 1)[1] if "." in target_id else target_id,
                        kind=edge.kind,
                        is_hard=edge.is_hard,
                        is_deferrable=edge.is_deferrable,
                        is_nullable=edge.is_nullable,
                    )
                    # If in authorized scope and accessible, enqueue for further transitive inspection
                    if obj.in_authorized_scope and obj.is_accessible:
                        queue.append(target_id)

        # 3. Inspect global and environment dependencies (Extensions, Principals, Jobs, Encodings)
        self._inspect_environment_and_catalogs()

        # 4. Detect dependency cycles
        cycles = detect_cycles(self.edges)
        cycle_dispositions: Dict[str, str] = {}
        for c in cycles:
            ckey = " -> ".join(c)
            disp = self._resolve_cycle(c)
            cycle_dispositions[ckey] = disp

        # 5. Resolve external out-of-scope references
        external_refs: List[str] = []
        external_dispositions: Dict[str, str] = {}
        for obj_id, obj in sorted(self.objects.items()):
            if not obj.in_authorized_scope:
                external_refs.append(obj_id)
                disp = self._resolve_external_reference(obj)
                external_dispositions[obj_id] = disp
                obj.disposition = disp

        # 6. Resolve unreadable objects
        unreadable_refs: List[str] = []
        for obj_id, obj in sorted(self.objects.items()):
            if not obj.is_accessible:
                unreadable_refs.append(obj_id)
                disp = self._resolve_unreadable_object(obj)
                obj.disposition = disp

        # 7. Resolve target conflicts
        target_dispositions: Dict[str, str] = {}
        for tc in self.target_conflicts:
            disp = self._resolve_target_conflict(tc)
            tc.disposition = disp
            target_dispositions[tc.conflict_id] = disp

        # 8. Compute closure evaluation verdict
        closure_choice = self._evaluate_final_closure(
            cycles=cycles,
            cycle_dispositions=cycle_dispositions,
            external_refs=external_refs,
            external_dispositions=external_dispositions,
            unreadable_refs=unreadable_refs,
        )

        is_complete = closure_choice.path == "certify_complete_closure"
        care_badge = (
            CARE_PASS
            if is_complete
            else (
                CARE_REVIEW
                if closure_choice.path == "advisory_partial_closure_with_warnings"
                else CARE_ACTION
            )
        )

        # Summary statistics
        summary = {
            "total_objects": len(self.objects),
            "required_objects": sum(
                1 for o in self.objects.values() if o.status == ObjectStatus.REQUIRED
            ),
            "optional_objects": sum(
                1 for o in self.objects.values() if o.status == ObjectStatus.OPTIONAL
            ),
            "unreadable_objects": len(unreadable_refs),
            "out_of_scope_objects": len(external_refs),
            "cycles": len(cycles),
            "target_conflicts": len(self.target_conflicts),
            "probes_executed": self.probe_budget.queries_executed,
        }

        return DiscoveryClosureResult(
            is_complete=is_complete,
            care_badge=care_badge,
            objects=self.objects,
            edges=self.edges,
            cycles=cycles,
            cycle_dispositions=cycle_dispositions,
            external_references=external_refs,
            external_dispositions=external_dispositions,
            target_conflicts=self.target_conflicts,
            target_dispositions=target_dispositions,
            unreadable_objects=unreadable_refs,
            probe_budget=self.probe_budget,
            decisions_log=self.decisions_log,
            summary=summary,
        )

    # -------------------------------------------------------------------------
    # Internal Inspection & Resolution Helpers
    # -------------------------------------------------------------------------

    def _register_or_inspect_object(
        self,
        object_id: str,
        name: str,
        kind: ObjectKind,
        is_hard: bool = True,
        is_deferrable: bool = False,
        is_nullable: bool = False,
    ) -> DiscoveredObject:
        """Register object and determine its access and scope status."""
        in_scope = self.is_in_authorized_scope(object_id)

        # Check source catalogue facts
        src_meta = self.source_catalogue.get("objects", {}).get(object_id, {})
        is_unreadable = src_meta.get("unreadable", False)
        error_detail = src_meta.get("error_detail")
        exists = src_meta.get("exists", True)

        if not in_scope:
            status = ObjectStatus.OUT_OF_SCOPE
            care_badge = CARE_REVIEW if not is_hard else CARE_ACTION
        elif is_unreadable:
            status = ObjectStatus.UNREADABLE
            care_badge = CARE_ACTION
        elif not exists:
            status = ObjectStatus.ABSENT
            care_badge = CARE_ACTION if is_hard else CARE_REVIEW
        elif is_hard:
            status = ObjectStatus.REQUIRED
            care_badge = CARE_PASS
        else:
            status = ObjectStatus.OPTIONAL
            care_badge = CARE_BENIGN

        schema = object_id.split(".", 1)[0] if "." in object_id else ""
        obj = DiscoveredObject(
            object_id=object_id,
            name=name,
            kind=kind,
            status=status,
            in_authorized_scope=in_scope,
            is_accessible=not is_unreadable,
            care_badge=care_badge,
            schema=schema,
            metadata=src_meta,
            error_detail=error_detail,
        )
        self.objects[object_id] = obj
        return obj

    def _probe_object_dependencies(self, object_id: str) -> List[DependencyEdge]:
        """Inspect dependencies of object_id using bounded probe budget."""
        edges: List[DependencyEdge] = []
        meta = self.source_catalogue.get("objects", {}).get(object_id, {})

        # Climb metadata probe budget ladder
        cached_facts_sufficient = "dependencies" in meta
        missing_required = not cached_facts_sufficient and meta.get("required_for_closure", True)
        missing_optional = not cached_facts_sufficient and not missing_required

        facts = {
            "cached_facts_sufficient": cached_facts_sufficient,
            "probe_queries_executed": self.probe_budget.queries_executed,
            "probe_budget_remaining": self.probe_budget.remaining_queries(),
            "missing_facts_required": missing_required,
            "missing_facts_are_optional": missing_optional,
            "do_not_force_full_scan": True,
        }
        choice = climb_metadata_probe_budget(f"probe dependencies for {object_id}", facts)
        self.decisions_log.append(
            {
                "ladder": "metadata_probe_budget_rungs",
                "object_id": object_id,
                "choice": choice.path,
                "reason": choice.reason,
            }
        )

        if choice.path == "cached_catalogue_facts":
            raw_deps = meta.get("dependencies", [])
        elif choice.path == "bounded_targeted_catalog_probe":
            # Execute targeted probe using budget
            self.probe_budget.record_probe(object_id, "targeted_dependency_lookup", read_bytes=512)
            raw_deps = meta.get("probed_dependencies", meta.get("dependencies", []))
        else:
            # skip_optional_probe_graceful: do not force full scan
            raw_deps = []

        for d in raw_deps:
            target = d["target"]
            kind = ObjectKind(d.get("kind", ObjectKind.TABLE.value))
            edge = DependencyEdge(
                source_id=object_id,
                target_id=target,
                kind=kind,
                is_deferrable=bool(d.get("deferrable", False)),
                is_nullable=bool(d.get("nullable", False)),
                is_hard=bool(d.get("hard", True)),
                attributes=d.get("attributes", {}),
            )
            edges.append(edge)

        return edges

    def _inspect_environment_and_catalogs(self) -> None:
        """Inspect partitions, sequences, views, routines, extensions, encoding/timezone, and target conflicts."""
        src_env = self.source_catalogue.get("environment", {})
        dst_env = self.target_catalogue.get("environment", {})

        # Collation, Timezone, Encoding checks
        for prop, kind in (
            ("encoding", ObjectKind.ENCODING),
            ("collation", ObjectKind.COLLATION),
            ("timezone", ObjectKind.TIMEZONE),
        ):
            src_val = src_env.get(prop)
            dst_val = dst_env.get(prop)
            if src_val and dst_val and src_val != dst_val:
                conflict = TargetConflict(
                    conflict_id=f"conflict_{prop}",
                    object_id=f"env.{prop}",
                    conflict_type=f"{prop}_mismatch",
                    source_value=src_val,
                    target_value=dst_val,
                    care_badge=CARE_ACTION,
                    details={"source": src_val, "target": dst_val},
                )
                self.target_conflicts.append(conflict)

        # Partitions
        for part_root, part_info in self.source_catalogue.get("partitions", {}).items():
            if self.is_in_authorized_scope(part_root):
                for p_child in part_info.get("children", []):
                    self._register_or_inspect_object(
                        object_id=p_child,
                        name=p_child.split(".", 1)[1] if "." in p_child else p_child,
                        kind=ObjectKind.PARTITION,
                        is_hard=True,
                    )
                    self.edges.append(
                        DependencyEdge(
                            source_id=part_root,
                            target_id=p_child,
                            kind=ObjectKind.PARTITION,
                            is_hard=True,
                        )
                    )

        # Sequences
        for seq_id, seq_info in self.source_catalogue.get("sequences", {}).items():
            owner_table = seq_info.get("owner_table")
            if owner_table and self.is_in_authorized_scope(owner_table):
                self._register_or_inspect_object(
                    object_id=seq_id,
                    name=seq_id.split(".", 1)[1] if "." in seq_id else seq_id,
                    kind=ObjectKind.SEQUENCE,
                    is_hard=True,
                )
                self.edges.append(
                    DependencyEdge(
                        source_id=owner_table,
                        target_id=seq_id,
                        kind=ObjectKind.SEQUENCE,
                        is_hard=True,
                    )
                )

        # Extensions
        for ext_id, ext_info in self.source_catalogue.get("extensions", {}).items():
            required = ext_info.get("required", False)
            if required:
                self._register_or_inspect_object(
                    object_id=f"extension:{ext_id}",
                    name=ext_id,
                    kind=ObjectKind.EXTENSION,
                    is_hard=True,
                )
                # Check if target supports extension
                target_exts = set(self.target_catalogue.get("extensions", {}).keys())
                if ext_id not in target_exts:
                    self.target_conflicts.append(
                        TargetConflict(
                            conflict_id=f"conflict_extension_{ext_id}",
                            object_id=f"extension:{ext_id}",
                            conflict_type="missing_target_extension",
                            source_value=ext_id,
                            target_value=None,
                            care_badge=CARE_ACTION,
                        )
                    )

        # Principals
        for role_id, role_info in self.source_catalogue.get("principals", {}).items():
            if role_info.get("required", False):
                self._register_or_inspect_object(
                    object_id=f"principal:{role_id}",
                    name=role_id,
                    kind=ObjectKind.PRINCIPAL,
                    is_hard=False,
                )

        # Replication slots / publications
        for rep_id, rep_info in self.source_catalogue.get("replication", {}).items():
            self._register_or_inspect_object(
                object_id=f"replication:{rep_id}",
                name=rep_id,
                kind=ObjectKind.REPLICATION,
                is_hard=bool(rep_info.get("required", False)),
            )

        # Target table conflicts (e.g. existing tables on target with schema drift)
        dst_tables = self.target_catalogue.get("tables", {})
        for t in self.authorized_tables:
            if t in dst_tables:
                dst_meta = dst_tables[t]
                src_meta = self.source_catalogue.get("objects", {}).get(t, {})
                src_cols = src_meta.get("columns", {})
                dst_cols = dst_meta.get("columns", {})
                if src_cols and dst_cols and src_cols != dst_cols:
                    self.target_conflicts.append(
                        TargetConflict(
                            conflict_id=f"conflict_table_{t}",
                            object_id=t,
                            conflict_type="column_definition_mismatch",
                            source_value=src_cols,
                            target_value=dst_cols,
                            care_badge=CARE_ACTION,
                        )
                    )

    def _resolve_cycle(self, cycle: List[str]) -> str:
        """Resolve disposition for a circular dependency cycle using `cycle_disposition_rungs`."""
        cycle_edges = [e for e in self.edges if e.source_id in cycle and e.target_id in cycle]
        all_deferrable = (
            bool(cycle_edges) and all(e.is_deferrable for e in cycle_edges)
        )
        at_least_one_nullable = any(e.is_nullable for e in cycle_edges)
        target_supports_deferred = bool(
            self.target_catalogue.get("environment", {}).get("supports_deferred_constraints", False)
        )
        target_supports_two_pass = bool(
            self.target_catalogue.get("environment", {}).get("supports_two_pass_update", False)
        )

        facts = {
            "all_cycle_fks_deferrable": all_deferrable,
            "target_supports_deferred_constraints": target_supports_deferred,
            "at_least_one_fk_nullable": at_least_one_nullable,
            "supports_two_pass_staged_update": target_supports_two_pass,
            "cycle_cannot_be_broken_automatically": not (
                (all_deferrable and target_supports_deferred)
                or (at_least_one_nullable and target_supports_two_pass)
            ),
        }
        choice = climb_cycle_disposition(f"cycle resolution for {' -> '.join(cycle)}", facts)
        self.decisions_log.append(
            {
                "ladder": "cycle_disposition_rungs",
                "cycle": cycle,
                "choice": choice.path,
                "reason": choice.reason,
            }
        )
        return choice.path

    def _resolve_external_reference(self, obj: DiscoveredObject) -> str:
        """Resolve disposition for an external out-of-scope reference using `scope_boundary_rungs`."""
        is_registered_in_target = obj.object_id in self.target_catalogue.get("objects", {})
        target_allows_preexisting = bool(
            self.target_catalogue.get("environment", {}).get("allow_preexisting_contract", False)
        )
        is_hard = obj.status == ObjectStatus.REQUIRED or obj.metadata.get("hard", True)

        facts = {
            "target_allows_preexisting_contract": target_allows_preexisting,
            "external_object_registered_in_target": is_registered_in_target,
            "boundary_strict_enforced": True,
            "external_dependency_is_optional": not is_hard,
            "fail_closed_on_unauthorized_scope": is_hard and not (target_allows_preexisting and is_registered_in_target),
        }
        choice = climb_scope_boundary(f"external reference {obj.object_id}", facts)
        self.decisions_log.append(
            {
                "ladder": "scope_boundary_rungs",
                "object_id": obj.object_id,
                "choice": choice.path,
                "reason": choice.reason,
            }
        )
        return choice.path

    def _resolve_unreadable_object(self, obj: DiscoveredObject) -> str:
        """Resolve disposition for an unreadable object using `unreadable_object_rungs`."""
        schema_grant = bool(
            self.source_catalogue.get("schemas", {}).get(obj.schema, {}).get("readable", False)
        )
        facts = {
            "enforce_honest_completeness": True,
            "has_unreadable_objects": True,
            "schema_read_grant_available": schema_grant,
            "probe_budget_remaining": self.probe_budget.remaining_queries(),
            "strict_security_failure_mode": not schema_grant,
        }
        choice = climb_unreadable_probe(f"unreadable object {obj.object_id}", facts)
        self.decisions_log.append(
            {
                "ladder": "unreadable_object_rungs",
                "object_id": obj.object_id,
                "choice": choice.path,
                "reason": choice.reason,
            }
        )
        return choice.path

    def _resolve_target_conflict(self, conflict: TargetConflict) -> str:
        """Resolve disposition for a target conflict using `target_conflict_rungs`."""
        target_obj = self.target_catalogue.get("objects", {}).get(conflict.object_id, {})
        target_exists = bool(target_obj)
        is_compatible = target_obj.get("compatible", False)
        shadow_supported = bool(
            self.target_catalogue.get("environment", {}).get("shadow_schema_supported", False)
        )
        tolerates_shadow = conflict.conflict_type in ("name_collision", "column_definition_mismatch")

        facts = {
            "target_object_exists": target_exists,
            "structural_and_encoding_compatible": is_compatible,
            "shadow_schema_supported": shadow_supported,
            "conflict_tolerates_shadowing": tolerates_shadow,
            "abort_on_incompatible_collision": not (
                (target_exists and is_compatible) or (shadow_supported and tolerates_shadow)
            ),
        }
        choice = climb_target_conflict(f"target conflict {conflict.conflict_id}", facts)
        self.decisions_log.append(
            {
                "ladder": "target_conflict_rungs",
                "conflict_id": conflict.conflict_id,
                "choice": choice.path,
                "reason": choice.reason,
            }
        )
        return choice.path

    def _evaluate_final_closure(
        self,
        cycles: List[List[str]],
        cycle_dispositions: Mapping[str, str],
        external_refs: Sequence[str],
        external_dispositions: Mapping[str, str],
        unreadable_refs: Sequence[str],
    ) -> decide.Choice:
        """Evaluate overall discovery closure using `closure_evaluation_rungs`."""
        has_unreadable = len(unreadable_refs) > 0
        has_unresolved_cycles = any(
            d == "halt_unresolvable_cycle_for_operator" for d in cycle_dispositions.values()
        )
        has_unauthorized_external = any(
            d == "block_unauthorized_scope_leak" for d in external_dispositions.values()
        )
        has_unresolved_conflicts = any(
            tc.disposition == "block_target_collision_action" for tc in self.target_conflicts
        )
        missing_required = any(
            obj.status == ObjectStatus.ABSENT for obj in self.objects.values()
        )

        all_req_present = not missing_required
        zero_unreadable = not has_unreadable
        all_cycles_disposed = not has_unresolved_cycles
        zero_unauthorized_ext = not has_unauthorized_external
        conflicts_resolved = not has_unresolved_conflicts

        has_blocking = (
            has_unreadable
            or has_unresolved_cycles
            or has_unauthorized_external
            or has_unresolved_conflicts
            or missing_required
        )

        facts = {
            "all_required_objects_present": all_req_present,
            "zero_unreadable_objects": zero_unreadable,
            "all_cycles_disposed": all_cycles_disposed,
            "zero_unauthorized_external_refs": zero_unauthorized_ext,
            "target_conflicts_resolved": conflicts_resolved,
            "optional_gaps_annotated": True,
            "cycles_and_external_disposed": all_cycles_disposed and zero_unauthorized_ext,
            "closure_has_blocking_defects": has_blocking,
        }
        choice = climb_closure_evaluation("overall discovery closure completeness", facts)
        self.decisions_log.append(
            {
                "ladder": "closure_evaluation_rungs",
                "choice": choice.path,
                "reason": choice.reason,
            }
        )
        return choice


# =============================================================================
# 6. Public Interface & Survey Extension
# =============================================================================


def discover_closure(
    authorized_tables: Iterable[str],
    authorized_schemas: Optional[Iterable[str]] = None,
    probe_budget: Optional[MetadataProbeBudget] = None,
    source_catalogue: Optional[Mapping[str, Any]] = None,
    target_catalogue: Optional[Mapping[str, Any]] = None,
) -> DiscoveryClosureResult:
    """Discover transitive dependency closure within authorized scope."""
    engine = DiscoveryClosureEngine(
        authorized_tables=authorized_tables,
        authorized_schemas=authorized_schemas,
        probe_budget=probe_budget,
        source_catalogue=source_catalogue,
        target_catalogue=target_catalogue,
    )
    return engine.discover()


def extend_survey_with_closure(
    survey_data: Dict[str, Any],
    authorized_tables: Optional[Iterable[str]] = None,
    target_catalogue: Optional[Mapping[str, Any]] = None,
    probe_budget: Optional[MetadataProbeBudget] = None,
) -> Dict[str, Any]:
    """Extend output of guide.survey with discovery closure verification."""
    tables = (
        list(authorized_tables)
        if authorized_tables is not None
        else list(survey_data.get("tables", []))
    )

    src_side = survey_data.get("sides", {}).get("source", {})
    src_facts = src_side.get("facts", {})

    # Build source catalogue representation from survey facts
    source_cat_objects: Dict[str, Any] = {}
    for t_name, t_fact in src_facts.items():
        if isinstance(t_fact, dict):
            source_cat_objects[t_name] = {
                "exists": True,
                "columns": t_fact.get("columns", {}),
                "dependencies": t_fact.get("dependencies", []),
                "unreadable": t_fact.get("unreadable", False),
            }

    source_catalogue = {
        "objects": source_cat_objects,
        "environment": {
            "encoding": src_side.get("encoding", "UTF8"),
            "collation": src_side.get("collation", "en_US.UTF-8"),
            "timezone": src_side.get("timezone", "UTC"),
        },
    }

    closure_result = discover_closure(
        authorized_tables=tables,
        source_catalogue=source_catalogue,
        target_catalogue=target_catalogue or {},
        probe_budget=probe_budget,
    )

    out = dict(survey_data)
    out["discovery_closure"] = closure_result.to_dict()
    out["closure_complete"] = closure_result.is_complete
    out["closure_care_badge"] = closure_result.care_badge

    # Update survey care notices if closure raised ACTION or REVIEW
    care_list = list(out.get("care", []))
    if closure_result.care_badge in (CARE_ACTION, CARE_REVIEW):
        care_list.append(
            f"Discovery closure: [{closure_result.care_badge}] "
            f"cycles={len(closure_result.cycles)}, "
            f"unreadable={len(closure_result.unreadable_objects)}, "
            f"external_refs={len(closure_result.external_references)}"
        )
    out["care"] = care_list

    return out
