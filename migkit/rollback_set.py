import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from migkit import decide


class CyclicDependencyError(Exception):
    pass


class EffectNotFoundError(Exception):
    pass


class RollbackResolutionError(Exception):
    pass


class PostconditionStatus(str, Enum):
    SATISFIED = "SATISFIED"
    CONCURRENT_OVERWRITE = "CONCURRENT_OVERWRITE"
    MODIFIED_BY_DEPENDENT = "MODIFIED_BY_DEPENDENT"
    ALREADY_ROLLED_BACK = "ALREADY_ROLLED_BACK"
    UNCHECKED = "UNCHECKED"


class CompensatingActionType(str, Enum):
    REVERSE_DDL = "REVERSE_DDL"
    REVERSE_DML = "REVERSE_DML"
    FORWARD_COMPENSATION = "FORWARD_COMPENSATION"
    NO_OP = "NO_OP"


class ForwardSafetyStatus(str, Enum):
    SAFE = "SAFE"
    UNSAFE_DESTRUCTIVE = "UNSAFE_DESTRUCTIVE"
    DANGLING_DEPENDENCY = "DANGLING_DEPENDENCY"
    CONCURRENT_CONFLICT = "CONCURRENT_CONFLICT"


_UNSAFE_PATTERNS = [
    re.compile(r"\bdrop\s+table\b", re.IGNORECASE),
    re.compile(r"\bdrop\s+database\b", re.IGNORECASE),
    re.compile(r"\bdrop\s+schema\b", re.IGNORECASE),
    re.compile(r"\btruncate\b", re.IGNORECASE),
]


def resolve_unsafe_patterns(extra_patterns: Optional[Iterable[Any]] = None) -> List[re.Pattern]:
    """Resolve unsafe pattern matchers dynamically without hardcoded literal limits."""
    patterns = list(_UNSAFE_PATTERNS)
    env_pats = os.environ.get("MIGKIT_EXTRA_UNSAFE_PATTERNS")
    if env_pats:
        for p in env_pats.split(","):
            p_clean = p.strip()
            if p_clean:
                patterns.append(re.compile(p_clean, re.IGNORECASE))
    if extra_patterns:
        for p in extra_patterns:
            if isinstance(p, str):
                patterns.append(re.compile(p, re.IGNORECASE))
            elif isinstance(p, re.Pattern):
                patterns.append(p)
    return patterns


@dataclass(frozen=True)
class Effect:
    effect_id: str
    target_entity: str
    operation_type: str
    owner: str = "migkit"
    tags: Tuple[str, ...] = ()
    depends_on: Tuple[str, ...] = ()
    forward_payload: str = ""
    reverse_payload: str = ""
    preconditions: Tuple[Tuple[str, Any], ...] = ()
    postconditions: Tuple[Tuple[str, Any], ...] = ()
    applied_at: float = field(default_factory=time.time)
    metadata: Tuple[Tuple[str, Any], ...] = ()


@dataclass(frozen=True)
class PostconditionCheckResult:
    effect_id: str
    status: PostconditionStatus
    passed: bool
    expected: Any = None
    actual: Any = None
    details: str = ""


@dataclass(frozen=True)
class CompensatingOperation:
    operation_id: str
    effect_id: str
    target_entity: str
    action_type: CompensatingActionType
    payload: str
    is_destructive: bool = False
    preserves_concurrent_data: bool = True
    transactional: bool = True


@dataclass(frozen=True)
class ForwardSafetyResult:
    is_safe: bool
    violations: Tuple[str, ...] = ()
    badge: str = "PASS"


@dataclass(frozen=True)
class RollbackSet:
    target_effect_ids: FrozenSet[str]
    cascading_effect_ids: FrozenSet[str]
    independent_effect_ids: FrozenSet[str]
    rollback_order: Tuple[str, ...]
    compensating_operations: Tuple[CompensatingOperation, ...]
    postcondition_results: Tuple[PostconditionCheckResult, ...]
    forward_safety: ForwardSafetyResult
    allowed: bool
    badge: str
    refusal_reasons: Tuple[str, ...] = ()


class EffectDependencyDAG:
    def __init__(self) -> None:
        self._effects: Dict[str, Effect] = {}
        self._dependencies: Dict[str, Set[str]] = {}
        self._dependents: Dict[str, Set[str]] = {}
        self._order_added: List[str] = []

    def add_effect(self, effect: Effect) -> None:
        effect_id = effect.effect_id
        if effect_id in self._effects:
            raise ValueError(f"effect {effect_id} already registered")

        for dep in effect.depends_on:
            if dep == effect_id or self._reaches(dep, effect_id):
                raise CyclicDependencyError(f"cycle detected between {dep} and {effect_id}")

        self._effects[effect_id] = effect
        self._dependencies[effect_id] = set(effect.depends_on)
        if effect_id not in self._dependents:
            self._dependents[effect_id] = set()

        for dep in effect.depends_on:
            if dep not in self._dependents:
                self._dependents[dep] = set()
            self._dependents[dep].add(effect_id)

        self._order_added.append(effect_id)

    def add_dependency(self, dependent_id: str, prerequisite_id: str) -> None:
        if dependent_id not in self._effects:
            raise EffectNotFoundError(f"effect {dependent_id} not found")
        if prerequisite_id not in self._effects:
            raise EffectNotFoundError(f"effect {prerequisite_id} not found")
        if dependent_id == prerequisite_id or self._reaches(prerequisite_id, dependent_id):
            raise CyclicDependencyError(f"cycle detected: {dependent_id} -> {prerequisite_id}")

        self._dependencies[dependent_id].add(prerequisite_id)
        self._dependents[prerequisite_id].add(dependent_id)

    def _reaches(self, src: str, dst: str, visited: Optional[Set[str]] = None) -> bool:
        if src == dst:
            return True
        if visited is None:
            visited = set()
        visited.add(src)
        for parent in self._dependencies.get(src, set()):
            if parent not in visited:
                if self._reaches(parent, dst, visited):
                    return True
        return False

    def get_effect(self, effect_id: str) -> Optional[Effect]:
        return self._effects.get(effect_id)

    def has_effect(self, effect_id: str) -> bool:
        return effect_id in self._effects

    def all_effects(self) -> Tuple[Effect, ...]:
        return tuple(self._effects.values())

    def get_dependencies(self, effect_id: str) -> FrozenSet[str]:
        if effect_id not in self._effects:
            raise EffectNotFoundError(f"effect {effect_id} not found")
        return frozenset(self._dependencies.get(effect_id, set()))

    def get_dependents(self, effect_id: str) -> FrozenSet[str]:
        if effect_id not in self._effects:
            raise EffectNotFoundError(f"effect {effect_id} not found")
        return frozenset(self._dependents.get(effect_id, set()))

    def get_transitive_dependents(self, effect_id: str) -> FrozenSet[str]:
        if effect_id not in self._effects:
            raise EffectNotFoundError(f"effect {effect_id} not found")
        visited: Set[str] = set()
        queue = list(self._dependents.get(effect_id, set()))
        while queue:
            curr = queue.pop(0)
            if curr not in visited:
                visited.add(curr)
                queue.extend(self._dependents.get(curr, set()))
        return frozenset(visited)

    def get_transitive_prerequisites(self, effect_id: str) -> FrozenSet[str]:
        if effect_id not in self._effects:
            raise EffectNotFoundError(f"effect {effect_id} not found")
        visited: Set[str] = set()
        queue = list(self._dependencies.get(effect_id, set()))
        while queue:
            curr = queue.pop(0)
            if curr not in visited:
                visited.add(curr)
                queue.extend(self._dependencies.get(curr, set()))
        return frozenset(visited)

    def is_independent(self, effect_a: str, effect_b: str) -> bool:
        if effect_a not in self._effects or effect_b not in self._effects:
            raise EffectNotFoundError("effect not found")
        if effect_a == effect_b:
            return False
        if effect_b in self.get_transitive_dependents(effect_a):
            return False
        if effect_a in self.get_transitive_dependents(effect_b):
            return False
        return True

    def topological_sort(self, effect_ids: Optional[Iterable[str]] = None) -> Tuple[str, ...]:
        sub_nodes = set(effect_ids) if effect_ids is not None else set(self._effects.keys())
        in_degree = {n: 0 for n in sub_nodes}
        for n in sub_nodes:
            deps = self._dependencies.get(n, set()) & sub_nodes
            in_degree[n] = len(deps)

        queue = [n for n in self._order_added if n in sub_nodes and in_degree[n] == 0]
        result: List[str] = []

        while queue:
            curr = queue.pop(0)
            result.append(curr)
            for child in self._dependents.get(curr, set()):
                if child in sub_nodes:
                    in_degree[child] -= 1
                    if in_degree[child] == 0:
                        queue.append(child)

        if len(result) != len(sub_nodes):
            raise CyclicDependencyError("cycle detected during topological sort")
        return tuple(result)

    def reverse_topological_sort(self, effect_ids: Optional[Iterable[str]] = None) -> Tuple[str, ...]:
        topo = self.topological_sort(effect_ids)
        return tuple(reversed(topo))

    def effects_by_tag(self, tag: str) -> FrozenSet[str]:
        matched = {eid for eid, eff in self._effects.items() if tag in eff.tags}
        return frozenset(matched)

    def effects_before_tag(self, tag: str) -> FrozenSet[str]:
        tag_effects = self.effects_by_tag(tag)
        if not tag_effects:
            return frozenset()
        tag_indices = [self._order_added.index(eid) for eid in tag_effects if eid in self._order_added]
        min_idx = min(tag_indices) if tag_indices else 0
        return frozenset(self._order_added[:min_idx])

    def effects_after_tag(self, tag: str) -> FrozenSet[str]:
        tag_effects = self.effects_by_tag(tag)
        if not tag_effects:
            return frozenset()
        tag_indices = [self._order_added.index(eid) for eid in tag_effects if eid in self._order_added]
        max_idx = max(tag_indices) if tag_indices else len(self._order_added)
        return frozenset(self._order_added[max_idx + 1:])


def generate_compensating_operation(effect: Effect, dialect: str = "postgres") -> CompensatingOperation:
    rev = effect.reverse_payload.strip() if effect.reverse_payload else ""
    if not rev:
        return CompensatingOperation(
            operation_id=f"comp_{effect.effect_id}",
            effect_id=effect.effect_id,
            target_entity=effect.target_entity,
            action_type=CompensatingActionType.NO_OP,
            payload="",
            is_destructive=False,
            preserves_concurrent_data=True,
            transactional=True,
        )

    is_destructive = any(bool(pat.search(rev)) for pat in resolve_unsafe_patterns())
    preserves_data = not is_destructive

    is_ddl = "TABLE" in rev.upper() or "COLUMN" in rev.upper() or "INDEX" in rev.upper()
    action_type = CompensatingActionType.REVERSE_DDL if is_ddl else CompensatingActionType.REVERSE_DML

    return CompensatingOperation(
        operation_id=f"comp_{effect.effect_id}",
        effect_id=effect.effect_id,
        target_entity=effect.target_entity,
        action_type=action_type,
        payload=rev,
        is_destructive=is_destructive,
        preserves_concurrent_data=preserves_data,
        transactional=not is_destructive,
    )


def verify_forward_safety(
    rollback_effects: Sequence[Effect],
    independent_effects: Sequence[Effect],
    compensating_ops: Sequence[CompensatingOperation],
) -> ForwardSafetyResult:
    violations: List[str] = []
    rollback_ids = {e.effect_id for e in rollback_effects}

    for ind in independent_effects:
        for dep in ind.depends_on:
            if dep in rollback_ids:
                violations.append(
                    f"dangling dependency: independent effect '{ind.effect_id}' depends on rolled back effect '{dep}'"
                )

    for op in compensating_ops:
        if op.is_destructive and not op.preserves_concurrent_data:
            violations.append(
                f"destructive operation rejected without explicit forward protection: {op.operation_id} ({op.payload})"
            )

    is_safe = len(violations) == 0
    badge = "PASS" if is_safe else "ACTION"
    return ForwardSafetyResult(is_safe=is_safe, violations=tuple(violations), badge=badge)


class RollbackSetResolver:
    def __init__(self, dag: Optional[EffectDependencyDAG] = None) -> None:
        self.dag = dag if dag is not None else EffectDependencyDAG()

    def resolve_rollback_set(self, target_ids: Iterable[str]) -> RollbackSet:
        return self.resolve_by_effect_ids(target_ids)

    def resolve_by_effect_ids(
        self,
        target_ids: Iterable[str],
        target_state: Optional[Dict[str, Any]] = None,
        require_forward_safety: bool = True,
    ) -> RollbackSet:
        target_set = set(target_ids)
        for tid in target_set:
            if not self.dag.has_effect(tid):
                raise EffectNotFoundError(f"target effect '{tid}' not found in DAG")

        if not target_set:
            return RollbackSet(
                target_effect_ids=frozenset(),
                cascading_effect_ids=frozenset(),
                independent_effect_ids=frozenset(self.dag._effects.keys()),
                rollback_order=(),
                compensating_operations=(),
                postcondition_results=(),
                forward_safety=ForwardSafetyResult(is_safe=True, badge="PASS"),
                allowed=True,
                badge="PASS",
            )

        cascading_set: Set[str] = set()
        for tid in target_set:
            cascading_set.update(self.dag.get_transitive_dependents(tid))
        cascading_set.difference_update(target_set)

        full_rollback_set = target_set | cascading_set
        all_ids = set(self.dag._effects.keys())
        independent_set = all_ids - full_rollback_set

        rollback_order = self.dag.reverse_topological_sort(full_rollback_set)

        postcondition_results: List[PostconditionCheckResult] = []
        refusal_reasons: List[str] = []

        if target_state is not None:
            for eid in rollback_order:
                eff = self.dag.get_effect(eid)
                if not eff:
                    continue
                res = self._check_postconditions(eff, target_state, cascading_set)
                postcondition_results.append(res)
                if not res.passed:
                    refusal_reasons.append(res.details)

        compensating_ops = tuple(
            generate_compensating_operation(self.dag.get_effect(eid))  # type: ignore
            for eid in rollback_order
            if self.dag.get_effect(eid)
        )

        rollback_effects = [self.dag.get_effect(eid) for eid in rollback_order if self.dag.get_effect(eid)]
        independent_effects = [self.dag.get_effect(eid) for eid in independent_set if self.dag.get_effect(eid)]

        safety_report = verify_forward_safety(
            rollback_effects=rollback_effects,  # type: ignore
            independent_effects=independent_effects,  # type: ignore
            compensating_ops=compensating_ops,
        )

        if require_forward_safety and not safety_report.is_safe:
            refusal_reasons.extend(safety_report.violations)

        allowed = len(refusal_reasons) == 0
        badge = "PASS" if allowed else "ACTION"

        return RollbackSet(
            target_effect_ids=frozenset(target_set),
            cascading_effect_ids=frozenset(cascading_set),
            independent_effect_ids=frozenset(independent_set),
            rollback_order=rollback_order,
            compensating_operations=compensating_ops,
            postcondition_results=tuple(postcondition_results),
            forward_safety=safety_report,
            allowed=allowed,
            badge=badge,
            refusal_reasons=tuple(refusal_reasons),
        )

    def resolve_by_tag(
        self,
        tag: str,
        target_state: Optional[Dict[str, Any]] = None,
        require_forward_safety: bool = True,
    ) -> RollbackSet:
        tagged_effects = self.dag.effects_by_tag(tag)
        if not tagged_effects:
            raise EffectNotFoundError(f"no effects found with tag '{tag}'")
        return self.resolve_by_effect_ids(tagged_effects, target_state, require_forward_safety)

    def resolve_surgical_set(
        self,
        target_ids: Iterable[str],
        target_state: Optional[Dict[str, Any]] = None,
    ) -> RollbackSet:
        return self.resolve_by_effect_ids(target_ids, target_state, require_forward_safety=True)

    def _check_postconditions(
        self,
        effect: Effect,
        target_state: Dict[str, Any],
        cascading_set: Set[str],
    ) -> PostconditionCheckResult:
        if not effect.postconditions:
            return PostconditionCheckResult(
                effect_id=effect.effect_id,
                status=PostconditionStatus.SATISFIED,
                passed=True,
                details="no postconditions defined",
            )

        entity_state = target_state.get(effect.target_entity)
        if entity_state is None:
            return PostconditionCheckResult(
                effect_id=effect.effect_id,
                status=PostconditionStatus.ALREADY_ROLLED_BACK,
                passed=True,
                details=f"target entity '{effect.target_entity}' not present in target state",
            )

        cascading_effects = [self.dag.get_effect(cid) for cid in cascading_set if self.dag.get_effect(cid)]

        for key, expected_val in effect.postconditions:
            actual_val = entity_state.get(key) if isinstance(entity_state, dict) else entity_state
            if actual_val != expected_val:
                dependent_modified = any(
                    ce.target_entity == effect.target_entity and any(k == key for k, _ in ce.postconditions)
                    for ce in cascading_effects
                )
                if dependent_modified:
                    return PostconditionCheckResult(
                        effect_id=effect.effect_id,
                        status=PostconditionStatus.MODIFIED_BY_DEPENDENT,
                        passed=True,
                        expected=expected_val,
                        actual=actual_val,
                        details=(
                            f"entity '{effect.target_entity}' attribute '{key}' modified by cascading dependent "
                            "scheduled for prior undo"
                        ),
                    )
                return PostconditionCheckResult(
                    effect_id=effect.effect_id,
                    status=PostconditionStatus.CONCURRENT_OVERWRITE,
                    passed=False,
                    expected=expected_val,
                    actual=actual_val,
                    details=(
                        f"accidental rollback of concurrent unrelated writes prevented: "
                        f"entity '{effect.target_entity}' attribute '{key}' expected {expected_val!r}, "
                        f"found {actual_val!r}"
                    ),
                )

        return PostconditionCheckResult(
            effect_id=effect.effect_id,
            status=PostconditionStatus.SATISFIED,
            passed=True,
            expected=dict(effect.postconditions),
            actual=entity_state,
            details="postconditions verified against target state",
        )


# Decide ladders for Rollback Set Resolution and Unsafe Pattern Detection

def _prove_independent_effect_isolated_rollback(facts: decide.Facts) -> bool:
    return bool(not facts.get("has_dependents") and facts.get("is_independent") and not facts.get("refused"))


def _prove_cascading_dependent_rollback(facts: decide.Facts) -> bool:
    return bool(facts.get("has_dependents") and facts.get("cascading_resolved") and not facts.get("refused"))


def _prove_atomic_all_or_nothing_rollback(facts: decide.Facts) -> bool:
    return bool(facts.get("require_atomic") and facts.get("atomic_supported") and not facts.get("refused"))


def _prove_compensating_forward_execution(facts: decide.Facts) -> bool:
    return bool(facts.get("forward_compensation") and not facts.get("refused"))


def _prove_refuse_unresolved_cyclic_rollback(facts: decide.Facts) -> bool:
    return not bool(facts.get("allow_cyclic_or_unsafe"))


def rollback_set_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "independent_effect_isolated_rollback",
            "surgical rollback of an isolated effect with zero downstream dependents",
            needs=(
                decide.Need(
                    "independent effect isolated",
                    _prove_independent_effect_isolated_rollback,
                    "effect has unresolved dependents or is not independent",
                ),
            ),
        ),
        decide.Rung(
            "cascading_dependent_rollback",
            "topologically ordered rollback including all transitive dependents",
            needs=(
                decide.Need(
                    "cascading dependents resolved",
                    _prove_cascading_dependent_rollback,
                    "dependents missing or cascading resolution incomplete",
                ),
            ),
        ),
        decide.Rung(
            "atomic_all_or_nothing_rollback",
            "atomic transactional rollback executing all operations in single unit",
            needs=(
                decide.Need(
                    "atomic rollback supported",
                    _prove_atomic_all_or_nothing_rollback,
                    "atomic rollback not requested or unsupported",
                ),
            ),
        ),
        decide.Rung(
            "compensating_forward_execution",
            "forward compensating operation preserving concurrent modifications",
            needs=(
                decide.Need(
                    "forward compensation available",
                    _prove_compensating_forward_execution,
                    "no forward compensation path available",
                ),
            ),
        ),
        decide.Rung(
            "refuse_unresolved_cyclic_rollback",
            "refuse rollback resolution when cyclic dependencies or unsafe mutations are detected",
            needs=(
                decide.Need(
                    "cyclic or unsafe rollback refused",
                    _prove_refuse_unresolved_cyclic_rollback,
                    "unresolved cyclic dependencies or unsafe mutations present",
                ),
            ),
        ),
    )


def decide_rollback_set(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("rollback set resolution", rollback_set_rungs(), fact_obj)


def _prove_drop_table_pattern(facts: decide.Facts) -> bool:
    stmt = str(facts.get("statement") or "").lower()
    return bool("drop table" in stmt and not facts.get("refused"))


def _prove_drop_database_schema_pattern(facts: decide.Facts) -> bool:
    stmt = str(facts.get("statement") or "").lower()
    return bool(("drop database" in stmt or "drop schema" in stmt) and not facts.get("refused"))


def _prove_truncate_pattern(facts: decide.Facts) -> bool:
    stmt = str(facts.get("statement") or "").lower()
    return bool("truncate" in stmt and not facts.get("refused"))


def _prove_destructive_alter_pattern(facts: decide.Facts) -> bool:
    stmt = str(facts.get("statement") or "").lower()
    return bool(("drop column" in stmt or facts.get("destructive_alter")) and not facts.get("refused"))


def _prove_safe_non_destructive_pattern(facts: decide.Facts) -> bool:
    return not bool(facts.get("is_unsafe"))


def unsafe_pattern_rungs() -> Tuple[decide.Rung, ...]:
    return (
        decide.Rung(
            "drop_table_pattern",
            "identify and classify DROP TABLE destructive mutations",
            needs=(
                decide.Need(
                    "drop table mutation detected",
                    _prove_drop_table_pattern,
                    "statement is not a drop table mutation",
                ),
            ),
        ),
        decide.Rung(
            "drop_database_schema_pattern",
            "identify and classify DROP DATABASE or SCHEMA destructive mutations",
            needs=(
                decide.Need(
                    "drop database or schema mutation detected",
                    _prove_drop_database_schema_pattern,
                    "statement is not a drop database or schema mutation",
                ),
            ),
        ),
        decide.Rung(
            "truncate_pattern",
            "identify and classify TRUNCATE destructive mutations",
            needs=(
                decide.Need(
                    "truncate mutation detected",
                    _prove_truncate_pattern,
                    "statement is not a truncate mutation",
                ),
            ),
        ),
        decide.Rung(
            "destructive_alter_pattern",
            "identify and classify DROP COLUMN or narrowing ALTER mutations",
            needs=(
                decide.Need(
                    "destructive alter mutation detected",
                    _prove_destructive_alter_pattern,
                    "statement is not a destructive alter mutation",
                ),
            ),
        ),
        decide.Rung(
            "safe_non_destructive_pattern",
            "classify non-destructive or forward-safe mutation",
            needs=(
                decide.Need(
                    "mutation is non-destructive",
                    _prove_safe_non_destructive_pattern,
                    "mutation is classified as unsafe",
                ),
            ),
        ),
    )


def decide_unsafe_pattern(facts: Dict[str, Any]) -> decide.Choice:
    fact_obj = facts if isinstance(facts, decide.Facts) else decide.Facts(dict(facts))
    return decide.climb("unsafe pattern detection", unsafe_pattern_rungs(), fact_obj)
