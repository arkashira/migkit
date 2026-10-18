"""Test the verifier against its own blind spots (backlog RCH39, row 647).

A verifier that says "same" for every target it is shown proves nothing.
Here a known-good source world and target world are changed on purpose -
a scope filter removed, a keyless row duplicated, a canonical form altered,
statistics reset, a table truncated, grants omitted - and each verifier is
asked to compare the changed pair. A verifier claims coverage classes; a
planted defect belongs to one class. The score is kept per (engine,
coverage class), and a surviving mutant is explicit proof debt, never a
passed test. A claimed class whose defect came back "same" is a false-same
and blocks the release.

The oracle that says a defect is real shares no code with the verifiers: it
compares multisets of `repr` text, the grants and the statistics directly.
A planted change the oracle cannot see is an equivalent mutant and is left
out of the score instead of being counted as caught or missed.

Verdict words are the verifier's own: "same" or "different". Care badges:
ACTION blocks, REVIEW is owned debt, BENIGN is a defect outside every claim
that was still caught, PASS is clean.
"""
from __future__ import annotations

import collections
import dataclasses
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from . import decide

ACTION, REVIEW, BENIGN, PASS = "ACTION", "REVIEW", "BENIGN", "PASS"

SCOPE, MULTIPLICITY, CONTENT, COMPLETENESS, PRIVILEGES, STATISTICS = (
    "scope", "multiplicity", "content", "completeness", "privileges",
    "statistics")
CLASSES = (SCOPE, MULTIPLICITY, CONTENT, COMPLETENESS, PRIVILEGES,
           STATISTICS)


@dataclasses.dataclass(frozen=True)
class World:
    """One side as a verifier may read it."""
    rows: Tuple[tuple, ...]
    grants: frozenset = frozenset()
    statistics: Tuple[Tuple[str, int], ...] = ()
    in_scope: Optional[Callable[[tuple], bool]] = None

    def scoped_rows(self):
        if self.in_scope is None:
            return self.rows
        return tuple(r for r in self.rows if self.in_scope(r))


@dataclasses.dataclass(frozen=True)
class Defect:
    name: str
    coverage_class: str
    apply: Callable[[World], World]


def remove_scope_filter(world, outside=(("out-of-scope", 0),)):
    return dataclasses.replace(world, rows=world.rows + tuple(outside),
                               in_scope=None)


def duplicate_keyless_row(world):
    return dataclasses.replace(world, rows=world.rows + world.rows[:1])


def alter_canonicalization(world):
    """The first text value gains a trailing space: equal after a lossy
    canonical form, different as the source holds it."""
    if not world.rows:
        return world
    first = tuple(v + " " if isinstance(v, str) else v
                  for v in world.rows[0])
    return dataclasses.replace(world, rows=(first,) + world.rows[1:])


def reset_statistics(world):
    return dataclasses.replace(
        world, statistics=tuple((k, 0) for k, _ in world.statistics))


def truncate_table(world):
    return dataclasses.replace(world, rows=())


def omit_grants(world):
    return dataclasses.replace(world, grants=frozenset())


DEFECTS = (
    Defect("remove-scope-filter", SCOPE, remove_scope_filter),
    Defect("duplicate-keyless-row", MULTIPLICITY, duplicate_keyless_row),
    Defect("alter-canonicalization", CONTENT, alter_canonicalization),
    Defect("truncate-table", COMPLETENESS, truncate_table),
    Defect("omit-grants", PRIVILEGES, omit_grants),
    Defect("reset-statistics", STATISTICS, reset_statistics),
)


def independent_differs(source: World, target: World) -> bool:
    """The oracle: no verifier code, no canonical form of the tool."""
    return (collections.Counter(map(repr, source.scoped_rows()))
            != collections.Counter(map(repr, target.scoped_rows()))
            or source.grants != target.grants
            or dict(source.statistics) != dict(target.statistics))


@dataclasses.dataclass(frozen=True)
class Verifier:
    name: str
    covers: frozenset
    compare: Callable[[World, World], str]


@dataclasses.dataclass(frozen=True)
class Outcome:
    verifier: str
    engine: str
    defect: str
    coverage_class: str
    claimed: bool
    equivalent: bool
    verdict: str

    @property
    def caught(self):
        return self.verdict != "same"

    @property
    def false_same(self):
        return self.claimed and not self.equivalent and not self.caught

    @property
    def care_badge(self):
        if self.equivalent:
            return PASS
        if self.false_same:
            return ACTION
        if self.caught:
            return PASS if self.claimed else BENIGN
        return REVIEW


@dataclasses.dataclass
class Report:
    outcomes: List[Outcome]

    def score(self) -> Dict[Tuple[str, str, str], float]:
        """{(verifier, engine, class): share of real defects caught}."""
        seen = collections.defaultdict(lambda: [0, 0])
        for o in self.outcomes:
            if o.equivalent:
                continue
            cell = seen[(o.verifier, o.engine, o.coverage_class)]
            cell[1] += 1
            cell[0] += o.caught
        return {k: c / n for k, (c, n) in seen.items()}

    def false_same(self) -> List[Outcome]:
        return [o for o in self.outcomes if o.false_same]

    def proof_debt(self) -> List[Outcome]:
        """Defects no verifier claimed and the verifier missed."""
        return [o for o in self.outcomes
                if not o.equivalent and not o.claimed and not o.caught]

    def release_blocked(self) -> bool:
        return bool(self.false_same())

    def lines(self) -> List[str]:
        out = []
        for (v, e, c), s in sorted(self.score().items()):
            out.append(f"MUTATION {v} engine={e} class={c} score={s:.2f}")
        for o in self.false_same():
            out.append(f"[ACTION] false-same {o.verifier} engine={o.engine}"
                       f" defect={o.defect} class={o.coverage_class}")
        for o in self.proof_debt():
            out.append(f"[REVIEW] proof debt {o.verifier} engine={o.engine}"
                       f" defect={o.defect} class={o.coverage_class}")
        return out


def run_suite(verifiers: Iterable[Verifier], engine: str,
              world: World, defects: Iterable[Defect] = DEFECTS) -> Report:
    """Plant each defect in a copy of `world` (the target), compare it with
    the untouched `world` (the source), and record what each verifier says.
    A verifier that already calls the unchanged pair different is refused:
    its other answers say nothing."""
    outcomes = []
    for v in verifiers:
        if v.compare(world, world) != "same":
            raise ValueError(f"{v.name} calls identical worlds different")
        for d in defects:
            mutated = d.apply(world)
            outcomes.append(Outcome(
                verifier=v.name, engine=engine, defect=d.name,
                coverage_class=d.coverage_class,
                claimed=d.coverage_class in v.covers,
                equivalent=not independent_differs(world, mutated),
                verdict=v.compare(world, mutated)))
    return Report(outcomes)


def release_gate(report: Report) -> Tuple[str, List[str]]:
    """(care badge, lines): ACTION while any claimed class false-sames."""
    lines = report.lines()
    if report.release_blocked():
        return ACTION, lines
    if report.proof_debt():
        return REVIEW, lines
    return PASS, lines


def anti_corruption_mutation_rungs() -> Tuple[decide.Rung, ...]:
    """Decision ladder for verifier mutation blind spots and proof debt certification."""
    return (
        decide.Rung(
            "certified_mutation_pass",
            "verifier caught all claimed planted defects with zero false-sames",
            needs=(
                decide.Need(
                    "zero false-sames across all claimed defect classes",
                    lambda f: not bool(f.get("has_false_same", False)),
                    "a verifier returned false-same on a claimed defect class",
                ),
                decide.Need(
                    "zero unclaimed proof debt",
                    lambda f: not bool(f.get("has_proof_debt", False)),
                    "verifier has uninspected proof debt on unclaimed defect classes",
                ),
            ),
            footprint=0,
            leaves="nothing",
        ),
        decide.Rung(
            "proof_debt_review",
            "verifier caught claimed classes but leaves explicit proof debt",
            needs=(
                decide.Need(
                    "zero false-sames across all claimed defect classes",
                    lambda f: not bool(f.get("has_false_same", False)),
                    "a verifier returned false-same on a claimed defect class",
                ),
            ),
            footprint=1,
            leaves="recorded proof debt in the verification report",
        ),
        decide.Rung(
            "false_same_refusal",
            "refuse verifier certification due to false-same on a planted defect",
            footprint=2,
            leaves="blocked release gate",
        ),
    )


def decide_mutation_gate(report: Report) -> decide.Choice:
    """Climb the anti_corruption_mutation_rungs ladder on a mutation report."""
    facts = decide.Facts({
        "has_false_same": report.release_blocked(),
        "has_proof_debt": bool(report.proof_debt()),
    })
    return decide.climb("verifier mutation certification", anti_corruption_mutation_rungs(), facts)

