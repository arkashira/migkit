"""Gate superiority claims on reproducible complete migrations (backlog
RCH40, row 648).

A feature count is not a migration. A claim that migkit is better than a
provider-native tool or another product is admitted only from complete,
verified runs of one named path, with the baseline run under the same proof,
scope, load and durability contract, enough repeats to name a p95, and no
loss or duplication. Evidence is labelled by where it came from (`real`,
`lane`, `simulated`, `fake`, `managed`) and only `real` evidence supports a
claim; the rest is reported beside it, never folded in. A claim over all
engines, all products or "fastest" is refused outright, whatever the runs.

The paths that must be run whole are `REQUIRED_PATHS` and a compound
failure with a restart; a path with no real run is an owned gap, said as
such, and no wider pair claim stands on it.

Care badges: ACTION refuses a claim, REVIEW is an owned gap, BENIGN is
evidence kept apart from the claim, PASS admits.
"""
from __future__ import annotations

import dataclasses
import math
from typing import Dict, Iterable, List, Optional, Tuple

ACTION, REVIEW, BENIGN, PASS = "ACTION", "REVIEW", "BENIGN", "PASS"

REAL, LANE, SIMULATED, FAKE, MANAGED = ("real", "lane", "simulated", "fake",
                                        "managed")
EVIDENCE_KINDS = (REAL, LANE, SIMULATED, FAKE, MANAGED)

REQUIRED_PATHS = (("postgres", "postgres"), ("mysql", "mysql"),
                  ("postgres", "mysql"), ("mysql", "postgres"))
COMPOUND = "compound-failure-restart"
MIN_RUNS = 5
UNIVERSAL = ("all-engines", "all-products", "fastest")


@dataclasses.dataclass(frozen=True)
class Contract:
    """What the run was held to; a comparison needs these equal."""
    proof: str
    scope: str
    load: str
    durability: str


@dataclasses.dataclass(frozen=True)
class Run:
    tool: str
    path: Tuple[str, str]
    evidence: str
    contract: Contract
    verified_completion_s: float
    source_reads: int = 0
    downtime_s: float = 0.0
    reverse_ready: bool = False
    bytes_moved: int = 0
    cost: float = 0.0
    lost_rows: int = 0
    duplicated_rows: int = 0
    setup_steps: int = 0
    manual_steps: int = 0
    forecast_error: Optional[float] = None
    scenario: str = "whole"
    verified: bool = True

    def __post_init__(self):
        if self.evidence not in EVIDENCE_KINDS:
            raise ValueError(f"unknown evidence kind {self.evidence!r}")


def percentile(values: Iterable[float], q: float) -> float:
    """Nearest-rank percentile; an empty set has none."""
    v = sorted(values)
    if not v:
        raise ValueError("no values")
    return v[max(0, math.ceil(q * len(v)) - 1)]


@dataclasses.dataclass(frozen=True)
class Verdict:
    claim: str
    admitted: bool
    care_badge: str
    reasons: Tuple[str, ...]

    def line(self):
        head = "[PASS]" if self.admitted else f"[{self.care_badge}]"
        return f"{head} claim={self.claim} " + "; ".join(self.reasons)


def _usable(runs: Iterable[Run], tool: str, path) -> List[Run]:
    return [r for r in runs if r.tool == tool and r.path == path
            and r.evidence == REAL and r.scenario == "whole" and r.verified]


def evaluate_faster_claim(runs: Iterable[Run], tool: str, baseline: str,
                          path: Tuple[str, str],
                          min_runs: int = MIN_RUNS) -> Verdict:
    """`tool` is faster than `baseline` on `path`: admitted only from real,
    verified, equal-contract runs whose p95 is lower and that lost or
    duplicated nothing."""
    runs = list(runs)
    claim = f"{tool}-faster-than-{baseline}:{path[0]}->{path[1]}"
    mine, theirs = _usable(runs, tool, path), _usable(runs, baseline, path)
    why = []
    if len(mine) < min_runs:
        why.append(f"{len(mine)} real verified runs of {tool}, need"
                   f" {min_runs}")
    if len(theirs) < min_runs:
        why.append(f"{len(theirs)} real verified runs of {baseline}, need"
                   f" {min_runs}")
    if why:
        return Verdict(claim, False, REVIEW, tuple(why))
    contracts = {r.contract for r in mine + theirs}
    if len(contracts) != 1:
        return Verdict(claim, False, ACTION,
                       ("runs were not held to one proof, scope, load and"
                        " durability contract",))
    bad = [r for r in mine + theirs if r.lost_rows or r.duplicated_rows]
    if bad:
        return Verdict(claim, False, ACTION,
                       (f"{len(bad)} runs lost or duplicated rows",))
    p95_mine = percentile((r.verified_completion_s for r in mine), 0.95)
    p95_theirs = percentile((r.verified_completion_s for r in theirs), 0.95)
    if not p95_mine < p95_theirs:
        return Verdict(claim, False, ACTION,
                       (f"p95 {p95_mine:.1f}s is not below baseline"
                        f" {p95_theirs:.1f}s",))
    return Verdict(claim, True, PASS,
                   (f"p95 {p95_mine:.1f}s below {p95_theirs:.1f}s over"
                    f" {len(mine)} and {len(theirs)} real runs",))


def evaluate_universal_claim(kind: str) -> Verdict:
    """Never admitted: a count of features or engines is not a run."""
    if kind not in UNIVERSAL:
        raise ValueError(f"not a universal claim: {kind}")
    return Verdict(kind, False, ACTION,
                   ("a universal claim cannot stand on a feature count or"
                    " on tested pairs; name a path",))


@dataclasses.dataclass(frozen=True)
class Coverage:
    covered: Tuple[str, ...]
    gaps: Tuple[str, ...]
    kept_apart: Dict[str, int]

    @property
    def complete(self):
        return not self.gaps

    def lines(self):
        out = [f"[PASS] covered {c}" for c in self.covered]
        out += [f"[REVIEW] owned gap {g}: no real verified whole run"
                for g in self.gaps]
        out += [f"[BENIGN] {n} {k} runs kept apart from every claim"
                for k, n in sorted(self.kept_apart.items())]
        return out


def coverage(runs: Iterable[Run], tool: str = "migkit") -> Coverage:
    """Which required whole paths and the compound failure have a real run
    of `tool`, and how much evidence of other kinds sits beside them."""
    runs = list(runs)
    covered, gaps = [], []
    for path in REQUIRED_PATHS:
        label = f"{path[0]}->{path[1]}"
        (covered if _usable(runs, tool, path) else gaps).append(label)
    compound = [r for r in runs if r.tool == tool and r.evidence == REAL
                and r.scenario == COMPOUND and r.verified]
    (covered if compound else gaps).append(COMPOUND)
    apart = {}
    for r in runs:
        if r.evidence != REAL:
            apart[r.evidence] = apart.get(r.evidence, 0) + 1
    return Coverage(tuple(covered), tuple(gaps), apart)


def pair_claim(runs: Iterable[Run], pair: Tuple[str, str],
               tool: str = "migkit") -> Verdict:
    """A claim that `pair` works whole: admitted only for a required path
    with a real run; any other pair stays an owned gap."""
    label = f"{pair[0]}->{pair[1]}"
    if pair not in REQUIRED_PATHS:
        return Verdict(label, False, REVIEW,
                       ("not a required whole path; owned gap until run",))
    if not _usable(runs, tool, pair):
        return Verdict(label, False, REVIEW,
                       ("no real verified whole run",))
    return Verdict(label, True, PASS, ("real verified whole run",))
