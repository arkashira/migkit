"""Diagnose and name the rung each side and leg stands on, along with its blockers (backlog P0-DE.2)."""
import dataclasses
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from . import legs, marks, movers, waysaid
from .config import Hop
from .decide import Facts, climb, prune


@dataclass(frozen=True)
class Diagnosis:
    name: str
    target_type: str
    rung: Optional[str]
    said: str
    holds: bool
    footprint: int = 0
    leaves: str = ""
    blockers: Tuple[str, ...] = ()
    reason: str = ""


def diagnose_legs(hop: Hop, go: bool = False) -> List[Diagnosis]:
    facts = Facts(
        {"engine": hop.engine, "hop": hop, "go": go,
         "db": legs._first_db(hop) if hop is not None else None},
        legs=lambda: legs.facts(hop),
        rtt=lambda: legs.round_trips(hop),
    )
    rungs = legs.rungs()
    results = []
    choice = climb(hop.name, rungs, facts=facts, prove=go)
    for r in rungs:
        kept, out = prune([r], facts, ())
        blockers = tuple(why for _, why in out)
        is_chosen = choice.rung is not None and choice.rung.name == r.name
        results.append(
            Diagnosis(
                name=r.name,
                target_type="leg",
                rung=r.name,
                said=r.said,
                holds=len(kept) > 0 and len(blockers) == 0,
                footprint=r.footprint,
                leaves=r.leaves or waysaid._leaves(r),
                blockers=blockers,
                reason=choice.reason if is_chosen else (blockers[0] if blockers else r.because),
            )
        )
    return results


def diagnose_sides(hop: Hop, db: str = "default") -> Dict[str, List[Diagnosis]]:
    diagnoses = {"source": [], "target": []}
    for side_name in ("source", "target"):
        side_engine = (hop.options or {}).get(f"{side_name}_engine", hop.engine)
        facts = {"family": side_engine, "db": db, "exact": True}

        class _ProbeSide:
            def __init__(self, fam, f):
                self.CANON_ENGINE = fam
                self.facts = dict(f)

            def mark_facts(self, s, d):
                return dict(self.facts)

            def mark_prove(self, s, d, r):
                return None

        probe = _ProbeSide(side_engine, facts)
        try:
            ranked, dropped = marks.choose_rung(probe, facts)
            for r in ranked:
                diagnoses[side_name].append(
                    Diagnosis(
                        name=r.name,
                        target_type="side",
                        rung=r.name,
                        said=r.said,
                        holds=True,
                        footprint=r.footprint,
                        leaves=r.leaves or "nothing on the server",
                        blockers=(),
                        reason=r.because or "holds on this side",
                    )
                )
            for r, why in dropped:
                diagnoses[side_name].append(
                    Diagnosis(
                        name=r.name,
                        target_type="side",
                        rung=r.name,
                        said=r.said,
                        holds=False,
                        footprint=r.footprint,
                        leaves=r.leaves or "nothing on the server",
                        blockers=(why,),
                        reason=why,
                    )
                )
        except Exception as e:
            diagnoses[side_name].append(
                Diagnosis(
                    name=side_engine,
                    target_type="side",
                    rung=None,
                    said=f"could not diagnose {side_name}",
                    holds=False,
                    blockers=(str(e),),
                    reason=str(e),
                )
            )
    return diagnoses


def diagnose(hop: Hop) -> Dict[str, object]:
    legs_diag = diagnose_legs(hop)
    sides_diag = diagnose_sides(hop)
    return {
        "hop": hop.name,
        "legs": legs_diag,
        "sides": sides_diag,
        "chosen_leg": next((d for d in legs_diag if d.holds), None),
    }


def lines(hop: Hop) -> List[str]:
    d = diagnose(hop)
    out = []
    chosen_leg = d["chosen_leg"]
    if chosen_leg:
        out.append(f"leg: {chosen_leg.said} (footprint {chosen_leg.footprint}; leaves {chosen_leg.leaves})")
    else:
        out.append("leg: no server-to-server leg holds")
    for leg in d["legs"]:
        if leg.blockers:
            out.append(f"  blocked leg {leg.said}: {'; '.join(leg.blockers)}")

    for side, side_diags in d["sides"].items():
        active = next((s for s in side_diags if s.holds), None)
        if active:
            out.append(f"{side}: stands on {active.said}")
        for s in side_diags:
            if s.blockers:
                out.append(f"  blocked {side} rung {s.said}: {'; '.join(s.blockers)}")
    return out
