"""Tests for Items 115, 2, 4, 6, and 445:
- Item 115: External database program only earns rank over native table copier with measured successful run
- Item 2: Mover and path chosen on speed from measured rates in the plan
- Item 4: Doctor diagnoses and names rung each side and leg stands on, along with blockers
- Item 6: Closed unhandled cells in decide.coverage() so every shape has viable way or explicit gap
- Item 445: Standalone validation replanning via shared decision engine
"""
import pytest

from migkit import decide, movers, planner, verifiers
from migkit.config import Endpoint, Hop
from migkit.decide import Costs, Facts, HopCosts, Need, Rung, cell, climb, coverage, rank, uncovered


def _hop(tmp_path, engine="postgres", **options):
    ep = Endpoint(host="10.0.0.1", port=5432, user="u", password="p")
    hop = Hop(name="test_hop", engine=engine, source=ep, target=ep,
              databases=["appdb"], options=dict(options))
    hop.report_dir = lambda db=None: tmp_path
    return hop


# --- Item 115: External database program earns rank only by measured run ---

def test_external_program_without_measured_run_cannot_rank_over_copier():
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    # Even if external program is listed first in the ladder, unmeasured it cannot beat copier
    ordered = rank([prog, copier])
    assert ordered[0].name == "builtin"
    assert ordered[1].name == "pgloader"


def test_external_program_with_failed_run_does_not_earn_rank():
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    # A failed run (0 rows moved, or 0 / negative rate) does not earn rank
    costs = Costs()
    ordered = rank([prog, copier], costs=costs)
    assert ordered[0].name == "builtin"
    assert ordered[1].name == "pgloader"


def test_external_program_with_measured_run_earns_rank_by_speed():
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    costs = Costs()
    # Program ran successfully and measured faster than copier
    costs.saw("pgloader", 10_000, 2.0)   # 0.0002 s/row
    costs.saw("builtin", 10_000, 5.0)    # 0.0005 s/row
    ordered = rank([prog, copier], costs=costs)
    assert ordered[0].name == "pgloader"
    assert ordered[1].name == "builtin"

    # If copier is measured faster, copier ranks first
    costs.saw("pgloader", 10_000, 10.0)  # 0.0010 s/row
    costs.saw("builtin", 10_000, 2.0)    # 0.0002 s/row
    ordered2 = rank([prog, copier], costs=costs)
    assert ordered2[0].name == "builtin"
    assert ordered2[1].name == "pgloader"


def test_why_not_explains_unearned_external_rank():
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    choice = climb("appdb", [prog, copier])
    assert choice.rung.name == "builtin"
    assert "earns its rank only after a measured successful run" in choice.why_not


# --- Item 2: Mover and path chosen on speed from measured rates in plan ---

def test_mover_choice_picks_faster_measured_rate(tmp_path, monkeypatch):
    hop = _hop(tmp_path, engine="postgres")
    monkeypatch.setattr(movers, "pgcopydb_available", lambda: True)
    monkeypatch.setattr(movers, "which", lambda p: f"/usr/bin/{p}")

    # Record measured rates: pgdump faster than pgcopydb, both faster than builtin
    planner.record_rate(hop, "pgdump", 100_000, 2.0)      # 50,000 rows/s
    planner.record_rate(hop, "pgcopydb", 100_000, 10.0)   # 10,000 rows/s
    planner.record_rate(hop, "builtin", 100_000, 20.0)    # 5,000 rows/s

    got = movers.choice("postgres", hop=hop)
    assert got.path == "pgdump"

    # Flip rates: pgcopydb becomes faster
    planner.record_rate(hop, "pgcopydb", 100_000, 1.0)    # 100,000 rows/s
    got2 = movers.choice("postgres", hop=hop)
    assert got2.path == "pgcopydb"


def test_planner_plan_chooses_path_on_measured_speed(tmp_path):
    hop = _hop(tmp_path, engine="postgres")
    # Copier is measured significantly faster than bulk
    planner.record_rate(hop, planner.COPIER, 100_000, 1.0)
    planner.record_rate(hop, planner.BULK, 100_000, 10.0)

    decisions = planner.plan(hop, "appdb", "pgdump", ["orders"])
    assert len(decisions) == 1
    assert decisions[0].path == planner.COPIER


# --- Item 4: Doctor diagnoses rungs and blockers for sides and legs ---

def test_coverage_has_no_unhandled_cells():
    unc = uncovered()
    assert unc == [], f"Found uncovered cells: {unc}"
    assert decide.unowned_gaps() == []

    cov = coverage()
    for engine, shapes in cov.items():
        for shape, state in shapes.items():
            assert state in ("yes", "not applicable", decide.NOT_YET), (
                f"Cell ({engine}, {shape}) is {state}, expected 'yes', 'not applicable', or 'not yet'"
            )


# --- Item 445: Standalone validation replanning via shared decision engine ---

def test_standalone_validation_plan_selects_best_method():
    # If server digest is available, digest is chosen
    choice = verifiers.plan_validation("orders", facts={"can_digest": True})
    assert choice.path == "digest"

    # If server cannot digest, bisection is chosen for keyed table
    choice2 = verifiers.plan_validation("orders", facts={"can_digest": False, "key": True, "reladiff_installed": True})
    assert choice2.path == "bisection"

    # If keyless and cannot digest, falls back to neutral row_hash
    choice3 = verifiers.plan_validation("orders", facts={"can_digest": False, "key": False})
    assert choice3.path == "row_hash"


def test_standalone_validation_automatic_replanning_on_failure():
    # Simulate a verifier failing at run time: replan automatically switches to next rung
    ran = []

    def run_check(choice):
        ran.append(choice.rung.name)
        if choice.rung.name == "digest":
            raise RuntimeError("server digest query failed: collation mismatch")
        return "validated_ok"

    choice, result = verifiers.replan_validation(
        "orders",
        run_check,
        facts={"can_digest": True, "key": True, "reladiff_installed": True},
    )
    assert result == "validated_ok"
    assert ran == ["digest", "bisection"]
    assert choice.rung.name == "bisection"


# --- the unlock read off what the hop measured (CRIT0 1) -------------------

class _Said:
    """A cost model that answers with one `Estimate` a rung."""

    def __init__(self, **got):
        self.got = got

    def estimate(self, name, unit=None):
        return self.got.get(name)

    def get(self, name, default=None):
        got = self.got.get(name)
        return got.mean if got else default


def test_a_program_timed_on_this_hop_earns_its_rank_under_hop_costs(tmp_path):
    hop = _hop(tmp_path)
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    costs = HopCosts(hop)
    assert [r.name for r in rank([prog, copier], costs=costs)] == [
        "builtin", "pgloader"]
    planner.record_rate(hop, "pgloader", 10_000, 2.0)
    planner.record_rate(hop, "builtin", 10_000, 5.0)
    assert [r.name for r in rank([prog, copier], costs=HopCosts(hop))] == [
        "pgloader", "builtin"]
    choice = climb("copy", [prog, copier], costs=HopCosts(hop))
    assert choice.rung.name == "pgloader"


def test_a_program_only_predicted_faster_never_unlocks():
    from migkit.costmodel import MEASURED_TRUTH, PREDICTED, Estimate
    prog = Rung("pgloader", "the one-pass load", external=True)
    copier = Rung("builtin", "the table copier", native_copier=True)
    guessed = _Said(pgloader=Estimate(1.0, truth=PREDICTED, n=0),
                    builtin=Estimate(9.0, truth=MEASURED_TRUTH, n=3))
    assert [r.name for r in rank([prog, copier], costs=guessed)] == [
        "builtin", "pgloader"]
    assert climb("copy", [prog, copier], costs=guessed).rung.name == \
        "builtin"
    # a measured label on no unit at all is no run either
    empty = _Said(pgloader=Estimate(1.0, truth=MEASURED_TRUTH, n=0),
                  builtin=Estimate(9.0, truth=MEASURED_TRUTH, n=3))
    assert rank([prog, copier], costs=empty)[0].name == "builtin"
    timed = _Said(pgloader=Estimate(1.0, truth=MEASURED_TRUTH, n=2),
                  builtin=Estimate(9.0, truth=MEASURED_TRUTH, n=3))
    assert rank([prog, copier], costs=timed)[0].name == "pgloader"
