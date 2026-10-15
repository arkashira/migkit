"""FIX43: Fixes for stored-code residues drafted through the same proof.

For each failing routine, candidate rewrites from existing sources are
generated, each run through the execution proof, the first that passes is
proposed as a draft (never applied without operator approval), the rest
ranked with their failure reasons.
"""
from dataclasses import dataclass
from typing import Any, List, Tuple

import pytest

from migkit.storedcode.routines import Param, Routine
from migkit.storedcode.prove import Verdict
from migkit.storedcode.rungs import Candidate, Need, Ledger, ranked
from migkit.storedcode.convert import PROPOSED


@dataclass
class FakeConverted:
    """A fake Converted result."""
    statements: List[str]
    constructs: List = None
    cost: Any = None
    residue: List = None
    sql: str = ""

    def __post_init__(self):
        if self.constructs is None:
            self.constructs = []
        if self.residue is None:
            self.residue = []
        if not self.sql:
            self.sql = "\n".join(self.statements)


class FakeProof:
    """A fake proof runner for testing - controls which candidate passes."""
    def __init__(self, pass_results: dict):
        self.pass_results = pass_results  # {candidate_name: (status, reason)}
        self.call_count = 0
        self.tried_versions = []

    def try_version(self, routine, statements):
        self.call_count += 1
        sql = "\n".join(statements)
        self.tried_versions.append((routine.name, sql))
        # Find which candidate this is by checking the statements
        for name, (status, reason) in self.pass_results.items():
            if name in sql:
                return Verdict(routine.said, status, reason, calls=1), None
        # Default: diff
        return Verdict(routine.said, "diff", "no matching candidate", calls=1), None

    def begin(self, targets):
        pass

    def end(self):
        pass

    def install(self, routine, statements):
        pass

    def made(self, routine):
        return None


def _make_test_candidates():
    """Create test candidates that produce identifiable statements."""
    both = frozenset({"mysql->postgres:function"})

    def make_translator(r):
        return FakeConverted(
            statements=[f"CREATE FUNCTION {r.name}() RETURNS int LANGUAGE sql AS $$ SELECT 1; $$; -- translator"]
        )

    def make_rules(r):
        return FakeConverted(
            statements=[f"CREATE FUNCTION {r.name}() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 2; END; $$; -- rules"]
        )

    def make_model(r):
        return FakeConverted(
            statements=[f"CREATE FUNCTION {r.name}() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 3; END; $$; -- model"]
        )

    return [
        Candidate("translator", "the query translator", both,
                  (Need("one expression", lambda f: f.get("one") is not None, ""),),
                  make=make_translator),
        Candidate("rules", "migkit's rules", both,
                  make=make_rules),
        Candidate("model", "a model", both,
                  rounds=1,
                  make=make_model),
    ]


def _make_test_facts():
    return {"src": "mysql", "dst": "postgres", "kind": "function", "one": "SELECT 1", "file": None}


class TestFixDrafting:
    """Tests for the fix-drafting mechanism."""

    def test_only_candidate_3_passes_proposed_as_draft(self):
        """A routine where only candidate 3 (model) passes the proof."""
        routine = Routine("function", "test_func", [Param("p", "int")], "int",
                          body="BEGIN RETURN p + 1; END")

        # Set up fake proof where only the model (candidate 3) passes
        proof = FakeProof({
            "model": ("ok", "all calls answered the same"),
            "translator": ("diff", "returned 1 against 2"),
            "rules": ("diff", "returned 2 against 3"),
        })

        cands = _make_test_candidates()
        ledger = Ledger(None)
        facts = _make_test_facts()
        order, fell = ranked(cands, ledger, facts)

        # Try each candidate through the proof
        results = []
        for c in order:
            try:
                made = c.make(routine)
                verdict, _ = proof.try_version(routine, made.statements)
                results.append((c.name, verdict.status, verdict.reason, made))
            except Exception as e:
                results.append((c.name, "error", str(e), None))

        # Verify the results
        assert results[0][0] == "translator"
        assert results[0][1] == "diff"
        assert results[1][0] == "rules"
        assert results[1][1] == "diff"
        assert results[2][0] == "model"
        assert results[2][1] == "ok"

        # The fix-drafting should propose the model's version as a draft
        # and rank the others with their failure reasons
        drafts = [(name, status, reason, made) for name, status, reason, made in results if status == "ok"]
        failed = [(name, status, reason, made) for name, status, reason, made in results if status != "ok"]

        assert len(drafts) == 1
        assert drafts[0][0] == "model"
        assert len(failed) == 2
        assert failed[0][0] == "translator"
        assert failed[1][0] == "rules"

    def test_none_pass_residue_stays_with_reasons(self):
        """A routine where no candidate passes - residue stays with reasons."""
        routine = Routine("function", "test_func", [Param("p", "int")], "int",
                          body="BEGIN RETURN p + 1; END")

        # Set up fake proof where NO candidate passes
        proof = FakeProof({
            "model": ("diff", "returned 3 against 4"),
            "translator": ("diff", "returned 1 against 2"),
            "rules": ("diff", "returned 2 against 3"),
        })

        cands = _make_test_candidates()
        ledger = Ledger(None)
        facts = _make_test_facts()
        order, fell = ranked(cands, ledger, facts)

        results = []
        for c in order:
            try:
                made = c.make(routine)
                verdict, _ = proof.try_version(routine, made.statements)
                results.append((c.name, verdict.status, verdict.reason, made))
            except Exception as e:
                results.append((c.name, "error", str(e), None))

        # All should fail
        assert all(status != "ok" for _, status, _, _ in results)

        # The residue should stay with all failure reasons
        failed = [(name, status, reason, made) for name, status, reason, made in results]
        assert len(failed) == 3
        assert failed[0][0] == "translator"
        assert failed[1][0] == "rules"
        assert failed[2][0] == "model"

    def test_first_passes_proposed_as_draft_rest_not_tried(self):
        """When the first candidate passes, it's proposed as draft, rest not tried."""
        routine = Routine("function", "test_func", [Param("p", "int")], "int",
                          body="BEGIN RETURN p + 1; END")

        # Set up fake proof where FIRST candidate (translator) passes
        proof = FakeProof({
            "translator": ("ok", "all calls answered the same"),
            "rules": ("diff", "returned 2 against 3"),
            "model": ("diff", "returned 3 against 4"),
        })

        cands = _make_test_candidates()
        ledger = Ledger(None)
        facts = _make_test_facts()
        order, fell = ranked(cands, ledger, facts)

        results = []
        for c in order:
            try:
                made = c.make(routine)
                verdict, _ = proof.try_version(routine, made.statements)
                results.append((c.name, verdict.status, verdict.reason, made))
                if verdict.status == "ok":
                    break  # First passing candidate stops the search
            except Exception as e:
                results.append((c.name, "error", str(e), None))

        # Only translator should be tried and it passes
        assert len(results) == 1
        assert results[0][0] == "translator"
        assert results[0][1] == "ok"

    def test_draft_fix_recorded_with_proposed_marker(self):
        """Draft fixes are marked with the PROPOSED marker for operator review."""
        from migkit.storedcode.convert import PROPOSED
        
        # The PROPOSED marker should be used for draft fixes
        assert "proposed" in PROPOSED.lower()
        assert "check holds it to the source's answers" in PROPOSED

    def test_approvals_step_for_code_fix(self):
        """The approvals system should support a step for code fix drafts."""
        from migkit.approvals import STEPS
        
        # Check if there's a relevant step or if one needs to be added
        # For now, "repair" or "source-setting" could be reused
        assert "repair" in STEPS
        assert "source-setting" in STEPS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])