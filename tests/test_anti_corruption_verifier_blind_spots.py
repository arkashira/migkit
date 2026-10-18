"""The verifier is tested against defects planted on purpose (RCH39)."""
import collections

import pytest

from migkit import anti_corruption as ac
import tests.rung_branches
from tests.rung_branches import watched

PROVES_RUNGS = [
    ("anti_corruption", "anti_corruption_mutation_rungs", "certified_mutation_pass", "holds"),
    ("anti_corruption", "anti_corruption_mutation_rungs", "certified_mutation_pass", "refuses"),
    ("anti_corruption", "anti_corruption_mutation_rungs", "proof_debt_review", "holds"),
    ("anti_corruption", "anti_corruption_mutation_rungs", "proof_debt_review", "refuses"),
    ("anti_corruption", "anti_corruption_mutation_rungs", "false_same_refusal", "holds"),
]

ROWS = (("a", 1), ("b", 2), ("c", 3))
WORLD = ac.World(rows=ROWS, grants=frozenset({"app:select"}),
                 statistics=(("t", 3),))


def count_only(src, tgt):
    return "same" if len(src.rows) == len(tgt.rows) else "different"


def set_hash(src, tgt):
    return "same" if set(src.rows) == set(tgt.rows) else "different"


def multiset_exact(src, tgt):
    return ("same" if collections.Counter(src.rows)
            == collections.Counter(tgt.rows) else "different")


def everything(src, tgt):
    if multiset_exact(src, tgt) != "same":
        return "different"
    if src.grants != tgt.grants or src.statistics != tgt.statistics:
        return "different"
    return "same"


def lossy(src, tgt):
    norm = lambda w: sorted(tuple(v.strip() if isinstance(v, str) else v
                                  for v in r) for r in w.rows)
    return "same" if norm(src) == norm(tgt) else "different"


def _ver(name, fn, *classes):
    return ac.Verifier(name, frozenset(classes), fn)


def _run(*verifiers):
    return ac.run_suite(verifiers, "postgres", WORLD)


def _o(report, verifier, defect):
    return next(o for o in report.outcomes
                if o.verifier == verifier and o.defect == defect)


def test_every_defect_is_real_to_the_independent_oracle():
    report = _run(_ver("all", everything, *ac.CLASSES))
    assert not any(o.equivalent for o in report.outcomes)


def test_a_count_only_verifier_claiming_content_is_a_false_same():
    report = _run(_ver("count", count_only, ac.CONTENT, ac.COMPLETENESS))
    o = _o(report, "count", "alter-canonicalization")
    assert o.false_same and o.care_badge == ac.ACTION
    assert report.release_blocked()


def test_count_only_cannot_see_a_duplicate_swapped_for_a_missing_row():
    swapped = ac.World(rows=(("a", 1), ("a", 1), ("c", 3)),
                       grants=WORLD.grants, statistics=WORLD.statistics)
    assert count_only(WORLD, swapped) == "same"
    assert ac.independent_differs(WORLD, swapped)


def test_a_set_hash_misses_a_duplicated_keyless_row():
    report = _run(_ver("set", set_hash, ac.MULTIPLICITY))
    o = _o(report, "set", "duplicate-keyless-row")
    assert o.false_same and report.release_blocked()


def test_the_multiset_verifier_catches_the_duplicate():
    report = _run(_ver("multi", multiset_exact, ac.MULTIPLICITY))
    assert _o(report, "multi", "duplicate-keyless-row").care_badge == ac.PASS


def test_a_lossy_canonical_form_misses_the_altered_value():
    report = _run(_ver("lossy", lossy, ac.CONTENT))
    assert _o(report, "lossy", "alter-canonicalization").false_same


def test_privileges_and_statistics_survive_a_row_only_verifier():
    report = _run(_ver("rows", multiset_exact, ac.CONTENT, ac.MULTIPLICITY,
                       ac.COMPLETENESS))
    debt = {o.defect for o in report.proof_debt()}
    assert debt == {"omit-grants", "reset-statistics"}
    assert not report.release_blocked()
    badge, lines = ac.release_gate(report)
    assert badge == ac.REVIEW
    assert any(l.startswith("[REVIEW] proof debt") for l in lines)


def test_a_full_verifier_claiming_everything_passes_the_gate():
    report = _run(_ver("full", everything, *ac.CLASSES))
    badge, lines = ac.release_gate(report)
    assert badge == ac.PASS
    assert all(v == 1.0 for v in report.score().values())


def test_the_score_is_kept_per_engine_and_class():
    report = ac.run_suite([_ver("full", everything, *ac.CLASSES)],
                          "mysql", WORLD)
    keys = set(report.score())
    assert {("full", "mysql", c) for c in ac.CLASSES} == keys


def test_a_caught_defect_outside_every_claim_is_benign():
    report = _run(_ver("multi", multiset_exact))
    assert _o(report, "multi", "duplicate-keyless-row").care_badge == ac.BENIGN


def test_an_equivalent_mutant_is_left_out_of_the_score():
    empty = ac.World(rows=(), grants=frozenset(), statistics=())
    report = ac.run_suite([_ver("all", everything, *ac.CLASSES)],
                          "postgres", empty,
                          defects=[ac.Defect("truncate-table",
                                             ac.COMPLETENESS,
                                             ac.truncate_table)])
    only = report.outcomes[0]
    assert only.equivalent and only.care_badge == ac.PASS
    assert report.score() == {}


def test_a_verifier_that_calls_identical_worlds_different_is_refused():
    with pytest.raises(ValueError):
        _run(_ver("broken", lambda s, t: "different", ac.CONTENT))


def test_a_removed_scope_filter_is_caught_by_a_scope_aware_verifier():
    scoped = ac.World(rows=ROWS + (("other-tenant", 9),),
                      in_scope=lambda r: r[0] != "other-tenant",
                      grants=WORLD.grants, statistics=WORLD.statistics)

    def scope_aware(src, tgt):
        return ("same" if src.scoped_rows() == tgt.scoped_rows()
                else "different")
    only = [d for d in ac.DEFECTS if d.name == "remove-scope-filter"]
    r = ac.run_suite([_ver("aware", scope_aware, ac.SCOPE)], "postgres",
                     scoped, defects=only)
    o = _o(r, "aware", "remove-scope-filter")
    assert not o.equivalent and o.caught and o.care_badge == ac.PASS


def test_the_report_is_ascii_unix_lines():
    report = _run(_ver("count", count_only, ac.CONTENT))
    for line in report.lines():
        assert line.isascii()
    assert any(l.startswith("MUTATION count engine=postgres") for l in
               report.lines())
    src = open(ac.__file__, "rb").read()
    assert all(b < 128 for b in src)


def test_anti_corruption_mutation_rungs_under_watched():
    with watched() as seen:
        # 1. certified_mutation_pass holds
        rep_full = _run(_ver("full", everything, *ac.CLASSES))
        c_pass = ac.decide_mutation_gate(rep_full)
        assert c_pass.rung.name == "certified_mutation_pass"

        # 2. certified_mutation_pass refuses, proof_debt_review holds
        rep_rows = _run(_ver("rows", multiset_exact, ac.CONTENT, ac.MULTIPLICITY, ac.COMPLETENESS))
        c_debt = ac.decide_mutation_gate(rep_rows)
        assert c_debt.rung.name == "proof_debt_review"

        # 3. certified_mutation_pass refuses, proof_debt_review refuses, false_same_refusal holds
        rep_false = _run(_ver("count", count_only, ac.CONTENT))
        c_refuse = ac.decide_mutation_gate(rep_false)
        assert c_refuse.rung.name == "false_same_refusal"

