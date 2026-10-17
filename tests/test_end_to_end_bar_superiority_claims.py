"""Superiority claims stand only on reproducible complete migrations (RCH40)."""
import pytest

from migkit import end_to_end_bar as bar

C = bar.Contract("row-hash", "all", "steady-50", "fsync-on")
PG = ("postgres", "postgres")


def runs(tool, secs, path=PG, evidence=bar.REAL, contract=C, **kw):
    return [bar.Run(tool, path, evidence, contract, s, **kw) for s in secs]


FAST = [10, 11, 12, 11, 13]
SLOW = [20, 22, 21, 23, 24]


def test_a_faster_claim_is_admitted_from_real_equal_contract_runs():
    v = bar.evaluate_faster_claim(runs("migkit", FAST) + runs("dms", SLOW),
                                  "migkit", "dms", PG)
    assert v.admitted and v.care_badge == bar.PASS
    assert v.line().startswith("[PASS] claim=migkit-faster-than-dms")


def test_too_few_runs_is_an_owned_gap_not_a_claim():
    v = bar.evaluate_faster_claim(runs("migkit", FAST[:2]) + runs("dms", SLOW),
                                  "migkit", "dms", PG)
    assert not v.admitted and v.care_badge == bar.REVIEW


def test_different_contracts_are_refused():
    other = bar.Contract("count", "all", "steady-50", "fsync-off")
    v = bar.evaluate_faster_claim(
        runs("migkit", FAST) + runs("dms", SLOW, contract=other),
        "migkit", "dms", PG)
    assert not v.admitted and v.care_badge == bar.ACTION


def test_a_run_that_lost_rows_refuses_the_claim():
    mine = runs("migkit", FAST[:4]) + runs("migkit", [10], lost_rows=1)
    v = bar.evaluate_faster_claim(mine + runs("dms", SLOW), "migkit", "dms",
                                  PG)
    assert not v.admitted and "lost or duplicated" in v.reasons[0]


def test_a_p95_that_is_not_lower_is_refused():
    v = bar.evaluate_faster_claim(runs("migkit", SLOW) + runs("dms", FAST),
                                  "migkit", "dms", PG)
    assert not v.admitted and v.care_badge == bar.ACTION


def test_a_fast_median_with_a_slow_tail_does_not_win_on_p95():
    tail = [5, 5, 5, 5, 40]
    v = bar.evaluate_faster_claim(runs("migkit", tail) + runs("dms", SLOW),
                                  "migkit", "dms", PG)
    assert not v.admitted


@pytest.mark.parametrize("evidence", [bar.LANE, bar.SIMULATED, bar.FAKE,
                                      bar.MANAGED])
def test_only_real_evidence_supports_a_claim(evidence):
    v = bar.evaluate_faster_claim(
        runs("migkit", FAST, evidence=evidence) + runs("dms", SLOW),
        "migkit", "dms", PG)
    assert not v.admitted


def test_an_unverified_run_does_not_count():
    mine = runs("migkit", FAST, verified=False)
    v = bar.evaluate_faster_claim(mine + runs("dms", SLOW), "migkit", "dms",
                                  PG)
    assert not v.admitted


@pytest.mark.parametrize("kind", bar.UNIVERSAL)
def test_universal_claims_are_always_refused(kind):
    v = bar.evaluate_universal_claim(kind)
    assert not v.admitted and v.care_badge == bar.ACTION


def test_a_feature_count_is_not_a_universal_claim_kind():
    with pytest.raises(ValueError):
        bar.evaluate_universal_claim("most-features")


def test_coverage_names_every_unrun_required_path_as_an_owned_gap():
    cov = bar.coverage(runs("migkit", FAST))
    assert cov.covered == ("postgres->postgres",)
    assert set(cov.gaps) == {"mysql->mysql", "postgres->mysql",
                             "mysql->postgres", bar.COMPOUND}
    assert not cov.complete
    assert all(l.startswith("[REVIEW] owned gap") for l in cov.lines()
               if "gap" in l)


def test_full_coverage_needs_the_four_paths_and_the_compound_run():
    allruns = []
    for p in bar.REQUIRED_PATHS:
        allruns += runs("migkit", [10], path=p)
    allruns += runs("migkit", [30], scenario=bar.COMPOUND)
    cov = bar.coverage(allruns)
    assert cov.complete and len(cov.covered) == 5


def test_non_real_evidence_is_kept_apart_and_never_covers():
    cov = bar.coverage(runs("migkit", FAST, evidence=bar.LANE)
                       + runs("migkit", FAST, evidence=bar.FAKE))
    assert cov.covered == () and cov.kept_apart == {"lane": 5, "fake": 5}
    assert any(l.startswith("[BENIGN] 5 lane runs") for l in cov.lines())


def test_a_pair_outside_the_required_paths_stays_a_gap():
    v = bar.pair_claim(runs("migkit", FAST, path=("oracle", "postgres")),
                       ("oracle", "postgres"))
    assert not v.admitted and v.care_badge == bar.REVIEW


def test_a_required_pair_needs_a_real_run():
    assert not bar.pair_claim([], PG).admitted
    assert bar.pair_claim(runs("migkit", FAST), PG).admitted


def test_unknown_evidence_kind_is_rejected():
    with pytest.raises(ValueError):
        bar.Run("migkit", PG, "marketing", C, 1.0)


def test_percentile_is_nearest_rank():
    assert bar.percentile([1, 2, 3, 4, 5], 0.95) == 5
    assert bar.percentile([1, 2, 3, 4, 5], 0.5) == 3
    with pytest.raises(ValueError):
        bar.percentile([], 0.5)


def test_module_is_ascii():
    assert all(b < 128 for b in open(bar.__file__, "rb").read())
