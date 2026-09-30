"""Rehearse, then forecast (backlog R23, 2).

A hop whose goal is `rehearse` marks what it rehearses in its reports; the
hop it rehearses is planned from what the rehearsal measured - its
resource model, its copies' rates, its checks' times, its cutover's
outage - before any other hop's, named, and on its own size; a smaller
rehearsal widens the p90. The real run is then held to the forecast it
started with, phase by phase, and each is said: within it, slower than its
p90 or faster than half its p50.

No servers here: the reports a rehearsal leaves are written as it writes
them, so what the real hop reads from them is known.
"""
import json

import pytest

from migkit import config as cfg
from migkit import hopkeys, model, planner, rehearsal
from migkit.config import Endpoint, Hop
from migkit.model import MEASURED, Model, Stage

MB = 2 ** 20


@pytest.fixture
def reports(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    (tmp_path / "reports").mkdir()
    return tmp_path / "reports"


def _hop(name, goal="", options=None, dbs=("orders",), engine="postgres"):
    ep = Endpoint(host="10.0.0.1", port=5432, user="app",
                  password="CHANGE_ME")
    return Hop(name=name, engine=engine, source=ep,
               target=Endpoint(host="10.0.0.2", port=5432, user="app",
                               password="CHANGE_ME"),
               databases=list(dbs), options=dict(options or {}), goal=goal)


def _rehearsal(**options):
    return _hop("orders-uat", "rehearse",
                {"rehearses": "orders", **options}, dbs=("orders_uat",))


def test_the_rehearsal_is_found_by_its_own_mark(reports):
    real = _hop("orders")
    assert rehearsal.of(real) is None
    uat = _rehearsal()
    rehearsal.mark(uat)
    got = rehearsal.of(real)
    assert got["name"] == "orders-uat" and got["of"] == "orders"
    # its one database stands for the real hop's one
    assert rehearsal.db_of(got, "orders") == "orders_uat"
    # a hop it does not rehearse finds nothing
    assert rehearsal.of(_hop("payments")) is None
    # and a hop that rehearses nothing leaves no mark
    assert rehearsal.mark(_hop("other", "copy")) is None


def test_databases_are_paired_as_the_rehearsal_says():
    rec = {"databases": {"shop_uat": "shop"}, "own": ["shop_uat", "x_uat"],
           "dir": __import__("pathlib").Path("/nonexistent")}
    assert rehearsal.db_of(rec, "shop") == "shop_uat"
    # two of its own and no pairing for this one: none stands for it
    assert rehearsal.db_of(rec, "billing") is None


def test_a_smaller_rehearsal_widens_the_p90_by_a_rule():
    assert rehearsal.sample_spread(None, 10) == 0.0
    assert rehearsal.sample_spread(10, 10) == 0.0
    assert rehearsal.sample_spread(20, 10) == 0.0
    # two factors of ten: two steps of the rule
    assert rehearsal.sample_spread(100_000, 10_000_000) == \
        pytest.approx(2 * rehearsal.PER_DECADE)
    # and no more than its most, however small the sample
    assert rehearsal.sample_spread(1, 10 ** 12) == rehearsal.WIDEST
    said = rehearsal.share_said(120_000, 12_000_000)
    assert "~120,000 rows of the ~12,000,000 here (1.0%)" in said, said


def _stages():
    return [Stage("source", "the source's reads", 40 * MB, 160 * MB,
                  MEASURED, side="src"),
            Stage("target", "the target's writes", 10 * MB, 30 * MB,
                  MEASURED, side="dst")]


def test_the_model_stands_on_the_rehearsals_stages_named(reports):
    real, uat = _hop("orders"), _rehearsal()
    rehearsal.mark(uat)
    Model(_stages(), rows=100_000, carried=100_000 * 100, per_row=100.0,
          measured=True, spread=0.05).save(uat.report_dir("orders_uat"))
    # another hop of the same engines measured another target: the
    # rehearsal's is taken before it
    other = _hop("elsewhere")
    Model([Stage("target", "the target's writes", 1 * MB, 1 * MB,
                 MEASURED, side="dst")], measured=True).save(
        other.report_dir("orders"))
    m = Model(rows=10_000_000, carried=10 ** 9)
    model._earlier(m, real, "orders")
    assert m.measured
    assert m.stages["target"].total == 30 * MB
    assert m.stages["target"].truth == model.REHEARSED
    # a hundred times the rows the rehearsal carried: widened by the rule
    assert m.spread == pytest.approx(rehearsal.sample_spread(100_000,
                                                             10_000_000))
    notes = " ".join(m.notes)
    assert "forecast from the rehearsal orders-uat" in notes, notes
    assert "(1.0%)" in notes, notes
    # the rehearsal's stages are costed like any measured ones
    assert "target" in m.costs()


def test_a_plan_that_could_not_write_takes_the_targets_writes_from_it(
        reports):
    real, uat = _hop("orders"), _rehearsal()
    rehearsal.mark(uat)
    Model(_stages(), rows=1000, measured=True).save(
        uat.report_dir("orders_uat"))
    m = Model([Stage("source", "the source's reads", 50 * MB, 200 * MB,
                     MEASURED, side="src")], rows=1000, measured=True)
    model._earlier(m, real, "orders", only=("target", "target-check"))
    # the source measured here now stands; the target's writes come from
    # the rehearsal and say so
    assert m.stages["source"].total == 200 * MB
    assert m.stages["source"].truth == MEASURED
    assert m.stages["target"].truth == model.REHEARSED
    assert any("the target's writes from there" in n for n in m.notes), \
        m.notes
    # without a rehearsal nothing is taken: the plan says the target's
    # writes were not measured, as before
    bare = Model(rows=1000, measured=True)
    model._earlier(bare, _hop("payments"), "orders",
                   only=("target", "target-check"))
    assert "target" not in bare.stages


def test_the_cutover_is_forecast_from_the_rehearsals_outage(reports):
    real, uat = _hop("orders"), _rehearsal()
    rehearsal.mark(uat)
    got = planner._with_cutover(real, [planner.Phase("cutover", 10.0)])
    assert got[-1].truth != "measured on the rehearsal orders-uat"
    with (uat.report_dir() / "cutover-runs.jsonl").open("w") as f:
        f.write(json.dumps({"state": "undone", "outage": 1.0}) + "\n")
        f.write(json.dumps({"state": "flipped", "outage": 2.5}) + "\n")
    got = planner._with_cutover(real, [planner.Phase("cutover", 10.0)])
    assert got[-1].p50 == pytest.approx(2.5)
    assert got[-1].p90 == pytest.approx(3.75)
    assert got[-1].truth == "measured on the rehearsal orders-uat"
    # the hop's own cutover, once measured, stands before the rehearsal's
    with (real.report_dir() / "cutover-runs.jsonl").open("w") as f:
        f.write(json.dumps({"state": "flipped", "outage": 7.0}) + "\n")
    got = planner._with_cutover(real, [planner.Phase("cutover", 10.0)])
    assert got[-1].p50 == pytest.approx(7.0)


def test_the_copy_and_the_check_are_timed_by_the_rehearsals_runs(reports):
    real, uat = _hop("orders"), _rehearsal()
    rehearsal.mark(uat)
    planner.record_rate(uat, "builtin", 100_000, 10.0, 100 * MB)
    planner.record_rate(uat, rehearsal.CHECK, 100_000, 4.0, 100 * MB)
    # another hop of the same engines, far slower: not the one taken
    planner.record_rate(_hop("elsewhere"), "builtin", 100_000, 1000.0)
    runs, whose = planner._elsewhere(real, "builtin")
    assert whose == ["the rehearsal orders-uat"]
    assert runs[0]["seconds"] == 10.0
    phase = planner._check_from_runs(real, 1000 * MB, 1_000_000)
    assert phase.p50 == pytest.approx(40.0)
    assert "of the rehearsal orders-uat" in phase.truth
    # ten times the rows the rehearsal carried: the p90 is widened
    assert phase.p90 > planner._p90(40.0, 0)
    out = []
    copy = planner._copy_from_runs(real, "builtin", 1000 * MB, 1_000_000,
                                   out)
    assert copy == pytest.approx(100.0)
    assert "of the rehearsal orders-uat" in out[0].truth
    assert out[0].p90 >= planner._p90(100.0, rehearsal.sample_spread(
        100_000, 1_000_000))


def test_a_check_is_kept_only_where_a_rehearsal_is(reports):
    class Eng:
        def table_facts(self, side, db):
            return {"public.t": {"rows": 500, "bytes": 5 * MB}}
    plain = _hop("plain")
    rehearsal.record_check(plain, Eng(), "orders", 3.0)
    assert planner.runs(plain, rehearsal.CHECK) == []
    uat = _rehearsal()
    rehearsal.record_check(uat, Eng(), "orders_uat", 3.0)
    got = planner.runs(uat, rehearsal.CHECK)
    assert got[-1]["rows"] == 500 and got[-1]["bytes"] == 5 * MB


def test_the_real_run_is_held_to_its_forecast_once_a_phase(reports):
    real, uat = _hop("orders"), _rehearsal()
    rehearsal.mark(uat)
    assert rehearsal.held_to(real, "orders", "copy", 50.0) == []
    rehearsal.keep_forecast(real, "orders", [
        planner.Phase("copy", 60.0, 90.0), planner.Phase("cutover", 2.0,
                                                          3.0)])
    said = rehearsal.held_to(real, "orders", "copy", 70.0)
    assert len(said) == 1 and "within its forecast" in said[0], said
    assert "forecast from the rehearsal orders-uat" in said[0], said
    # a phase is held to the forecast once: a later run without one of its
    # own is not held to this one
    assert rehearsal.held_to(real, "orders", "copy", 70.0) == []
    # and it can say otherwise
    said = rehearsal.held_to(real, "orders", "cutover", 9.0)
    assert "slower than its p90" in said[0], said
    rehearsal.keep_forecast(real, "orders", [planner.Phase("copy", 60.0,
                                                           90.0)])
    said = rehearsal.held_to(real, "orders", "copy", 10.0)
    assert "faster than half its p50" in said[0], said
    assert rehearsal.last_held(real, "orders") == {
        "copy": "faster than half its p50",
        "cutover": "slower than its p90"}


def test_a_path_that_differs_is_named_and_places_are_not():
    a = _rehearsal(cutover={"budget": 30, "flip": "ssh uat-app flip"})
    b = _hop("orders", options={"cutover": {"budget": 30,
                                            "flip": "ssh prod-app flip"}})
    a.exclude, b.exclude = ["orders_uat.public.audit"], ["orders.public.audit"]
    a.databases, b.databases = ["orders_uat"], ["orders"]
    assert rehearsal.differences(rehearsal.path_of(a),
                                 rehearsal.path_of(b)) == []
    b.options["cutover"]["budget"] = 10
    b.mapping = {"where": {"orders.public.t": "id > 5"}}
    diff = rehearsal.differences(rehearsal.path_of(a), rehearsal.path_of(b))
    assert diff == ["mapping.where", "options.cutover"], diff
    b.engine = "mysql"
    assert rehearsal.differences(rehearsal.path_of(a), rehearsal.path_of(
        b))[0].startswith("the engines"), diff


def test_the_goal_is_declared_and_held_to_its_settings():
    g = hopkeys.GOALS["rehearse"]
    assert g["writes"] and "cutover" not in g["options"]
    assert any("--drop --go" in s for s in g["next"])

    def problems(raw):
        return [(p.level, p.where) for p in hopkeys.check_hop("uat", raw)]
    base = {"source": "postgres://app@10.0.0.1/orders_uat",
            "target": "postgres://app@10.0.0.3/orders_uat"}
    assert ("warn", "hops.uat.options.rehearses") in problems(
        {**base, "goal": "rehearse"})
    assert ("error", "hops.uat.options.rehearses") in problems(
        {**base, "goal": "rehearse", "options": {"rehearses": "uat"}})
    assert ("warn", "hops.uat.options.rehearses") in problems(
        {**base, "goal": "copy", "options": {"rehearses": "orders"}})
    assert not [p for p in problems({**base, "goal": "rehearse",
                                     "options": {"rehearses": "orders"}})
                if p[1].endswith("rehearses")]
