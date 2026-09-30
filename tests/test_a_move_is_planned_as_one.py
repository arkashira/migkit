"""A move's tables are planned as one schedule under the hop's limits
(backlog P0-SMART, item 4, and the limits of item 7).

`jointplan` chooses each table's way as one mix, the order, the workers,
the index builds beside the copy and each check once its table is in -
simulated as flows through the resources - and is never later than the
per-choice greedy on the same model beyond the fewer workers it keeps.
The hop's limits (`deadline:`, `limits:`) are hard: money is never passed
where any mix keeps within it, the source's share caps its reads, and a
plan that cannot keep within them says what binds. No server: the moves
are made up for the purpose.
"""
import math
import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from migkit import jointplan as jp
from migkit.config import Endpoint, Hop

MB = 2 ** 20


def _ways():
    pas = jp.Way("pass", "server to server", 40 * MB,
                 {"source": 2, "target": 1}, 0.2)
    pipe = jp.Way("pipe", "through migkit", 25 * MB,
                  {"source": 1, "link:src": 1, "link:dst": 1, "target": 1},
                  0.05, bills=0.09 / 2 ** 30)
    return pas, pipe


def _limits(**kw):
    base = dict(caps={"source": 200 * MB, "target": 150 * MB,
                      "link:src": 100 * MB, "link:dst": 100 * MB},
                workers=8, index_rate=30 * MB, index_at_once=2)
    base.update(kw)
    return jp.Limits(**base)


def _jobs(sizes, ways=None, index=0.3, check=None):
    ways = ways or _ways()
    return [jp.Job(f"t{i}", s * MB, int(s * MB / 200), s * MB * index,
                   max(1, int(s * MB / 200) // 100_000), ways, check)
            for i, s in enumerate(sizes)]


def _hop(tmp_path, **options):
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"], options=options)
    hop.report_dir = lambda db="", _p=tmp_path: _p
    return hop


# --- the flows --------------------------------------------------------------

def test_a_table_alone_goes_at_its_streams_and_no_resource_past_its_cap():
    pas, _ = _ways()
    job = jp.Job("t", 400 * MB, 2_000_000, 0, 20, (pas,))
    got = jp.simulate([job], {"t": pas}, _limits(workers=8))
    # eight streams of 40 MB/s want 320 MB/s; the source reads each byte
    # twice and carries 200 MB/s: 100 MB/s of the table
    assert got.makespan == pytest.approx(0.2 + 400 / 100, rel=1e-3)
    assert got.placed["t"].copied == pytest.approx(got.makespan)


def test_each_step_follows_what_it_reads():
    check = jp.Way("check", "both sides digested", 80 * MB,
                   {"source": 1, "target": 1})
    jobs = _jobs([100, 50], check=check)
    choice = {j.name: j.ways[0] for j in jobs}
    got = jp.simulate(jobs, choice, _limits())
    for p in got.placed.values():
        assert p.start < p.copied <= p.indexed <= p.checked
    after = jp.simulate(jobs, choice, _limits(), check_after_all=True)
    last_build = max(p.indexed for p in after.placed.values())
    assert min(p.checked for p in after.placed.values()) > last_build


def test_the_fluid_bound_is_never_above_the_simulated_end():
    rng = random.Random(3)
    for _ in range(30):
        jobs = _jobs([math.exp(rng.gauss(4, 1.5)) for _ in range(12)])
        choice = {j.name: rng.choice(j.ways) for j in jobs}
        lim = _limits(workers=rng.choice([2, 4, 8]))
        got = jp.simulate(jobs, choice, lim)
        low, _ = jp.bound(jobs, choice, lim)
        assert low <= got.makespan * (1 + 1e-6), (low, got.makespan)


# --- the joint plan against the greedy ---------------------------------------

@settings(max_examples=40, deadline=None)
@given(st.lists(st.floats(1, 5000), min_size=1, max_size=25),
       st.sampled_from([2, 4, 8, 16]), st.floats(50, 600),
       st.floats(40, 400))
def test_the_joint_plan_is_never_later_than_each_choice_alone(sizes, w,
                                                               src, dst):
    jobs = _jobs(sizes)
    lim = _limits(workers=w, caps={"source": src * MB, "target": dst * MB,
                                   "link:src": 100 * MB,
                                   "link:dst": 100 * MB})
    greedy, _ = jp.greedy(jobs, lim)
    plan = jp.joint(jobs, lim)
    assert plan.makespan <= greedy.makespan * (1 + jp.WORKERS_WITHIN) \
        + 1e-6
    assert plan.bound <= plan.makespan * (1 + 1e-6)
    assert 1 <= plan.workers <= w
    assert plan.p90 >= plan.makespan


def test_a_mix_of_ways_ends_sooner_where_one_way_loads_one_resource():
    """Every table is fastest alone server to server, which reads the
    source twice; the source binds. Sending some tables through migkit
    loads the links instead, and the whole ends sooner."""
    pas = jp.Way("pass", "server to server", 40 * MB,
                 {"source": 2, "target": 1}, 0.2)
    pipe = jp.Way("pipe", "through migkit", 10 * MB,
                  {"source": 1, "link:src": 1, "link:dst": 1, "target": 1},
                  0.05)
    jobs = _jobs([800] * 10, ways=(pas, pipe), index=0)
    lim = _limits(workers=16, caps={"source": 400 * MB,
                                    "target": 400 * MB,
                                    "link:src": 200 * MB,
                                    "link:dst": 200 * MB})
    greedy, first = jp.greedy(jobs, lim)
    assert set(w.name for w in first.values()) == {"pass"}
    plan = jp.joint(jobs, lim)
    assert set(plan.ways().values()) == {"pass", "pipe"}
    assert plan.makespan < 0.95 * greedy.makespan
    assert plan.how == "jointly"
    said = jp.lines(plan)
    assert "sooner than each choice made alone" in said[1]


def test_money_is_a_hard_limit():
    cheap = jp.Way("pass", "server to server", 20 * MB, {"source": 1},
                   0.2)
    dear = jp.Way("pipe", "through migkit", 80 * MB, {"link:src": 1},
                  0.0, bills=1.0 / 2 ** 30)
    jobs = _jobs([1000] * 4, ways=(cheap, dear), index=0)
    caps = {"source": 200 * MB, "target": 150 * MB,
            "link:src": 1000 * MB, "link:dst": 100 * MB}
    lim = _limits(workers=16, money=1.5, caps=caps)
    _, first = jp.greedy(jobs, lim)
    assert jp.money(jobs, first) > lim.money
    plan = jp.joint(jobs, lim)
    assert plan.money <= lim.money + 1e-9
    assert plan.meets
    assert list(plan.ways().values()).count("pipe") == 1
    # a budget no way keeps to: said
    lim0 = _limits(workers=16, money=0.0, caps=caps)
    only = _jobs([1000], ways=(dear,), index=0)
    got = jp.joint(only, lim0)
    assert not got.meets and "bills more" in got.why


def test_the_source_share_caps_its_reads():
    jobs = _jobs([2000] * 4)
    free = jp.joint(jobs, _limits(workers=16))
    held = jp.joint(jobs, _limits(workers=16, source_share=0.25))
    assert held.makespan > free.makespan * 1.5
    assert held.binding == "source"


def test_a_deadline_the_plan_cannot_keep_says_what_binds():
    jobs = _jobs([5000] * 4)
    got = jp.joint(jobs, _limits(deadline=10.0))
    assert not got.meets
    assert got.why.startswith("the move needs ~")
    assert "sets it" in got.why
    assert any(line.startswith("NOT WITHIN THE HOP'S LIMITS")
               for line in jp.lines(got))


def test_fewer_workers_where_more_would_only_wait():
    """Three tables of one range each cannot use sixteen workers: the plan
    keeps the fewest that end as soon."""
    jobs = [jp.Job(f"t{i}", 500 * MB, 1000, 0, 1, _ways()[:1])
            for i in range(3)]
    plan = jp.joint(jobs, _limits(workers=16))
    assert plan.workers == 3


# --- the hop's limits ---------------------------------------------------------

def test_the_hops_limits_are_read_and_a_wrong_one_stops(tmp_path):
    hop = _hop(tmp_path, deadline="2h", transfer_price_per_gb=0.09,
               limits={"source_load": "30%", "money": 20,
                       "bytes_billed": "100GB", "target_connections": 10,
                       "target_memory": "256MB", "outage": 30})
    lim = jp.limits_of(hop, None, workers=16)
    assert lim.deadline == 7200.0
    assert lim.source_share == pytest.approx(0.3)
    # 100 GB billed at 0.09 is 9, under the 20 the hop allows
    assert lim.money == pytest.approx(9.0)
    # 10 connections carry 5 writers; 256 MB of the target's memory, 4
    assert lim.workers == 4
    for bad in ({"source_load": "130%"}, {"source_load": "most"},
                {"money": "a lot"}, {"target_memory": "lots"}):
        with pytest.raises(SystemExit):
            jp.limits_of(_hop(tmp_path, limits=bad), None)


def test_the_hops_limits_are_explained(tmp_path):
    from migkit import hopkeys
    hop = _hop(tmp_path, deadline="6h",
               limits={"source_load": 0.3, "replica_lag": 20})
    got = hopkeys.explain_limits(hop)
    assert got[0].startswith("limit deadline 6h: the whole move")
    assert any(x.startswith("limit source_load 0.3: the share") for x in got)
    assert any(x.startswith("limit replica_lag 20:") for x in got)
    assert hopkeys.explain_limits(_hop(tmp_path)) == []
    assert "limits" in hopkeys.HOP_OPTIONS
    assert set(hopkeys.HOP_OPTIONS["limits"].subs) >= {
        "source_load", "money", "outage", "replica_lag"}
    assert set(hopkeys.HOP_OPTIONS["limits"].subs) <= set(hopkeys.LIMITS)


def test_the_cutovers_outage_is_held_to_the_hops(tmp_path):
    from migkit.planner import Phase
    hop = _hop(tmp_path, limits={"outage": 30})
    fits = [Phase("cutover", 10.0, 20.0)]
    assert "keeps within the 30s" in jp.outage_line(hop, fits)
    over = [Phase("cutover", 20.0, 45.0)]
    assert jp.outage_line(hop, over).startswith(
        "NOT WITHIN THE HOP'S LIMITS")
    # the cutover's own budget where limits say none
    hop = _hop(tmp_path, cutover={"budget": 60})
    assert "keeps within the 60s" in jp.outage_line(hop, over)
    assert jp.outage_line(_hop(tmp_path), over) == ""


# --- from the resource model, kept for the move ---------------------------------

def _model():
    from migkit.model import Model, Stage
    return Model([
        Stage("source", "the source's reads", 50e6, 200e6, "measured",
              steps=("copy", "check"), side="src"),
        Stage("target", "the target's writes", 30e6, 90e6, "measured",
              side="dst"),
        Stage("link:src", "the link to the source", 100e6, 300e6,
              "measured", side="src"),
        Stage("link:dst", "the link to the target", 100e6, 300e6,
              "measured", side="dst"),
        Stage("cpu", "this machine's CPUs", 80e6, 600e6, "measured")],
        measured=True, path="its ranges pass server to server, each"
        " digested by both servers")


def test_the_plan_is_kept_and_the_move_starts_its_tables_in_its_order(
        tmp_path):
    hop = _hop(tmp_path)
    facts = {f"public.t{i}": {"bytes": int(2e8 * (i + 1)),
                              "rows": int(1e6 * (i + 1)), "key": True,
                              "index_bytes": int(5e7 * (i + 1))}
             for i in range(6)}
    plan = jp.for_move(hop, _model(), facts, None, 8, "appdb")
    assert plan is not None and plan.placed
    assert {w for w in plan.ways().values()} <= {"as measured",
                                                 "through migkit"}
    order = jp.order_of(hop, "appdb", list(facts))
    assert order == list(plan.order)
    # a plan made for other tables is not the move's order
    assert jp.order_of(hop, "appdb", list(facts) + ["public.new"]) is None
    assert jp.planned_way(hop, "appdb", "public.t0") in (
        "as measured", "through migkit")
    # a way the probes alone measured is a prior with a wide spread
    assert jp.way_of(_model()).spread >= jp.MODEL_SPREAD
    acc = jp.account(hop, _model(), facts, plan)
    assert "bytes" in acc["known"] and "costs" in acc["measured"]
    assert "deadline" in acc["unknown"]


def test_no_plan_where_nothing_was_measured(tmp_path):
    from migkit.model import Model
    assert jp.for_move(_hop(tmp_path), Model(), {"t": {"bytes": 1}},
                       None, 4, "appdb") is None
    assert jp.for_move(_hop(tmp_path), None, {}, None, 4, "appdb") is None


def test_a_strategy_space_feeds_the_plan_its_ways():
    from migkit import decide as d
    R = d.Rung
    parts = [d.Part("write", (R("a", "way a", loads={"source": 1}),
                              R("b", "way b", loads={"target": 2},
                                startup=1.0)))]
    sp = d.space("t", parts, costs=d.Costs({"write/a": 4.0,
                                            "write/b": 5.0}))
    ways = jp.ways_of_space(sp, 100 * MB)
    assert [w.uses for w in ways] == [{"source": 1}, {"target": 2}]
    assert ways[0].per_stream == pytest.approx(100 * MB / 4.0)
    assert ways[1].startup == 1.0
    assert ways[1].per_stream == pytest.approx(100 * MB / 4.0)
