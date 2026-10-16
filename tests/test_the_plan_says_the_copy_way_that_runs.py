"""The plan's choices are the ones the run takes (W14C): the copy way by
the tables' storage is one answer (`scenarios.storage_copy_way`) the plan
says and `physical.choose` / `physical.carry` take; the wide leg's
compression through a relay is chosen by measure, said in the plan and
taken by the copy of the pages; a leg's tables go in the overlap schedule
its measured rates give. Fakes only; no server."""
from types import SimpleNamespace

import pytest

from migkit import decide, legs, movers, physical, planner, scenarios


class _Eng:
    """A pair whose tables are stored as `storage` says, per side."""

    def __init__(self, storage):
        self.storage = storage

    def table_storage(self, side, db):
        return self.storage[side]

    def in_scope(self, db, table):
        return True

    def databases(self):
        return ["shop"]


SHAPES = {
    "heap": {"src": {"public.a": "heap"}, "dst": {}},
    "columnar": {"src": {"public.a": "heap", "public.c": "columnar"},
                 "dst": {}},
    "unread": {"src": "not read here", "dst": "not read here"},
}


@pytest.fixture
def seed(monkeypatch):
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    monkeypatch.setattr(physical, "settings", lambda hop: {"data": "/d"})
    monkeypatch.setattr(physical, "_engine_of", lambda hop: "postgres")
    monkeypatch.setattr(physical, "costs", lambda hop: {})
    monkeypatch.setattr(physical, "facts_for", lambda hop, eng, mode: {})
    monkeypatch.setattr(physical.sourcechange, "put_answers",
                        lambda *a, **k: None)
    climbed, real = [], decide.climb

    def climb(case, candidates, facts=None, **k):
        if case != "the seed":
            return real(case, candidates, facts, **k)
        climbed.append([r.name for r in candidates])
        return SimpleNamespace(rung=candidates[0], reason="climbed",
                               passed=(), ranked=())
    monkeypatch.setattr(physical.decide, "climb", climb)
    return climbed


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_the_plan_and_the_run_take_one_copy_way(seed, shape):
    hop = SimpleNamespace(engine="postgres", goal="")
    eng = _Eng(SHAPES[shape])
    lines = scenarios.copy_lines(hop, eng, "shop")
    rung, why = physical.choose(hop, eng, "full+cdc")
    told = []
    carried = physical.carry(hop, eng, "shop", "full+cdc", False, told.append) \
        if shape == "columnar" else None
    if shape == "heap":
        assert len(lines) == 1 and "pages or parts copied" in lines[0]
        assert rung is not None and seed and rung.name == seed[0][0]
    elif shape == "columnar":
        assert len(lines) == 1 and "rows read and written" in lines[0]
        assert rung is None and not seed
        assert "public.c: stored as columnar" in why
        assert "rows read and written" in why
        assert carried is False
        assert told and "public.c: stored as columnar" in told[0]
    else:
        # nothing read: the plan says nothing of storage, the rungs decide
        assert lines == []
        assert rung is not None and seed


def test_an_innodb_table_is_copied_as_pages_and_another_engine_is_not():
    clone = scenarios.storage_copy_case(decide.Facts({
        "source": {"shop.a": "innodb"}, "target": {}, "not_read": None}))
    assert clone.choice.path == "physical"
    mixed = scenarios.storage_copy_case(decide.Facts({
        "source": {"shop.a": "innodb", "shop.m": "myisam"}, "target": {},
        "not_read": None}))
    assert mixed.choice.path == "logical"
    assert any(d.startswith("shop.m: stored as myisam") for d in mixed.details)


# ---- the relay's compression ------------------------------------------------

@pytest.fixture
def relay(monkeypatch):
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    monkeypatch.setattr(legs, "_relayed", lambda hop: True)
    monkeypatch.setattr(legs, "round_trips", lambda hop, db=None: {
        "src": 80.0, "dst": 1.0})
    monkeypatch.setattr(legs, "servers_seen", lambda hop: None)
    monkeypatch.setattr(legs, "allowed", lambda hop: True)
    monkeypatch.setattr(movers, "cpu_headroom", lambda: 4.0)
    monkeypatch.setattr(physical.decide, "HopCosts",
                        lambda hop, prefix: decide.Costs())
    speed = {}
    monkeypatch.setattr(planner, "link_speed", lambda hop: speed.get("rows"))
    return speed


SAMPLE = b"".join(b"%06d,customer-%06d,active,2026-10-16\n" % (i, i % 50)
                  for i in range(4000))


def test_a_slow_wide_leg_compresses_and_the_pages_take_that_codec(
        relay, monkeypatch):
    monkeypatch.setattr(physical, "_far_tls", lambda hop: "")
    monkeypatch.setattr(physical, "_source_address",
                        lambda hop, opts: ("10.0.0.1", 5432))
    hop = SimpleNamespace()
    relay["rows"] = 5.0
    chosen = legs.relay_choice(hop, sample=SAMPLE, rows=4000)
    assert chosen is not None and chosen.level is not None
    line = legs.plan_line(hop, "pgpull", sample=SAMPLE, rows=4000)
    assert "relay to here" in line and chosen.squeeze.said in line
    way, why = physical.compression(hop, 160000, chosen=chosen)
    assert way is physical.ZSTD and "relay to here" in why
    level = physical.zstd_level(chosen)
    script = physical._pg_script(
        SimpleNamespace(source=SimpleNamespace(host="10.0.0.1", port=5432,
                                               user="u")),
        {"data": "/d"}, True, level)
    assert f"server-zstd:level={level}" in script
    # a source before 15 cannot compress what it sends: the pages go plain
    way, _ = physical.compression(hop, 140000, chosen=chosen)
    assert way is physical.PLAIN


def test_a_fast_leg_sends_plain_and_an_unmeasured_link_chooses_nothing(relay):
    hop = SimpleNamespace()
    relay["rows"] = 1e12
    chosen = legs.relay_choice(hop, sample=SAMPLE, rows=4000)
    assert chosen is not None and chosen.level is None
    way, _ = physical.compression(hop, 160000, chosen=chosen)
    assert way is physical.PLAIN
    assert physical.zstd_level(chosen) == 1
    relay.clear()
    assert legs.relay_choice(hop, sample=SAMPLE, rows=4000) is None
    assert "relay to here" not in legs.plan_line(hop, "pgpull",
                                                 sample=SAMPLE, rows=4000)


# ---- the leg's schedule -----------------------------------------------------

def _model(rate=100.0, little=2):
    bound = lambda v: SimpleNamespace(bound=lambda: v)
    return SimpleNamespace(measured=True, little=little, carried=0,
                           rate=lambda n: rate,
                           stages={"target": bound(50.0),
                                   "source": bound(200.0)})


def test_the_legs_tables_go_in_the_schedule_that_overlaps_read_and_send():
    known = {"public.big": {"bytes": 4000, "rows": 40},
             "public.mid": {"bytes": 2000, "rows": 20},
             "public.small": {"bytes": 200, "rows": 2}}
    tables = ["public.small", "public.mid", "public.big"]
    plan = legs.schedule(SimpleNamespace(), "shop", known, tables, 2,
                         model=_model())
    assert plan is not None
    sent_end = max(e for _, _, e in plan.copies)
    # a table's check reads the source while another table is still sent
    assert plan.checks and min(c[-2] for c in plan.checks) < sent_end
    assert plan.makespan < plan.serial
    work = [("public.small", "s1"), ("public.mid", "m1"),
            ("public.big", "b1"), ("public.big", "b2")]
    ordered = legs.in_schedule(work, plan)
    assert sorted(ordered) == sorted(work)
    assert [w[0] for w in ordered][:2] == ["public.big", "public.big"]
    # nothing measured: no schedule, the plan's order stands
    assert legs.schedule(SimpleNamespace(), "shop", known, tables, 2,
                         model=SimpleNamespace(measured=False)) is None
    assert legs.in_schedule(work, None) == work
