"""One way of deciding, for every choice migkit makes (backlog P0).

A rung says what it gives, needs, how it is proved and what it cost; the
climb drops what lacks, ranks by measurement (never measured first, so each
gets its number once; the footprint only breaks a tie), proves the top one
and falls a rung with the reason said. The engine itself is pinned here on
rungs built for the purpose, and the four choices that go through it - the
mover, the plan by table, the verify way, the servers' two ways - on their
own lists, with no server. Their live behaviour stays pinned by their own
tests.
"""
import os
import re
from pathlib import Path

import pytest

from migkit import decide as d
from migkit.config import Endpoint, Hop
from tests.test_the_report_does_not_name_its_tools import TOOLS

R = d.Rung
BACKLOG = (Path(__file__).parent.parent / "docs" / "backlog.md").read_text()


def _hop(tmp_path=None, engine="postgres"):
    hop = Hop(name="h", engine=engine,
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"),
              databases=["appdb"])
    if tmp_path is not None:
        hop.report_dir = lambda db="", _p=tmp_path: _p
    return hop


def _ways():
    return [R("digest", "digesting each range"),
            R("read back", "reading each range back")]


def test_never_measured_first_then_the_cheaper_as_the_verify_way_did():
    c = d.Costs()
    assert d.climb("range", _ways(), costs=c).path == "digest"
    c.saw("digest", 400000, 2.4)
    assert d.climb("range", _ways(), costs=c).path == "read back"
    c.saw("read back", 400000, 1.9)
    assert d.climb("range", _ways(), costs=c).path == "read back"
    # the measurement from the copier's own comment, 20 MB/s a connection
    c.saw("read back", 400000, 4.4)
    c.saw("digest", 400000, 3.2)
    got = d.climb("range", _ways(), costs=c)
    assert got.path == "digest" and "measured the cheapest" in got.reason


def test_the_footprint_breaks_a_tie_and_never_ranks():
    light = R("table", "the table", footprint=1)
    heavy = R("slot", "a slot", footprint=2)
    # within the tie: the smaller footprint, whatever the list says
    c = d.Costs({"table": 1.0, "slot": 0.97})
    assert d.climb("side", [heavy, light], costs=c).path == "table"
    # measured faster with a footprint wins: the owner's rule
    c = d.Costs({"table": 1.0, "slot": 0.5})
    assert d.climb("side", [light, heavy], costs=c).path == "slot"


def test_a_rung_lacking_a_capability_is_dropped_with_the_planners_words():
    bulk = R("bulk", "the bulk copy", because="the fastest path here")
    cop = R("copier", "the table copier",
            gives=frozenset({"row filter", "column mapping"}))
    got = d.climb("public.orders", [bulk, cop], need=("row filter",))
    assert got.path == "copier"
    assert got.reason == ("its row filter is applied on both sides, which"
                          " the bulk copy cannot do")
    assert d.climb("t", [bulk, cop]).reason == "the fastest path here"
    assert str(got).startswith("public.orders: the table copier - its row")


def test_a_way_below_the_chosen_one_is_no_reason_for_it():
    bulk = R("bulk", "the bulk copy", because="the fastest path here")
    never = R("never", "a way that never holds",
              needs=(d.Need("nothing", lambda f: False),))
    assert d.climb("t", [bulk, never]).reason == "the fastest path here"
    got = d.climb("t", [never, bulk])
    assert got.reason == "a way that never holds needs nothing"


def test_a_need_says_its_own_sentence():
    n = d.Need("MySQL into PostgreSQL",
               lambda f: f.get("pair") == ("mysql", "postgres"),
               "{rung} reads MySQL into PostgreSQL only")
    one = R("one-pass", "the one-pass load", needs=(n,))
    got = d.climb("appdb", [one, R("builtin", "the table copier")],
                  facts={"pair": ("postgres", "mysql")})
    assert got.path == "builtin"
    assert got.reason == "the one-pass load reads MySQL into PostgreSQL only"
    assert d.climb("appdb", [one], facts={"pair": ("mysql", "postgres")}
                   ).path == "one-pass"


def test_a_fact_is_read_once_and_only_for_a_rung_still_in_the_running():
    asked = []

    def server():
        asked.append(1)
        return "16"
    facts = d.Facts({"engine": "mysql"}, version=server)
    pg = R("pg", "a way for PostgreSQL", needs=(
        d.Need("PostgreSQL", lambda f: f["engine"] == "postgres"),
        d.Need("16", lambda f: f["version"] == "16")))
    d.climb("t", [pg, R("any", "any way")], facts)
    assert asked == []
    facts = d.Facts({"engine": "postgres"}, version=server)
    twice = [pg, R("pg2", "another", needs=pg.needs), R("any", "any way")]
    assert d.climb("t", twice, facts).path == "pg"
    d.climb("t", twice[1:], facts)
    assert asked == [1]
    assert facts.get("nothing", "default") == "default"


def test_a_failed_proof_falls_a_rung_and_says_so():
    flaky = R("tag", "a tag of migkit's own",
              prove=lambda f: "the probe did not come back marked")
    got = d.climb("side", [flaky, R("table", "the table")])
    assert got.path == "table"
    assert got.reason == ("a tag of migkit's own did not prove itself: the"
                          " probe did not come back marked")
    none = d.climb("side", [flaky])
    assert none.rung is None and none.path is None
    assert "did not prove itself" in none.reason


def test_a_kept_rung_is_climbed_again_and_a_gone_one_is_said():
    a, b = R("a", "the first"), R("b", "the second")
    got = d.climb("side", [a, b], keep="b")
    assert got.path == "b" and got.reason.startswith("the way the last run")
    gone = R("b", "the second", needs=(d.Need("a grant", lambda f: False),))
    got = d.climb("side", [a, gone], keep="b")
    assert got.path == "a"
    assert got.reason == "the way the last run took: the second needs a grant"


def test_the_choice_is_kept_beside_the_position(tmp_path):
    pos = tmp_path / "move.json"
    assert d.remembered(pos, "t") is None
    d.remember(pos, "t", "digest")
    # a position that is gone takes its way with it
    assert d.remembered(pos, "t") is None
    pos.write_text("{}")
    assert d.remembered(pos, "t") == "digest"
    assert os.path.exists(tmp_path / "move-ways.json")
    d.forget(pos)
    assert d.remembered(pos, "t") is None


def test_a_restart_climbs_the_rung_the_last_run_stood_on(tmp_path):
    """The first run takes the top rung and keeps it beside the tail's
    token; the next run, with a rung above it now holding, stays on it
    until the token is gone."""
    token = tmp_path / "tail-token.json"
    grant = {"on": False}
    tag = R("tag", "a tag of migkit's own",
            needs=(d.Need("the grant", lambda f: grant["on"]),))
    table = R("table", "the table", footprint=1)
    assert d.choose_kept(token, "src", [tag, table]).path == "table"
    token.write_text("{}")
    grant["on"] = True
    got = d.choose_kept(token, "src", [tag, table])
    assert got.path == "table" and "last run" in got.reason
    token.unlink()
    assert d.choose_kept(token, "src", [tag, table]).path == "tag"


def test_the_hops_costs_live_with_its_rates(tmp_path):
    hop = _hop(tmp_path)
    kept = d.HopCosts(hop, "verify way/")
    assert kept.get("digest") is None
    kept.saw("digest", 400000, 2.4)
    kept.saw("read back", 400000, 1.9)
    again = d.HopCosts(hop, "verify way/")
    assert again.get("digest") == pytest.approx(2.4 / 400000)
    assert d.climb("range", _ways(), costs=again).path == "read back"
    # the rates the plan estimates from are untouched by another prefix
    from migkit import planner
    assert planner.measured_rate(hop, "digest") is None


def _passes(facts_stream=False):
    """Verification passes A-D (backlog F1) as parts."""
    same = d.Need("the same build on both sides", lambda f: f["same build"])
    int_key = d.Need("one integer key ordered alike on both sides",
                     lambda f: f["key"] == "integer")
    slow = d.Need("a link that is the limit", lambda f: f["slow link"])
    stream = d.Need("a change stream to mark leaves", lambda f: f["stream"])

    def after(part, *names):
        return lambda ch: ch[part].name in names or (
            f"it reads what {part} {ch[part].name} does not give")
    return [
        d.Part("prove", (
            R("md5", "a salted row hash", gives=frozenset({"byte exact"})),
            R("record hash", "the server's own row hash", needs=(same,)))),
        d.Part("leaves", (
            R("range", "range buckets", needs=(int_key,)),
            R("hash", "hash buckets", gives=frozenset({"keyless"})))),
        d.Part("sketch", (
            R("iblt", "a sketch of the differences", needs=(slow,),
              prove=lambda f: f.get("iblt fails")),
            R("none", "no sketch"))),
        d.Part("localize", (
            R("decode", "the sketch decoded", fits=after("sketch", "iblt")),
            R("range reads", "reading the differing ranges",
              fits=after("leaves", "range")),
            R("scan", "one filtered scan", fits=after("leaves", "hash")))),
        d.Part("keep", (
            R("generations", "generations of dirty leaves", needs=(stream,),
              footprint=1),
            R("rescan", "a full rescan"))),
    ]


def test_passes_a_to_d_are_composed_from_the_facts():
    keyless = {"same build": False, "key": None, "slow link": False,
               "stream": False}
    s = d.compose("public.log", _passes(), keyless, need=("keyless",))
    assert s.names == {"prove": "md5", "leaves": "hash", "sketch": "none",
                       "localize": "scan", "keep": "rescan"}, s.names
    assert s.reasons["leaves"] == ("the table has no key, which"
                                  " range buckets cannot carry"), s.reasons
    keyed = {"same build": True, "key": "integer", "slow link": True,
             "stream": True}
    s = d.compose("public.orders", _passes(), keyed)
    assert s.names == {"prove": "md5", "leaves": "range", "sketch": "iblt",
                       "localize": "decode", "keep": "generations"}
    # the sketch that does not add up is passed over, and the localizing
    # that read it goes with it
    s = d.compose("public.orders", _passes(), {**keyed, "iblt fails":
                                               "the decode did not add up"})
    assert s.names["sketch"] == "none" and s.names["localize"] == \
        "range reads", s.names
    assert "did not prove itself" in s.reasons["sketch"], s.reasons


def test_a_strategy_is_ranked_by_its_parts_measured_costs():
    facts = {"same build": True, "key": "integer", "slow link": False,
             "stream": False}
    c = d.Costs({"prove/md5": 3.0, "prove/record hash": 1.0,
                 "leaves/range": 1.0, "leaves/hash": 1.0,
                 "sketch/none": 0.0, "localize/range reads": 1.0,
                 "localize/scan": 2.0, "keep/rescan": 1.0})
    s = d.compose("t", _passes(), facts, costs=c)
    assert s.names["prove"] == "record hash", s.names
    # the final proof must hold byte for byte: the cheaper hash cannot
    s = d.compose("t", _passes(), facts, need=("byte exact",), costs=c)
    assert s.names["prove"] == "md5", s.names
    none = d.compose("t", _passes(), facts, need=("apart",))
    assert not none and "apart" in none.why_not


def test_every_shape_has_a_way_or_an_honest_gap_on_every_engine():
    assert d.uncovered() == []
    cov = d.coverage()
    from migkit.engines import NAMES
    assert set(cov) == set(NAMES)
    assert cov["postgres"]["keyless"] == "yes"
    assert cov["generic"]["keyless"] == d.NOT_YET
    assert cov["mongodb"]["keyless"] == d.NOT_APPLICABLE


def test_a_gap_carries_its_reason_or_its_backlog_item():
    for shape, ways in d.WAYS.items():
        assert shape in d.SHAPES
        for engine, (state, why) in ways.items():
            if state == d.NOT_YET:
                assert re.search(rf"^(\*\*|### ){re.escape(why)}\. ",
                                 BACKLOG, re.M), (shape, engine, why)
            elif state == d.NOT_APPLICABLE:
                assert len(why.split()) >= 4, (shape, engine, why)


def test_a_way_removed_from_the_code_turns_its_cell_stale(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.delattr(PostgresEngine, "_copy_in_spans")
    assert ("postgres", "keyless") in d.uncovered()
    assert ("postgres", "wide key") in d.uncovered()


def test_an_undeclared_cell_is_noticed(monkeypatch):
    monkeypatch.setitem(d.WAYS, "slow link", {})
    assert ("postgres", "slow link") in d.uncovered()


# --- the four choices, on their own lists -----------------------------------

def test_the_mover_ladder_names_no_program(monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "which", lambda n: None)
    monkeypatch.setattr(movers, "pgcopydb_available", lambda: False)
    got = d.climb("appdb", movers._ladder(), {"engine": "postgres"})
    assert got.path == "builtin"
    said = [r.said for r in movers._ladder()] + [w for _, w in got.passed]
    assert "the streaming copy needs its version-matched build" in said
    assert not [t for t in TOOLS for s in said if t in s.lower()], said


def test_the_mover_is_the_first_way_whose_programs_are_here(monkeypatch):
    from migkit import movers
    asked = []
    monkeypatch.setattr(movers, "which", lambda n: "/bin/" + n)
    monkeypatch.setattr(movers, "pgcopydb_available",
                        lambda: asked.append(1) or False)
    assert movers.pick("postgres") == "pgdump"
    assert movers.pick("mysql") == "mydumper"
    assert movers.pick("hetero") == "pgloader"
    assert movers.pick("mongodb") == "mongosync"
    assert movers.pick("sqlite") == "native"
    assert movers.pick("redis") == "builtin"
    assert movers.pick("mysql", table="app.t") == "builtin"
    # the container is asked about only for a PostgreSQL hop
    assert asked == [1]


def test_the_fit_falls_down_the_same_ladder(monkeypatch):
    from migkit import movers
    monkeypatch.setattr(movers, "which",
                        lambda n: "/bin/" + n if n.startswith("mongo")
                        else None)
    monkeypatch.setattr(movers, "_mongosync_unfit",
                        lambda hop: "the source is older than MongoDB 6.0")
    hop = _hop(engine="mongodb")
    assert movers.fitted(hop, "mongodb", "mongosync") == (
        "mongodump", "the source is older than MongoDB 6.0")
    monkeypatch.setattr(movers, "which", lambda n: None)
    assert movers.fitted(hop, "mongodb", "mongosync") == (
        "builtin", "the source is older than MongoDB 6.0")
    monkeypatch.setattr(movers, "_mongosync_unfit", lambda hop: None)
    assert movers.fitted(hop, "mongodb", "mongosync") == ("mongosync", None)


def test_the_plan_climbs_the_ways_a_table_can_go():
    from migkit import planner
    hop = Hop(name="p", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["appdb"], exclude=["audit"],
              mapping={"where": {"orders": "region = 'apac'"},
                       "columns": {"orders": {"drop": ["x"]},
                                   "people": {"drop": ["y"]}}})
    got = {x.table: x for x in planner.plan(
        hop, "appdb", "pgdump",
        ["public.orders", "public.audit", "public.people", "public.plain"])}
    assert (got["public.audit"].path, got["public.audit"].reason) == (
        planner.LEFT, "the hop excludes it")
    # both a filter and a mapping: the filter is said, as it always was
    assert got["public.orders"].reason.startswith("its row filter")
    assert got["public.people"].reason.startswith("its columns are mapped")
    assert (got["public.plain"].path, got["public.plain"].reason) == (
        planner.BULK, "the fastest path here")
    # a bulk path that filters rows itself keeps the filtered table
    mine = {x.table: x for x in planner.plan(hop, "appdb", "mydumper",
                                             ["orders"], qualifier=None)}
    assert mine["orders"].reason.startswith("its columns are mapped")


def _pg(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    return PostgresEngine(_hop(tmp_path))


def test_the_verify_way_is_a_climb_on_the_runs_own_costs(tmp_path):
    eng = _pg(tmp_path)
    assert eng._verify_way() == "digest"
    eng._verify_took("digest", 2.4, 400000)
    assert eng._verify_way() == "read back"
    eng._verify_took("read back", 1.9, 400000)
    assert eng._verify_way() == "read back"
    eng._verify_took("read back", 4.4, 400000)
    assert eng._verify_way() == "digest"
    # a run of its own starts over: the numbers are this run's
    assert _pg(tmp_path)._verify_way() == "digest"


class _Said:
    """A MySQL pair as `loops_prevented` reads it: each side's settings,
    and the count of auto-increment columns, counted when asked."""

    def __init__(self, src, dst, keyed=1):
        self.said = {"src": src, "dst": dst}
        self.keyed = keyed
        self.counted = 0

    def __call__(self, side, sql, args=None):
        if "count(*)" in sql:
            self.counted += 1
            return [[self.keyed]]
        name = sql.split("@@", 1)[1]
        if name not in self.said[side]:
            raise RuntimeError("unknown variable")
        return [[self.said[side][name]]]


_GOOD = {"server_id": "1", "log_replica_updates": "1",
         "replicate_same_server_id": "0", "gtid_mode": "ON",
         "auto_increment_increment": "2", "auto_increment_offset": "1"}


def _loops(src=None, dst=None, keyed=1):
    from migkit.engines.mysql import MySQLEngine
    eng = MySQLEngine(_hop(engine="mysql"))
    def kept(d):
        return {k: v for k, v in d.items() if v is not None}
    said = _Said(kept({**_GOOD, **(src or {})}),
                 kept({**_GOOD, "server_id": "2",
                       "auto_increment_offset": "2", **(dst or {})}), keyed)
    eng._q = said
    return eng.loops_prevented("app"), said


def test_two_ways_that_hold_say_nothing():
    why, said = _loops()
    assert why == "" and said.counted == 1


@pytest.mark.parametrize("src,dst,starts", [
    ({}, {"server_id": "1"}, "both sides have server_id 1: each would"),
    ({"replicate_same_server_id": "1"}, {},
     "the source applies changes carrying its own server id"),
    ({}, {"gtid_mode": "OFF"}, "GTIDs are OFF on the target: without them"),
    ({"log_replica_updates": "OFF"}, {},
     "the source does not pass on what it applies"),
    ({}, {"auto_increment_offset": "1"},
     "both sides hand out the same auto-increment values (increment 2 and"
     " 2, offset 1 and 1)"),
])
def test_what_would_go_round_or_collide_is_the_first_need_missing(
        src, dst, starts):
    why, said = _loops(src, dst)
    assert why.startswith(starts), why
    # the columns are counted only once the settings allow two ways
    assert said.counted == (1 if "auto-increment" in starts else 0)


def test_the_old_name_of_a_setting_is_read_where_the_new_is_missing():
    """MySQL before 8.0.26 knows only `log_slave_updates`."""
    why, _ = _loops({"log_replica_updates": None,
                     "log_slave_updates": "0"})
    assert why.startswith("the source does not pass on"), why


def test_a_database_without_auto_increment_needs_no_offsets():
    why, _ = _loops({}, {"auto_increment_offset": "1"}, keyed=0)
    assert why == ""


def test_nothing_here_names_a_program():
    import inspect
    text = inspect.getsource(d).lower()
    assert not [t for t in TOOLS if t in text]
    for shape in d.SHAPES.values():
        assert not [t for t in TOOLS if t in shape.lower()]
