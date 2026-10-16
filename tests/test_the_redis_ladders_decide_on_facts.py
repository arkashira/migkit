"""The Redis ladders' rungs each held and refused on the facts that decide
them (backlog AUD-Z.rungs-owed): a tail follows as a replica only with the
grant, one server and the room measured, else sweeps by idle time only
where idle times are kept; two sites written at once are made alike by the
overlap measured and the rule the hop states, never one site's write
silently over the other's. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("redis_live", "replica stream", "holds"),
    ("redis_live", "replica stream", "refuses"),
    ("redis_live", "idle-time sweep", "holds"),
    ("redis_live", "idle-time sweep", "refuses"),
    ("two_site_rule", "apart", "holds"),
    ("two_site_rule", "apart", "refuses"),
    ("two_site_rule", "merged", "holds"),
    ("two_site_rule", "merged", "refuses"),
    ("two_site_rule", "merged, else newest", "holds"),
    ("two_site_rule", "merged, else newest", "refuses"),
    ("two_site_rule", "merged, else held", "holds"),
    ("redis_site_rule", "apart", "holds"),
    ("redis_site_rule", "apart", "refuses"),
    ("redis_site_rule", "merged", "holds"),
    ("redis_site_rule", "merged", "refuses"),
    ("redis_site_rule", "newest", "holds"),
    ("redis_site_rule", "newest", "refuses"),
]


def _live(**over):
    facts = {"replica grant": True, "cluster": False, "headroom": 10_000,
             "stream needs": 4_000, "idletime": True}
    facts.update(over)
    return facts


def test_a_tail_follows_as_a_replica_where_granted_alone_and_with_room():
    with watched() as seen:
        got = decide.redis_live(_live())
    assert got.path == "replica stream"
    assert branch(seen, "replica stream") == [HOLDS]
    assert branch(seen, "idle-time sweep") == [HOLDS]


def test_a_tail_without_the_stream_sweeps_and_without_idle_times_stops():
    with watched() as seen:
        got = decide.redis_live(_live(headroom=1_000))
    assert got.path == "idle-time sweep"
    assert why(seen, "replica stream") == ("the replica stream needs 4,000"
                                           " bytes free on the source, and"
                                           " 1,000 are")
    with watched() as seen:
        got = decide.redis_live(_live(cluster=True, idletime=False))
    assert got.path is None
    assert "the source is a cluster" in why(seen, "replica stream")
    assert "evicts by how often keys are read" in why(seen,
                                                      "idle-time sweep")
    with watched() as seen:
        decide.redis_live(_live(**{"replica grant": False}))
    assert "psync, replconf and ping" in why(seen, "replica stream")


def test_two_sites_are_carried_apart_merged_or_held_by_the_overlap():
    with watched() as seen:
        got = decide.two_site_rule({"overlap": 0, "unmergeable": 0,
                                    "skew_ms": 3})
    assert got.path == "apart"
    for name in ("apart", "merged", "merged, else newest",
                 "merged, else held"):
        assert branch(seen, name) == [HOLDS], name
    with watched() as seen:
        got = decide.two_site_rule({"overlap": 1200, "unmergeable": 0})
    assert got.path == "merged"
    assert why(seen, "apart") == "1,200 keys were written on both sites"
    assert "needs the two clocks' skew measured" in why(
        seen, "merged, else newest")
    with watched() as seen:
        got = decide.two_site_rule({"overlap": 9, "unmergeable": 4,
                                    "skew_ms": None})
    assert got.path == "merged, else held"
    assert why(seen, "merged") == ("4 keys written on both are neither"
                                   " counters nor sets")
    assert branch(seen, "merged, else newest") == [REFUSES]


def test_the_stated_rule_settles_two_sites_only_where_it_holds():
    with watched() as seen:
        got = decide.redis_site_rule({"overlap": 0, "stated": "delta"})
    assert got.path == "apart"
    assert branch(seen, "merged") == [HOLDS]
    assert why(seen, "newest") == "the hop states delta, not newest"
    with watched() as seen:
        got = decide.redis_site_rule({"overlap": 7, "stated": " Newest ",
                                      "skew_ms": 2})
    assert got.path == "newest"
    assert why(seen, "apart") == "7 keys were written on both sites"
    assert why(seen, "merged") == ("the hop states newest, not delta or"
                                   " union")
    with watched() as seen:
        got = decide.redis_site_rule({"overlap": 7, "stated": "newest"})
    assert got.path is None
    assert "clocks were not measured" in why(seen, "newest")
    with watched() as seen:
        got = decide.redis_site_rule({"overlap": 7})
    assert got.path is None
    assert why(seen, "merged") == "the hop states no rule, not delta or union"
