from migkit import scenarios, topology
from migkit.topology import KeySpace, Route

TABLES = {"orders": {"key": ["id"], "columns": ["id", "total"]},
          "items": {"key": ["id"], "columns": ["id", "sku"]}}


def _merge(spaces, conflict=None):
    return scenarios.topology_facts("many-to-one", ("a", "b", "c"), ("hub",),
                                    TABLES, spaces=spaces, conflict=conflict)


def _strides(n):
    return {s: {t: {"stride": [i + 1, n]} for t in TABLES}
            for i, s in enumerate(("a", "b", "c")[:n])}


def test_strides_and_ranges_are_solved_apart_or_met():
    assert topology.common({"stride": [1, 2]}, {"stride": [2, 2]}) is None
    assert topology.common({"stride": [1, 4]}, {"stride": [3, 6]}) == 9
    assert topology.common({"stride": [1, 4], "range": [10, 20]},
                           {"stride": [3, 6]}) is None
    assert topology.common({"stride": [1, 4], "range": [10, 30]},
                           {"stride": [3, 6]}) == 21
    assert topology.common({"stride": [1, 4], "range": [10, 20]},
                           {"stride": [3, 6], "range": [0, 15]}) is None
    assert topology.common({"range": [0, 99]}, {"range": [100, 199]}) is None
    assert topology.common({"range": [0, 100]}, {"range": [100, 199]}) == 100
    assert topology.common({"values": [5, 7]}, {"stride": [1, 2]}) == 5
    assert topology.common({"values": [4, 8]}, {"stride": [1, 2]}) is None
    assert topology.common(None, {"range": [3, 4]}) == 3
    assert KeySpace.of({"stride": [2, 3]}).holds(5)
    assert not KeySpace.of({"stride": [2, 3]}).holds(6)


def test_many_to_one_with_keys_apart_lands_each_row_once():
    facts = _merge(_strides(3))
    got = scenarios.topology_case(facts)
    assert got.choice.path == "union-apart"
    assert dict(got.choice.guarantees)["exactly once"] == "exactly once"
    assert "deletes are scoped to the source" in got.words()
    hops = scenarios.topology_hops(facts, got)
    assert [(h["source"], h["target"], h["origin"]) for h in hops] == [
        ("a", "hub", "a"), ("b", "hub", "b"), ("c", "hub", "c")]
    assert hops[1]["keys"]["orders"] == {"stride": [2, 3]}


def test_overlapping_key_spaces_with_no_rule_are_refused():
    spaces = {"a": {"orders": {"range": [1, 100]}},
              "b": {"orders": {"range": [50, 150]}},
              "c": {"orders": {"range": [200, 300]}}}
    got = scenarios.topology_case(_merge(spaces))
    assert got.choice.rung is None
    assert "orders: a and b both write key 50" in got.choice.reason
    assert "items" in got.choice.reason
    assert scenarios.topology_hops(_merge(spaces), got) == ()
    refused = scenarios.topology_case(_merge(spaces, conflict="error"))
    assert refused.choice.rung is None


def test_a_conflict_rule_settles_meeting_keys_and_says_what_it_gives_up():
    facts = _merge({}, conflict="last_update_wins")
    got = scenarios.topology_case(facts)
    assert got.choice.path == "union-settled"
    assert dict(got.choice.guarantees)["exactly once"] == "at least once"
    assert "gives up: exactly once: at least once" in got.words()
    assert all(h["settle"] == "last_update_wins"
               for h in scenarios.topology_hops(facts, got))


def test_the_shape_is_fenced_before_the_keys():
    facts = scenarios.topology_facts("many-to-one", ("a",), ("hub",), TABLES)
    got = scenarios.topology_case(facts)
    assert got.choice.rung is None
    assert "needs many sources and one target" in got.choice.reason
    unknown = scenarios.topology_case(
        scenarios.topology_facts("ring", ("a", "b"), ("a", "b"), TABLES))
    assert unknown.choice.rung is None and "'ring' is not one of" in unknown.choice.reason


def test_the_union_is_proved_and_order_per_source_held():
    facts = _merge(_strides(2))
    sources = {"a": {"orders": {(1,): "x1", (3,): "x3"}},
               "b": {"orders": {(2,): "y2"}}}
    good = scenarios.topology_verify(facts, {
        "sources": sources, "target": {"orders": {(1,): "x1", (3,): "x3", (2,): "y2"}},
        "applied": [("a", 1), ("b", 1), ("a", 2), ("b", 5)]})
    assert good.ok, good.lines
    lost = scenarios.topology_verify(facts, {
        "sources": sources, "target": {"orders": {(1,): "x1", (2,): "zz", (9,): "q"}},
        "applied": [("a", 2), ("a", 1)]})
    assert not lost.ok
    said = " ".join(lost.lines)
    assert "1 missing" in said and "1 differs" in said and "1 extra" in said
    assert "out of their source's order" in said


def test_two_sources_writing_one_key_fail_the_union_unless_settled():
    sources = {"a": {"orders": {(1,): "x"}}, "b": {"orders": {(1,): "y"}}}
    rows = {"sources": sources, "target": {"orders": {(1,): "y"}}}
    assert not scenarios.topology_verify(_merge({}), rows).ok
    assert scenarios.topology_verify(_merge({}, "keep_local"), rows).ok
    bad = dict(rows, target={"orders": {(1,): "neither"}})
    assert not scenarios.topology_verify(_merge({}, "keep_local"), bad).ok


def _fan(route, tables=TABLES):
    return scenarios.topology_facts("one-to-many", ("src",), ("t0", "t1", "t2"),
                                    tables, route=route)


def test_fan_out_takes_the_rule_the_hop_names():
    by_table = _fan({"by": "table", "tables": {"orders": "t0", "items": "t2"}})
    got = scenarios.topology_case(by_table)
    assert got.choice.path == "fanout-by-table"
    assert "slowest target" in got.words()
    hops = scenarios.topology_hops(by_table, got)
    assert {h["target"]: h["tables"] for h in hops} == {
        "t0": ["orders"], "t1": [], "t2": ["items"]}
    assert scenarios.topology_case(
        _fan({"by": "range", "bounds": [100, 200]})).choice.path == "fanout-by-range"
    assert scenarios.topology_case(_fan({"by": "hash"})).choice.path == "fanout-by-hash"
    assert scenarios.topology_case(_fan({})).choice.path == "fanout-by-hash"


def test_a_fan_out_rule_that_leaves_rows_nowhere_is_refused():
    half = scenarios.topology_case(_fan({"by": "table", "tables": {"orders": "t0"}}))
    assert half.choice.rung is None and "no target named for items" in half.choice.reason
    gaps = scenarios.topology_case(_fan({"by": "range", "bounds": [100]}))
    assert gaps.choice.rung is None and "1 bounds cut 2 ranges for 3 targets" in gaps.choice.reason
    falling = scenarios.topology_case(_fan({"by": "range", "bounds": [200, 100]}))
    assert falling.choice.rung is None and "not in rising order" in falling.choice.reason
    keyless = dict(TABLES, logs={"key": [], "columns": ["line"]})
    assert scenarios.topology_case(_fan({"by": "hash"}, keyless)).choice.rung is None


def test_the_partition_is_proved_against_the_rule():
    route = Route("range", ("t0", "t1", "t2"), bounds=(100, 200))
    assert route.place("orders", (5,)) == "t0"
    assert route.place("orders", (100,)) == "t1"
    assert route.place("orders", (250,)) == "t2"
    facts = _fan({"by": "range", "bounds": [100, 200]})
    whole = {"orders": {(5,): "a", (150,): "b", (250,): "c"}}
    good = {"t0": {"orders": {(5,): "a"}}, "t1": {"orders": {(150,): "b"}},
            "t2": {"orders": {(250,): "c"}}}
    assert scenarios.topology_verify(facts, {"source": whole, "targets": good}).ok
    wrong = {"t0": {"orders": {(5,): "a", (150,): "b"}}, "t1": {"orders": {(150,): "b"}},
             "t2": {"orders": {}}}
    proof = topology.prove_partition(whole, wrong, route)
    assert proof.twice == [("orders", (150,), ("t0", "t1"))]
    assert proof.missing == [("orders", (250,))]
    assert not scenarios.topology_verify(facts, {"source": whole, "targets": wrong}).ok


def test_hash_fan_out_places_each_key_once_and_alike_every_run():
    route = Route("hash", ("t0", "t1", "t2"))
    keys = [(str(i),) for i in range(300)]
    placed = [route.place("orders", k) for k in keys]
    assert placed == [route.place("orders", k) for k in keys]
    assert set(placed) == {"t0", "t1", "t2"}
    whole = {"orders": {k: "d" + k[0] for k in keys}}
    parts = {t: {"orders": {}} for t in route.targets}
    for k, t in zip(keys, placed):
        parts[t]["orders"][k] = "d" + k[0]
    assert topology.prove_partition(whole, parts, route).ok
    moved = keys[0]
    other = next(t for t in route.targets if t != placed[0])
    parts[other]["orders"][moved] = parts[placed[0]]["orders"].pop(moved)
    assert topology.prove_partition(whole, parts, route).misplaced


NODES = ("n1", "n2", "n3")


def _mesh(links=None, marks=None, spaces=None, conflict=None):
    return scenarios.topology_facts(
        "mesh", NODES, NODES, TABLES,
        spaces=spaces if spaces is not None else {
            n: {t: {"stride": [i, 3]} for t in TABLES} for i, n in enumerate(NODES)},
        conflict=conflict,
        marks=marks if marks is not None else {n: "origin" for n in NODES},
        links=topology.mesh_links(NODES) if links is None else links)


def test_a_full_mesh_tags_origins_and_links_every_pair():
    facts = _mesh()
    got = scenarios.topology_case(facts)
    assert got.choice.path == "mesh-apart"
    hops = scenarios.topology_hops(facts, got)
    assert len(hops) == 6 and all(h["origin"] == h["source"] for h in hops)
    assert {(h["source"], h["target"]) for h in hops} == set(topology.mesh_links(NODES))


def test_a_forwarding_ring_and_an_unmarked_node_are_refused():
    ring = [("n1", "n2"), ("n2", "n3"), ("n3", "n1")]
    got = scenarios.topology_case(_mesh(links=ring))
    assert got.choice.rung is None and "forwarding ring" in got.choice.reason
    assert "n2 to n1" in got.choice.reason
    blind = scenarios.topology_case(_mesh(marks={"n1": "origin", "n2": "origin"}))
    assert blind.choice.rung is None and "no mark of migkit's own writes on n3" in blind.choice.reason
    meet = scenarios.topology_case(_mesh(spaces={}))
    assert meet.choice.rung is None and "both write key" in meet.choice.reason
    assert scenarios.topology_case(_mesh(spaces={}, conflict="apply_remote")).choice.path == "mesh-settled"


def test_a_mesh_is_proved_converged_with_no_change_come_round():
    rows = {"orders": {(1,): "a", (2,): "b"}}
    facts = _mesh()
    same = {"nodes": {n: rows for n in NODES},
            "applied": [("n2", "n1", "n1", 7), ("n3", "n1", "n1", 7)]}
    assert scenarios.topology_verify(facts, same).ok
    echo = dict(same, applied=same["applied"] + [("n1", "n2", "n1", 7)])
    came = scenarios.topology_verify(facts, echo)
    assert not came.ok and "came round" in " ".join(came.lines)
    passed_on = dict(same, applied=[("n3", "n2", "n1", 7)])
    assert not scenarios.topology_verify(facts, passed_on).ok
    apart = {"nodes": {"n1": rows, "n2": rows, "n3": {"orders": {(1,): "a"}}}}
    split = scenarios.topology_verify(facts, apart)
    assert not split.ok and "1 missing" in " ".join(split.lines)
