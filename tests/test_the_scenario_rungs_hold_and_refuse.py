"""The rungs the decided scenarios are chosen by - a provider's managed
path, the architecture change, a shard split or merge, the topologies of
many nodes and the copy way by storage - each reached on the branches it
has through the case itself, on fakes (backlog
AUD-Z.rungs-outside-owed): a way the facts allow is held, one they do not
is refused with its own reason. `topology-unknown` is a blocker built as
a rung whose need never holds: it is proved refused here, and its held
branch stays owed as the defect it is."""

from migkit import decide, scenarios
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why
from tests.test_decided_scenarios import NEVER, hosts, managed
from tests.test_many_node_topologies_are_decided import (
    TABLES, _fan, _merge, _strides)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('scenarios', '_apart', '<name>', 'holds'),
    ('scenarios', '_apart', '<name>', 'refuses'),
    ('scenarios', '_one_to_many', 'fanout-by-hash', 'holds'),
    ('scenarios', '_one_to_many', 'fanout-by-hash', 'refuses'),
    ('scenarios', '_one_to_many', 'fanout-by-range', 'holds'),
    ('scenarios', '_one_to_many', 'fanout-by-range', 'refuses'),
    ('scenarios', '_one_to_many', 'fanout-by-table', 'holds'),
    ('scenarios', '_one_to_many', 'fanout-by-table', 'refuses'),
    ('scenarios', '_settling', '<name>', 'holds'),
    ('scenarios', '_settling', '<name>', 'refuses'),
    ('scenarios', 'architecture_case', "<o['name']>", 'holds'),
    ('scenarios', 'architecture_case', "<o['name']>", 'refuses'),
    ('scenarios', 'architecture_case', 'architecture-logical', 'holds'),
    ('scenarios', 'architecture_case', 'architecture-pages', 'holds'),
    ('scenarios', 'architecture_case', 'architecture-pages', 'refuses'),
    ('scenarios', 'managed_case', '<name>', 'holds'),
    ('scenarios', 'managed_case', '<name>', 'refuses'),
    ('scenarios', 'shard_case', 'shard-hash-range', 'holds'),
    ('scenarios', 'shard_case', 'shard-hash-range', 'refuses'),
    ('scenarios', 'shard_case', 'shard-merge', 'holds'),
    ('scenarios', 'shard_case', 'shard-merge', 'refuses'),
    ('scenarios', 'storage_copy_case', 'logical', 'holds'),
    ('scenarios', 'storage_copy_case', 'physical', 'holds'),
    ('scenarios', 'storage_copy_case', 'physical', 'refuses'),
    ('scenarios', 'topology_case', 'topology-unknown', 'refuses'),
]


def test_a_managed_path_is_held_where_the_provider_offers_it():
    facts = managed()
    with watched() as seen:
        assert scenarios.managed_case(facts).choice.path == "provider-upgrade"
        offer = dict(facts["offers"][0], checks=NEVER)
        refused = scenarios.managed_case(decide.Facts(dict(facts,
                                                           offers=(offer,))))
    assert refused.choice.path is None
    assert branch(seen, "provider-upgrade") == [HOLDS, REFUSES]
    assert "does not offer it between these classes" in why(
        seen, "provider-upgrade")


def test_the_pages_are_held_only_between_one_architecture():
    with watched() as seen:
        moved = scenarios.architecture_case(hosts())
        same = hosts()
        same["dst"]["architecture"] = same["src"]["architecture"]
        kept = scenarios.architecture_case(same)
    assert moved.choice.path == "architecture-logical"
    assert kept.choice.path in ("architecture-pages", "architecture-logical")
    assert branch(seen, "architecture-pages") == [REFUSES, HOLDS]
    assert "same architecture, OS and byte order" in why(
        seen, "architecture-pages")
    assert branch(seen, "architecture-logical") == [HOLDS]


def _with_offer(new_instance):
    facts = dict(managed(), **hosts())
    offer = dict(facts["offers"][0], name="provider-move",
                 said="the service's own move", new_instance=new_instance)
    return decide.Facts(dict(facts, offers=(offer,)))


def test_a_providers_operation_is_held_only_where_it_takes_another_instance():
    with watched() as seen:
        scenarios.architecture_case(_with_offer(True))
        scenarios.architecture_case(_with_offer(False))
    assert branch(seen, "provider-move") == [HOLDS, REFUSES]
    assert "changes the instance in place" in why(seen, "provider-move")


def _shard(kind, sources, targets):
    return scenarios.shard_facts(kind, sources, targets, {
        "orders": {"key": ["customer_id"],
                   "columns": ["id", "customer_id", "total"],
                   "whole": False}})


def test_a_split_and_a_merge_are_each_held_on_their_own_shape():
    with watched() as seen:
        split = scenarios.shard_case(_shard("split", 1, 3))
        merge = scenarios.shard_case(_shard("consolidate", 4, 1))
    assert split.choice.path == "shard-hash-range"
    assert merge.choice.path == "shard-merge"
    assert branch(seen, "shard-hash-range") == [HOLDS, REFUSES]
    assert branch(seen, "shard-merge") == [REFUSES, HOLDS]
    assert "more than one target" in why(seen, "shard-hash-range")
    assert "one target" in why(seen, "shard-merge")


def test_keys_apart_are_held_and_keys_that_meet_need_a_rule():
    met = {"a": {"orders": {"range": [1, 100]}},
           "b": {"orders": {"range": [50, 150]}},
           "c": {"orders": {"range": [200, 300]}}}
    with watched() as seen:
        apart = scenarios.topology_case(_merge(_strides(3)))
        meets = scenarios.topology_case(_merge(met))
        settled = scenarios.topology_case(_merge({}, "last_update_wins"))
    assert apart.choice.path == "union-apart"
    assert meets.choice.rung is None
    assert settled.choice.path == "union-settled"
    assert branch(seen, "union-apart") == [HOLDS, REFUSES]
    assert "orders: a and b both write key" in why(seen, "union-apart")
    assert branch(seen, "union-settled") == [REFUSES, HOLDS]
    assert "or name a conflict rule" in why(seen, "union-settled")


def test_each_fan_out_is_held_on_the_rule_the_hop_names():
    with watched() as seen:
        assert scenarios.topology_case(_fan({"by": "table", "tables": {
            "orders": "t0", "items": "t2"}})).choice.path == "fanout-by-table"
        assert scenarios.topology_case(_fan({
            "by": "range", "bounds": [100, 200]})).choice.path \
            == "fanout-by-range"
        assert scenarios.topology_case(_fan({"by": "hash"})).choice.path \
            == "fanout-by-hash"
    for name in ("fanout-by-table", "fanout-by-range", "fanout-by-hash"):
        assert set(branch(seen, name)) == {HOLDS, REFUSES}, (name, seen)
        assert why(seen, name), name


def test_a_topology_not_known_is_refused_with_the_ones_that_are():
    with watched() as seen:
        got = scenarios.topology_case(
            scenarios.topology_facts("ring", ("a", "b"), ("a", "b"), TABLES))
    assert got.choice.rung is None
    assert branch(seen, "topology-unknown") == [REFUSES]
    assert "'ring' is not one of" in why(seen, "topology-unknown")


def test_the_pages_are_copied_only_where_every_table_is_plain():
    plain = decide.Facts({"source": {"t": "heap"}, "target": {"t": "heap"}})
    odd = decide.Facts({"source": {"t": "columnar"},
                        "target": {"t": "heap"}})
    with watched() as seen:
        assert scenarios.storage_copy_case(plain).choice.path == "physical"
        assert scenarios.storage_copy_case(odd).choice.path == "logical"
    assert branch(seen, "physical") == [HOLDS, REFUSES]
    assert "tables stored otherwise: t" in why(seen, "physical")
    assert branch(seen, "logical") == [HOLDS]
