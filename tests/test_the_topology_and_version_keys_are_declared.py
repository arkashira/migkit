"""The keys the scenarios read by name are declared and read once
(backlog P0-SCENARIOS): `keys: prefix` of a consolidation, a warehouse's
`versions`, a topology's `nodes`, and the copy way by storage asked from
the plan."""
import pytest

from migkit import consolidate, hopkeys, scenarios, topology
from migkit.config import build_hop

PG = {"source": "postgres://app@db1/shop", "target": "postgres://app@db2/shop"}


def _errors(options, goal=None):
    raw = {**PG, "options": options, **({"goal": goal} if goal else {})}
    return {p.where: p.text for p in hopkeys.check({"hops": {"h": raw}})
            if p.level == "error"}


def _hop(options, goal=""):
    return build_hop("h", {**PG, "options": options,
                           **({"goal": goal} if goal else {})})


def test_every_consolidation_rule_is_a_declared_choice():
    for rule in consolidate.RULES:
        got = _errors({"consolidate": {"column": "t", "value": "eu",
                                       "keys": rule, "offsets": {"eu": 1}}},
                      "consolidate")
        assert not got, (rule, got)
    assert "prefix" in consolidate.RULES


def test_a_consolidation_rule_is_read_in_its_own_spelling():
    hop = _hop({"consolidate": {"column": "t", "keys": "Prefix"}},
               "consolidate")
    assert consolidate.settings(hop)["keys"] == "prefix"
    assert consolidate.settings(_hop({"consolidate": {"column": "t"}},
                                     "consolidate"))["keys"] == "refuse"


def test_a_rule_that_is_not_one_is_refused_at_load():
    got = _errors({"consolidate": {"column": "t", "keys": "merge"}},
                  "consolidate")
    assert "hops.h.options.consolidate" in got, got
    assert "one of refuse, tenant, offset, remap, prefix" in got[
        "hops.h.options.consolidate"], got
    got = _errors({"consolidate": {"column": "t", "keyz": "tenant"}},
                  "consolidate")
    assert "hops.h.options.consolidate.keyz" in got, got


def test_versions_are_a_declared_option_and_a_hops_own_attribute():
    assert not _errors({"versions": {"orders": "updated_at"}})
    assert _hop({"versions": {"orders": "updated_at"}}).versions == {
        "orders": "updated_at"}
    assert _hop({}).versions == {}
    assert _errors({"versions": ["orders"]}), "a list is not a mapping"


def test_nodes_are_read_into_key_spaces_a_rule_and_a_route():
    hop = _hop({"nodes": {
        "spaces": {"a": {"orders": {"range": [1, 100]}},
                   "b": {"orders": {"stride": [1, 2]}}},
        "conflict": "last_update_wins",
        "route": {"by": "range", "bounds": [10]},
        "marks": {"a": True}}})
    got = scenarios.topology_settings(hop)
    assert got["conflict"] == "last_update_wins"
    assert got["spaces"]["a"]["orders"] == topology.KeySpace(lo=1, hi=100)
    assert got["spaces"]["b"]["orders"].step == 2
    assert got["route"] == {"by": "range", "bounds": [10]}
    assert got["marks"] == {"a": True}
    assert not _errors({"nodes": {"conflict": "keep_local"}})
    empty = scenarios.topology_settings(_hop({}))
    assert empty == {"spaces": {}, "conflict": None, "route": {}, "marks": {}}


def test_a_node_word_that_is_not_one_is_refused():
    for bad in ({"conflict": "newest"}, {"route": {"by": "zone"}},
                {"spaces": {"a": {"t": {"stride": ["x"]}}}}):
        with pytest.raises(SystemExit):
            scenarios.topology_settings(_hop({"nodes": bad}))
    assert _errors({"nodes": {"conflict": "newest"}}), "load must refuse it"
    assert _errors({"nodes": {"spaced": {}}})


def test_the_plan_asks_the_storage_before_the_copy_way(monkeypatch):
    from migkit import decide, tableengine
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    seen = {}

    def observed(eng, db):
        return seen["storage"]
    monkeypatch.setattr(tableengine, "observed", observed)
    hop = _hop({})
    seen["storage"] = {"src": {"t": "heap"}, "dst": {"t": "heap"}}
    way, got = scenarios.storage_copy_way(hop, object(), "shop")
    assert way == "physical", got.words()
    seen["storage"] = {"src": {"t": "heap", "c": "columnar"},
                       "dst": {"t": "heap"}}
    way, got = scenarios.storage_copy_way(hop, object(), "shop")
    assert way == "logical" and "c: stored as columnar" in got.words()
    lines = scenarios.copy_lines(hop, object(), "shop")
    assert len(lines) == 1 and "copy way by storage" in lines[0], lines
    assert scenarios.copy_lines(hop, object(), "") == []


def test_unread_storage_keeps_the_logical_way(monkeypatch):
    from migkit import decide, tableengine
    monkeypatch.setattr(decide, "record", lambda *a, **k: None)
    monkeypatch.setattr(tableengine, "observed", lambda eng, db: {
        "src": None, "dst": None, "not_read": "no grant"})
    assert scenarios.storage_copy_way(_hop({}), object(), "shop") is None

    def boom(eng, db):
        raise RuntimeError("down")
    monkeypatch.setattr(tableengine, "observed", boom)
    assert scenarios.storage_copy_way(_hop({}), object(), "shop") is None
