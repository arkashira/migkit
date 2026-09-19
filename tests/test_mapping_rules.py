"""One mapping, read by the move and by the check.

DMS and DTS rename schemas, tables and columns and filter rows while they
move. migkit had a database-name map and a deny list and nothing between
them. This is the core: what a source table is called on the target, and
which of its rows travel.

The part worth building it for is not the rename. It is that **the same
mapping drives the verification** - a renamed table is compared against
its renamed counterpart, and a filtered load is compared against the same
predicate. AWS documents its own validation as unable to validate a
transformed target; a filtered target read against the whole source is a
wall of missing rows that means nothing.

Two refusals are part of the feature rather than edge cases:

* two source tables mapped onto one target is refused, not resolved -
  whichever copy ran second would overwrite the first, and the check would
  then compare one source against a target holding the other;
* a rule that matches nothing is reported, because that is exactly how a
  table quietly fails to move: the operator believes it was renamed, and
  it was never considered.

The naming rule is deliberately the one `excluded` already uses -
right-anchored, so `orders`, `public.orders` and `appdb.public.orders` all
name the same table. One idea of "which table is this", not two that can
disagree.
"""
import pytest

from migkit.config import Endpoint, Hop


def _hop(mapping=None, exclude=()):
    return Hop(name="m", engine="postgres",
               source=Endpoint(host="h", port=1, user="u", password="p"),
               target=Endpoint(host="h", port=2, user="u", password="p"),
               databases=["appdb"], exclude=list(exclude),
               mapping=mapping or {})


def test_an_unmapped_table_keeps_its_name():
    """The answer for almost every table, and the reason callers can ask
    unconditionally instead of checking first."""
    hop = _hop()
    assert hop.target_table("appdb", "public", "orders") == \
        "appdb.public.orders"
    assert hop.row_filter("appdb", "public", "orders") is None


@pytest.mark.parametrize("key", ["orders", "public.orders",
                                 "appdb.public.orders"])
def test_a_rule_matches_the_same_table_written_any_way(key):
    """`excluded` already accepts all three spellings. A mapping that only
    accepted one would be a second, quieter rule about the same names."""
    hop = _hop({"tables": {key: "sales.orders_v2"}})
    assert hop.target_table("appdb", "public", "orders") == "sales.orders_v2"


def test_the_longest_name_wins_when_two_rules_could_match():
    """`appdb.public.orders` is more specific than `orders`, so it decides.
    Without an order, which rule applied would depend on dict iteration."""
    hop = _hop({"tables": {"orders": "wrong.orders",
                           "appdb.public.orders": "right.orders"}})
    assert hop.target_table("appdb", "public", "orders") == "right.orders"


def test_a_row_filter_is_returned_as_written():
    hop = _hop({"where": {"public.orders": "created_at >= '2024-01-01'"}})
    assert hop.row_filter("appdb", "public", "orders") == \
        "created_at >= '2024-01-01'"
    assert hop.row_filter("appdb", "public", "customers") is None


def test_two_tables_onto_one_target_is_refused_not_resolved():
    hop = _hop({"tables": {"public.orders": "archive.rows",
                           "public.invoices": "archive.rows",
                           "public.people": "hr.people"}})
    bad = hop.ambiguous_mapping()
    assert list(bad) == ["archive.rows"], bad
    assert bad["archive.rows"] == ["public.invoices", "public.orders"], bad
    # the unambiguous rule is not dragged in with it
    assert "hr.people" not in bad


def test_a_clean_mapping_is_not_ambiguous():
    """The cry-wolf guard: renaming several tables is the normal case."""
    hop = _hop({"tables": {"public.orders": "sales.orders",
                           "public.invoices": "sales.invoices"}})
    assert hop.ambiguous_mapping() == {}


def test_a_rule_that_matches_nothing_is_reported():
    """The failure this catches is silent: the table was never renamed,
    never filtered, and nobody was told."""
    hop = _hop({"tables": {"public.orders": "sales.orders",
                           "public.typo_here": "sales.x"},
                "where": {"public.ghost": "1=1"}})
    assert hop.unused_mapping(["appdb.public.orders",
                               "appdb.public.people"]) == \
        ["public.ghost", "public.typo_here"]


def test_rules_that_all_match_report_nothing():
    hop = _hop({"tables": {"orders": "sales.orders"}})
    assert hop.unused_mapping(["appdb.public.orders"]) == []


def test_mapping_is_read_from_the_hop_file(tmp_path, monkeypatch):
    """It has to survive the config loader, not just the dataclass."""
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  m:\n    engine: postgres\n"
        "    source: {host: h, port: 1, user: u, password: p}\n"
        "    target: {host: h, port: 2, user: u, password: p}\n"
        "    databases: [appdb]\n"
        "    mapping:\n"
        "      tables:\n"
        '        "public.orders": "sales.orders_v2"\n'
        "      where:\n"
        "        \"public.orders\": \"region = 'apac'\"\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    hop = cfg.load_hops()["m"]
    assert hop.target_table("appdb", "public", "orders") == "sales.orders_v2"
    assert hop.row_filter("appdb", "public", "orders") == "region = 'apac'"


def test_a_hop_without_a_mapping_behaves_as_before(tmp_path, monkeypatch):
    """Every existing hops.yaml has no `mapping:` block. Nothing about it
    may change shape because this feature exists."""
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  m:\n    engine: postgres\n"
        "    source: {host: h, port: 1, user: u, password: p}\n"
        "    target: {host: h, port: 2, user: u, password: p}\n"
        "    databases: [appdb]\n")
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(conf))
    hop = cfg.load_hops()["m"]
    assert hop.mapping == {}
    assert hop.target_table("appdb", "public", "orders") == \
        "appdb.public.orders"
    assert hop.row_filter("appdb", "public", "orders") is None
    assert hop.ambiguous_mapping() == {}
    assert hop.unused_mapping(["appdb.public.orders"]) == []
