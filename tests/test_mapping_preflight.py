"""A mapping's mistakes are invisible after the move.

A rename that collides leaves the target holding one table's rows under
both names. A rule whose name is misspelt leaves the table exactly where
it was while the operator believes it moved. Neither shows up as a
difference later, because the check would be comparing the wrong pair in
the first place - so both have to be said *before* anything is copied,
which is what `assess` is for.

These run against `PostgresEngine` with the servers stubbed out: what is
being tested is the reading of the mapping, and a live pair would only
slow down a decision that needs no database to make.
"""
import pytest

from migkit.config import Endpoint, Hop


def _engine(tmp_path, mapping, tables=("appdb.public.orders",
                                       "appdb.public.people")):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="m", engine="postgres",
              source=Endpoint(host="h", port=1, user="u", password="p"),
              target=Endpoint(host="h", port=2, user="u", password="p"),
              databases=["appdb"], mapping=mapping)
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)
    eng.databases = lambda: ["appdb"]
    if tables is None:
        def boom(side, db):
            raise RuntimeError("permission denied for schema public")
        eng.neutral_tables = boom
    else:
        eng.neutral_tables = lambda side, db: list(tables)
    return eng


def _mapping(items):
    return [i for i in items if i["item"] == "mapping"]


def test_a_hop_without_a_mapping_says_nothing_at_all(tmp_path):
    """The cry-wolf guard, and it matters here: every hop in use today has
    no mapping, and a pre-flight line about an unused feature is a line
    people learn to skip."""
    assert _engine(tmp_path, {})._mapping_items() == []


def test_two_tables_onto_one_target_fails_the_preflight(tmp_path):
    eng = _engine(tmp_path, {"tables": {"public.orders": "archive.rows",
                                        "public.people": "archive.rows"}})
    hit = _mapping(eng._mapping_items())
    assert [i["level"] for i in hit] == ["fail"], hit
    detail = hit[0]["detail"]
    assert "archive.rows" in detail and "public.orders" in detail, detail
    assert "overwrites the first" in detail, detail
    assert "[fix]" in detail, detail


def test_a_rule_that_matches_nothing_is_a_warning_with_the_name(tmp_path):
    eng = _engine(tmp_path, {"tables": {"public.odrers": "sales.orders"}})
    hit = _mapping(eng._mapping_items())
    assert [i["level"] for i in hit] == ["warn"], hit
    assert "'public.odrers'" in hit[0]["detail"], hit[0]["detail"]
    assert "will move exactly as it is" in hit[0]["detail"], hit[0]["detail"]


def test_a_mapping_that_is_right_passes_and_counts_itself(tmp_path):
    eng = _engine(tmp_path, {"tables": {"public.orders": "sales.orders"},
                             "where": {"public.people": "active"}})
    hit = _mapping(eng._mapping_items())
    assert [i["level"] for i in hit] == ["pass"], hit
    assert "1 renames and 1 row filters" in hit[0]["detail"], hit[0]["detail"]


def test_tables_that_cannot_be_listed_are_unknown_not_clean(tmp_path):
    """"I could not ask" is not "every rule matches". A pass here would be
    the reassurance the whole section exists to avoid."""
    eng = _engine(tmp_path, {"tables": {"public.orders": "sales.orders"}},
                  tables=None)
    hit = _mapping(eng._mapping_items())
    assert [i["level"] for i in hit] == ["warn"], hit
    assert "unknown" in hit[0]["detail"], hit[0]["detail"]
    assert "permission denied" in hit[0]["detail"], hit[0]["detail"]


def test_both_problems_are_reported_together(tmp_path):
    """One is a fail and one is a warning; the fail must not swallow the
    warning, because fixing only what is shown leaves the other."""
    eng = _engine(tmp_path, {"tables": {"public.orders": "archive.rows",
                                        "public.people": "archive.rows",
                                        "public.ghost": "x.y"}})
    hit = _mapping(eng._mapping_items())
    assert sorted(i["level"] for i in hit) == ["fail", "warn"], hit


def test_it_reaches_assess_through_the_preflight(tmp_path):
    eng = _engine(tmp_path, {"tables": {"public.orders": "archive.rows",
                                        "public.people": "archive.rows"}})
    eng.PREFLIGHT = ()
    rows = eng._preflight_items()
    assert _mapping(rows), rows
    assert rows[0]["scope"] == "before the move", rows[0]


def test_the_row_the_preflight_emits_has_the_shape_assess_expects(tmp_path):
    eng = _engine(tmp_path, {"tables": {"public.orders": "sales.orders"}})
    for row in eng._mapping_items():
        assert set(row) == {"level", "scope", "item", "detail"}, row
        assert row["level"] in ("pass", "warn", "fail"), row
