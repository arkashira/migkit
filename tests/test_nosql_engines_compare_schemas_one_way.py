"""The NoSQL, warehouse and lake engines' schema checks go through the one
comparer over the neutral shape, which also holds what a collection, a
topic or a column family keeps beyond columns - its time to live, its
retention, its partition count - and never says ok over a table or a
setting it could not read (backlog AUD-H, W12G leftover)."""
from types import SimpleNamespace

import pytest

from migkit import schemaformats, verifiers
from migkit.catalog import Catalog, Column
from migkit.schemaformats import NeutralTable

#: engines that now check their schema through `verifiers.compare_schema`
MOVED = ["bigtable", "cassandra", "clickhouse", "cosmosdb", "couchbase",
         "datastore", "delta", "dynamodb", "firestore", "geminidb-cassandra",
         "iceberg", "keyspaces", "kafka", "lindorm", "neptune", "opensearch",
         "parquet", "saas", "sapodp", "spanner", "timestream"]


class Side:
    """One side of a hop: tables with their columns, key and settings."""

    CANON_ENGINE = "dynamodb"

    def __init__(self, tables, traits=None, broken=(), bulk=False,
                 broken_traits=()):
        self.tables, self.traits = tables, traits or {}
        self.broken, self.broken_traits = set(broken), set(broken_traits)
        self.asked = []
        if bulk:
            self.neutral_traits_all = self._all
        else:
            self.neutral_traits = self._one

    def neutral_tables(self, side, db):
        return list(self.tables)

    def neutral_columns(self, side, db, table):
        if table in self.broken:
            raise TimeoutError("describe timed out")
        return [(c, "S") for c in self.tables[table]]

    def neutral_key(self, side, db, table):
        return [self.tables[table][0]]

    def neutral_indexes(self, side, db, table):
        return []

    def _one(self, side, db, table):
        self.asked.append(table)
        if table in self.broken_traits:
            raise TimeoutError("settings timed out")
        return self.traits.get(table, {})

    def _all(self, side, db):
        self.asked.append("all")
        return self.traits


class Hop:
    def __init__(self, name):
        self.name = name

    def takes_table(self, db, table):
        return True


def _check(name, src, dst):
    both = SimpleNamespace(hop=Hop(name), src_engine=src, dst_engine=dst)
    return verifiers.compare_schema(both, "app")


ORDERS = {"orders": ["id", "total"]}


def test_equal_tables_and_settings_say_ok():
    ttl = {"orders": {"ttl expires": "ENABLED"}}
    got = _check("nosql-equal", Side(ORDERS, ttl), Side(ORDERS, ttl))
    assert [(r.status, r.scope) for r in got] == [("ok", "app")]


def test_a_time_to_live_only_the_source_has_stops_the_cutover():
    got = _check("nosql-ttl-src", Side(ORDERS, {"orders": {
        "ttl expires": "ENABLED"}}), Side(ORDERS))
    assert [(r.status, r.scope) for r in got] == [("diff", "app.orders")]
    assert "ttl expires is ENABLED on the source and not set on the target" \
        in got[0].detail


def test_a_time_to_live_only_the_target_has_is_a_diff_not_a_note():
    got = _check("nosql-ttl-dst", Side(ORDERS), Side(ORDERS, {"orders": {
        "ttl expires": "ENABLED"}}))
    assert got[0].status == "diff"
    assert "not set on the source and ENABLED on the target" in got[0].detail


def test_a_setting_that_differs_names_both_values():
    got = _check("nosql-value", Side(ORDERS, {"orders": {"retention": "7d"}}),
                 Side(ORDERS, {"orders": {"retention": "1d"}}))
    assert got[0].status == "diff"
    assert "retention is 7d on the source and 1d on the target" \
        in got[0].detail


def test_a_table_one_side_would_not_describe_is_not_said_missing_or_ok():
    src = Side({"orders": ["id"], "items": ["id"]})
    dst = Side({"orders": ["id"], "items": ["id"]}, broken={"items"})
    got = _check("nosql-unread-table", src, dst)
    assert [r.status for r in got] == ["warn"]
    assert "items" in got[0].detail and "could not be read on the target" \
        in got[0].detail


def test_a_table_unreadable_on_both_sides_never_reads_as_a_difference():
    src = Side({"orders": ["id"], "items": ["id"]}, broken={"items"})
    dst = Side({"orders": ["id"], "items": ["id", "extra"]}, broken={"items"})
    got = _check("nosql-unread-both", src, dst)
    assert all(r.status != "diff" for r in got)
    assert got[0].status == "warn"


def test_settings_that_could_not_be_read_are_named_not_passed():
    src = Side(ORDERS, {"orders": {"retention": "7d"}})
    dst = Side(ORDERS, {"orders": {"retention": "7d"}},
               broken_traits={"orders"})
    got = _check("nosql-unread-traits", src, dst)
    assert [r.status for r in got] == ["warn"]
    assert "settings" in got[0].detail


def test_one_listing_gives_every_tables_settings_once():
    both = {"orders": {"ttl a": "ENABLED"}, "items": {"ttl b": "ENABLED"}}
    src = Side({**ORDERS, "items": ["id"]}, both, bulk=True)
    dst = Side({**ORDERS, "items": ["id"]}, both, bulk=True)
    got = _check("nosql-bulk", src, dst)
    assert got[0].status == "ok"
    assert src.asked == ["all"] and dst.asked == ["all"]


def test_an_engine_without_a_settings_reader_compares_columns_only():
    class Plain(Side):
        pass
    src, dst = Plain(ORDERS), Plain(ORDERS)
    del src.neutral_traits, dst.neutral_traits
    got = _check("nosql-plain", src, dst)
    assert got[0].status == "ok"


def test_a_column_one_side_lacks_is_still_found():
    got = _check("nosql-column", Side(ORDERS),
                 Side({"orders": ["id"]}))
    assert got[0].status == "diff" and "total" in got[0].detail


# --- the comparer itself ------------------------------------------------------

def _shape(**traits):
    return {"t": NeutralTable([], traits=traits)}


def test_a_setting_difference_is_structured_for_callers_that_word_it_alone():
    got = schemaformats.compare(_shape(partitions="3"), _shape(partitions="6"),
                                True)
    (d,) = got.diffs()
    assert (d.kind, d.aspect, d.src, d.dst) == ("setting", "partitions", "3",
                                               "6")


def test_across_two_engines_settings_are_named_unread_not_compared():
    got = schemaformats.compare(_shape(ttl="x"), _shape(ttl="y"), False)
    assert got.diffs() == []
    assert ("t", "ttl", "the two engines hold it in ways that cannot be held"
            " to each other") in got.unread
    res = schemaformats.results(got, "app")
    assert res[0].status == "warn" and "ttl" in res[0].detail


def test_a_table_with_only_a_missed_read_is_unread_and_its_partner_with_it():
    src = {"a": NeutralTable([]), "b": NeutralTable([])}
    dst = {"a": NeutralTable([])}
    got = schemaformats.compare(src, dst, True,
                                unread={"dst": {"b": "timed out"}})
    assert got.diffs() == []
    assert got.unread == [("b", "table", "could not be read on the target"
                                        " (timed out)")]
    assert got.compared == 1


def test_a_table_that_is_really_absent_is_still_said_missing():
    got = schemaformats.compare({"a": NeutralTable([])}, {}, True)
    assert [(d.table, d.kind) for d in got.diffs()] == [("a", "table")]


def test_a_setting_with_no_value_is_not_a_setting():
    cat = Catalog(tables=["t"], columns={"t": [Column("id", "S")]})
    got = schemaformats.neutral("dynamodb", cat, traits={"t": {
        "ttl": None, "retention": "7d"}})
    assert got["t"].traits == {"retention": "7d"}


# --- who is still on their own comparer --------------------------------------

def test_the_moved_engines_are_off_the_list_of_their_own_comparers():
    left = verifiers.engines_with_own_compare()
    assert [n for n in MOVED if n in left] == []
    assert "mysql" in left


@pytest.mark.parametrize("name", ["bigtable", "firestore"])
def test_an_engine_reading_settings_names_them_for_a_table(name):
    from migkit import engines
    cls = engines._class_for(name)
    assert hasattr(cls, "neutral_traits") or hasattr(cls, "neutral_traits_all")


def test_a_table_outside_the_scope_the_engine_asks_is_not_compared():
    both = SimpleNamespace(hop=Hop("nosql-scope"),
                           src_engine=Side({"orders": ["id"], "x": ["id"]}),
                           dst_engine=Side({"orders": ["id"]}))
    got = verifiers.compare_schema(both, "app", takes=lambda t: t == "orders")
    assert [r.status for r in got] == ["ok"]
