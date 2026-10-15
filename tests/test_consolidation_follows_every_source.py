"""A consolidation keeps its sources' keys apart by the way the hop names,
each source's changes shaped as its copy shaped its rows (`prefix` beside
`offset`), the key spaces checked on the topology math; and a database is
copied as pages only where every table's storage is the plain one
(PostgreSQL access methods, ClickHouse table engines). Fakes only."""
import pytest

from migkit import consolidate, scenarios
from migkit.config import Endpoint, Hop
from migkit.engines.clickhouse import ClickHouseEngine


def _hop(keys="prefix"):
    return Hop(name="c", engine="postgres",
               source=Endpoint(host="10.0.0.1", port=5432),
               target=Endpoint(host="10.0.0.9", port=5432),
               goal="consolidate", databases=["shop"],
               options={"consolidate": {"column": "tenant_id",
                                        "value": "eu", "keys": keys}})


class _Src:
    CANON_ENGINE = "postgres"

    def __init__(self, columns, key):
        self.columns, self.key = columns, key

    def neutral_key(self, side, db, table):
        return self.key

    def neutral_columns(self, side, db, table):
        return self.columns


class _Pair:
    def __init__(self, src):
        self.src_engine = src


def test_prefix_is_a_rule_and_leads_a_text_key_by_its_value():
    assert "prefix" in consolidate.RULES
    assert consolidate.settings(_hop("prefix"))["keys"] == "prefix"
    p = consolidate.TablePlan("orders", "orders", ["code"], "prefix", {},
                              None, {})
    row = consolidate._shift(["A1", 5], ["code", "qty"], p, {}, "eu")
    assert row == ["eu:A1", 5]


def test_a_column_naming_a_prefixed_key_is_led_with_it():
    p = consolidate.TablePlan("lines", "lines", ["id"], "apart",
                              {"order_code": "orders"}, None,
                              {"orders": "prefix"})
    row = consolidate._shift([7, "A1"], ["id", "order_code"], p, {}, "eu")
    assert row == [7, "eu:A1"]
    q = consolidate.TablePlan("lines", "lines", ["id"], "apart",
                              {"order_id": "orders"}, None,
                              {"orders": "offset"})
    assert consolidate._shift([7, 3], ["id", "order_id"], q,
                              {"orders": 1000}, "eu") == [7, 1003]


def test_prefix_is_chosen_for_a_text_key_and_refused_otherwise():
    text = _Pair(_Src([("code", "text"), ("qty", "integer")], ["code"]))
    got = consolidate.key_way(_hop(), text, "shop", "orders", None,
                              ["eu", "us"])
    assert got.path == "prefix"
    number = _Pair(_Src([("id", "integer")], ["id"]))
    got = consolidate.key_way(_hop(), number, "shop", "orders", None,
                              ["eu", "us"])
    assert got.rung is None
    assert "not text" in got.reason
    got = consolidate.key_way(_hop(), text, "shop", "orders", None,
                              ["eu", "us:west"])
    assert got.rung is None
    assert "separator" in got.reason or "holds" in got.reason


def test_a_prefixed_text_column_gets_room_for_the_prefix():
    p = consolidate.TablePlan("orders", "orders", ["code"], "prefix", {},
                              None, {})
    cols = [("code", "text", (20,), {}), ("qty", "integer", (), {})]
    got = consolidate._led_wider(cols, p)
    assert got[0][2] == (20 + 64 + 1,)
    assert got[1] == cols[1]


# --- the storage that decides the copy ----------------------------------------------

def test_pages_are_copied_only_where_every_table_is_a_plain_heap():
    plain = scenarios.storage_copy_case(scenarios.decide.Facts({
        "source": {"public.a": "heap"}, "target": {"public.a": "heap"},
        "not_read": None}))
    assert plain.choice.path == "physical"
    columnar = scenarios.storage_copy_case(scenarios.decide.Facts({
        "source": {"public.a": "heap", "public.e": "columnar"},
        "target": {"public.a": "heap"}, "not_read": None}))
    assert columnar.choice.path == "logical"
    assert any("public.e" in d and "columnar" in d for d in columnar.details)
    unread = scenarios.storage_copy_case(scenarios.decide.Facts({
        "source": None, "target": None, "not_read": "not read here"}))
    assert unread.choice.path == "logical"


def test_a_merging_clickhouse_engine_is_not_copied_as_parts():
    got = scenarios.storage_copy_case(scenarios.decide.Facts({
        "source": {"t": "mergetree", "r": "replacingmergetree"},
        "target": {"t": "mergetree", "r": "mergetree"}, "not_read": None}))
    assert got.choice.path == "logical"
    assert any(d.startswith("r:") for d in got.details)


class _CH(ClickHouseEngine):
    def __init__(self, answers):
        self.answers = answers
        self.asked = []

    def _d(self, side, db):
        return db

    def _rows(self, side, db, sql, params=None, settings=None):
        self.asked.append((sql, params))
        return self.answers(sql)


def test_clickhouse_storage_is_told_without_replication_and_by_what_it_holds():
    eng = _CH(lambda sql: [["a", "ReplicatedReplacingMergeTree"],
                           ["b", "MergeTree"], ["c", "SharedMergeTree"]])
    assert eng.table_storage("src", "db") == {
        "a": "replacingmergetree", "b": "mergetree", "c": "mergetree"}
    sql, params = eng.asked[0]
    assert "system.tables" in sql and params["skip"]
    merged = ClickHouseEngine.storage_traits("replacingmergetree", "src")
    plain = ClickHouseEngine.storage_traits("mergetree", "dst")
    assert merged.holds == "merged" and plain.holds == "rows"
    assert ClickHouseEngine.storage_traits("nosuch", "src") is None
    assert ClickHouseEngine.storage_traits("postgres", "dst").said.startswith(
        "PostgreSQL")


def test_an_insert_block_follows_the_measured_row_width():
    wide = _CH(lambda sql: [[2 ** 20]])
    narrow = _CH(lambda sql: [[32]])
    assert wide.insert_blocks("dst", "db", "t") < narrow.insert_blocks(
        "dst", "db", "t")
    assert wide.insert_blocks("dst", "db", "t") >= wide.KEYS_A_STATEMENT
    assert narrow.insert_blocks("dst", "db", "t", rows_total=5000) == max(
        5000, narrow.KEYS_A_STATEMENT)
    before = len(wide.asked)
    first = wide._block_for("dst", "db", "t")
    assert wide._block_for("dst", "db", "t") == first
    assert len(wide.asked) == before + 1
