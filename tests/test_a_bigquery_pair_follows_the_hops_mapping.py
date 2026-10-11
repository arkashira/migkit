"""A hop on BigQuery is held to its mapping the way every pair is: the name
rules, the row filter and the changes it carries are read by the copy and
by the check alike, and a finding names the table as the source names it.

Three things were missing before BigQuery named its SQL dialect
(`SQL_DIALECT = "bigquery"`): a row filter written for another engine had
no translation into BigQuery's, and BigQuery's own refused with "takes
none"; a default and a row filter could not be given to a target of it.

No project is reached: what is held is migkit's own logic, over the
engine's existing reads and writes, answered in-process (the digest, the
columns and the keys of a table). The new calls of BigQuery's deep checks
are in `test_bigquery_deep_checks.py`.
"""
import pytest

pytest.importorskip("google.cloud.bigquery")

from migkit import canon  # noqa: E402
from migkit.config import Endpoint, Hop  # noqa: E402
from migkit.engines.hetero import HeteroEngine  # noqa: E402
from migkit.engines.warehouse import BigQueryEngine  # noqa: E402
from tests import bigquery_fake as fake  # noqa: E402

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("scope.names", "bigquery"), ("scope.ops", "bigquery"),
          ("scope.row-filter", "bigquery"), ("verify.table", "bigquery")]

COLUMNS = [("id", "INT64"), ("region", "STRING")]


def _tables(monkeypatch, digests, calls=None, listed=None, **hop):
    """A BigQuery hop whose tables have these digests: {table: (rows,
    digest)}; `listed` is the tables each side lists (all of them by
    default)."""
    eng = fake.engine(monkeypatch, **hop)
    listed = listed or {"src": list(digests), "dst": list(digests)}
    for side in ("src", "dst"):
        eng.clients[side].on("information_schema.tables",
                             [[t] for t in listed[side]])
    monkeypatch.setattr(BigQueryEngine, "neutral_columns",
                        lambda self, side, db, table: COLUMNS)
    monkeypatch.setattr(BigQueryEngine, "neutral_key",
                        lambda self, side, db, table: ["id"])

    def digest(self, side, db, table, columns, where=None, **_):
        if calls is not None:
            calls.append((side, table, where))
        return digests[table]
    monkeypatch.setattr(BigQueryEngine, "neutral_digest", digest)
    return eng


def test_a_renamed_table_is_compared_under_both_names(monkeypatch, tmp_path):
    eng = _tables(monkeypatch, {"orders": (3, "99"), "orders_v2": (3, "99")},
                  listed={"src": ["orders"], "dst": ["orders_v2"]},
                  mapping={"tables": {"orders": "orders_v2"}})
    eng.hop.report_dir = lambda db=None: tmp_path
    got = eng.check_data("sales")
    assert [(r.scope, r.status, r.table) for r in got] == [
        ("sales.orders", "ok", "orders")]


def test_a_finding_names_its_table_as_the_source_does(monkeypatch, tmp_path):
    eng = _tables(monkeypatch, {"orders": (3, "99"), "orders_v2": (3, "7")},
                  listed={"src": ["orders"], "dst": ["orders_v2"]},
                  mapping={"tables": {"orders": "orders_v2"}})
    eng.hop.report_dir = lambda db=None: tmp_path
    got = eng.check_data("sales")
    assert len(got) == 1 and got[0].status == "diff"
    assert got[0].table == "orders" and got[0].scope == "sales.orders"


def test_the_copy_makes_the_table_under_the_name_the_hop_gives_it(
        monkeypatch, tmp_path):
    eng = _tables(monkeypatch, {"orders": (3, "99")},
                  listed={"src": ["orders"], "dst": []},
                  mapping={"tables": {"orders": "orders_v2"}})
    eng.hop.report_dir = lambda db=None: tmp_path
    made = []
    monkeypatch.setattr(
        BigQueryEngine, "neutral_create",
        lambda self, side, db, table, columns, key=(): made.append(
            (side, db, table, key)) or "ddl")
    pair = eng._as_pair()
    assert sorted(pair.list_move_tables("sales")) == [("", "orders")]
    pair.create_missing("sales", lambda m: None)
    assert [m[:3] for m in made] == [("dst", "sales", "orders_v2")]


def test_a_row_filter_is_read_by_the_check_on_both_sides(monkeypatch,
                                                         tmp_path):
    calls = []
    eng = _tables(monkeypatch, {"orders": (2, "5")}, calls,
                  mapping={"where": {"orders": "region = 'TH'"}})
    eng.hop.report_dir = lambda db=None: tmp_path
    eng.check_data("sales")
    # both sides through the filter; and the target's rows outside it,
    # which are counted and named so that narrowing hides nothing
    assert sorted(calls) == [
        ("dst", "orders", "(region = 'TH') is not true"),
        ("dst", "orders", "region = 'TH'"),
        ("src", "orders", "region = 'TH'")]


def test_a_row_filter_written_for_postgres_is_translated_for_bigquery(
        tmp_path):
    EP = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="c", engine="hetero", source=EP,
              target=Endpoint(options={"database": "proj-b"}),
              databases=["d"],
              options={"source_engine": "postgres",
                       "target_engine": "bigquery"},
              mapping={"where": {"public.orders":
                                 "region = 'TH' and id::int > 3"}})
    hop.report_dir = lambda db=None: tmp_path
    pair = HeteroEngine(hop)
    pair.src_engine.neutral_columns = lambda side, db, t: [
        ("id", "integer"), ("region", "text")]
    assert pair._row_scope("d", "public.orders") == (
        "region = 'TH' and id::int > 3",
        "region = 'TH' AND CAST(id AS INT64) > 3")
    assert BigQueryEngine.SQL_DIALECT == "bigquery"


def test_the_copy_reads_a_page_through_the_filter(monkeypatch):
    eng = fake.engine(monkeypatch)
    eng.neutral_key = lambda side, db, table: ["id"]
    eng.neutral_columns = lambda side, db, table: COLUMNS
    sql, args, key, names = eng._read_query(
        "src", "sales", "orders", [("id", "integer"), ("region", "text")],
        (5,), 100, "region = 'TH'")
    assert "region = 'TH'" in sql and "limit 100" in sql
    assert args == [5]


EP = Endpoint(options={"database": "proj-b"})


def _tail(ops=None):
    hop = Hop(name="c", engine="hetero", source=EP, target=EP,
              databases=["d"],
              options={"source_engine": "bigquery",
                       "target_engine": "bigquery"},
              mapping={"ops": ops} if ops else {})
    pair = HeteroEngine(hop)
    pair._row_scope = lambda db, t: (None, None)
    return pair


BATCH = [canon.change("insert", "orders", {"id": 1}, {"id": 1, "v": "x"}),
         canon.change("delete", "orders", {"id": 2}),
         canon.change("update", "orders", {"id": 3}, {"id": 3, "v": "y"})]


def _applied(pair):
    """What the BigQuery target is given of a batch: the table's net
    changes by key, as the target merges them."""
    out, _ = pair._to_target("d", BATCH, {})
    seen = []
    dst = pair.dst_engine
    dst._apply_batch = lambda side, db, by_table, ledger: seen.append(
        by_table)
    dst.neutral_key = lambda side, db, t: ["id"]
    dst.neutral_columns = lambda side, db, t: [("id", "INT64"),
                                               ("v", "STRING")]
    dst.neutral_apply("dst", "d", out)
    return seen[0]["orders"]


def test_a_change_the_hop_does_not_carry_never_reaches_the_target():
    assert _applied(_tail({"orders": "insert, update"})) == [
        ("upsert", {"id": 1}, {"id": 1, "v": "x"}),
        ("upsert", {"id": 3}, {"id": 3, "v": "y"})]


def test_a_hop_that_carries_everything_is_as_it_was():
    got = _applied(_tail())
    assert [k for k, _, _ in got] == ["upsert", "delete", "upsert"]
