"""A BigQuery dataset moved to another is copied by BigQuery itself: one
copy job a table, which carries the schema, partitions, clusters, key and
policies with the rows, reads no row into migkit and is not billed as a
query (`BigQueryChecks.native_bulk`).

A copy job is atomic, so a table the target holds is empty or whole - which
is how the state of an earlier run is reconciled before anything is
written again: a table with the source's rows is left to the check to
prove, one with any other count stops the move, an empty one is filled.
The copy never replaces a table (`WRITE_EMPTY`).

The client is a stand-in that answers as the documentation says
(`tests/bigquery_fake.py`); no project has answered a copy job, so the cell
is in `capabilities.UNPROVED`.
"""
import pytest

pytest.importorskip("google.cloud.bigquery")

from tests import bigquery_fake as fake  # noqa: E402
from tests.bigquery_fake import Field, Table  # noqa: E402


def _eng(monkeypatch, src, dst, **hop):
    eng = fake.engine(monkeypatch, **hop)
    for side, tables in (("src", src), ("dst", dst)):
        for t in tables:
            eng.clients[side].dataset("sales").add(t)
    return eng


def _t(name="orders", rows=0):
    return Table(name, [Field("id", "INTEGER", "REQUIRED")], rows=rows)


def test_a_dry_run_says_the_steps_and_does_nothing(monkeypatch):
    eng = _eng(monkeypatch, [_t()], [])
    steps = eng.native_bulk("sales", ["orders"], False, None,
                            shape_only=["lines"])
    assert steps[0].startswith("orders: copied by BigQuery itself, a copy"
                               " job") and "no row passes" in steps[0]
    assert steps[1].startswith("lines: made like the source's, empty")
    assert eng.clients["dst"].copies == []


def test_a_table_the_target_lacks_is_copied_by_a_job_that_replaces_nothing(
        monkeypatch):
    eng = _eng(monkeypatch, [_t(rows=5)], [])
    said = []
    eng.native_bulk("sales", ["orders"], True, said.append)
    assert eng.clients["dst"].copies == [
        ("proj-a.sales.orders", "proj-b.sales.orders", "WRITE_EMPTY")]
    assert said == ["orders: copied"]


def test_an_empty_table_the_target_has_is_filled(monkeypatch):
    eng = _eng(monkeypatch, [_t(rows=5)], [_t(rows=0)])
    eng.native_bulk("sales", ["orders"], True, None)
    assert len(eng.clients["dst"].copies) == 1


def test_a_table_already_holding_the_sources_rows_is_left_for_the_check(
        monkeypatch):
    """The copy job is atomic: a table with rows is a table copied. That
    the rows are the source's is for the check to prove, and is said."""
    eng = _eng(monkeypatch, [_t(rows=5)], [_t(rows=5)])
    said = []
    eng.native_bulk("sales", ["orders"], True, said.append)
    assert eng.clients["dst"].copies == []
    assert "left as it is, the check proves it" in said[0]


def test_a_table_holding_other_rows_stops_the_move(monkeypatch):
    eng = _eng(monkeypatch, [_t(rows=5)], [_t(rows=2)])
    with pytest.raises(SystemExit) as e:
        eng.native_bulk("sales", ["orders"], True, None)
    assert "the target holds 2 rows and the source 5" in str(e.value)
    assert "will not replace a table that is already there" in str(e.value)
    assert eng.clients["dst"].copies == []


def test_a_job_the_project_refuses_is_said_and_stops(monkeypatch):
    eng = _eng(monkeypatch, [_t()], [])
    eng.clients["dst"].refuse = "Access Denied: bigquery.tables.getData"
    with pytest.raises(SystemExit) as e:
        eng.native_bulk("sales", ["orders"], True, None)
    assert "orders: the copy job was refused" in str(e.value)
    assert "Access Denied" in str(e.value)


def test_a_table_the_copier_fills_is_made_like_the_sources_first(
        monkeypatch):
    eng = _eng(monkeypatch, [_t("lines")], [])
    eng.native_bulk("sales", [], True, None, shape_only=["lines"])
    assert eng.clients["dst"].asked == [
        "create table if not exists `sales`.`lines` like"
        " `proj-a.sales.lines`"]
    # and one the target has is left as it is
    eng = _eng(monkeypatch, [_t("lines")], [_t("lines")])
    eng.native_bulk("sales", [], True, None, shape_only=["lines"])
    assert eng.clients["dst"].asked == []


def test_the_planner_is_given_each_tables_rows_bytes_and_key(monkeypatch):
    eng = _eng(monkeypatch, [], [], exclude=["audit_log"])
    eng.clients["src"].on("table_constraints", [["orders"]])
    eng.clients["src"].on("__tables__", [
        ["orders", 10, 4096], ["lines", 5, 100], ["audit_log", 1, 1],
        ["_migkit_ledger", 1, 1]])
    assert eng.table_facts("src", "sales") == {
        "orders": {"rows": 10, "bytes": 4096, "key": True},
        "lines": {"rows": 5, "bytes": 100, "key": False}}


def test_the_engine_is_one_the_move_can_send_the_fast_way():
    from migkit import movers
    assert movers.supported("bigquery", "native")
