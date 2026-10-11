"""What `assess` says of a BigQuery project before a move, and the
catalogue a pair reads from it: what a copy of the rows leaves (counted
and named), who else writes to the target, what the project is running,
the views, routines, defaults and foreign keys a pair is given, and the
state of the target a repair can be put back from.

The client is a stand-in that answers as the documentation says
(`tests/bigquery_fake.py`); no project has answered these calls, so none
of these cells is in a `PROVES` (`capabilities.UNPROVED`).
"""
import datetime
from types import SimpleNamespace as NS

import pytest

pytest.importorskip("google.cloud.bigquery")

from tests import bigquery_fake as fake  # noqa: E402
from tests.bigquery_fake import Field, Table  # noqa: E402


def _eng(monkeypatch, tmp_path, src=(), dst=(), **hop):
    eng = fake.engine(monkeypatch, report=tmp_path, **hop)
    for side, tables in (("src", src), ("dst", dst)):
        for t in tables:
            eng.clients[side].dataset("sales").add(t)
    return eng


def _routine(name, kind="SCALAR_FUNCTION"):
    return NS(routine_id=name, type_=kind, language="SQL", body="x + 1",
              arguments=[NS(name="x", data_type=NS(type_kind="INT64"),
                            mode="IN")],
              return_type=NS(type_kind="INT64"))


SOURCE = [
    Table("orders", [Field("id", "INTEGER", "REQUIRED")],
          constraints=fake.constraints(primary=["id"])),
    Table("events", [Field("id", "INTEGER"), Field("day", "DATE")],
          time_partitioning=fake.time_partitioning("day"),
          clustering_fields=["id"]),
    Table("v", table_type="VIEW", view_query="select 1"),
    Table("mv", table_type="MATERIALIZED_VIEW", mview_query="select 1"),
]


def test_assess_counts_and_names_what_a_copy_of_the_rows_leaves(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, SOURCE)
    eng.clients["src"].dataset("sales").routines["f"] = _routine("f")
    rows = eng._source_items()
    by = {r["item"]: r for r in rows}
    keyless = by["tables with no primary key or unique index"]
    assert "events" in keyless["detail"] and "orders" not in \
        keyless["detail"]
    code = by["routines, triggers and views a data mover does not carry"]
    assert "view v" in code["detail"] and "routine f" in code["detail"]
    left = [r for r in rows if r["item"] == "objects whose contents no data"
            " mover copies"][0]
    assert "events (time (day))" in left["detail"]
    assert "materialized view mv" in left["detail"]
    # and the same said of a source in a pair
    assert eng.reach_items("src") == eng._source_items() + \
        eng._tail_items()


def test_a_dataset_that_cannot_be_read_is_unknown_not_none(monkeypatch,
                                                           tmp_path):
    eng = _eng(monkeypatch, tmp_path, SOURCE)

    def refuse(ref):
        raise RuntimeError("Access Denied: bigquery.tables.list")
    eng.clients["src"].list_tables = refuse
    rows = eng._source_items()
    assert any("UNKNOWN" in r["detail"] for r in rows), rows


def test_no_version_is_asked_of_a_service_that_has_none(monkeypatch,
                                                        tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    row = eng._version_row(None, None)
    assert row["level"] == "info" and "no version" in row["detail"]


def test_the_project_is_asked_what_it_is_running(monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.clients["dst"].on("state != 'done'", [[7]])
    assert eng.capacity("dst", "sales") == {"running": 7}
    assert "`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT" in \
        eng.clients["dst"].asked[0]


def test_another_account_writing_to_the_target_is_named(monkeypatch,
                                                        tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    eng.clients["dst"].on("jobs_by_project", [
        ["etl@example.com", "MERGE", 12],
        ["svc@example.com", "LOAD", 3]])
    eng._whoami = lambda side: "svc@example.com"
    got = eng.stream_writers("sales")
    assert got == [("etl@example.com: 12 MERGE jobs in the last hour",
                    False)]
    assert "destination_table.dataset_id = 'sales'" in \
        eng.clients["dst"].asked[0]


def test_a_table_is_given_to_a_pair_with_its_rules(monkeypatch, tmp_path):
    t = Table("orders", [Field("id", "INTEGER", "REQUIRED"),
                         Field("at", "TIMESTAMP", default="CURRENT_TIMESTAMP()"),
                         Field("rec", "RECORD", fields=[Field("x", "STRING")])])
    eng = _eng(monkeypatch, tmp_path, [t])
    rules = eng.neutral_column_rules("src", "sales", "orders")
    assert rules["id"] == {"null": False, "default": None, "identity": False}
    assert rules["at"]["default"] == "CURRENT_TIMESTAMP()"
    # a record's fields are the record's, not columns of the table
    assert "rec.x" not in rules


def test_the_views_and_routines_a_pair_is_given_are_the_hops(monkeypatch,
                                                            tmp_path):
    eng = _eng(monkeypatch, tmp_path, SOURCE, exclude=["hidden"])
    eng.clients["src"].dataset("sales").add(
        Table("hidden", table_type="VIEW", view_query="select 2"))
    ds = eng.clients["src"].dataset("sales")
    ds.routines["f"] = _routine("f")
    ds.routines["p"] = _routine("p", "PROCEDURE")
    assert eng.neutral_views("src", "sales") == [("v", "select 1")]
    got = eng.code_routines("src", "sales")
    assert [(r.kind, r.name, r.returns, r.language) for r in got] == [
        ("function", "f", "INT64", "sql"), ("procedure", "p", None, "sql")]
    assert got[0].params[0].name == "x" and got[0].params[0].mode == "in"
    assert got[0].body == "x + 1"
    # the statement as BigQuery keeps it, where it answers
    eng.clients["src"].on("routine_name = 'f'", [["CREATE FUNCTION f(x INT64)"
                                                  " AS (x + 1)"]])
    assert eng.code_routines("src", "sales")[0].definition.startswith(
        "CREATE FUNCTION f")
    # a routine the hop's `objects` leaves out is not read
    none = _eng(monkeypatch, tmp_path, SOURCE, objects={"routines": False})
    none.clients["src"].dataset("sales").routines["f"] = _routine("f")
    assert none.code_routines("src", "sales") == []


def test_the_target_is_kept_in_snapshots_a_repair_is_put_back_from(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path, [], SOURCE[:2])
    now = datetime.datetime(2026, 3, 4, 5, 6, 7,
                            tzinfo=datetime.timezone.utc)
    eng.clients["dst"].on("select current_timestamp()", [[now]])
    eng.clients["dst"].on("information_schema.tables",
                          [["orders"], ["events"]])
    state = tmp_path / "state"
    state.mkdir()
    eng.snapshot_state("sales", state, "rows")
    made = eng.clients["dst"].snapshots
    assert len(made) == 2
    assert made[0].startswith(
        "create snapshot table `sales`.`_migkit_snapshot_20260304050607_"
        "orders` clone `sales`.`orders` options(expiration_timestamp = "
        "timestamp_add(current_timestamp(), interval 7 day))")
    put_back = (state / "dst-snapshots.sql").read_text()
    assert "create or replace table `sales`.`orders` clone `sales`." \
        "`_migkit_snapshot_20260304050607_orders`;" in put_back
    # a counter is nothing here
    eng.clients["dst"].snapshots.clear()
    eng.snapshot_state("sales", state, "sequences")
    assert eng.clients["dst"].snapshots == []


def test_a_repair_is_the_pairs_and_so_is_the_table_the_target_lacks(
        monkeypatch, tmp_path):
    eng = _eng(monkeypatch, tmp_path)
    pair = []

    class Pair:
        def repair_plan(self, db, kind):
            pair.append(("plan", kind))
            return ["rows"]

        def apply(self, db, action):
            pair.append(("apply", action))

        def create_missing(self, db, log=None):
            pair.append(("create", db))

        def finish_created(self, db, log=None):
            pair.append(("finish", db))
    monkeypatch.setattr(type(eng), "_as_pair", lambda self, within=None:
                        Pair())
    assert eng.repair_plan("sales", "rows") == ["rows"]
    assert eng.repair_plan("sales", "all") == ["rows"]
    # a counter has nothing to repair
    assert eng.repair_plan("sales", "sequences") == []
    eng.apply("sales", "act")
    eng.create_missing("sales")
    eng.finish_created("sales")
    assert pair == [("plan", "rows"), ("plan", "rows"), ("apply", "act"),
                    ("create", "sales"), ("finish", "sales")]
