"""A Cassandra target is made and loaded with care (P0-PARITY.all): a table
the target lacks is made by the source's own definition - its clustering
order kept - without its indexes and triggers, which are made once the rows
are in; the triggers of the tables a load writes are off for the load and
come back whether it worked or not; the user types the tables use are made
first; the rows are sized across the ring; and the pair reads and makes a
secondary index.

Why: an index built as the rows arrive is built a row at a time; a trigger
runs for every row written, and the rows a copy writes are rows the source
already ran it for. The statements held back are written down first
(`held-off.json`), so a run that stopped part way is finished by the next.

No server: the cluster is `tests/cassandra_fake.py`. `create trigger`,
`drop trigger`, `create index`, `create type` and the schema tables they
are checked against are statements this engine has not yet sent to a real
cluster, so the cells stand in `capabilities.UNPROVED` and this module
names none in `PROVES`.
"""
import json
import os
import socket

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.cassandra import CassandraEngine
from tests import cassandra_fake as fake

COLS = {"id": "int", "day": "int", "name": "text"}


def _engine(tmp_path, monkeypatch, src, dst, **kw):
    fake.install(monkeypatch, src, dst)
    hop = Hop(name="cs", engine="cassandra", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["shop"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return CassandraEngine(hop)


def _made(src):
    src.table("shop", "events", COLS, ["id"], ["day"])
    src.keyspaces["shop"]["events"].indexes["by_name"] = {
        "kind": "COMPOSITES", "options": {"target": "name"}}
    src.keyspaces["shop"]["events"].triggers["audit"] = {"class": "A"}


def test_a_missing_table_is_made_as_the_source_has_it_without_its_extras(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    said = []
    eng.create_missing("shop", said.append)
    made = dst.keyspaces["shop"]["events"]
    # keyed as the source is keyed, not by all its key columns at once
    assert made.partition == ["id"] and made.clustering == ["day"]
    assert not made.indexes and not made.triggers
    held = json.loads((tmp_path / "held-off.json").read_text())
    assert [s.split(" ")[1] for s in held["events"]] == ["INDEX", "TRIGGER"]
    assert 'ON "shop".events' in held["events"][0]
    assert said == ["events: not on the target, made as the source has it"]


def test_the_held_back_statements_are_made_once_the_rows_are_in(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    eng.create_missing("shop")
    eng.finish_created("shop")
    made = dst.keyspaces["shop"]["events"]
    assert list(made.indexes) == ["by_name"]
    assert made.triggers == {"audit": {"class": "A"}}
    assert not (tmp_path / "held-off.json").exists()
    # and again: nothing is made twice
    eng.finish_created("shop")


def test_a_statement_the_target_refuses_is_kept_for_the_next_run(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    eng.create_missing("shop")
    dst.refuse = ["create trigger"]
    with pytest.raises(SystemExit) as e:
        eng.finish_created("shop")
    assert "were not made on the target" in str(e.value)
    kept = json.loads((tmp_path / "held-off.json").read_text())
    assert [s.split(" ")[1] for s in kept["events"]] == ["TRIGGER"]
    dst.refuse = []
    eng.finish_created("shop")
    assert dst.keyspaces["shop"]["events"].triggers
    assert not (tmp_path / "held-off.json").exists()


def test_a_run_that_stopped_after_the_table_still_has_its_statements(
        tmp_path, monkeypatch):
    """The statements are written down before the table is made: a table
    that is there with no record would lose its indexes for good."""
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    dst.refuse = ["create table"]
    with pytest.raises(Exception):
        eng.create_missing("shop")
    assert (tmp_path / "held-off.json").exists()


def test_the_views_of_a_table_are_made_with_it_once_its_rows_are_in(
        tmp_path, monkeypatch):
    """The source's definition of a table carries its views: made at once
    they fill as the rows arrive, and in the source's keyspace, whatever the
    target's is called."""
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shopping")
    _made(src)
    src.add_view("shop", "recent", "events")
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  db_map={"shop": "shopping"})
    eng.create_missing("shop")
    assert not dst.views
    held = json.loads((tmp_path / "held-off.json").read_text())["events"]
    view = [s for s in held if "MATERIALIZED VIEW" in s][0]
    assert 'CREATE MATERIALIZED VIEW "shopping".recent' in view
    assert 'FROM "shopping".events' in view
    eng.finish_created("shop")
    assert list(dst.views) == [("shopping", "recent")]
    eng.finish_created("shop")


def test_a_keyspace_the_target_lacks_is_made_only_with_its_replication(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    with pytest.raises(SystemExit) as e:
        eng.create_missing("shop")
    assert "not migkit's to choose" in str(e.value)
    assert not dst.keyspaces


def test_a_table_that_is_there_is_left_alone(tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    _made(src)
    dst.table("shop", "events", COLS, ["id", "day"])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    eng.create_missing("shop")
    assert dst.keyspaces["shop"]["events"].partition == ["id", "day"]
    assert not (tmp_path / "held-off.json").exists()


def test_a_renamed_table_is_the_pairs_to_make(tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    _made(src)
    eng = _engine(tmp_path, monkeypatch, src, dst,
                  mapping={"tables": {"events": "events_v2"}})
    eng.create_missing("shop")
    assert "events" not in dst.keyspaces["shop"]


def test_the_user_types_the_tables_use_are_made_first_and_in_order(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.keyspace("shop")
    src.table("shop", "people", {"id": "int", "home": "frozen<address>"},
              ["id"])
    src.add_type("shop", "address", [("geo", "frozen<point>")])
    src.add_type("shop", "point", [("x", "double")])
    src.add_type("shop", "unused", [("a", "int")])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    eng.create_missing("shop")
    made = [q.split()[2] for q in dst.log if q.startswith("create type")]
    assert made == ['"shop"."point"', '"shop"."address"']
    assert ("shop", "unused") not in dst.types


# ---- the load's window ----------------------------------------------------------

def _loaded(tmp_path, monkeypatch, **kw):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.table("shop", "events", COLS, ["id"])
    dst.table("shop", "other", COLS, ["id"])
    dst.keyspaces["shop"]["events"].triggers["audit"] = {"class": "A"}
    dst.keyspaces["shop"]["other"].triggers["mail"] = {"class": "M"}
    return _engine(tmp_path, monkeypatch, src, dst, **kw), dst


def test_the_triggers_of_the_tables_a_load_writes_are_off_for_it(
        tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch)
    with eng.load_window("shop", None, {"events"}):
        assert not dst.keyspaces["shop"]["events"].triggers
        # a table the load does not write keeps its own
        assert dst.keyspaces["shop"]["other"].triggers
        assert list(tmp_path.glob("dropped-triggers.*.json"))
    assert dst.keyspaces["shop"]["events"].triggers == {
        "audit": {"class": "A"}}
    assert not list(tmp_path.glob("dropped-triggers.*.json"))


def test_the_triggers_come_back_when_the_load_fails(tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError):
        with eng.load_window("shop", None, {"events"}):
            raise RuntimeError("the load stopped")
    assert dst.keyspaces["shop"]["events"].triggers


def test_a_trigger_that_cannot_be_put_back_is_said_and_kept(
        tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch)
    with pytest.raises(SystemExit) as e:
        with eng.load_window("shop", None, {"events"}):
            dst.refuse = ["create trigger"]
    assert "were not put back" in str(e.value)
    assert list(tmp_path.glob("dropped-triggers.*.json"))


def test_what_a_load_that_died_took_off_goes_back_at_the_next_one(
        tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch)
    dead = tmp_path / f"dropped-triggers.{socket.gethostname()}.4194301.1.json"
    dead.write_text(json.dumps({
        "other.mail2": "create trigger \"mail2\" on \"shop\".\"other\""
                       " using 'M2'"}))
    assert not os.path.exists("/proc/4194301")
    with eng.load_window("shop", None, {"events"}):
        pass
    assert "mail2" in dst.keyspaces["shop"]["other"].triggers
    assert not dead.exists()


def test_a_trigger_taken_off_and_never_put_back_is_a_finding(
        tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch)
    dead = tmp_path / f"dropped-triggers.{socket.gethostname()}.4194301.1.json"
    dead.write_text(json.dumps({"events.gone": "create trigger ..."}))
    got = eng.set_aside("shop")
    assert got.status == "diff" and "events.gone" in got.detail
    assert [r.status for r in eng.check_deep("shop")
            if r.scope == "shop triggers set aside"] == ["diff"]
    dead.unlink()
    assert eng.set_aside("shop") is None


def test_the_triggers_of_a_table_the_hop_leaves_out_are_never_taken_off(
        tmp_path, monkeypatch):
    eng, dst = _loaded(tmp_path, monkeypatch, exclude=["events"])
    with eng.load_window("shop", None, None):
        assert dst.keyspaces["shop"]["events"].triggers
        assert not dst.keyspaces["shop"]["other"].triggers


def test_a_tail_holds_the_window_for_as_long_as_it_applies(tmp_path,
                                                          monkeypatch):
    """The changes a tail carries are changes the source already ran its
    triggers for; a plan that only shows what it would do takes none off."""
    from migkit import tailctl
    eng, dst = _loaded(tmp_path, monkeypatch)
    seen = []
    monkeypatch.setattr(tailctl, "follow", lambda *a, **k: seen.append(
        dict(dst.keyspaces["shop"]["events"].triggers)))
    eng.tail_apply("shop", True, tmp_path / "token.json", None)
    eng.tail_apply("shop", False, tmp_path / "token.json", None)
    assert seen == [{}, {"audit": {"class": "A"}}]
    assert dst.keyspaces["shop"]["events"].triggers


# ---- indexes the pair reads and makes ---------------------------------------------

def test_a_plain_index_is_carried_and_one_over_a_collection_is_not(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "events", COLS, ["id"])
    t = src.keyspaces["shop"]["events"]
    t.indexes["by_name"] = {"kind": "COMPOSITES",
                            "options": {"target": "name"}}
    t.indexes["by_keys"] = {"kind": "COMPOSITES",
                            "options": {"target": "keys(tags)"}}
    t.indexes["by_sai"] = {"kind": "CUSTOM", "options": {
        "target": "day", "class_name": "StorageAttachedIndex"}}
    eng = _engine(tmp_path, monkeypatch, src, dst)
    assert eng.neutral_indexes("src", "shop", "events") == [
        ("by_keys", False, ["keys(tags)"], False),
        ("by_name", False, ["name"], True),
        ("by_sai", False, ["day"], False)]


def test_an_index_is_made_by_its_statement_and_a_unique_one_is_refused(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    dst.table("shop", "events", COLS, ["id"])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    sql = eng.neutral_create_index_sql("dst", "shop", "events", "by_name",
                                       False, ["name"])
    assert sql == 'create index "by_name" on "shop"."events" ("name")'
    eng.execute_ddl("dst", "shop", sql)
    assert "by_name" in dst.keyspaces["shop"]["events"].indexes
    for unique, cols in ((True, ["name"]), (False, ["name", "day"])):
        sql = eng.neutral_create_index_sql("dst", "shop", "events", "x",
                                           unique, cols)
        assert sql.startswith("-- not carried")
        with pytest.raises(RuntimeError) as e:
            eng.execute_ddl("dst", "shop", sql)
        assert "not carried" in str(e.value)


# ---- rows sized across the ring ---------------------------------------------------------

def test_a_small_table_is_sized_whole_and_a_large_one_by_a_sample(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    rows = [{"id": i, "day": i, "name": "x" * (10 if i != 7 else 5000)}
            for i in range(40)]
    src.table("shop", "events", COLS, ["id"], rows=rows)
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = eng.row_sizes("src", "shop", ["events"])["events"]
    assert got["rows"] == 40 and got["exact"] is True
    assert got["largest"] > 5000 > got["mean"]
    eng.SIZE_SAMPLE = 8
    part = eng.row_sizes("src", "shop", ["events"])["events"]
    assert part["exact"] is False and part["rows"] < 40


def test_a_table_that_cannot_be_read_is_not_sized_and_the_rest_are(
        tmp_path, monkeypatch):
    src, dst = fake.Cluster(), fake.Cluster()
    src.table("shop", "events", COLS, ["id"], rows=[
        {"id": 1, "day": 1, "name": "a"}])
    eng = _engine(tmp_path, monkeypatch, src, dst)
    got = eng.row_sizes("src", "shop", ["gone", "events"])
    assert list(got) == ["events"]
