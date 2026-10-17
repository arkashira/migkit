"""The rungs a resharded table is placed by and its rows written to a
node by, each held where its need holds and refused with its own reason
where it does not, on fakes (backlog AUD-Z.rungs-outside-owed)."""
import pytest

from migkit import decide, reshard
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ('reshard', 'node_ways', 'node', 'holds'),
    ('reshard', 'node_ways', 'node', 'refuses'),
    ('reshard', 'node_ways', 'router', 'holds'),
    ('reshard', 'table_ways', 'hash', 'holds'),
    ('reshard', 'table_ways', 'hash', 'refuses'),
    ('reshard', 'table_ways', 'list', 'holds'),
    ('reshard', 'table_ways', 'list', 'refuses'),
    ('reshard', 'table_ways', 'one', 'holds'),
    ('reshard', 'table_ways', 'one', 'refuses'),
    ('reshard', 'table_ways', 'owned', 'holds'),
    ('reshard', 'table_ways', 'owned', 'refuses'),
    ('reshard', 'table_ways', 'range', 'holds'),
    ('reshard', 'table_ways', 'range', 'refuses'),
    ('reshard', 'table_ways', 'rule', 'holds'),
    ('reshard', 'table_ways', 'rule', 'refuses'),
    ('reshard', 'table_ways', 'whole', 'holds'),
    ('reshard', 'table_ways', 'whole', 'refuses'),
]


def _held_then_refused(rung, ok, bad):
    with watched() as seen:
        kept, _ = decide.prune([rung], decide.Facts(ok))
        _, out = decide.prune([rung], decide.Facts(bad))
    assert kept == [rung] and [r for r, _ in out] == [rung]
    assert branch(seen, rung.name) == [HOLDS, REFUSES], seen
    assert why(seen, rung.name) == out[0][1]
    return out[0][1]


def _way(ladder, name):
    return next(r for r in ladder if r.name == name)


@pytest.mark.parametrize("name,ok,bad,said", [
    ("whole", {"shared": True, "reference": False},
     {"shared": False, "reference": False}, "does not list it in shared"),
    ("rule", {"group": "split", "filtered": False},
     {"group": "consolidate", "filtered": False}, "filters no row of it"),
    ("one", {"group": "consolidate"}, {"group": "split"},
     "not a consolidation"),
    ("owned", {"shard by": "auto", "shard targets": 1, "distributed": True},
     {"shard by": "auto", "shard targets": 3}, "more than one target"),
    ("owned", {"shard by": "target", "shard targets": 1, "distributed": True},
     {"shard by": "target", "shard targets": 1, "distributed": False,
      "spread_unknown": None}, "does not spread the table"),
    ("range", {"shard by": "range"}, {"shard by": "hash"},
     "does not place rows by ranges"),
    ("list", {"shard by": "lookup"}, {"shard by": "range"},
     "by listed values"),
    ("hash", {"shard by": "hash"}, {"shard by": "list"},
     "by its own function"),
])
def test_a_table_way_is_held_where_its_need_holds_and_refused_where_not(
        name, ok, bad, said):
    assert said in _held_then_refused(_way(reshard.table_ways(), name), ok,
                                      bad)


def test_rows_go_to_their_node_only_where_migkit_can_address_it():
    got = _held_then_refused(_way(reshard.node_ways(), "node"),
                             {"node address": "10.0.0.5:5432"},
                             {"node address": None})
    assert "names no node migkit can write to" in got
    with watched() as seen:
        kept, _ = decide.prune(reshard.node_ways(),
                               decide.Facts({"node address": None}))
    assert [r.name for r in kept] == ["router"]
    assert branch(seen, "router") == [HOLDS]


def test_cluster_history_and_slots_read_from_sides():
    the_lead = type("Lead", (), {"mapping": {"columns": {"t1": {}}}})()

    class FakeReader:
        hop = the_lead

        def history_tables(self, side, db):
            return {"alembic_version": "alembic_version"}

        def history_columns(self, side, db, ref):
            return [("version_num", "varchar(32)")]

        def history_rows(self, side, db, ref, columns, where=""):
            return [("abc123",)]

        def own_slots(self, db):
            return [f"slot_{db}"]

    class FakeSource:
        name = "s1"
        db = "db1"

    class FakeGroup:
        db = "db1"
        kind = "goal"
        sources = [FakeSource()]
        targets = [FakeSource()]
        lead = the_lead

        def reader(self, i):
            return FakeReader()

        def pair(self, i, j):
            return type("Pair", (), {
                "hop": the_lead,
                "_quote_ident": lambda s, n: f'"{n}"',
            })()

    g = FakeGroup()
    cluster = reshard.Cluster(g)
    assert cluster.history_tables("src", "db1") == {"alembic_version": "alembic_version"}
    assert cluster.history_columns("src", "db1", "alembic_version") == [("version_num", "varchar(32)")]
    assert cluster.history_rows("src", "db1", "alembic_version", ["version_num"]) == [("abc123",)]
    assert cluster.own_slots("db1") == ["slot_db1"]
    assert cluster._quote_ident("tbl") == '"tbl"'
    assert "columns of 1 tables" in cluster.own_schema_changes("db1")[0]


def test_leg_cleans_up_slots_and_terminates_active_pid(monkeypatch):
    from migkit import tailctl
    calls = []

    class FakeReader:
        def release_changes(self):
            calls.append("release_changes")

        def own_slots(self, db):
            return ["test_slot"]

        def _psql(self, side, db, sql):
            calls.append((side, db, sql))

    class FakeSource:
        name = "s1"
        db = "db1"

    class FakeGroup:
        db = "db1"
        lead = type("Lead", (), {})()
        sources = [FakeSource()]

        def report_dir(self):
            from pathlib import Path
            return Path("/tmp")

        def reader(self, i):
            return FakeReader()

    g = FakeGroup()
    alive_calls = [True, True, False]
    monkeypatch.setattr(tailctl, "alive", lambda where, lead=None: alive_calls.pop(0) if alive_calls else False)
    monkeypatch.setattr(tailctl, "stop", lambda where: None)
    monkeypatch.setattr(tailctl, "resume", lambda where: None)
    monkeypatch.setattr(reshard.time, "sleep", lambda s: None)

    l = reshard.leg(g, lambda m: None)
    l.end("db1")
    assert "release_changes" in calls
    assert any("pg_drop_replication_slot" in c[2] and "pg_terminate_backend" in c[2]
               for c in calls if isinstance(c, tuple))

    calls.clear()
    l.after_flip("db1")
    assert "release_changes" in calls
    assert any("pg_drop_replication_slot" in c[2] and "pg_terminate_backend" in c[2]
               for c in calls if isinstance(c, tuple))

