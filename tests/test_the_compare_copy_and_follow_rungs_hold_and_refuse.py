"""The rungs a table is compared, copied and followed by through a driver,
a differing table's rows are found by, a schema change runs as on a
target of another engine, a tail reads its source by, and a MongoDB
collection is walked and its changes followed by - reached under
`watched()` on both their branches on fakes (backlog
AUD-Z.rungs-outside-owed): a way that stands on a key, a column, an
oplog or a position the source does not give is refused in those words,
never taken."""
import pytest

from migkit import decide
from migkit.engines import hetero
from migkit.engines.generic import GenericEngine
from migkit.engines.hetero import HeteroEngine
from migkit.engines.mongodb import MongoEngine
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("engines.generic", "GenericEngine", "key-ranges", "holds"),
    ("engines.generic", "GenericEngine", "key-ranges", "refuses"),
    ("engines.generic", "GenericEngine", "row-digests", "holds"),
    ("engines.generic", "GenericEngine", "row-digests", "refuses"),
    ("engines.generic", "GenericEngine", "key-order", "holds"),
    ("engines.generic", "GenericEngine", "key-order", "refuses"),
    ("engines.generic", "GenericEngine", "whole-table", "holds"),
    ("engines.generic", "GenericEngine", "named-column", "holds"),
    ("engines.generic", "GenericEngine", "named-column", "refuses"),
    ("engines.generic", "GenericEngine", "catalogue-column", "holds"),
    ("engines.generic", "GenericEngine", "catalogue-column", "refuses"),
    ("engines.hetero", "HeteroEngine", "leaves", "holds"),
    ("engines.hetero", "HeteroEngine", "walk", "holds"),
    ("engines.hetero", "_ddl_ways", "statement", "holds"),
    ("engines.hetero", "_ddl_ways", "statement", "refuses"),
    ("engines.hetero", "_ddl_ways", "create", "holds"),
    ("engines.hetero", "_ddl_ways", "create", "refuses"),
    ("engines.hetero", "_Reader", "<n>", "holds"),
    ("engines.mongodb", "MongoEngine", "id hash", "holds"),
    ("engines.mongodb", "MongoEngine", "bytes", "holds"),
    ("engines.mongodb", "MongoEngine", "change stream", "holds"),
    ("engines.mongodb", "MongoEngine", "change stream", "refuses"),
    ("engines.mongodb", "MongoEngine", "oplog", "holds"),
    ("engines.mongodb", "MongoEngine", "oplog", "refuses"),
    ("engines.mongodb", "MongoEngine", "reseed", "holds"),
    ("engines.mongodb", "MongoEngine", "reseed", "refuses"),
    ("engines.mongodb", "MongoEngine", "bucket sweep", "holds"),
]

GENERIC = object.__new__(GenericEngine)

COMPARE = {"key": ["id"], "columns": ["id", "v"], "unique": True,
           "comparer": True, "held": 10, "memory": 10 ** 9}


@pytest.mark.parametrize("change,refused,said", [
    ({"key": ["id", "at"], "columns": ["id", "at"]}, "key-ranges",
     "2 key columns"),
    ({"unique": False}, "key-ranges", "the catalogue holds no unique rule"),
    ({"comparer": False}, "key-ranges",
     "the comparison component is not installed"),
    ({"key": ["code"]}, "key-ranges", "the table has no column named code"),
    ({"held": None}, "row-digests", "could not be counted or sampled"),
    ({"memory": None}, "row-digests", "free memory could not be read"),
    ({"held": 10 ** 12, "memory": 10 ** 6}, "row-digests", "MiB held here"),
])
def test_a_table_is_compared_by_the_way_its_key_and_size_allow(
        change, refused, said):
    with watched() as seen:
        kept, _ = decide.prune(GENERIC.compare_rungs(), COMPARE)
        assert [r.name for r in kept] == ["key-ranges", "row-digests"]
        kept, _ = decide.prune(GENERIC.compare_rungs(),
                               dict(COMPARE, **change))
    assert refused not in [r.name for r in kept]
    assert branch(seen, refused) == [HOLDS, REFUSES]
    assert said in why(seen, refused), why(seen, refused)


COPY = {"key_absent": [], "unique": True, "key_masked": []}


@pytest.mark.parametrize("change,said", [
    ({"key_absent": ["id"]}, "the key is not on both sides: id"),
    ({"unique": False}, "does not hold the key unique"),
    ({"key_masked": ["id"]}, "the hop transforms the key (id)"),
])
def test_a_table_is_copied_in_key_order_only_where_the_key_says_whats_done(
        change, said):
    with watched() as seen:
        kept, _ = decide.prune(GENERIC.copy_rungs(), COPY)
        assert [r.name for r in kept] == ["key-order", "whole-table"]
        kept, _ = decide.prune(GENERIC.copy_rungs(), dict(COPY, **change))
    assert [r.name for r in kept] == ["whole-table"]
    assert branch(seen, "key-order") == [HOLDS, REFUSES]
    assert said in why(seen, "key-order")
    assert branch(seen, "whole-table") == [HOLDS]


FOLLOW = {"key_absent": [], "named": "updated_at",
          "both": {"updated_at", "id"}, "found": ["updated_at"]}


@pytest.mark.parametrize("change,refused,said", [
    ({"named": ""}, "named-column", "the hop names no column"),
    ({"named": "touched"}, "named-column", "touched is not on both sides"),
    ({"found": []}, "catalogue-column", "no column on both sides is named"),
    ({"found": ["a", "b"]}, "catalogue-column", "more than one column"),
    ({"key_absent": ["id"]}, "named-column", "the key is not on both sides"),
    ({"key_absent": ["id"]}, "catalogue-column", "cannot be told from a new"),
])
def test_a_tables_changes_are_followed_by_the_column_that_says_when(
        change, refused, said):
    with watched() as seen:
        kept, _ = decide.prune(GENERIC.follow_rungs(), FOLLOW)
        assert [r.name for r in kept] == ["named-column", "catalogue-column"]
        kept, _ = decide.prune(GENERIC.follow_rungs(),
                               dict(FOLLOW, **change))
    assert refused not in [r.name for r in kept]
    assert branch(seen, refused) == [HOLDS, REFUSES]
    assert said in why(seen, refused), why(seen, refused)


def test_a_differing_tables_rows_are_found_by_either_drill():
    with watched() as seen:
        kept, out = decide.prune(HeteroEngine.DRILLS, {})
    assert [r.name for r in kept] == ["leaves", "walk"] and not out
    assert branch(seen, "leaves") == [HOLDS] and branch(seen, "walk") == [
        HOLDS]


@pytest.mark.parametrize("change,refused,said", [
    ({"own_ddl": False}, "statement", "is not one the target's engine runs"),
    ({"own_create": False}, "create", "runs no converted create"),
])
def test_a_schema_change_runs_as_the_target_engine_takes_it(change, refused,
                                                            said):
    facts = {"own_ddl": True, "own_create": True}
    with watched() as seen:
        kept, _ = decide.prune(hetero._ddl_ways(), facts)
        assert [r.name for r in kept] == ["statement", "create"]
        decide.prune(hetero._ddl_ways(), dict(facts, **change))
    assert branch(seen, refused) == [HOLDS, REFUSES]
    assert said in why(seen, refused)


class _Way:
    def next(self, token, limit):
        return [{"op": "i"}], token + 1


def test_a_tail_reads_on_every_reader_way_it_ranks():
    names = [n for n, _, _ in hetero.READ_RUNGS]
    r = object.__new__(hetero._Reader)
    r.fresh, r.on, r.log, r.chosen = False, names[0], None, None
    r.readers, r.names, r.costs = {}, list(names), {}
    r._timed = lambda token, now: None
    r.ranked = lambda: [(n, 0.0) for n in names]
    r._rung = lambda name: _Way()
    r._switch = lambda name: None
    with watched() as seen:
        changes, after = r.next(7, 100)
    assert changes == [{"op": "i"}] and after == 8 and r.on == names[0]
    for n in names:
        assert branch(seen, n) == [HOLDS]


def test_a_differing_collection_is_walked_by_either_way():
    with watched() as seen:
        kept, out = decide.prune(MongoEngine.WALKS, {})
    assert [r.name for r in kept] == ["id hash", "bytes"] and not out
    assert branch(seen, "id hash") == [HOLDS]
    assert branch(seen, "bytes") == [HOLDS]


FOLLOWS = {"replica set": True, "stream opens": True, "oplog reads": True,
           "position held": True}


def test_a_held_position_is_followed_and_a_lost_one_reseeded():
    with watched() as seen:
        held = MongoEngine.follow_choice(dict(FOLLOWS))
        lost = MongoEngine.follow_choice(dict(
            FOLLOWS, **{"position held": "the oplog no longer holds it"}))
    assert held.path == "change stream" and lost.path == "reseed"
    assert branch(seen, "change stream") == [HOLDS, REFUSES]
    assert why(seen, "change stream") == "the oplog no longer holds it"
    assert branch(seen, "oplog") == [HOLDS, REFUSES]
    assert branch(seen, "reseed") == [REFUSES, HOLDS]
    assert branch(seen, "bucket sweep") == [HOLDS]


def test_a_standalone_server_is_followed_by_its_sweep():
    with watched() as seen:
        got = MongoEngine.follow_choice({"replica set": False,
                                         "stream opens": False,
                                         "oplog reads": False})
    assert got.path == "bucket sweep"
    for name in ("change stream", "oplog", "reseed"):
        assert branch(seen, name) == [REFUSES]
        assert "standalone server and keeps no oplog" in why(seen, name)


def test_a_refused_stream_on_a_replica_set_is_read_from_its_oplog():
    with watched() as seen:
        got = MongoEngine.follow_choice(dict(
            FOLLOWS, **{"stream opens": "not authorized on admin"}))
    assert got.path == "oplog"
    assert "refused to open a change stream: not authorized" in why(
        seen, "change stream")
