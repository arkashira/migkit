"""The rungs the engines' own ways are chosen by - how a table's ranges
share one moment and a batch is applied, how a Bigtable, Cassandra,
ClickHouse, DynamoDB, Firestore and GaussDB table goes, where a DynamoDB
table's description is kept, how a Firestore group is read and how a
GaussDB source names its log's end and decodes its slot - reached under
`watched()` on both their branches on fakes (backlog
AUD-Z.rungs-outside-owed): the service's own way is held only where the
table and the servers allow it, and the copier always stands under it."""
import pytest

from migkit import decide
from migkit.engines import base, gaussdb
from migkit.engines.bigtable import BigtableEngine
from migkit.engines.cassandra import CassandraEngine
from migkit.engines.clickhouse import ClickHouseEngine
from migkit.engines.dynamodb import DynamoDBEngine
from migkit.engines.firestore import FirestoreEngine, _Documents
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("engines.base", "Engine", "exported", "holds"),
    ("engines.base", "Engine", "exported", "refuses"),
    ("engines.base", "Engine", "own", "holds"),
    ("engines.base", "_apply_ways", "lanes", "holds"),
    ("engines.base", "_apply_ways", "lanes", "refuses"),
    ("engines.base", "_apply_ways", "one", "holds"),
    ("engines.bigtable", "BigtableEngine", "restore", "holds"),
    ("engines.bigtable", "BigtableEngine", "restore", "refuses"),
    ("engines.bigtable", "BigtableEngine", "copier", "holds"),
    ("engines.cassandra", "CassandraEngine", "loader", "holds"),
    ("engines.cassandra", "CassandraEngine", "loader", "refuses"),
    ("engines.cassandra", "CassandraEngine", "ranges", "holds"),
    ("engines.clickhouse", "ClickHouseEngine", "<cls.PULL>", "holds"),
    ("engines.clickhouse", "ClickHouseEngine", "<cls.PULL>", "refuses"),
    ("engines.clickhouse", "ClickHouseEngine", "<cls.COPIER>", "holds"),
    ("engines.dynamodb", "DynamoDBEngine", "tags", "holds"),
    ("engines.dynamodb", "DynamoDBEngine", "tags", "refuses"),
    ("engines.dynamodb", "DynamoDBEngine", "here", "holds"),
    ("engines.dynamodb", "DynamoDBEngine", "import", "holds"),
    ("engines.dynamodb", "DynamoDBEngine", "import", "refuses"),
    ("engines.dynamodb", "DynamoDBEngine", "export", "holds"),
    ("engines.dynamodb", "DynamoDBEngine", "export", "refuses"),
    ("engines.dynamodb", "DynamoDBEngine", "scan", "holds"),
    ("engines.firestore", "_Documents", "export", "holds"),
    ("engines.firestore", "_Documents", "export", "refuses"),
    ("engines.firestore", "_Documents", "copier", "holds"),
    ("engines.firestore", "FirestoreEngine", "split", "holds"),
    ("engines.firestore", "FirestoreEngine", "one", "holds"),
    ("engines.gaussdb", "GaussEngine", "copy", "holds"),
    ("engines.gaussdb", "GaussEngine", "copy", "refuses"),
    ("engines.gaussdb", "GaussEngine", "copier", "holds"),
    ("engines.gaussdb", "GaussEngine", "xlog", "holds"),
    ("engines.gaussdb", "GaussEngine", "wal", "holds"),
    ("engines.gaussdb", "GaussEngine", "<p>", "holds"),
]


def _bare(cls):
    """An engine of `cls` with nothing connected: only its ladders."""
    return object.__new__(cls)


def _prune(rungs, held, refused):
    """Each fact set's kept names, both seen under one watch."""
    with watched() as seen:
        a, _ = decide.prune(rungs, held)
        b, _ = decide.prune(rungs, refused)
    return [r.name for r in a], [r.name for r in b], seen


class _Exports(base.Engine):
    def export_moment(self, db, **kw):
        return "snap-1", lambda: None


class _Own(base.Engine):
    pass


def test_a_tables_ranges_share_a_moment_only_where_the_source_exports_one():
    with watched() as seen:
        with _bare(_Exports).read_moment("app") as token:
            assert token == "snap-1"
        with _bare(_Own).read_moment("app") as token:
            assert token is None
    assert branch(seen, "exported") == [HOLDS, REFUSES]
    assert why(seen, "exported") == "the source shares no snapshot between" \
                                    " its sessions"
    assert branch(seen, "own") == [HOLDS]


def test_a_batch_goes_in_lanes_only_where_its_tables_allow():
    held, refused, seen = _prune(base._apply_ways(), {"lanes": [1, 2]},
                                 {"lanes": []})
    assert held == ["lanes", "one"] and refused == ["one"]
    assert branch(seen, "lanes") == [HOLDS, REFUSES]
    assert why(seen, "lanes") == "the batch goes in one transaction"
    assert branch(seen, "one") == [HOLDS]


@pytest.mark.parametrize("cls,way,copier,held,refused,said", [
    (BigtableEngine, "restore", "copier", {"unfit": None},
     {"unfit": "the target holds the table already"},
     "the target holds the table already"),
    (CassandraEngine, "loader", "ranges", {"loader": True, "unfit": None},
     {"loader": False, "unfit": None}, "the bulk loader is not installed"),
    (CassandraEngine, "loader", "ranges", {"loader": True, "unfit": None},
     {"loader": True, "unfit": "a counter table"}, "a counter table"),
    (_Documents, "export", "copier", {"unfit": None},
     {"unfit": "the hop names no bucket"}, "the hop names no bucket"),
    (gaussdb.GaussEngine, "copy", "copier", {"unfit": None},
     {"unfit": "the target's table has another shape"}, "another shape"),
])
def test_the_services_own_table_way_holds_only_where_the_table_fits_it(
        cls, way, copier, held, refused, said):
    ways = cls._table_ways(_bare(cls))
    kept, left, seen = _prune(ways, held, refused)
    assert kept == [way, copier] and left == [copier]
    assert branch(seen, way) == [HOLDS, REFUSES]
    assert said in why(seen, way)
    assert branch(seen, copier) == [HOLDS]


@pytest.mark.parametrize("refused,said", [
    ({"kind": "Memory", "there": None}, "a Memory table keeps no partitions"),
    ({"kind": "MergeTree", "there": "Log"},
     "the target's table keeps no partitions"),
    ({"kind": "ReplicatedMergeTree", "there": None},
     "names the source's replication path"),
])
def test_a_clickhouse_table_is_pulled_only_where_both_sides_keep_partitions(
        refused, said):
    pull, copier = ClickHouseEngine.PULL, ClickHouseEngine.COPIER
    kept, left, seen = _prune(
        ClickHouseEngine._ways(), {"kind": "MergeTree", "there": None,
                                   "reach": {}}, dict(refused, reach={}))
    assert kept == [pull, copier] and left == [copier]
    assert branch(seen, pull) == [HOLDS, REFUSES]
    assert said in why(seen, pull)
    assert branch(seen, copier) == [HOLDS]


def test_a_dynamodb_description_is_kept_on_tags_only_where_there_are_some():
    kept, left, seen = _prune(DynamoDBEngine.DESCRIBED,
                              {"tags": {"migkit:columns": "[]"}}, {"tags": {}})
    assert kept == ["tags", "here"] and left == ["here"]
    assert branch(seen, "tags") == [HOLDS, REFUSES]
    assert "keeps no tags of migkit's" in why(seen, "tags")
    assert branch(seen, "here") == [HOLDS]


def test_a_dynamodb_table_goes_by_export_only_where_it_repays_the_wait():
    from migkit import ddbexport
    ways = _bare(DynamoDBEngine)._table_ways()
    big = ddbexport.EXPORT_FROM
    with watched() as seen:
        kept, _ = decide.prune(ways, {"bytes": big, "export": None,
                                      "import": None})
        assert [r.name for r in kept] == ["import", "export", "scan"]
        kept, _ = decide.prune(ways, {"bytes": big, "export": None,
                                      "import": "a table with an LSI"})
        assert [r.name for r in kept] == ["export", "scan"]
        assert "a table with an LSI" in why(seen, "import")
        kept, _ = decide.prune(ways, {"bytes": big - 1, "export": None,
                                      "import": None})
    assert [r.name for r in kept] == ["scan"]
    assert branch(seen, "import") == [HOLDS, REFUSES]
    assert branch(seen, "export") == [HOLDS, REFUSES]
    assert "a scan copies it before an export" in why(seen, "export")
    assert branch(seen, "scan") == [HOLDS]


def test_a_firestore_group_is_read_whole_where_the_service_splits_none():
    with watched() as seen:
        kept, out = decide.prune(FirestoreEngine.PART_WAYS, {})
    assert [r.name for r in kept] == ["split", "one"] and not out
    assert branch(seen, "split") == [HOLDS] and branch(seen, "one") == [HOLDS]


class _Source:
    options = {}


class _GaussHop:
    source = _Source()


def test_a_gauss_source_names_its_log_end_and_decodes_by_the_way_it_has():
    eng = _bare(gaussdb.GaussEngine)
    eng.hop = _GaussHop()
    asked = []

    def rows(side, db, sql, params=()):
        asked.append(sql)
        if "xlog" in sql:
            raise RuntimeError("function pg_current_xlog_location() does not"
                               " exist")
        return [["0/3000060"]]
    eng._rows = rows
    eng._slot_row = lambda db: None
    eng._slot = lambda db: "migkit_app"
    with watched() as seen:
        assert eng._now_lsn("src", "app") == "0/3000060"
        assert eng._make_slot("app") == gaussdb.PLUGINS[0]
    assert branch(seen, "xlog")[0] == HOLDS
    assert branch(seen, "wal") == [HOLDS]
    assert any("pg_current_wal_lsn" in s for s in asked)
    for p in gaussdb.PLUGINS:
        assert branch(seen, p) == [HOLDS]
