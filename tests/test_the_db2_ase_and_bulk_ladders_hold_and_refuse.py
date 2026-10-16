"""How a Db2, an SAP ASE, a CockroachDB, a Couchbase and a Cosmos DB table
is carried, how their statistics are refreshed, what a rollback point
keeps and how their changes are found - each rung held and refused on the
catalogue's and the server's facts (backlog AUD-Z.rungs-owed): a server's
own load only on an empty table it takes as it is, on one server, where
it outruns the copier; an insert-select only where the target's log holds
the whole table; a log read whole only where every table logs its whole
row. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("db2_bulk_way", "load", "holds"), ("db2_bulk_way", "load", "refuses"),
    ("db2_bulk_way", "cross-load", "holds"),
    ("db2_bulk_way", "cross-load", "refuses"),
    ("db2_bulk_way", "copy-file", "holds"),
    ("db2_bulk_way", "copy-file", "refuses"),
    ("db2_bulk_way", "copier", "holds"),
    ("db2_snapshot_mark", "log mark", "holds"),
    ("db2_snapshot_mark", "log mark", "refuses"),
    ("db2_snapshot_mark", "together", "holds"),
    ("db2_delta_way", "log", "holds"), ("db2_delta_way", "log", "refuses"),
    ("db2_delta_way", "stamped", "holds"),
    ("db2_delta_way", "stamped", "refuses"),
    ("db2_delta_way", "none", "holds"),
    ("db2_log_way", "images", "holds"), ("db2_log_way", "images", "refuses"),
    ("db2_log_way", "by record", "holds"),
    ("db2_log_way", "by record", "refuses"),
    ("ase_bulk_way", "insert-select", "holds"),
    ("ase_bulk_way", "insert-select", "refuses"),
    ("ase_bulk_way", "copier", "holds"),
    ("ase_statistics", "index", "holds"), ("ase_statistics", "index", "refuses"),
    ("ase_statistics", "table", "holds"), ("ase_statistics", "table", "refuses"),
    ("ase_statistics", "current", "holds"),
    ("ase_snapshot_mark", "timestamp mark", "holds"),
    ("ase_snapshot_mark", "timestamp mark", "refuses"),
    ("ase_snapshot_mark", "together", "holds"),
    ("ase_delta_way", "stamped", "holds"),
    ("ase_delta_way", "stamped", "refuses"),
    ("ase_delta_way", "none", "holds"),
    ("crdb_bulk_ways", "import", "holds"),
    ("crdb_bulk_ways", "import", "refuses"),
    ("couchbase_bulk_ways", "loader", "holds"),
    ("couchbase_bulk_ways", "loader", "refuses"),
    ("cosmos_bulk_ways", "bulk", "holds"),
    ("cosmos_bulk_ways", "bulk", "refuses"),
    ("_bulk_copier", "copier", "holds"),
]

DB2 = {"rows": 1000, "width": 100, "empty": True, "mapped": False,
       "same_place": True, "archived": False, "alone": True}


def test_a_db2_table_goes_by_its_platforms_own_load_where_it_may():
    for plat, way in (("luw", "load"), ("zos", "cross-load"),
                      ("i", "copy-file")):
        with watched() as seen:
            got = decide.db2_bulk_way({**DB2, "platform": plat})
        assert got.path == way, plat
        assert branch(seen, way) == [HOLDS]
        assert branch(seen, "copier") == [HOLDS]
        others = {"load", "cross-load", "copy-file"} - {way}
        assert all(why(seen, o) == "not this platform" for o in others)


def test_a_db2_load_is_refused_on_its_own_facts_and_the_copier_carries():
    for off, said in (
            ({"archived": True}, "archives its log"),
            ({"mapped": True}, "renames or transforms"),
            ({"rows": None}, "the catalogue says unknown rows"),
            ({"empty": False}, "a load only adds"),
            ({"same_place": False}, "the two sides are two servers"),
            ({"copier_rate": 1e6, "load_start": 5.0},
             "finishes before a load starts")):
        with watched() as seen:
            got = decide.db2_bulk_way({**DB2, "platform": "luw", **off})
        assert got.path == "copier", off
        assert branch(seen, "load") == [REFUSES]
        assert said in why(seen, "load")
    with watched() as seen:
        got = decide.db2_bulk_way({**DB2, "platform": "zos", "alone": False})
    assert got.path == "copier"
    assert "holds other tables" in why(seen, "cross-load")
    with watched() as seen:
        decide.db2_bulk_way({**DB2, "platform": "i", "empty": False})
    assert "a load only adds" in why(seen, "copy-file")


def test_a_db2_rollback_point_keeps_the_log_position_where_it_answers():
    with watched() as seen:
        got = decide.db2_snapshot_mark({"log": None})
    assert got.path == "log mark"
    assert branch(seen, "together") == [HOLDS]
    with watched() as seen:
        got = decide.db2_snapshot_mark({"log": "SQL1224N connection lost"})
    assert got.path == "together"
    assert branch(seen, "log mark") == [REFUSES]
    assert why(seen, "log mark") == ("the log's position was not read:"
                                     " SQL1224N connection lost")


def test_db2_changes_are_found_by_log_then_stamp_then_the_full_check():
    with watched() as seen:
        got = decide.db2_delta_way({"log": True})
    assert got.path == "log"
    assert branch(seen, "log") == [HOLDS]
    with watched() as seen:
        got = decide.db2_delta_way({"log": False, "stamped": 2, "tables": 5,
                                    "same": True})
    assert got.path == "stamped"
    assert "no reader of the source's log" in why(seen, "log")
    assert branch(seen, "stamped") == [HOLDS]
    with watched() as seen:
        got = decide.db2_delta_way({"log": False, "stamped": 0, "tables": 5,
                                    "same": True})
    assert got.path == "none"
    assert why(seen, "stamped") == ("none of the 5 tables has a row change"
                                    " timestamp")
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        decide.db2_delta_way({"stamped": 2, "tables": 5, "same": False})
    assert "the target is another engine" in why(seen, "stamped")


def test_a_db2_log_is_read_whole_only_where_every_table_logs_its_row():
    base = {"library": True, "archived": True, "tables": 3}
    with watched() as seen:
        got = decide.db2_log_way({**base, "captured": 3})
    assert got.path == "images"
    assert branch(seen, "by record") == [HOLDS]
    with watched() as seen:
        got = decide.db2_log_way({**base, "captured": 1})
    assert got.path == "by record"
    assert why(seen, "images") == ("only 1 of the 3 tables in scope log"
                                   " their whole row")
    with watched() as seen:
        got = decide.db2_log_way({**base, "archived": False, "captured": 3})
    assert got.path is None
    assert branch(seen, "by record") == [REFUSES]
    assert "the source's log is circular" in why(seen, "by record")
    with watched() as seen:
        decide.db2_log_way({**base, "tables": 0})
    assert why(seen, "by record") == "no table is in scope"


ASE = {"rows": 1000, "pages": 50, "log_free": 100, "empty": True,
       "mapped": False, "same_place": True}


def test_an_ase_table_is_inserted_whole_only_where_the_log_holds_it():
    with watched() as seen:
        got = decide.ase_bulk_way(ASE)
    assert got.path == "insert-select"
    assert branch(seen, "copier") == [HOLDS]
    for off, said in (({"log_free": 10}, "has 10 pages free"),
                      ({"log_free": None}, "an unread number of pages"),
                      ({"same_place": False}, "two servers"),
                      ({"empty": False}, "an insert only adds")):
        with watched() as seen:
            got = decide.ase_bulk_way({**ASE, **off})
        assert got.path == "copier", off
        assert branch(seen, "insert-select") == [REFUSES]
        assert said in why(seen, "insert-select")


def test_ase_statistics_are_gathered_only_where_older_than_the_rows():
    old = {"changed": 50, "gathered": 3, "indexes": 2, "ratio": 0.1}
    with watched() as seen:
        got = decide.ase_statistics(old)
    assert got.path == "index"
    assert branch(seen, "table") == [HOLDS]
    assert branch(seen, "current") == [HOLDS]
    with watched() as seen:
        got = decide.ase_statistics({**old, "indexes": 0})
    assert got.path == "table"
    assert why(seen, "index") == "the table has no index"
    with watched() as seen:
        got = decide.ase_statistics({**old, "changed": 1})
    assert got.path == "current"
    assert branch(seen, "table") == [REFUSES]
    assert "the statistics are current" in why(seen, "table")


def test_an_ase_rollback_point_keeps_the_timestamp_where_it_answers():
    with watched() as seen:
        got = decide.ase_snapshot_mark({"dbts": None})
    assert got.path == "timestamp mark"
    assert branch(seen, "together") == [HOLDS]
    with watched() as seen:
        got = decide.ase_snapshot_mark({"dbts": "permission denied"})
    assert got.path == "together"
    assert why(seen, "timestamp mark") == ("the database timestamp was not"
                                           " read: permission denied")


def test_ase_changes_are_found_by_timestamp_only_between_two_ase_sides():
    with watched() as seen:
        got = decide.ase_delta_way({"stamped": 2, "tables": 4, "same": True})
    assert got.path == "stamped"
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got = decide.ase_delta_way({"stamped": 0, "tables": 4, "same": True,
                                    "agent": True})
    assert got.path == "none"
    assert branch(seen, "stamped") == [REFUSES]
    assert "none of the 4 tables has a timestamp column (the replication" \
        in why(seen, "stamped")
    with watched() as seen:
        decide.ase_delta_way({"stamped": 2, "tables": 4, "same": False})
    assert "the target is another engine" in why(seen, "stamped")


def _bulk(rungs, facts):
    return decide.climb("t", rungs, decide.Facts(facts), prove=False)


def test_a_cockroachdb_import_needs_a_stage_a_source_a_key_and_room():
    whole = {"stage": "s3://bucket.example.com/stage", "same": True,
             "key": True, "empty": True}
    with watched() as seen:
        _bulk(decide.crdb_bulk_ways(), whole)
    assert branch(seen, "import") == [HOLDS]
    assert branch(seen, "copier") == [HOLDS]
    for off, said in (({"stage": None}, "no stage is given"),
                      ({"same": False}, "cannot export into the stage"),
                      ({"key": False}, "the table has no key"),
                      ({"empty": False}, "an import only adds")):
        with watched() as seen:
            got = _bulk(decide.crdb_bulk_ways(), {**whole, **off})
        assert got.path == "copier", off
        assert branch(seen, "import") == [REFUSES]
        assert said in why(seen, "import")


def test_couchbase_and_cosmos_bulk_ways_hold_on_their_own_facts():
    with watched() as seen:
        _bulk(decide.couchbase_bulk_ways(), {"loader": True})
    assert branch(seen, "loader") == [HOLDS]
    with watched() as seen:
        got = _bulk(decide.couchbase_bulk_ways(), {"loader": False})
    assert got.path == "copier"
    assert branch(seen, "loader") == [REFUSES]
    assert "the SDK's batched upserts" in got.rung.said
    with watched() as seen:
        got = _bulk(decide.cosmos_bulk_ways(), {"pk": "/tenant"})
    assert got.path == "bulk"
    with watched() as seen:
        got = _bulk(decide.cosmos_bulk_ways(), {"pk": None})
    assert got.path == "copier"
    assert why(seen, "bulk") == ("the target container's partition key"
                                 " cannot be read")
