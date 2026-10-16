"""The ladders a copy and a follow choose their way by each held and
refused on the facts that decide them (backlog AUD-Z.rungs-owed): the
settings a program sends, whether a wide row goes whole, where a transform
runs, a bulk load only into an empty table, Parquet parts only where a
batch came back exact and smaller, a service followed only by a log it
keeps, a replica id nobody else holds, and a release migkit has run on.
Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("session_settings", "send", "holds"),
    ("session_settings", "send", "refuses"),
    ("session_settings", "drop", "holds"),
    ("session_settings", "drop", "refuses"),
    ("session_settings", "copier", "holds"),
    ("written_in_pieces", "whole", "holds"),
    ("written_in_pieces", "whole", "refuses"),
    ("written_in_pieces", "pieces", "holds"),
    ("transform_place", "in_flight", "holds"),
    ("transform_place", "in_flight", "refuses"),
    ("transform_place", "on_target", "holds"),
    ("transform_place", "on_target", "refuses"),
    ("tidb_bulk_ways", "load", "holds"),
    ("tidb_bulk_ways", "load", "refuses"),
    ("rows_container", "parquet", "holds"),
    ("rows_container", "parquet", "refuses"),
    ("rows_container", "pickled rows", "holds"),
    ("service_follow", "logical", "holds"),
    ("service_follow", "logical", "refuses"),
    ("service_follow", "binlog", "holds"),
    ("service_follow", "binlog", "refuses"),
    ("reader_server_id", "named", "holds"),
    ("reader_server_id", "named", "refuses"),
    ("reader_server_id", "recorded", "holds"),
    ("reader_server_id", "recorded", "refuses"),
    ("reader_server_id", "derived", "holds"),
    ("reader_server_id", "derived", "refuses"),
    ("version_seen", "unread", "holds"),
    ("version_seen", "unread", "refuses"),
    ("version_seen", "seen", "holds"),
    ("version_seen", "seen", "refuses"),
]


def test_the_settings_are_sent_where_the_server_has_them_else_dropped():
    sends = (("idle_session_timeout", 140000), ("work_mem", 80000))
    with watched() as seen:
        got = decide.session_settings({"server": 160002, "build": 16,
                                       "sends": sends, "can drop": True})
    assert got.path == "send"
    for name in ("send", "drop", "copier"):
        assert branch(seen, name) == [HOLDS], name
    with watched() as seen:
        got = decide.session_settings({"server": 130011, "build": 16,
                                       "sends": sends, "can drop": True})
    assert got.path == "drop"
    assert "the server lacks idle_session_timeout" in why(seen, "send")
    with watched() as seen:
        got = decide.session_settings({"server": None, "sends": sends})
    assert got.path == "copier"
    assert "cannot be told to leave a setting out" in why(seen, "drop")
    assert "the server is not read" in why(seen, "send")


def test_a_row_goes_whole_only_where_its_widest_value_was_read_and_fits():
    with watched() as seen:
        pieces, got = decide.written_in_pieces({"packet": 64 << 20,
                                                "widest": 1 << 20,
                                                "exact": True,
                                                "margin": 1024})
    assert (pieces, got.path) == (False, "whole")
    assert branch(seen, "pieces") == [HOLDS]
    with watched() as seen:
        pieces, got = decide.written_in_pieces({"packet": 4 << 20,
                                                "widest": 3 << 20,
                                                "exact": True})
    assert (pieces, got.path) == (True, "pieces")
    assert "may double once escaped" in why(seen, "whole")
    with watched() as seen:
        pieces, _ = decide.written_in_pieces({"packet": 64 << 20,
                                              "widest": 10, "exact": False})
    assert pieces is True
    assert "a sample bounds nothing" in why(seen, "whole")
    with watched() as seen:
        decide.written_in_pieces({"widest": 10, "exact": True})
    assert why(seen, "whole") == ("the target's max_allowed_packet could not"
                                  " be read")


def _transform(**over):
    facts = {"statement": "upper(name)", "measured": True, "sampled": 1000,
             "in_flight_s": 4.0, "rows": 100000, "copy_s": 10.0,
             "target_runs": True, "authorized": True}
    facts.update(over)
    return facts


def test_a_transform_runs_in_flight_while_it_keeps_up_else_on_the_target():
    with watched() as seen:
        got = decide.transform_place(_transform())
    assert branch(seen, "in_flight") == [HOLDS]
    assert branch(seen, "on_target") == [HOLDS]
    with watched() as seen:
        got = decide.transform_place(_transform(in_flight_s=12.0))
    assert got.path == "on_target"
    assert why(seen, "in_flight") == ("12.0s in flight against 10.0s the"
                                      " copy itself takes")
    with watched() as seen:
        got = decide.transform_place(_transform(in_flight_s=4.0,
                                                budget_s=12.0))
    assert got.path == "on_target"
    assert "against 2.0s the budget leaves after the copy" in why(
        seen, "in_flight")


def test_a_transform_neither_measured_nor_said_by_the_target_is_refused():
    with watched() as seen:
        got = decide.transform_place(_transform(measured=False,
                                                authorized=False))
    assert got.path is None
    assert "nothing is measured to place it by" in why(seen, "in_flight")
    assert "does not let the target be changed" in why(seen, "on_target")
    with watched() as seen:
        got = decide.transform_place(_transform(in_flight_s=99.0,
                                                target_runs=False))
    assert got.path is None
    assert why(seen, "on_target") == ("the target's SQL has no form of the"
                                      " statement")


def test_a_bulk_load_goes_only_into_an_empty_table():
    rungs = decide.tidb_bulk_ways()
    with watched() as seen:
        got = decide.climb("a tidb table", rungs,
                           decide.Facts({"empty": True}), prove=False)
    assert branch(seen, "load") == [HOLDS]
    assert got.path in ("load", "copier")
    with watched() as seen:
        got = decide.climb("a tidb table", rungs,
                           decide.Facts({"empty": False}), prove=False)
    assert got.path == "copier"
    assert why(seen, "load") == ("the target's table holds rows: a load only"
                                 " adds, and a row it holds already would"
                                 " stop it")
    with watched() as seen:
        decide.climb("a tidb table", rungs, decide.Facts({}), prove=False)
    # an emptiness not read is not taken as empty
    assert branch(seen, "load") == [REFUSES]


def test_parts_are_parquet_only_where_a_batch_came_back_exact_and_smaller():
    with watched() as seen:
        got = decide.rows_container({"sample": {"parquet": 400,
                                                "pickled": 900,
                                                "rows": 10}})
    assert got.path == "parquet"
    assert branch(seen, "pickled rows") == [HOLDS]
    with watched() as seen:
        got = decide.rows_container({"sample": {"parquet": 900,
                                                "pickled": 400,
                                                "rows": 10}})
    assert got.path == "pickled rows"
    assert why(seen, "parquet") == ("held as Parquet a row takes 90 bytes"
                                    " against 40 as pickled rows")
    with watched() as seen:
        got = decide.rows_container({"sample": None,
                                     "why": "a column Arrow cannot hold"})
    assert got.path == "pickled rows"
    assert why(seen, "parquet") == "a column Arrow cannot hold"


def test_a_service_is_followed_by_a_slot_or_every_tables_own_log():
    with watched() as seen:
        got = decide.service_follow({"engine": "yugabytedb", "logical": True,
                                     "slots": 2, "tables": 3, "binlog": 3})
    assert got.path in ("logical", "binlog")
    assert branch(seen, "logical") == [HOLDS]
    assert branch(seen, "binlog") == [HOLDS]
    with watched() as seen:
        got = decide.service_follow({"engine": "analyticdb", "tables": 3,
                                     "binlog": 1, "slots": None})
    assert got.path is None
    assert why(seen, "logical") == ("the service serves no logical"
                                    " replication to a client")
    assert why(seen, "binlog").startswith("only 1 of the 3 tables in scope")
    with watched() as seen:
        got = decide.service_follow({"logical": True, "slots": None,
                                     "tables": 2, "binlog": 2})
    assert got.path == "binlog"
    assert why(seen, "logical") == ("the free replication slots were not"
                                    " read")
    with watched() as seen:
        decide.service_follow({"logical": True, "slots": 0})
    assert why(seen, "logical") == "no replication slot is free"


def test_a_reader_attaches_under_a_free_named_recorded_or_derived_id():
    taken = {1: "the source", 2147483700: "another hop"}
    with watched() as seen:
        got, rid = decide.reader_server_id({"named": 77, "taken": taken})
    assert (got.path, rid) == ("named", 77)
    assert why(seen, "recorded") == "the hop names server_id 77"
    assert why(seen, "derived") == "the hop names server_id 77"
    with watched() as seen:
        got, rid = decide.reader_server_id({"named": 1, "taken": taken})
    assert (got.path, rid) == (None, None)
    assert why(seen, "named") == "1 is held by the source"
    with watched() as seen:
        got, rid = decide.reader_server_id({"recorded": 3000000000,
                                            "taken": taken})
    assert (got.path, rid) == ("recorded", 3000000000)
    assert why(seen, "named") == "no id is named"
    assert branch(seen, "derived") == [HOLDS]
    with watched() as seen:
        got, rid = decide.reader_server_id({"recorded": 2147483700,
                                            "identity": ("hop", "db", "tail"),
                                            "taken": taken})
    assert got.path == "derived"
    assert why(seen, "recorded") == "2147483700 is held by another hop"
    assert rid not in taken
    assert decide.READER_IDS[0] <= rid <= decide.READER_IDS[1]


def test_a_release_is_taken_where_unread_or_run_on_and_refused_outside():
    with watched() as seen:
        got = decide.version_seen({"family": "postgres", "version": (16, 2)})
    assert got.path == "seen"
    assert why(seen, "unread") == "the server said its release"
    with watched() as seen:
        got = decide.version_seen({"family": "postgres", "version": None})
    assert got.path == "unread"
    assert branch(seen, "unread") == [HOLDS]
    assert branch(seen, "seen") == [REFUSES]
    with watched() as seen:
        got = decide.version_seen({"family": "mysql", "version": (9, 9)})
    assert got.path is None
    assert "newer than 9.4" in why(seen, "seen")
    with watched() as seen:
        got = decide.version_seen({"family": "mysql", "version": (5, 7)})
    assert "older than 8.4" in why(seen, "seen")
    with watched() as seen:
        got = decide.version_seen({"family": "oracle", "version": (19,)})
    assert why(seen, "seen") == "no release of oracle is listed as run on"
