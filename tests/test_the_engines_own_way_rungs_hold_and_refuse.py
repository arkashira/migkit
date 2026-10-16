"""The engines' own ways each held and refused on what their servers say
(backlog AUD-Z.rungs-owed): Db2's statistics, a document store's change
images and schema changes, the wire-compatible services' value bound and
partition catalogue, SQL Server's ways of following, Oracle's redo reader
and row digest, a PostgreSQL load's quiet triggers, a dialect's bulk way,
a spread service's table spread and loader, and a row wider than the
packet - each taken only on the facts read, and passed over with the
reason its need gives. Fakes only."""
from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

PROVES_RUNGS = [
    ("db2_statistics", "distribution", "holds"),
    ("db2_statistics", "distribution", "refuses"),
    ("db2_statistics", "table", "holds"),
    ("db2_statistics", "table", "refuses"),
    ("db2_statistics", "current", "holds"),
    ("mongo_change_images", "images", "holds"),
    ("mongo_change_images", "images", "refuses"),
    ("mongo_change_images", "looked up", "holds"),
    ("ddl_follow", "events", "holds"), ("ddl_follow", "events", "refuses"),
    ("ddl_follow", "snapshots", "holds"),
    ("oceanbase_value_bound", "<f'v{since}'>", "holds"),
    ("oceanbase_value_bound", "<f'v{since}'>", "refuses"),
    ("oceanbase_value_bound", "unread", "holds"),
    ("gauss_partition_catalogue", "own", "holds"),
    ("gauss_partition_catalogue", "own", "refuses"),
    ("gauss_partition_catalogue", "declarative", "holds"),
    ("gauss_partition_catalogue", "declarative", "refuses"),
    ("mssql_log_way", "live log", "holds"),
    ("mssql_log_way", "live log", "refuses"),
    ("mssql_log_way", "log backups", "holds"),
    ("mssql_log_way", "log backups", "refuses"),
    ("mssql_follow_way", "capture", "holds"),
    ("mssql_follow_way", "capture", "refuses"),
    ("mssql_follow_way", "tracking", "holds"),
    ("mssql_follow_way", "tracking", "refuses"),
    ("mssql_follow_way", "log", "holds"),
    ("mssql_follow_way", "log", "refuses"),
    ("oracle_reader_rungs", "redo-files", "holds"),
    ("oracle_reader_rungs", "redo-files", "refuses"),
    ("oracle_reader_rungs", "logminer", "holds"),
    ("oracle_reader_rungs", "logminer", "refuses"),
    ("oracle_digest_rungs", "server", "holds"),
    ("oracle_digest_rungs", "server", "refuses"),
    ("oracle_digest_rungs", "here", "holds"),
    ("quiet_way", "untouched", "holds"), ("quiet_way", "untouched", "refuses"),
    ("quiet_way", "flag", "holds"), ("quiet_way", "flag", "refuses"),
    ("quiet_way", "session", "holds"), ("quiet_way", "session", "refuses"),
    ("dialect_bulk", "client", "holds"), ("dialect_bulk", "client", "refuses"),
    ("dialect_bulk", "landing", "holds"),
    ("dialect_bulk", "landing", "refuses"),
    ("dialect_bulk", "tool", "holds"), ("dialect_bulk", "tool", "refuses"),
    ("dialect_bulk", "copier", "holds"),
    ("mpp_spread", "replicate", "holds"),
    ("mpp_spread", "replicate", "refuses"),
    ("mpp_spread", "hash", "holds"), ("mpp_spread", "hash", "refuses"),
    ("mpp_spread", "even", "holds"),
    ("landing_load", "landing", "holds"),
    ("landing_load", "landing", "refuses"),
    ("landing_load", "copier", "holds"),
    ("packet_way", "kept", "holds"), ("packet_way", "kept", "refuses"),
    ("packet_way", "raised", "holds"), ("packet_way", "raised", "refuses"),
    ("packet_way", "refused", "holds"),
]


def test_db2_statistics_are_gathered_by_what_the_catalogue_holds():
    stale = {"rows": 1000, "card": None, "indexes": 2, "ratio": 0.1}
    with watched() as seen:
        got = decide.db2_statistics(stale)
    assert got.path == "distribution"
    assert branch(seen, "distribution") == [HOLDS]
    assert branch(seen, "table") == [HOLDS]
    assert branch(seen, "current") == [HOLDS]
    with watched() as seen:
        got = decide.db2_statistics(dict(stale, indexes=0))
    assert got.path == "table"
    assert why(seen, "distribution") == "the table has no index"
    with watched() as seen:
        got = decide.db2_statistics(dict(stale, card=1050))
    assert got.path == "current"
    assert branch(seen, "table") == [REFUSES]
    assert why(seen, "table").endswith(": the statistics are current")


def test_a_change_event_carries_images_only_where_every_collection_keeps_them():
    facts = {"version": (6, 0), "collections": ["a", "b"],
             "without images": []}
    with watched() as seen:
        got = decide.mongo_change_images(facts)
    assert got.path == "images"
    assert branch(seen, "images") == [HOLDS]
    assert branch(seen, "looked up") == [HOLDS]
    with watched() as seen:
        got = decide.mongo_change_images(dict(facts, version=(5, 0)))
    assert got.path == "looked up"
    assert "predates 6.0" in why(seen, "images")
    with watched() as seen:
        got = decide.mongo_change_images(dict(facts, **{"without images":
                                                        ["b"]}))
    assert got.path == "looked up"
    assert why(seen, "images").endswith("keeps its images (b)")


def test_schema_changes_are_read_from_events_only_on_a_brand_told_apart():
    with watched() as seen:
        got = decide.ddl_follow({"told": True, "expanded_events": True})
    assert got.path == "events"
    assert branch(seen, "events") == [HOLDS]
    assert branch(seen, "snapshots") == [HOLDS]
    with watched() as seen:
        got = decide.ddl_follow({"told": False, "expanded_events": True})
    assert got.path == "snapshots"
    assert why(seen, "events") == "the brand is not told apart"
    with watched() as seen:
        got = decide.ddl_follow({"told": True, "expanded_events": False})
    assert why(seen, "events") == "the server emits no schema-change events"


def test_oceanbase_bounds_a_value_by_the_version_the_tenant_gave():
    with watched() as seen:
        got = decide.oceanbase_value_bound({"version": (4, 2)})
    assert got.path == "v4"
    assert branch(seen, "v4") == [HOLDS]
    with watched() as seen:
        got = decide.oceanbase_value_bound({"version": (3, 2)})
    assert got.path == "v0"
    assert branch(seen, "v4") == [REFUSES]
    assert why(seen, "v4") == "the tenant's version is before 4"
    with watched() as seen:
        got = decide.oceanbase_value_bound({"version": None})
    assert got.path == "unread"
    assert branch(seen, "v0") == [REFUSES]
    assert branch(seen, "unread") == [HOLDS]


def test_gaussdb_partitions_are_read_from_the_catalogue_that_has_them():
    with watched() as seen:
        got = decide.gauss_partition_catalogue({"pg_partition": True})
    assert got.path == "own"
    assert branch(seen, "own") == [HOLDS]
    assert why(seen, "declarative") == "the catalogue has no pg_partitioned_table"
    with watched() as seen:
        got = decide.gauss_partition_catalogue({"pg_partitioned_table": True})
    assert got.path == "declarative"
    assert branch(seen, "declarative") == [HOLDS]
    assert why(seen, "own") == "the catalogue has no pg_partition"


def test_sql_servers_log_is_read_live_or_from_an_unbroken_backup_chain():
    backups = {"live": False, "recovery": "FULL", "standby": True,
               "chain": True}
    with watched() as seen:
        got = decide.mssql_log_way(dict(backups, live=True))
    assert got.path == "live log"
    assert branch(seen, "live log") == [HOLDS]
    assert branch(seen, "log backups") == [HOLDS]
    with watched() as seen:
        got = decide.mssql_log_way(backups)
    assert got.path == "log backups"
    assert "administrator role" in why(seen, "live log")
    with watched() as seen:
        got = decide.mssql_log_way(dict(backups, chain=False))
    assert got.path is None
    assert "the chain is broken" in why(seen, "log backups")
    with watched() as seen:
        got = decide.mssql_log_way(dict(backups, recovery="SIMPLE"))
    assert why(seen, "log backups") == ("the recovery model is SIMPLE, which"
                                        " takes no log backups")


def test_sql_server_is_followed_by_what_runs_on_every_table_in_scope():
    every = {"capture": True, "capturing": True, "captured": 3, "tables": 3,
             "tracking": True, "tracked": 3, "log": True}
    with watched() as seen:
        got = decide.mssql_follow_way(every)
    assert got.path == "capture"
    for name in ("capture", "tracking", "log"):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        got = decide.mssql_follow_way(dict(every, capturing=False))
    assert got.path == "tracking"
    assert "Agent is not running" in why(seen, "capture")
    with watched() as seen:
        got = decide.mssql_follow_way(dict(every, captured=2, tracked=2))
    assert got.path == "log"
    assert why(seen, "capture").startswith("only 2 of the 3 tables")
    assert why(seen, "tracking").startswith("only 2 of the 3 tables")
    with watched() as seen:
        got = decide.mssql_follow_way(dict(every, capture=False,
                                           tracking=False, log=False))
    assert got.path is None
    assert why(seen, "log") == "the source's log can be followed no way here"


READER = {"listening": False, "platform": None, "program": None,
          "redo here": None, "rates": None}


def _reader(facts):
    return decide.climb("t", decide.oracle_reader_rungs(),
                        decide.Facts(facts), prove=False)


def test_oracles_redo_is_read_beside_it_or_mined_as_fast_as_it_is_written():
    with watched() as seen:
        got = _reader(dict(READER, listening=True))
    assert got.path == "redo-files"
    assert branch(seen, "redo-files") == [HOLDS]
    assert branch(seen, "logminer") == [HOLDS]
    with watched() as seen:
        got = _reader(dict(READER, rates=(2 ** 20, 2 * 2 ** 20)))
    assert got.path == "logminer"
    assert "names no redo_reader" in why(seen, "redo-files")
    with watched() as seen:
        got = _reader(dict(READER, rates=(10 * 2 ** 20, 2 ** 20)))
    assert got.path is None
    assert branch(seen, "logminer") == [REFUSES]
    assert "it would fall behind" in why(seen, "logminer")


DIGEST = {"charset": "AL32UTF8", "version": 19, "rendered": (),
          "width": 100, "limit": 4000}


def _digest(facts):
    return decide.climb("t", decide.oracle_digest_rungs(),
                        decide.Facts(facts), prove=False)


def test_an_oracle_digest_is_summed_in_the_server_only_where_it_hashes_alike():
    with watched() as seen:
        got = _digest(DIGEST)
    assert got.path == "server"
    assert branch(seen, "server") == [HOLDS]
    assert branch(seen, "here") == [HOLDS]
    with watched() as seen:
        got = _digest(dict(DIGEST, charset="WE8ISO8859P1"))
    assert got.path == "here"
    assert "not the UTF-8" in why(seen, "server")
    with watched() as seen:
        got = _digest(dict(DIGEST, version=11))
    assert "no standard_hash" in why(seen, "server")
    with watched() as seen:
        got = _digest(dict(DIGEST, width=8000))
    assert why(seen, "server") == ("a row's text may take 8000 bytes, past"
                                   " the 4000 a hash takes")


def test_a_load_quiets_triggers_only_by_what_the_user_may_do():
    with watched() as seen:
        got = decide.quiet_way({"triggers": (), "superuser": True,
                                "settable": True})
    assert got.path == "untouched"
    for name in ("untouched", "flag", "session"):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        got = decide.quiet_way({"triggers": ("t1",), "superuser": True})
    assert got.path == "flag"
    assert why(seen, "untouched") == ("the target's tables t1 have triggers"
                                      " or keys")
    assert why(seen, "session") == ("the user may not set"
                                    " session_replication_role")
    with watched() as seen:
        got = decide.quiet_way({"triggers": ("t1",), "settable": True})
    assert got.path == "session"
    assert why(seen, "flag") == "the user is not a superuser"


BULK = {"may": True, "rows": 10, "empty": True}


def test_a_dialects_bulk_way_holds_only_on_its_kind_and_every_fact_read():
    with watched() as seen:
        got = decide.dialect_bulk(dict(BULK, way="client", streams=True))
    assert got.path == "client"
    assert branch(seen, "client") == [HOLDS]
    assert why(seen, "landing") == "the dialect's bulk way is not a landing one"
    assert branch(seen, "copier") == [HOLDS]
    with watched() as seen:
        got = decide.dialect_bulk(dict(BULK, way="landing", writer=True,
                                       landing="s3://bucket/x"))
    assert got.path == "landing"
    assert branch(seen, "landing") == [HOLDS]
    assert why(seen, "client") == "the dialect's bulk way is not a client one"
    with watched() as seen:
        got = decide.dialect_bulk(dict(BULK, way="tool", tool=True))
    assert got.path == "tool"
    assert branch(seen, "tool") == [HOLDS]
    with watched() as seen:
        got = decide.dialect_bulk(dict(BULK, way="tool", tool=False))
    assert got.path == "copier"
    assert why(seen, "tool") == ("its command-line client is not on this"
                                 " machine")
    with watched() as seen:
        got = decide.dialect_bulk(dict(BULK, way="client", streams=True,
                                       empty=False))
    assert got.path == "copier"
    assert "reconciled by key" in why(seen, "client")
    with watched() as seen:
        got = decide.dialect_bulk({"way": None, "none_why": "no bulk way"})
    assert got.path == "copier"
    assert why(seen, "client") == "no bulk way"


def test_a_table_is_spread_whole_by_hash_or_evenly_on_its_facts():
    with watched() as seen:
        got = decide.mpp_spread({"replicated": True, "rows": 5, "small": 100,
                                 "key": ["id"]})
    assert got.path == "replicate"
    for name in ("replicate", "hash", "even"):
        assert branch(seen, name) == [HOLDS]
    with watched() as seen:
        got = decide.mpp_spread({"replicated": True, "rows": 500,
                                 "small": 100, "key": ["id"]})
    assert got.path == "hash"
    assert why(seen, "replicate") == "500 rows are over the 100 the service" \
                                     " advises"
    with watched() as seen:
        got = decide.mpp_spread({"replicated": True, "rows": None,
                                 "small": 100, "key": []})
    assert got.path == "even"
    assert why(seen, "replicate") == "the table's rows are not read"
    assert why(seen, "hash") == "the table has no key to spread it by"


LAND = {"landing": "s3://bucket/x", "writer": True, "may": True, "rows": 10,
        "empty": True}


def test_a_table_is_landed_only_where_every_fact_of_the_loader_holds():
    with watched() as seen:
        got = decide.landing_load(LAND)
    assert got.path == "landing"
    assert branch(seen, "landing") == [HOLDS]
    assert branch(seen, "copier") == [HOLDS]
    with watched() as seen:
        got = decide.landing_load(dict(LAND, may=None))
    assert got.path == "copier"
    assert why(seen, "landing") == "the target's grants were not read"
    with watched() as seen:
        got = decide.landing_load(dict(LAND, writer=False))
    assert why(seen, "landing") == "nothing here writes s3://bucket/x"


PACKET = {"widest": 100, "exact": True, "packet": 1000, "slack": 0,
          "want": 4000, "ceiling": 10000, "authorized": True,
          "may set": True}


def test_a_row_past_the_packet_is_kept_raised_or_refused():
    with watched() as seen:
        got = decide.packet_way(PACKET)
    assert got.path == "kept"
    for name in ("kept", "raised", "refused"):
        assert branch(seen, name) == [HOLDS]
    wide = dict(PACKET, widest=2000)
    with watched() as seen:
        got = decide.packet_way(wide)
    assert got.path == "raised"
    assert why(seen, "kept") == ("2,000 bytes is past the target's"
                                 " max_allowed_packet of 1,000")
    with watched() as seen:
        got = decide.packet_way(dict(wide, authorized=False))
    assert got.path == "refused"
    assert "does not allow the target's settings" in why(seen, "raised")
    with watched() as seen:
        got = decide.packet_way(dict(wide, ceiling=3000))
    assert got.path == "refused"
    assert "past the most the server takes (3,000)" in why(seen, "raised")
    with watched() as seen:
        got = decide.packet_way(dict(PACKET, exact=False))
    assert got.path == "refused"
    assert "a sample bounds nothing" in why(seen, "kept")
