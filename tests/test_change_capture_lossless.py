import bson
from bson.raw_bson import RawBSONDocument
import pytest

from migkit.change_parity import (
    InvalidationScope,
    MongoResumeToken,
    MongoStandaloneVerifier,
    MongosyncRebuilder,
    MySQLStatementInvalidator,
    PostgresReplicaWalDecoder,
    RedisIdletimeSweep,
    RedisPsyncParser,
    StatementInvalidation,
)


# ---------------------------------------------------------------------------
# Item 272 (LT.R8): MySQL STATEMENT / MIXED Binlog Invalidation Tests
# ---------------------------------------------------------------------------

def test_mysql_statement_invalidation_key_scope():
    invalidator = MySQLStatementInvalidator(default_schema="shop", primary_keys={"users": "id"})

    inv1 = invalidator.parse_statement("UPDATE users SET name = 'alice' WHERE id = 42")
    assert inv1.scope == InvalidationScope.KEY
    assert inv1.schema == "shop"
    assert inv1.table == "users"
    assert inv1.keys == [42]
    assert inv1.key_column == "id"

    inv2 = invalidator.parse_statement("DELETE FROM `shop`.`users` WHERE id IN (10, 20, 30)")
    assert inv2.scope == InvalidationScope.KEY
    assert inv2.table == "users"
    assert inv2.keys == [10, 20, 30]


def test_mysql_statement_invalidation_range_and_table_scope():
    invalidator = MySQLStatementInvalidator(default_schema="shop", primary_keys={"items": "id"})

    inv_range = invalidator.parse_statement("UPDATE items SET qty = qty - 1 WHERE id BETWEEN 100 AND 200")
    assert inv_range.scope == InvalidationScope.RANGE
    assert inv_range.table == "items"
    assert inv_range.range_bounds == (100, 200)

    inv_table = invalidator.parse_statement("UPDATE items SET price = price * 1.1 WHERE category = 'books'")
    assert inv_table.scope == InvalidationScope.TABLE
    assert inv_table.table == "items"

    inv_nowhere = invalidator.parse_statement("TRUNCATE TABLE items")
    assert inv_nowhere.table == "items"

    inv_load = invalidator.parse_statement("LOAD DATA INFILE '/tmp/data.csv' INTO TABLE items")
    assert inv_load.scope == InvalidationScope.TABLE
    assert inv_load.table == "items"


def test_mysql_statement_unsupported_side_effects():
    invalidator = MySQLStatementInvalidator(default_schema="app")
    inv = invalidator.parse_statement("CALL migrate_batch_proc()")
    assert inv.scope == InvalidationScope.UNSUPPORTED


def test_mysql_mixed_mode_classification():
    invalidator = MySQLStatementInvalidator(default_schema="app")

    kind, inv = invalidator.classify_binlog_event("WRITE_ROWS", {"rows": [1, 2]})
    assert kind == "ROW"
    assert inv is None

    kind, inv = invalidator.classify_binlog_event("STATEMENT", "UPDATE users SET active = 1 WHERE id = 7")
    assert kind == "STATEMENT"
    assert inv is not None
    assert inv.scope == InvalidationScope.KEY
    assert inv.keys == [7]


def test_mysql_range_checksum_planning_and_diff():
    invalidator = MySQLStatementInvalidator()
    ranges = invalidator.plan_range_checksum_fallback(
        schema="app", table="orders", key_column="id", min_key=1, max_key=100, num_ranges=4
    )
    assert len(ranges) == 4
    assert ranges[0] == (1, 25)
    assert ranges[-1] == (76, 100)

    sql = invalidator.generate_range_checksum_sql(
        schema="app", table="orders", key_column="id", range_min=1, range_max=25, columns=["id", "val"]
    )
    assert "BIT_XOR(CRC32" in sql
    assert "`app`.`orders`" in sql

    src_checksums = {(1, 25): (12345, 25), (26, 50): (67890, 25), (51, 100): (11111, 50)}
    dst_checksums = {(1, 25): (12345, 25), (26, 50): (99999, 25), (51, 100): (11111, 50)}
    diffs = invalidator.diff_range_checksums(src_checksums, dst_checksums)
    assert diffs == [(26, 50)]


# ---------------------------------------------------------------------------
# Item 273 (LT.R9): PostgreSQL wal_level=replica Decoder Contract Tests
# ---------------------------------------------------------------------------

def test_postgres_replica_walinspect_parser():
    decoder = PostgresReplicaWalDecoder()
    decoder.register_table(16390, "public.accounts")

    decoder.record_ctid_mapping(16390, 0, 1, key=101)
    decoder.record_ctid_mapping(16390, 0, 2, key=102)

    records = [
        {"rmgr": "Heap", "record_type": "INSERT", "xid": 500, "lsn": "0/1000", "relfilenode": 16390, "block": 0, "offset": 3},
        {"rmgr": "Heap", "record_type": "DELETE", "xid": 500, "lsn": "0/1010", "relfilenode": 16390, "block": 0, "offset": 1},
        {"rmgr": "Heap", "record_type": "UPDATE", "xid": 501, "lsn": "0/1020", "relfilenode": 16390, "block": 0, "offset": 2},
        {"rmgr": "Transaction", "record_type": "ABORT", "xid": 501, "lsn": "0/1030"},
        {"rmgr": "Transaction", "record_type": "COMMIT", "xid": 500, "lsn": "0/1040", "commit_timestamp": "2026-10-15 12:00:00 UTC"},
    ]

    changes = decoder.parse_walinspect_records(records)

    assert len(changes) == 2
    ins_change = next(c for c in changes if c.change_type == "INSERT")
    assert ins_change.table == "public.accounts"
    assert ins_change.ctid == (0, 3)

    del_change = next(c for c in changes if c.change_type == "DELETE")
    assert del_change.table == "public.accounts"
    assert del_change.resolved_key == 101

    assert (16390, 0, 1) not in decoder.ctid_key_map


def test_postgres_replica_relation_rewrite_detection():
    decoder = PostgresReplicaWalDecoder()
    decoder.register_table(16390, "public.orders")
    decoder.register_table(28999, "public.orders")

    records = [
        {"rmgr": "Heap", "record_type": "INSERT", "xid": 600, "lsn": "0/2000", "relfilenode": 28999, "block": 1, "offset": 1},
        {"rmgr": "Transaction", "record_type": "COMMIT", "xid": 600, "lsn": "0/2010", "commit_timestamp": "2026-10-15 13:00:00 UTC"},
    ]
    changes = decoder.parse_walinspect_records(records)
    assert len(changes) == 1
    assert changes[0].is_table_rewrite is True


def test_postgres_replica_waldump_text_parser():
    decoder = PostgresReplicaWalDecoder()
    decoder.register_table(16390, "public.items")
    decoder.record_ctid_mapping(16390, 2, 5, key=555)

    sample_waldump = """
rmgr: Heap len(rec): 75, tx: 701, lsn: 0/16B2D40, prev 0/16B2CE8, desc: INSERT+INIT ... blkref #0: rel 1663/16384/16390 fork 0 blk 2 offset 4
rmgr: Heap len(rec): 80, tx: 701, lsn: 0/16B2D90, prev 0/16B2D40, desc: DELETE ... blkref #0: rel 1663/16384/16390 fork 0 blk 2 offset 5
rmgr: Transaction len(rec): 34, tx: 701, lsn: 0/16B3000, prev 0/16B2D90, desc: COMMIT 2026-10-15 14:00:00.000000 UTC
"""
    changes = decoder.parse_waldump_text(sample_waldump)
    assert len(changes) == 2
    del_change = next(c for c in changes if c.change_type == "DELETE")
    assert del_change.resolved_key == 555
    assert del_change.table == "public.items"

    scan_sql = decoder.generate_ctid_scan_sql("public.items", 2)
    assert "ctid >= '(2, 0)'" in scan_sql


# ---------------------------------------------------------------------------
# Item 274 (LT.R10): MongoDB Standalone Verification Tests
# ---------------------------------------------------------------------------

def test_mongodb_dbhash_comparison():
    src_dbhash = {"collections": {"users": "hash_a", "orders": "hash_b", "logs": "hash_c"}, "md5": "all_src"}
    dst_dbhash = {"collections": {"users": "hash_a", "orders": "hash_diff", "logs": "hash_c"}, "md5": "all_dst"}

    clean, dirty = MongoStandaloneVerifier.compare_db_hashes(src_dbhash, dst_dbhash)
    assert clean == {"users", "logs"}
    assert dirty == {"orders"}


def test_mongodb_partition_id_buckets():
    ids = list(range(1, 101))
    buckets = MongoStandaloneVerifier.partition_id_buckets(ids, num_buckets=4)
    assert len(buckets) == 4
    assert buckets[0] == (1, 25)
    assert buckets[-1] == (76, 100)


def test_mongodb_raw_bson_bucket_digest_and_verification():
    docs_bucket1_src = [{"_id": 1, "name": "alice"}, {"_id": 2, "name": "bob"}]
    docs_bucket1_dst = [{"_id": 1, "name": "alice"}, {"_id": 2, "name": "bob"}]
    docs_bucket2_src = [{"_id": 3, "val": 100}]
    docs_bucket2_dst = [{"_id": 3, "val": 999}]

    src_buckets = {(1, 2): docs_bucket1_src, (3, 4): docs_bucket2_src}
    dst_buckets = {(1, 2): docs_bucket1_dst, (3, 4): docs_bucket2_dst}

    clean_buckets, dirty_buckets = MongoStandaloneVerifier.verify_collection_buckets(src_buckets, dst_buckets)
    assert clean_buckets == {(1, 2)}
    assert dirty_buckets == {(3, 4)}


def test_mongodb_raw_bson_document_support():
    raw_doc1 = RawBSONDocument(bson.encode({"_id": 10, "data": "abc"}))
    raw_doc2 = RawBSONDocument(bson.encode({"_id": 20, "data": "xyz"}))
    digest = MongoStandaloneVerifier.compute_raw_bson_bucket_digest([raw_doc1, raw_doc2])
    assert isinstance(digest, str)
    assert len(digest) == 64


def test_mongodb_deletions_and_updates_detection():
    src = [{"_id": 1, "v": "a"}, {"_id": 2, "v": "b_updated"}, {"_id": 3, "v": "c"}]
    dst = [{"_id": 1, "v": "a"}, {"_id": 2, "v": "b_old"}, {"_id": 4, "v": "extra"}]

    to_insert, to_update, to_delete = MongoStandaloneVerifier.detect_deletions_and_updates(src, dst)
    assert to_insert == {3}
    assert to_update == {2}
    assert to_delete == {4}


def test_mongodb_narrow_candidates_by_updated_at():
    docs = [
        {"_id": 1, "updatedAt": 100.0},
        {"_id": 2, "updatedAt": 150.0},
        {"_id": 3, "updatedAt": 200.0},
    ]
    candidates = MongoStandaloneVerifier.narrow_candidates_by_updated_at(docs, since_timestamp=150.0)
    assert [d["_id"] for d in candidates] == [2, 3]


# ---------------------------------------------------------------------------
# Item 275 (LT.C1): Mongosync Change Streams Rebuilder Tests
# ---------------------------------------------------------------------------

def test_mongosync_resume_token_preservation():
    rebuilder = MongosyncRebuilder()
    event = {
        "_id": {"_data": "resume_token_xyz_123"},
        "clusterTime": 1700000000,
        "operationType": "insert",
        "fullDocument": {"_id": 10, "val": "test"},
    }
    emitted = rebuilder.buffer_change_event(event)
    assert emitted == event

    token = rebuilder.latest_resume_token
    assert token is not None
    assert token.data == {"_data": "resume_token_xyz_123"}
    assert token.cluster_time == 1700000000

    watch_args = token.to_watch_args()
    assert watch_args == {"resumeAfter": {"_data": "resume_token_xyz_123"}}


def test_mongosync_multi_document_transactions():
    rebuilder = MongosyncRebuilder()
    lsid = "session_uuid_1"

    ev1 = {"_id": {"_data": "t1"}, "lsid": {"id": lsid}, "txnNumber": 101, "op": "i1"}
    ev2 = {"_id": {"_data": "t2"}, "lsid": {"id": lsid}, "txnNumber": 101, "op": "i2"}

    assert rebuilder.buffer_change_event(ev1) is None
    assert rebuilder.buffer_change_event(ev2) is None

    committed_events = rebuilder.commit_transaction(lsid, 101)
    assert len(committed_events) == 2
    assert committed_events[0]["op"] == "i1"
    assert committed_events[1]["op"] == "i2"

    ev_abort = {"_id": {"_data": "t3"}, "lsid": {"id": lsid}, "txnNumber": 102, "op": "aborted_op"}
    rebuilder.buffer_change_event(ev_abort)
    rebuilder.abort_transaction(lsid, 102)
    assert rebuilder.commit_transaction(lsid, 102) == []


def test_mongosync_d3_update_application():
    initial_doc = {"_id": 1, "profile": {"name": "alice", "age": 30}, "tags": ["a", "b", "c"], "flag": True}
    update_desc = {
        "updatedFields": {"profile.age": 31, "city": "Berlin"},
        "removedFields": ["flag"],
        "truncatedArrays": {"tags": 2},
    }

    updated = MongosyncRebuilder.apply_d3_update(initial_doc, update_desc)
    assert updated["profile"]["age"] == 31
    assert updated["profile"]["name"] == "alice"
    assert updated["city"] == "Berlin"
    assert "flag" not in updated
    assert updated["tags"] == ["a", "b"]


def test_mongosync_range_generation_tracking():
    rebuilder = MongosyncRebuilder()
    ranges = [(1, 100), (101, 200), (201, 300)]

    rebuilder.track_touched_range(doc_id=50, generation=1, range_bounds=ranges)
    rebuilder.track_touched_range(doc_id=75, generation=1, range_bounds=ranges)
    rebuilder.track_touched_range(doc_id=150, generation=1, range_bounds=ranges)

    dirty_gen1 = rebuilder.get_generation_dirty_ranges(generation=1)
    assert dirty_gen1 == {(1, 100), (101, 200)}


def test_mongosync_write_blocking_audit():
    users = [
        {"user": "app_user", "roles": [{"role": "readWrite"}]},
        {"user": "backup_agent", "roles": [{"role": "restore"}]},
        {"user": "cluster_admin", "roles": [{"role": "clusterAdmin"}]},
    ]

    audit = MongosyncRebuilder.audit_write_blocking(users)
    assert "backup_agent" in audit["bypassing_users"]
    assert "cluster_admin" in audit["privileged_users"]
    assert "app_user" in audit["safe_users"]
    assert audit["fence_secure"] is False


def test_mongosync_unique_index_validation():
    docs_valid = [{"_id": 1, "email": "a@x.com"}, {"_id": 2, "email": "b@x.com"}]
    ok, dups = MongosyncRebuilder.validate_and_convert_unique_index(docs_valid, ["email"])
    assert ok is True
    assert dups == []

    docs_dup = [{"_id": 1, "email": "dup@x.com"}, {"_id": 2, "email": "dup@x.com"}]
    ok_dup, dups_found = MongosyncRebuilder.validate_and_convert_unique_index(docs_dup, ["email"])
    assert ok_dup is False
    assert len(dups_found) == 1


# ---------------------------------------------------------------------------
# Item 280 (LT.C6): Lossless Live Redis Replication Stream Parser Tests
# ---------------------------------------------------------------------------

def test_redis_psync_request_and_handshake():
    req = RedisPsyncParser.build_psync_request("abc123replid", 100500)
    assert req == b"PSYNC abc123replid 100500\r\n"

    cont_stream = b"+CONTINUE abc123replid\r\n"
    mode, replid, offset, rdb, consumed = RedisPsyncParser.parse_handshake_response(cont_stream)
    assert mode == "CONTINUE"
    assert replid == "abc123replid"
    assert consumed == len(cont_stream)

    rdb_payload = b"REDIS0009\xff\x00\x00\x00\x00\x00\x00\x00\x00"
    full_stream = (
        b"+FULLRESYNC 777replid 0\r\n"
        + f"${len(rdb_payload)}\r\n".encode("utf-8")
        + rdb_payload
    )
    mode, replid, offset, rdb, consumed = RedisPsyncParser.parse_handshake_response(full_stream)
    assert mode == "FULLRESYNC"
    assert replid == "777replid"
    assert offset == 0
    assert rdb == rdb_payload
    assert consumed == len(full_stream)


def test_redis_psync_replication_stream_and_replconf_ack():
    stream_chunk = (
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n"
        b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    )
    commands, new_offset, unparsed = RedisPsyncParser.parse_replication_stream(stream_chunk, initial_offset=100)
    assert len(commands) == 3
    assert commands[0]["command"] == "SET"
    assert commands[0]["args"] == ["foo", "bar"]
    assert commands[1]["command"] == "PING"
    assert commands[2]["command"] == "REPLCONF"
    assert unparsed == b""
    assert new_offset > 100

    ack_bytes = RedisPsyncParser.format_replconf_ack(new_offset)
    assert ack_bytes.startswith(b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n")


def test_redis_idletime_sweep_lossless():
    assert RedisIdletimeSweep.check_client_no_touch_supported("7.2.4") is True
    assert RedisIdletimeSweep.check_client_no_touch_supported("8.0.0") is True
    assert RedisIdletimeSweep.check_client_no_touch_supported("7.0.15") is False

    scan_keys = {
        "active_key_1": 5,
        "active_key_2": 25,
        "stale_key_1": 300,
        "stale_key_2": 1200,
    }
    previous_keys = {"active_key_1", "active_key_2", "stale_key_1", "stale_key_2", "deleted_key_old"}

    result = RedisIdletimeSweep.execute_lossless_sweep(
        scan_keys_idletime=scan_keys,
        previous_keys=previous_keys,
        window_seconds=30.0,
        maxmemory_policy="volatile-lru",
    )
    assert result["fallback_to_digest_scan"] is False
    assert set(result["dirty_keys"]) == {"active_key_1", "active_key_2"}
    assert set(result["untouched_keys"]) == {"stale_key_1", "stale_key_2"}
    assert result["deleted_keys"] == ["deleted_key_old"]

    lfu_result = RedisIdletimeSweep.execute_lossless_sweep(
        scan_keys_idletime=scan_keys,
        previous_keys=previous_keys,
        window_seconds=30.0,
        maxmemory_policy="allkeys-lfu",
    )
    assert lfu_result["fallback_to_digest_scan"] is True
