import io
import json
import sqlite3
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from migkit.engines.extended import (
    DuckDBSecondReader,
    DuckDBSecondReaderAdapter,
    Neo4jEngine,
    Neo4jGraphEngine,
    ParquetManifestFenceReader,
    ParquetStorageReader,
    QdrantEngine,
    QdrantVectorEngine,
    SQLiteChangeTrackingEngine,
    SQLiteSessionAdapter,
    SQLiteSessionEngine,
    TrinoSecondReader,
    TrinoSecondReaderContract,
)


def test_sqlite_session_wal_and_snapshot(tmp_path):
    eng = SQLiteSessionEngine()
    db_file = str(tmp_path / "source.db")
    conn = eng.open(db_file)
    mode = eng.enable_wal(conn)
    assert mode == "wal"

    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
    cur.execute("INSERT INTO users (name) VALUES ('Alice');")
    conn.commit()

    ckpt = eng.wal_checkpoint(conn, "PASSIVE")
    assert isinstance(ckpt, tuple)
    assert len(ckpt) == 3

    backup_file = str(tmp_path / "backup.db")
    eng.snapshot(conn, backup_file)
    bconn = sqlite3.connect(backup_file)
    bcur = bconn.cursor()
    bcur.execute("SELECT name FROM users WHERE id = 1;")
    assert bcur.fetchone()[0] == "Alice"
    conn.close()
    bconn.close()


def test_sqlite_session_changeset_tracking(tmp_path):
    eng = SQLiteSessionAdapter()
    conn = eng.open(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT);")
    conn.commit()

    session = eng.create_session(conn)
    eng.attach_table(session, "items")

    cur.execute("INSERT INTO items (title) VALUES ('Widget A');")
    cur.execute("INSERT INTO items (title) VALUES ('Widget B');")
    cur.execute("UPDATE items SET title = 'Widget A+' WHERE id = 1;")
    conn.commit()

    changes = eng.generate_changeset(session)
    assert len(changes) >= 3
    ops = [c["op"] for c in changes]
    assert "INSERT" in ops
    assert "UPDATE" in ops

    patchset = eng.generate_patchset(session)
    assert len(patchset) >= 2

    inv = eng.invert_changeset(changes)
    assert len(inv) == len(changes)
    assert inv[0]["op"] == "UPDATE"
    conn.close()


def test_parquet_manifest_and_footer_fence(tmp_path):
    reader = ParquetStorageReader()
    table = pa.Table.from_pydict({
        "id": [1, 2, 3],
        "score": [10.5, 20.0, 30.2],
    })
    pfile = str(tmp_path / "data.parquet")
    pq.write_table(table, pfile)

    fence = reader.read_footer_fence(pfile)
    assert fence["num_rows"] == 3
    assert fence["num_row_groups"] == 1
    assert "id" in fence["schema_columns"]
    assert "score" in fence["schema_columns"]
    assert len(fence["fence_token"]) == 64

    manifest_files = [{
        "path": "data.parquet",
        "rows": fence["num_rows"],
        "hash": fence["fence_token"],
        "size": 1024,
    }]
    manifest = reader.create_manifest("snap-001", manifest_files)
    assert manifest["snapshot_id"] == "snap-001"
    f_token = reader.compute_fence(manifest)
    assert len(f_token) == 64

    new_manifest_files = [
        manifest_files[0],
        {"path": "data2.parquet", "rows": 5, "hash": "token2", "size": 2048},
    ]
    new_manifest = reader.create_manifest("snap-002", new_manifest_files)
    changes = reader.detect_changes(manifest, new_manifest)
    assert len(changes["added"]) == 1
    assert changes["added"][0]["path"] == "data2.parquet"
    assert len(changes["unchanged"]) == 1

    verif = reader.verify_changed_only(manifest, new_manifest)
    assert verif["checked_count"] == 1
    assert verif["status"] == "ok"


def test_duckdb_second_reader_aggregates_and_verify():
    reader = DuckDBSecondReader(threads=2)
    conn = reader.connect()

    src_data = [
        {"id": 1, "val": 10.0, "status": "active"},
        {"id": 2, "val": 20.0, "status": "pending"},
        {"id": 3, "val": 30.0, "status": "active"},
    ]
    dst_data = [
        {"id": 1, "val": 10.0, "status": "active"},
        {"id": 2, "val": 20.0, "status": "pending"},
        {"id": 3, "val": 30.0, "status": "active"},
    ]

    reader.register_table(conn, "src_t", src_data)
    reader.register_table(conn, "dst_t", dst_data)

    aggs = reader.column_aggregates(conn, "src_t", ["val"])
    assert aggs["__total_rows"] == 3
    assert aggs["val__sum"] == 60.0

    res = reader.verify_table_pair(conn, "src_t", "dst_t", ["id"], ["val", "status"])
    assert res.status == "ok"

    diff_data = [
        {"id": 1, "val": 10.0, "status": "active"},
        {"id": 2, "val": 99.0, "status": "pending"},
        {"id": 3, "val": 30.0, "status": "active"},
    ]
    reader.register_table(conn, "dst_diff", diff_data)
    diff_res = reader.verify_table_pair(conn, "src_t", "dst_diff", ["id"], ["val", "status"])
    assert diff_res.status == "diff"

    mismatches = reader.find_mismatches(conn, "src_t", "dst_diff", ["id"], ["val", "status"])
    assert len(mismatches) == 1
    assert mismatches[0]["id"] == 2


def test_qdrant_vector_engine_contract():
    eng = QdrantEngine()
    cfg = eng.collection_config(128, distance="Cosine", hnsw_m=32, ef_construct=128)
    assert cfg["vectors"]["size"] == 128
    assert cfg["vectors"]["distance"] == "Cosine"
    assert cfg["hnsw_config"]["m"] == 32

    v1 = [0.1] * 128
    d1 = eng.point_digest(101, v1, {"category": "test"})
    d2 = eng.point_digest(101, v1, {"category": "test"})
    assert d1 == d2
    assert len(d1) == 64

    points = [
        {"id": 2, "vector": v1, "payload": {"tag": "b"}},
        {"id": 1, "vector": v1, "payload": {"tag": "a"}},
        {"id": 3, "vector": v1, "payload": {"tag": "c"}},
    ]
    batch, next_id = eng.scroll_points(points, limit=2)
    assert len(batch) == 2
    assert batch[0]["id"] == 1
    assert batch[1]["id"] == 2
    assert next_id == 2

    c_digest = eng.compute_collection_digest(points)
    assert len(c_digest) == 64

    src_top = [[1, 2, 3, 4], [10, 11, 12, 13]]
    dst_top = [[1, 2, 3, 4], [10, 11, 12, 14]]
    recall = eng.evaluate_recall_at_k(src_top, dst_top, k=4)
    assert 0.85 <= recall <= 0.90

    res_ok = eng.verify_ann_queries(src_top, src_top, query_vectors=[[0.0]], k=4, min_recall=0.99)
    assert res_ok.status == "ok"


def test_neo4j_graph_engine_contract():
    eng = Neo4jEngine()
    facts = eng.schema_facts(
        labels=["User", "Product"],
        rel_types=["PURCHASED", "RATED"],
        constraints=[{"name": "uniq_user_email", "type": "UNIQUENESS"}],
        indexes=[{"name": "user_id_idx"}],
    )
    assert "User" in facts["labels"]
    assert "PURCHASED" in facts["relationship_types"]
    assert len(facts["constraints"]) == 1

    nd = eng.node_digest("User", "usr-123", {"name": "Bob", "tier": "gold"})
    assert len(nd) == 64

    rd = eng.relationship_digest("PURCHASED", "usr-123", "prod-456", {"qty": 2})
    assert len(rd) == 64

    rels = [
        {"type": "PURCHASED", "from_bkey": "usr-123", "to_bkey": "prod-456"},
        {"type": "PURCHASED", "from_bkey": "usr-123", "to_bkey": "prod-999"},
    ]
    ok, missing = eng.verify_endpoints(rels, {"usr-123", "prod-456"})
    assert not ok
    assert len(missing) == 1
    assert "prod-999" in missing[0]

    dump_cmd = eng.bulk_export_cmd("neo4j", "/tmp/backup.dump")
    assert dump_cmd[0] == "neo4j-admin"
    assert "dump" in dump_cmd

    load_cmd = eng.bulk_import_cmd("/tmp/backup.dump", "neo4j", overwrite=True)
    assert "--overwrite-destination=true" in load_cmd


def test_trino_second_reader_contract():
    reader = TrinoSecondReader(host="trino-coord", port=8080, catalog="lakehouse", schema="sales")
    info = reader.connection_info()
    assert info["source_type"] == "Trino"
    assert info["catalog"] == "lakehouse"

    agg_q = reader.column_aggregates_query(None, None, "orders", ["amount", "tax"])
    assert "lakehouse" in agg_q
    assert "amount__sum" in agg_q
    assert "tax__cardinality" in agg_q

    hash_q = reader.row_hash_query(None, None, "orders", ["order_id"], ["amount", "tax"])
    assert "sha256" in hash_q
    assert "__row_hash" in hash_q

    sample_q = reader.sample_query(None, None, "orders", percentage=5.0)
    assert "TABLESAMPLE BERNOULLI" in sample_q

    src_agg = {"row_count": 1000, "amount__sum": 50000.0}
    dst_agg = {"row_count": 1000, "amount__sum": 50000.0}
    match_res = reader.run_contract_check({
        "table": "orders",
        "src_aggregates": src_agg,
        "dst_aggregates": dst_agg,
    })
    assert match_res.status == "ok"

    dst_diff_agg = {"row_count": 999, "amount__sum": 49950.0}
    diff_res = reader.run_contract_check({
        "table": "orders",
        "src_aggregates": src_agg,
        "dst_aggregates": dst_diff_agg,
    })
    assert diff_res.status == "diff"
