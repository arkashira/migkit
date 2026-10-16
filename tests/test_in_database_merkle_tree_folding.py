import hashlib
import sqlite3
import pytest

from migkit import merkle
from migkit.merkle import (
    MerkleTreeFolder,
    find_discordant_ranges,
    register_sqlite_merkle_functions,
    row_hash_sql,
    bucket_expr,
    bucket_hash_agg_sql,
    leaf_buckets_query,
    hierarchical_fold_sql,
)


def test_postgres_sql_dialect_expressions():
    cols = ["id", "name", "val"]
    h_sha = row_hash_sql("postgres", cols, "sha256")
    assert "sha256" in h_sha
    assert "encode(" in h_sha
    assert "concat_ws('|'" in h_sha

    h_md5 = row_hash_sql("postgres", cols, "md5")
    assert "md5(" in h_md5
    assert "concat_ws('|'" in h_md5

    b_keyed = bucket_expr("postgres", num_buckets=64, key_col="id", min_key=1, max_key=1000)
    assert "double precision" in b_keyed
    assert "::int" in b_keyed

    b_ctid = bucket_expr("postgres", num_buckets=64, keyless_mode="ctid")
    assert "hashtext(ctid::text)" in b_ctid

    b_modulo = bucket_expr("postgres", num_buckets=64, keyless_mode="modulo_hash", columns=cols)
    assert "bit(32)::bigint" in b_modulo

    agg_sha = bucket_hash_agg_sql("postgres", h_sha, "sha256", order_by="id")
    assert "string_agg" in agg_sha
    assert "ORDER BY id" in agg_sha


def test_mysql_sql_dialect_expressions():
    cols = ["id", "title", "content"]
    h_sha = row_hash_sql("mysql", cols, "sha256")
    assert "sha2(" in h_sha
    assert "256" in h_sha

    h_md5 = row_hash_sql("mysql", cols, "md5")
    assert "md5(" in h_md5

    b_keyed = bucket_expr("mysql", num_buckets=32, key_col="id", min_key=10, max_key=500)
    assert "cast(id as double)" in b_keyed

    b_modulo = bucket_expr("mysql", num_buckets=32, keyless_mode="modulo_hash", columns=cols)
    assert "conv(substr(" in b_modulo

    agg_sha = bucket_hash_agg_sql("mysql", h_sha, "sha256", order_by="id")
    assert "group_concat" in agg_sha
    assert "separator ''" in agg_sha


def test_sqlite_sql_dialect_expressions():
    cols = ["id", "data"]
    h_sha = row_hash_sql("sqlite", cols, "sha256")
    assert "sha256(" in h_sha
    assert " || '|' || " in h_sha

    h_md5 = row_hash_sql("sqlite", cols, "md5")
    assert "md5(" in h_md5

    b_keyed = bucket_expr("sqlite", num_buckets=16, key_col="id", min_key=1, max_key=50)
    assert "cast((cast(id as real)" in b_keyed

    b_rowid = bucket_expr("sqlite", num_buckets=16, keyless_mode="rowid")
    assert "abs(rowid)" in b_rowid

    b_modulo = bucket_expr("sqlite", num_buckets=16, keyless_mode="modulo_hash", columns=cols)
    assert "unicode(substr(" in b_modulo


def test_merkle_tree_folder_generates_32_byte_root():
    folder = MerkleTreeFolder(dialect="postgres", num_buckets=16)
    buckets = {i: (10, hashlib.sha256(f"bucket_{i}".encode()).hexdigest()) for i in range(16)}

    root_bytes = folder.fold_bucket_hashes(buckets)
    assert isinstance(root_bytes, bytes)
    assert len(root_bytes) == 32
    assert len(folder.root_hex) == 64
    assert folder.tree is not None
    assert folder.tree.depth == 4
    assert len(folder.tree.leaves) == 16


def test_merkle_tree_folder_root_sensitivity_and_repeatability():
    folder1 = MerkleTreeFolder(dialect="sqlite", num_buckets=8)
    folder2 = MerkleTreeFolder(dialect="sqlite", num_buckets=8)
    folder3 = MerkleTreeFolder(dialect="sqlite", num_buckets=8)

    buckets_a = {i: (5, hashlib.sha256(f"b_{i}".encode()).hexdigest()) for i in range(8)}
    buckets_b = {i: (5, hashlib.sha256(f"b_{i}".encode()).hexdigest()) for i in range(8)}
    buckets_c = dict(buckets_a)
    buckets_c[3] = (5, hashlib.sha256(b"altered").hexdigest())

    r1 = folder1.fold_bucket_hashes(buckets_a)
    r2 = folder2.fold_bucket_hashes(buckets_b)
    r3 = folder3.fold_bucket_hashes(buckets_c)

    assert r1 == r2
    assert r1 != r3


def test_in_database_folding_keyed_table_matching():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL)")
        for i in range(1, 101):
            conn.execute("INSERT INTO products VALUES (?, ?, ?)", (i, f"prod_{i}", i * 2.5))

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=100)
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=100)

    root_s = folder_s.compute_from_db(conn_s, "products", ["id", "name", "price"])
    root_t = folder_t.compute_from_db(conn_t, "products", ["id", "name", "price"])

    assert root_s == root_t
    assert len(root_s) == 32
    discordant = find_discordant_ranges(folder_s, folder_t)
    assert discordant == []


def test_in_database_folding_drifted_row_bisection():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount REAL, status TEXT)")
        for i in range(1, 201):
            conn.execute("INSERT INTO orders VALUES (?, ?, ?)", (i, float(i * 10), "PAID"))

    conn_t.execute("UPDATE orders SET status = 'REFUNDED' WHERE id = 77")

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=200)
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=200)

    root_s = folder_s.compute_from_db(conn_s, "orders", ["id", "amount", "status"])
    root_t = folder_t.compute_from_db(conn_t, "orders", ["id", "amount", "status"])

    assert root_s != root_t

    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) == 1
    target_range = discordant[0]
    assert target_range.range_min <= 77 <= target_range.range_max
    assert target_range.key_column == "id"
    assert not target_range.is_keyless

    drifted_keys = folder_s.bisect_discordant_range(
        conn_s, conn_t, "orders", target_range, ["id", "amount", "status"]
    )
    assert (77, 77) in drifted_keys


def test_in_database_folding_inserted_and_deleted_rows():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE events (id INT PRIMARY KEY, kind TEXT)")
        for i in range(1, 101):
            conn.execute("INSERT INTO events VALUES (?, ?)", (i, f"evt_{i}"))

    conn_t.execute("INSERT INTO events VALUES (101, 'evt_101')")
    conn_t.execute("DELETE FROM events WHERE id = 15")

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=101)
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=101)

    root_s = folder_s.compute_from_db(conn_s, "events", ["id", "kind"])
    root_t = folder_t.compute_from_db(conn_t, "events", ["id", "kind"])

    assert root_s != root_t
    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) == 2

    bucket_15 = [d for d in discordant if d.range_min <= 15 <= d.range_max]
    bucket_101 = [d for d in discordant if d.range_min <= 101 <= d.range_max]
    assert len(bucket_15) == 1
    assert len(bucket_101) == 1


def test_keyless_table_rowid_folding_and_bisection():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE access_logs (ip TEXT, path TEXT)")
        for i in range(100):
            conn.execute("INSERT INTO access_logs VALUES (?, ?)", (f"10.0.0.{i}", f"/api/{i}"))

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=8, keyless_mode="rowid")
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=8, keyless_mode="rowid")

    root_s = folder_s.compute_from_db(conn_s, "access_logs", ["ip", "path"])
    root_t = folder_t.compute_from_db(conn_t, "access_logs", ["ip", "path"])
    assert root_s == root_t

    conn_t.execute("UPDATE access_logs SET path = '/api/hacked' WHERE rowid = 25")
    root_t_drift = folder_t.compute_from_db(conn_t, "access_logs", ["ip", "path"])
    assert root_s != root_t_drift

    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) == 1
    assert discordant[0].is_keyless is True
    assert discordant[0].bucket_id == (25 % 8)


def test_keyless_table_modulo_hash_folding():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE audit (action TEXT, actor TEXT)")
        for i in range(50):
            conn.execute("INSERT INTO audit VALUES (?, ?)", (f"ACTION_{i}", f"USER_{i}"))

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=8, keyless_mode="modulo_hash")
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=8, keyless_mode="modulo_hash")

    root_s = folder_s.compute_from_db(conn_s, "audit", ["action", "actor"])
    root_t = folder_t.compute_from_db(conn_t, "audit", ["action", "actor"])
    assert root_s == root_t

    conn_t.execute("UPDATE audit SET actor = 'ADMIN' WHERE action = 'ACTION_12'")
    root_t_drift = folder_t.compute_from_db(conn_t, "audit", ["action", "actor"])
    assert root_s != root_t_drift

    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) >= 1
    assert discordant[0].is_keyless is True


def test_hierarchical_in_database_cte_execution():
    conn = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn)

    conn.execute("CREATE TABLE metrics (id INT PRIMARY KEY, val REAL)")
    for i in range(1, 65):
        conn.execute("INSERT INTO metrics VALUES (?, ?)", (i, float(i)))

    folder = MerkleTreeFolder(dialect="sqlite", num_buckets=8, key_column="id", min_key=1, max_key=64)
    rows_cnt, root_hex = folder.compute_hierarchical_root(conn, "metrics", ["id", "val"])
    assert rows_cnt == 64
    assert len(root_hex) == 64

    conn.execute("UPDATE metrics SET val = 999.0 WHERE id = 10")
    _, root_hex_modified = folder.compute_hierarchical_root(conn, "metrics", ["id", "val"])
    assert root_hex != root_hex_modified


def test_binary_bisection_log_n_steps_on_wide_tree():
    folder_s = MerkleTreeFolder(dialect="postgres", num_buckets=64)
    folder_t = MerkleTreeFolder(dialect="postgres", num_buckets=64)

    buckets_s = {i: (10, hashlib.sha256(f"bucket_{i}".encode()).hexdigest()) for i in range(64)}
    buckets_t = dict(buckets_s)
    buckets_t[47] = (10, hashlib.sha256(b"drift_47").hexdigest())

    folder_s.fold_bucket_hashes(buckets_s)
    folder_t.fold_bucket_hashes(buckets_t)

    assert folder_s.root_bytes != folder_t.root_bytes
    discordant = find_discordant_ranges(folder_s, folder_t)

    assert len(discordant) == 1
    assert discordant[0].bucket_id == 47


def test_empty_table_merkle_folding():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE empty_tbl (id INT PRIMARY KEY, name TEXT)")

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=100)
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=16, key_column="id", min_key=1, max_key=100)

    root_s = folder_s.compute_from_db(conn_s, "empty_tbl", ["id", "name"])
    root_t = folder_t.compute_from_db(conn_t, "empty_tbl", ["id", "name"])

    assert root_s == root_t
    assert len(root_s) == 32
    assert find_discordant_ranges(folder_s, folder_t) == []


def test_single_value_key_range():
    conn_s = sqlite3.connect(":memory:")
    conn_t = sqlite3.connect(":memory:")
    register_sqlite_merkle_functions(conn_s)
    register_sqlite_merkle_functions(conn_t)

    for conn in (conn_s, conn_t):
        conn.execute("CREATE TABLE singleton (id INT PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO singleton VALUES (1, 'initial')")

    folder_s = MerkleTreeFolder(dialect="sqlite", num_buckets=8, key_column="id", min_key=1, max_key=1)
    folder_t = MerkleTreeFolder(dialect="sqlite", num_buckets=8, key_column="id", min_key=1, max_key=1)

    root_s = folder_s.compute_from_db(conn_s, "singleton", ["id", "val"])
    root_t = folder_t.compute_from_db(conn_t, "singleton", ["id", "val"])
    assert root_s == root_t

    conn_t.execute("UPDATE singleton SET val = 'mutated' WHERE id = 1")
    root_t_mut = folder_t.compute_from_db(conn_t, "singleton", ["id", "val"])
    assert root_s != root_t_mut

    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) == 1
    assert discordant[0].bucket_id == 0


def test_md5_algorithm_mode_folding():
    folder_s = MerkleTreeFolder(dialect="postgres", hash_alg="md5", num_buckets=8)
    folder_t = MerkleTreeFolder(dialect="postgres", hash_alg="md5", num_buckets=8)

    buckets_s = {i: (1, hashlib.md5(f"data_{i}".encode()).hexdigest()) for i in range(8)}
    buckets_t = dict(buckets_s)
    buckets_t[2] = (1, hashlib.md5(b"differ").hexdigest())

    r_s = folder_s.fold_bucket_hashes(buckets_s)
    r_t = folder_t.fold_bucket_hashes(buckets_t)

    assert len(r_s) == 32
    assert len(r_t) == 32
    assert r_s != r_t

    discordant = find_discordant_ranges(folder_s, folder_t)
    assert len(discordant) == 1
    assert discordant[0].bucket_id == 2


def test_unsupported_dialect_raises():
    with pytest.raises(ValueError):
        row_hash_sql("invalid_dialect", ["col"])

    with pytest.raises(ValueError):
        bucket_hash_agg_sql("invalid_dialect", "rh")

