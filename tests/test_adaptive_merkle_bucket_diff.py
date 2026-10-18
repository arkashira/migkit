import time
from types import SimpleNamespace
from migkit import leaves, scanplan
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("scanplan", "adaptive_merkle_rungs", "hierarchical_merkle_partitioning", "holds"),
    ("scanplan", "adaptive_merkle_rungs", "hierarchical_merkle_partitioning", "refuses"),
    ("scanplan", "adaptive_merkle_rungs", "content_hash_integrity_preservation", "holds"),
    ("scanplan", "adaptive_merkle_rungs", "content_hash_integrity_preservation", "refuses"),
    ("scanplan", "adaptive_merkle_rungs", "recursive_bisection_envelope", "holds"),
    ("scanplan", "adaptive_merkle_rungs", "recursive_bisection_envelope", "refuses"),
    ("scanplan", "adaptive_merkle_rungs", "o1_identical_range_pruning", "holds"),
    ("scanplan", "adaptive_merkle_rungs", "o1_identical_range_pruning", "refuses"),
    ("scanplan", "adaptive_merkle_rungs", "refuse_unindexable_merkle_slice", "holds"),
    ("scanplan", "adaptive_merkle_rungs", "refuse_unindexable_merkle_slice", "refuses"),
]


def test_adaptive_merkle_bucket_query_mysql_structure():
    sql = scanplan.adaptive_merkle_bucket_query(
        "mysql", "orders", "id", hash_expr="row_hash",
        num_buckets=1024, lo=1, hi=10_000_000, aliases=True)
    assert "((`id` - 1) DIV 9766) as bucket_id" in sql
    assert "COUNT(*) as row_count" in sql
    assert "BIT_XOR(row_hash) as bucket_hash" in sql
    assert "WHERE (`id` >= 1 AND `id` <= 10000000)" in sql
    assert "GROUP BY ((`id` - 1) DIV 9766)" in sql
    assert "ORDER BY bucket_id" in sql


def test_adaptive_merkle_bucket_query_postgres_structure():
    sql = scanplan.adaptive_merkle_bucket_query(
        "postgres", "public.orders", "id", hash_expr="row_hash",
        num_buckets=1024, lo=1, hi=10_000_000, aliases=False)
    assert 'div("id" - 1, 9766)' in sql
    assert "COUNT(*)" in sql
    assert "bit_xor(row_hash)" in sql
    assert 'WHERE ("id" >= 1 AND "id" <= 10000000)' in sql
    assert 'GROUP BY div("id" - 1, 9766)' in sql


def test_adaptive_merkle_bucket_differ_all_identical_o1():
    differ = leaves.AdaptiveBucketDiffer(
        key_col="id", lo=1, hi=10_000_000, num_buckets=1024)

    queries_issued = []
    def fake_query(side, lo, hi, nb):
        queries_issued.append((side, lo, hi, nb))
        return [(b, 9765, 0x12345678) for b in range(nb)]

    drilldown_rows = []
    def fake_row_query(side, lo, hi):
        drilldown_rows.append((side, lo, hi))
        return []

    res = differ.run(fake_query, row_fn=fake_row_query, table="orders", total_rows=10_000_000)
    assert res.certified_complete is True
    assert res.status == "ok"
    assert res.identical_buckets == 1024
    assert res.divergent_buckets == 0
    assert len(res.mismatched_keys) == 0
    assert len(queries_issued) == 2
    assert len(drilldown_rows) == 0
    assert res.drilldown_rows_examined == 0


def test_adaptive_merkle_acceptance_10m_rows_5_mismatched():
    total_rows = 10_000_000
    mismatched_pks = {42, 100_000, 2_500_000, 7_000_000, 9_999_999}
    differ = leaves.AdaptiveBucketDiffer(
        key_col="id", lo=1, hi=total_rows, num_buckets=1024, leaf_threshold=64)

    def row_val(side, k):
        base = (k * 1337) ^ 0xDEADBEEF
        if side == "dst" and k in mismatched_pks:
            return base ^ 0xFFFF
        return base

    def query_fn(side, lo, hi, nb):
        span = hi - lo + 1
        width = max(1, (span + nb - 1) // nb)
        buckets = {}
        for b in range(nb):
            b_lo = lo + b * width
            b_hi = min(hi, lo + (b + 1) * width - 1)
            if b_lo > hi:
                break
            count = b_hi - b_lo + 1
            h = 0
            has_diff = any(k in mismatched_pks for k in range(b_lo, b_hi + 1))
            if not has_diff:
                h = (b * 99991) ^ 0xABCDEF
            else:
                for k in range(b_lo, b_hi + 1):
                    h ^= row_val(side, k)
            buckets[b] = (b, count, h)
        return list(buckets.values())

    def row_fn(side, lo, hi):
        return [(k, row_val(side, k)) for k in range(lo, hi + 1)]

    t0 = time.monotonic()
    res = differ.run(query_fn, row_fn=row_fn, table="big_table", total_rows=total_rows)
    duration = time.monotonic() - t0

    assert duration < 15.0
    assert set(res.mismatched_keys) == mismatched_pks
    assert res.certified_complete is False
    assert res.status == "diff"
    assert res.identical_buckets >= 1019
    max_allowed_rows = total_rows * 0.001
    assert res.drilldown_rows_examined < max_allowed_rows


def test_mysql_engine_adaptive_merkle_diff_identical():
    eng = object.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(slice=10)
    eng._scope = lambda side, db, t: f"`{db}`.`{t}`"

    asked = []
    def fake_q(side, sql):
        asked.append((side, sql))
        if "MIN(`id`)" in sql:
            return [(1, 1_000_000, 1_000_000)]
        return [(b, 1000, 0x5555) for b in range(1024)]

    eng._q = fake_q
    res = eng.adaptive_merkle_bucket_diff("prod", "users", "id", num_buckets=1024)
    assert res.certified_complete is True
    assert res.status == "ok"
    assert res.identical_buckets == 1024
    assert len(res.mismatched_keys) == 0


def test_postgres_engine_adaptive_merkle_diff_mismatches():
    eng = object.__new__(PostgresEngine)
    eng.hop = SimpleNamespace(slice=10)
    eng._scope = lambda db, t: f'"{t}"'

    asked = []
    def fake_psql(side, db, sql):
        asked.append((side, sql))
        if "min(" in sql.lower():
            return "1|1000000|1000000\n"
        if "group by" in sql.lower():
            lines = []
            for b in range(128):
                h = 0xAAAA if (side == "dst" and b == 5) else 0x5555
                lines.append(f"{b}|7812|{h}")
            return "\n".join(lines)
        return "40000|1111\n40001|2222\n" if side == "src" else "40000|1111\n40001|9999\n"

    eng._psql = fake_psql
    res = eng.adaptive_merkle_bucket_diff("prod", "users", "id", num_buckets=128, leaf_threshold=10000)
    assert res.certified_complete is False
    assert 40001 in res.mismatched_keys


def test_adaptive_merkle_empty_table():
    differ = leaves.AdaptiveBucketDiffer(key_col="id", lo=10, hi=5)
    res = differ.run(lambda s, lo, hi, nb: [], table="empty_tbl")
    assert res.certified_complete is True
    assert res.total_rows == 0
    assert len(res.mismatched_keys) == 0


def test_adaptive_merkle_missing_and_extra_keys():
    differ = leaves.AdaptiveBucketDiffer(key_col="id", lo=1, hi=100, num_buckets=4, leaf_threshold=50)

    def fake_query(side, lo, hi, nb):
        if side == "src":
            return [(0, 25, 0x1111), (1, 25, 0x2222)]
        return [(0, 24, 0x1111), (1, 26, 0x9999)]

    def fake_row_fn(side, lo, hi):
        if side == "src":
            return [(10, 100), (20, 200)]
        return [(20, 200), (30, 300)]

    res = differ.run(fake_query, row_fn=fake_row_fn, table="users")
    assert res.certified_complete is False
    assert 10 in res.mismatched_keys
    assert 30 in res.mismatched_keys


def test_adaptive_merkle_query_modulo_fallback_and_where():
    mysql_sql = scanplan.adaptive_merkle_bucket_query(
        "mysql", "orders", "uuid", num_buckets=256, where="status = 'active'")
    assert "conv(substr(md5(cast(`uuid` as char)), 1, 8), 16, 10) % 256" in mysql_sql
    assert "WHERE (status = 'active')" in mysql_sql

    pg_sql = scanplan.adaptive_merkle_bucket_query(
        "postgres", "orders", "uuid", num_buckets=256, where="status = 'active'")
    assert "abs(('x' || substr(md5(cast(\"uuid\" as text)), 1, 8))::bit(32)::bigint) % 256" in pg_sql
    assert "WHERE (status = 'active')" in pg_sql


def test_adaptive_merkle_rungs_hold_and_refuse():
    with watched() as seen:
        c = scanplan.decide_adaptive_merkle({"total_rows": 2_000_000, "threshold": 1_000_000, "key_col": "id"})
    assert c.rung.name == "hierarchical_merkle_partitioning"
    assert branch(seen, "hierarchical_merkle_partitioning") == [HOLDS]

    with watched() as seen:
        c = scanplan.decide_adaptive_merkle({
            "total_rows": 500_000, "threshold": 1_000_000, "key_col": "id",
            "hash_expr": "row_hash", "key_only_hashing": False
        })
    assert branch(seen, "hierarchical_merkle_partitioning") == [REFUSES]
    assert c.rung.name == "content_hash_integrity_preservation"
    assert branch(seen, "content_hash_integrity_preservation") == [HOLDS]

    with watched() as seen:
        c = scanplan.decide_adaptive_merkle({
            "total_rows": 500_000, "threshold": 1_000_000, "key_col": "id",
            "hash_expr": "", "depth": 2, "max_depth": 8, "bisection_active": True
        })
    assert branch(seen, "content_hash_integrity_preservation") == [REFUSES]
    assert c.rung.name == "recursive_bisection_envelope"
    assert branch(seen, "recursive_bisection_envelope") == [HOLDS]

    with watched() as seen:
        c = scanplan.decide_adaptive_merkle({
            "total_rows": 500_000, "threshold": 1_000_000, "key_col": "id",
            "hash_expr": "", "depth": 10, "max_depth": 8, "identical_buckets": 100
        })
    assert branch(seen, "recursive_bisection_envelope") == [REFUSES]
    assert c.rung.name == "o1_identical_range_pruning"
    assert branch(seen, "o1_identical_range_pruning") == [HOLDS]

    with watched() as seen:
        c = scanplan.decide_adaptive_merkle({
            "total_rows": 500_000, "threshold": 1_000_000, "key_col": "id",
            "hash_expr": "", "depth": 10, "max_depth": 8, "identical_buckets": 0
        })
    assert branch(seen, "o1_identical_range_pruning") == [REFUSES]
    assert c.rung.name == "refuse_unindexable_merkle_slice"
    assert branch(seen, "refuse_unindexable_merkle_slice") == [HOLDS]

    with watched() as seen:
        try:
            scanplan.decide_adaptive_merkle({
                "total_rows": 5_000_000, "threshold": 1_000_000, "key_col": "id",
                "hash_expr": "", "depth": 10, "max_depth": 8, "identical_buckets": 0
            })
        except Exception:
            pass
    assert branch(seen, "refuse_unindexable_merkle_slice") == [REFUSES]


def test_merkle_content_drift_caught_by_hash_expression():
    mysql_sql = scanplan.adaptive_merkle_bucket_query("mysql", "orders", "id", hash_expr="row_hash_expr")
    assert "row_hash_expr" in mysql_sql
    assert "BIT_XOR(row_hash_expr)" in mysql_sql

    mysql_sql_auto = scanplan.adaptive_merkle_bucket_query("mysql", "orders", "id", hash_expr=None)
    assert "md5(cast(`id` as char))" in mysql_sql_auto


def test_dynamic_merkle_getters_resolution(monkeypatch):
    monkeypatch.setenv("MIGKIT_MERKLE_THRESHOLD", "500000")
    monkeypatch.setenv("MIGKIT_MERKLE_BUCKETS", "512")
    monkeypatch.setenv("MIGKIT_MERKLE_LEAF_THRESHOLD", "32")
    monkeypatch.setenv("MIGKIT_MERKLE_MAX_DEPTH", "12")

    assert scanplan.get_default_merkle_threshold() == 500000
    assert scanplan.get_default_merkle_buckets() == 512
    assert scanplan.get_default_merkle_leaf_threshold() == 32
    assert scanplan.get_default_merkle_max_depth() == 12


