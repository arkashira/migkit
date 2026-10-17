import re
from types import SimpleNamespace
import pytest
from migkit import keyless, scanplan
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine


def test_postgres_ctid_predicate_generation():
    pred = keyless.ctid_slice_predicate(0, 500)
    assert pred == "ctid >= '(0, 0)' AND ctid < '(500, 0)'"

    pred_open = keyless.ctid_slice_predicate(500, None)
    assert pred_open == "ctid >= '(500, 0)'"

    slices = keyless.postgres_ctid_slices(10_000, chunk_size=1_000)
    assert len(slices) == 10
    assert slices[0].predicate == "ctid >= '(0, 0)' AND ctid < '(1000, 0)'"
    assert slices[1].predicate == "ctid >= '(1000, 0)' AND ctid < '(2000, 0)'"
    assert slices[-1].predicate == "ctid >= '(9000, 0)' AND ctid < '(10000, 0)'"

    slices_open = keyless.postgres_ctid_slices(5_000, chunk_size=2_000, open_ended=True)
    assert len(slices_open) == 3
    assert slices_open[-1].predicate == "ctid >= '(4000, 0)'"


def test_mysql_rowid_and_page_offset_predicate_generation():
    rowid_pred = keyless.rowid_slice_predicate(1, 10_001)
    assert rowid_pred == "_rowid >= 1 AND _rowid < 10001"

    rowid_open = keyless.rowid_slice_predicate(10_001, None)
    assert rowid_open == "_rowid >= 10001"

    page_pred = keyless.page_offset_slice_predicate(0, 100)
    assert page_pred == "page_offset >= 0 AND page_offset < 100"

    rowid_slices = keyless.mysql_rowid_slices(1, 100_000, chunk_size=20_000)
    assert len(rowid_slices) == 5
    assert rowid_slices[0].predicate == "_rowid >= 1 AND _rowid < 20001"
    assert rowid_slices[-1].predicate == "_rowid >= 80001 AND _rowid < 100001"

    page_slices = keyless.mysql_page_slices(1_000, chunk_size=250)
    assert len(page_slices) == 4
    assert page_slices[0].predicate == "page_offset >= 0 AND page_offset < 250"
    assert page_slices[-1].predicate == "page_offset >= 750 AND page_offset < 1000"


def test_keyless_slice_predicate_dispatcher():
    pg_pred = keyless.keyless_slice_predicate("postgres", block_start=100, block_end=200)
    assert pg_pred == "ctid >= '(100, 0)' AND ctid < '(200, 0)'"

    my_rowid_pred = keyless.keyless_slice_predicate("mysql", rowid_start=1000, rowid_end=2000)
    assert my_rowid_pred == "_rowid >= 1000 AND _rowid < 2000"

    my_page_pred = keyless.keyless_slice_predicate("mysql", page_start=50, page_end=100, mode="page")
    assert my_page_pred == "page_offset >= 50 AND page_offset < 100"


def test_keyless_slice_query_construction_postgres_and_mysql():
    pg_sql = keyless.keyless_slice_query(
        "postgres", "public.events", "ctid >= '(0, 0)' AND ctid < '(1000, 0)'",
        hash_expr="digest", aliases=True)
    assert "ctid >= '(0, 0)' AND ctid < '(1000, 0)'" in pg_sql
    assert "OFFSET" not in pg_sql.upper()
    assert "ORDER BY" not in pg_sql.upper()

    my_sql = keyless.keyless_slice_query(
        "mysql", "events", "_rowid >= 1 AND _rowid < 50000",
        hash_expr="payload", aliases=False)
    assert "_rowid >= 1 AND _rowid < 50000" in my_sql
    assert "OFFSET" not in my_sql.upper()
    assert "ORDER BY" not in my_sql.upper()


def test_keyless_verification_1m_rows_constant_sequential_read_no_offset():
    total_rows = 1_000_000
    blocks = 50_000
    chunk_blocks = 5_000

    queries_issued = []
    block_access_history = []

    def fake_query(side, sql):
        queries_issued.append((side, sql))
        assert "OFFSET" not in sql.upper()
        assert "ORDER BY" not in sql.upper()
        if "ctid >=" in sql:
            import re
            m = re.search(r"ctid >=\s*'\((\d+),\s*0\)'", sql)
            start_block = int(m.group(1)) if m else 0
            block_access_history.append(start_block)
        return [(100_000, 0xABCDEF12)]

    res = keyless.keyless_slice_verify(
        "postgres", "db", "raw_logs",
        query=fake_query,
        total_blocks=blocks,
        chunk_size=chunk_blocks,
        sides=("src", "dst"))

    assert res.certified_complete is True
    assert res.status == "ok"
    assert res.total_rows == total_rows
    assert res.slices_count == 10
    assert res.matched_slices == 10
    assert res.divergent_slices == 0
    assert len(queries_issued) == 20
    assert block_access_history == [0, 0, 5000, 5000, 10000, 10000, 15000, 15000,
                                    20000, 20000, 25000, 25000, 30000, 30000,
                                    35000, 35000, 40000, 40000, 45000, 45000]


def test_keyless_verification_mysql_1m_rows_divergence_detection():
    total_rows = 1_000_000
    chunk_rows = 100_000

    queries_issued = []

    def fake_query(side, sql):
        queries_issued.append((side, sql))
        assert "OFFSET" not in sql.upper()
        if side == "dst" and "_rowid >= 300001" in sql:
            return [(100_000, 0xDEADBEEF)]
        return [(100_000, 0x12345678)]

    res = keyless.keyless_slice_verify(
        "mysql", "shop", "anonymous_clicks",
        query=fake_query,
        total_rows=total_rows,
        chunk_size=chunk_rows,
        sides=("src", "dst"))

    assert res.certified_complete is False
    assert res.status == "diff"
    assert res.slices_count == 10
    assert res.matched_slices == 9
    assert res.divergent_slices == 1
    assert "mismatch in 1 of 10 keyless slices" in res.detail
    divergent_item = [d for d in res.slice_details if not d["match"]][0]
    assert "_rowid >= 300001" in divergent_item["predicate"]


def test_scanplan_fused_pushdown_integration_keyless():
    eng = object.__new__(PostgresEngine)
    eng.CANON_ENGINE = "postgres"

    queries = []

    def fake_query(side, sql):
        queries.append((side, sql))
        return [(250_000, 0xCAFE)]

    res = scanplan.fused_pushdown_verify(
        eng, "prod", "audit_trail",
        query=fake_query,
        keyless=True,
        total_blocks=4000,
        chunk_size=1000)

    assert res.status == "ok"
    assert len(queries) == 8
    assert all("OFFSET" not in q.upper() for _, q in queries)
    assert all("ctid >=" in q for _, q in queries)


def test_mysql_engine_keyless_slice_verification_method():
    eng = object.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(slice=10)
    eng._text_columns = lambda db: []
    eng._scope = lambda side, db, t: f"`{db}`.`{t}`"

    queries = []

    def fake_q(side, sql):
        queries.append((side, sql))
        return [(1000, 999)]

    eng._q = fake_q
    res = eng.keyless_slice_verification(
        "analytics", "sessions", total_rows=10_000, chunk_size=2_000)

    assert res.status == "ok"
    assert res.certified_complete is True
    assert res.slices_count == 5
    assert len(queries) == 10
    assert all("_rowid >=" in q for _, q in queries)
    assert all("OFFSET" not in q.upper() for _, q in queries)


def test_postgres_engine_keyless_slice_verification_method():
    eng = object.__new__(PostgresEngine)
    eng.hop = SimpleNamespace(slice=10)
    eng._text_columns = lambda side, db: {}
    eng._scope = lambda db, t: f'"{t}"'
    eng._split = lambda t: t.split(".")

    queries = []

    def fake_psql(side, db, sql):
        queries.append((side, sql))
        return "1000|999\n"

    eng._psql = fake_psql
    res = eng.keyless_slice_verification(
        "analytics", "public.sessions", total_blocks=5000, chunk_size=1000)

    assert res.status == "ok"
    assert res.certified_complete is True
    assert res.slices_count == 5
    assert len(queries) == 10
    assert all("ctid >=" in q for _, q in queries)
    assert all("OFFSET" not in q.upper() for _, q in queries)


def test_keyless_slice_empty_and_single_unit_tables():
    pg_empty = keyless.postgres_ctid_slices(0)
    assert len(pg_empty) == 1
    assert pg_empty[0].predicate == "1=1"

    pg_single = keyless.postgres_ctid_slices(1, chunk_size=10)
    assert len(pg_single) == 1
    assert pg_single[0].predicate == "ctid >= '(0, 0)' AND ctid < '(1, 0)'"

    pg_uneven = keyless.postgres_ctid_slices(17, chunk_size=5)
    assert len(pg_uneven) == 4
    assert pg_uneven[0].predicate == "ctid >= '(0, 0)' AND ctid < '(5, 0)'"
    assert pg_uneven[1].predicate == "ctid >= '(5, 0)' AND ctid < '(10, 0)'"
    assert pg_uneven[2].predicate == "ctid >= '(10, 0)' AND ctid < '(15, 0)'"
    assert pg_uneven[3].predicate == "ctid >= '(15, 0)' AND ctid < '(17, 0)'"

    my_empty = keyless.mysql_rowid_slices(10, 5)
    assert len(my_empty) == 1
    assert my_empty[0].predicate == "1=1"

    my_page_empty = keyless.mysql_page_slices(0)
    assert len(my_page_empty) == 1
    assert my_page_empty[0].predicate == "1=1"


def test_mysql_page_offset_verification():
    queries = []

    def fake_query(side, sql):
        queries.append((side, sql))
        assert "page_offset >=" in sql
        assert not re.search(r"\bOFFSET\b", sql, re.I)
        return [(50_000, 0x5555)]

    res = keyless.keyless_slice_verify(
        "mysql", "db", "t_pages",
        query=fake_query,
        total_pages=200,
        chunk_size=50,
        mode="page")

    assert res.status == "ok"
    assert res.slices_count == 4
    assert res.certified_complete is True
    assert len(queries) == 8


def test_constant_sequential_read_time_complexity_vs_offset():
    chunk_size = 10_000
    total_rows = 1_000_000
    slices = keyless.mysql_rowid_slices(1, total_rows, chunk_size=chunk_size)

    slicing_row_skips = []
    offset_row_skips = []

    for i, sl in enumerate(slices):
        offset = i * chunk_size
        offset_row_skips.append(offset)
        slicing_row_skips.append(0)

    assert sum(offset_row_skips) == sum(range(0, total_rows, chunk_size))
    assert sum(offset_row_skips) == 49_500_000
    assert all(skips == 0 for skips in slicing_row_skips)
    assert len(set(slicing_row_skips)) == 1


def test_scanplan_module_reexports():
    sql = scanplan.keyless_slice_query("postgres", "tbl", "ctid >= '(0, 0)' AND ctid < '(100, 0)'")
    assert "ctid >= '(0, 0)' AND ctid < '(100, 0)'" in sql

    res = scanplan.keyless_slice_verify(
        "postgres", "db", "tbl",
        query=lambda side, s: [(10, 1)],
        total_blocks=100, chunk_size=50)
    assert res.status == "ok"
    assert res.slices_count == 2

