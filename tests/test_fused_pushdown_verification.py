from types import SimpleNamespace
from migkit import scanplan
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine


def test_fused_pushdown_query_sql_structure():
    sql = scanplan.fused_pushdown_query(
        "mysql", "orders", hash_agg="<hash_agg>",
        charset_columns=["c1", "c2"],
        null_empty_conditions=["c1 IS NULL AND c2 = ''"],
        quote_identifiers=False)
    assert sql.startswith("SELECT count(*), <hash_agg>, ")
    assert "SUM(CASE WHEN c1 LIKE '%\ufffd%' OR c2 LIKE '%\ufffd%' THEN 1 ELSE 0 END)" in sql
    assert "SUM(CASE WHEN c1 IS NULL AND c2 = '' THEN 1 ELSE 0 END)" in sql
    assert sql.endswith("FROM orders")

    sql_aliases = scanplan.fused_pushdown_query(
        "mysql", "orders", hash_agg="BIT_XOR(HASH_EXPR)",
        charset_columns=["c1", "c2"],
        null_empty_conditions=["c1 IS NULL AND c2 = ''"],
        aliases=True, quote_identifiers=False)
    assert "COUNT(*) as total_rows" in sql_aliases
    assert "BIT_XOR(HASH_EXPR) as data_hash" in sql_aliases
    assert "as charset_anomalies" in sql_aliases
    assert "as null_empty_diffs" in sql_aliases

    pg_sql = scanplan.fused_pushdown_query(
        "postgres", "public.orders", hash_agg="<hash_agg>",
        charset_columns=["c1", "c2"],
        null_empty_columns=["c1", "c2"],
        where="id >= 100 AND id < 200")
    assert '"c1" LIKE \'%\ufffd%\'' in pg_sql
    assert '"c2" LIKE \'%\ufffd%\'' in pg_sql
    assert 'WHERE id >= 100 AND id < 200' in pg_sql


def test_fused_pushdown_single_query_across_30_columns():
    cols = [f"col_{i}" for i in range(1, 31)]
    sql = scanplan.fused_pushdown_query(
        "mysql", "order_items", hash_agg="<hash_agg>",
        charset_columns=cols, null_empty_columns=cols)
    assert sql.count("SELECT ") == 1
    assert sql.count(" FROM ") == 1
    for c in cols:
        assert f"`{c}` LIKE \'%\ufffd%\'" in sql


def test_fused_pushdown_result_unpacking():
    res = scanplan.FusedPushdownResult(rows=100, data_hash=999, charset_anomalies=0, null_empty_diffs=0)
    assert res.rows == 100
    assert res.data_hash == 999
    rows, h, cs, ne = res
    assert (rows, h, cs, ne) == (100, 999, 0, 0)
    assert res[0] == 100


def test_mysql_fused_pushdown_verification_acceptance_30_columns():
    eng = object.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(slice=10)
    cols = [f"col_{i}" for i in range(1, 31)]
    eng._text_columns = lambda db: [("order_items", c) for c in cols]
    eng._scope = lambda side, db, t: f"`{db}`.`{t}`"

    asked = []
    def fake_q(side, sql):
        asked.append((side, sql))
        return [(1000, 54321, 0, 0)]

    eng._q = fake_q
    res = eng.fused_pushdown_verification("prod", "order_items", text_columns=cols, hash_agg="<hash_agg>")
    assert len(asked) == 2
    assert {s for s, _ in asked} == {"src", "dst"}
    assert res.status == "ok"
    assert "clean" in res.detail

    asked.clear()
    def fake_q_anom(side, sql):
        asked.append((side, sql))
        return [(1000, 54321, 5 if side == "dst" else 0, 0)]

    eng._q = fake_q_anom
    res_anom = eng.fused_pushdown_verification("prod", "order_items", text_columns=cols, hash_agg="<hash_agg>")
    assert res_anom.status == "diff"
    assert "excess replacement characters" in res_anom.detail


def test_postgres_fused_pushdown_verification_execution():
    eng = object.__new__(PostgresEngine)
    eng.hop = SimpleNamespace(slice=10)
    cols = [f"col_{i}" for i in range(1, 31)]
    eng._text_columns = lambda side, db: {"public.order_items": cols}
    eng._scope = lambda db, t: f'"{t}"'
    eng._split = lambda t: t.split(".")

    asked = []
    def fake_psql(side, db, sql):
        asked.append((side, sql))
        return "1000|88888|0|0\n"

    eng._psql = fake_psql
    res = eng.fused_pushdown_verification("prod", "public.order_items", text_columns=cols, hash_agg="<hash_agg>")
    assert len(asked) == 2
    assert res.status == "ok"

    asked.clear()
    def fake_psql_diff(side, db, sql):
        asked.append((side, sql))
        return "1000|88888|0|2\n" if side == "dst" else "1000|88888|0|0\n"

    eng._psql = fake_psql_diff
    res_diff = eng.fused_pushdown_verification("prod", "public.order_items", text_columns=cols, hash_agg="<hash_agg>")
    assert res_diff.status == "diff"
    assert "null/empty drift" in res_diff.detail


def test_fused_pushdown_multi_slice_execution():
    eng = object.__new__(MySQLEngine)
    eng.hop = SimpleNamespace(slice=10)
    eng._scope = lambda side, db, t: f"`{db}`.`{t}`"

    asked = []
    def fake_q(side, sql):
        asked.append((side, sql))
        if "id < 100" in sql:
            return [(50, 10, 0, 0)]
        return [(50, 20, 0, 0)]

    eng._q = fake_q
    res = eng.fused_pushdown_verification(
        "prod", "order_items", text_columns=["c1", "c2"],
        hash_algo="xor", parts=["id < 100", "id >= 100"])
    assert len(asked) == 4
    assert res.status == "ok"
    assert "rows=100" in res.detail
