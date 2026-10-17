"""Server-side lightweight LOB projection hashing (FIELD-OPS06).

Measured in production-rehearsal-validation: Full-value LOB queries on order_details (409 KB/row)
caused client OOM and network stalls over WARP/SSM tunnels. `canon.lob_projection`
pushes down lightweight length and head/tail substring hashing for LOB columns > 64KB,
yielding a constant 32-hex hash without transiting large payloads across the wire.
"""
import sqlite3
import pytest
from migkit import canon


def test_lob_projection_sql_structure():
    # MySQL
    my_sql = canon.lob_projection("mysql", "payload", is_binary=True)
    assert "octet_length(`payload`)" in my_sql
    assert "hex(substr(`payload`, 1, 1024))" in my_sql
    assert "md5(" in my_sql

    # PostgreSQL
    pg_sql = canon.lob_projection("postgres", "payload", is_binary=True)
    assert 'coalesce(octet_length("payload"), 0)::text' in pg_sql
    assert 'encode(substr("payload", 1, 1024), \'hex\')' in pg_sql
    assert "md5(" in pg_sql


def test_row_expr_uses_lob_projection_for_lobs():
    cols = [("id", "integer"), ("content", "text")]
    # Without lobs: regular expression doesn't have the lob_projection signature
    reg = canon.row_expr("postgres", cols)
    assert 'encode(substr(' not in reg

    # With lobs={"content"}: content is projected via lob_projection
    projected = canon.row_expr("postgres", cols, lobs={"content"})
    assert 'substr("content"::text, 1, 1024)' in projected
    assert '"id"' in projected


def test_sqlite_execution_handles_large_lob():
    con = sqlite3.connect(":memory:")
    con.execute("create table t (id int, data text);")
    large_payload = "A" * 500_000  # 500 KB string
    con.execute("insert into t values (1, ?);", (large_payload,))
    con.commit()

    # Query using lob projection on sqlite
    sql = canon.lob_projection("sqlite", "data", is_binary=False)
    res = con.execute(f"select {sql} from t;").fetchone()[0]
    assert res.startswith("500000:AAAA")
    assert len(res) <= 2100  # length + 1024 + 1024 + colons, constant bounded size!
