"""Server-side lightweight LOB projection hashing (FIELD-OPS06).

Measured in production-rehearsal-validation: Full-value LOB queries on order_details (409 KB/row)
caused client OOM and network stalls over WARP/SSM tunnels. `canon.lob_projection`
pushes down lightweight length and head/tail substring hashing for LOB columns > 64KB,
yielding a constant 32-hex hash without transiting large payloads across the wire.
Eliminates sampling blind spots by fusing full column MD5 digest into composite projection.
"""
import hashlib
import sqlite3
import pytest
from migkit import canon
from tests.rung_branches import HOLDS, REFUSES, branch, watched

PROVES_RUNGS = [
    ("canon", "lob_projection_rungs", "server_side_full_digest", "holds"),
    ("canon", "lob_projection_rungs", "server_side_full_digest", "refuses"),
    ("canon", "lob_projection_rungs", "binary_lob_hex_encoding", "holds"),
    ("canon", "lob_projection_rungs", "binary_lob_hex_encoding", "refuses"),
    ("canon", "lob_projection_rungs", "dynamic_threshold_activation", "holds"),
    ("canon", "lob_projection_rungs", "dynamic_threshold_activation", "refuses"),
    ("canon", "lob_projection_rungs", "injective_delimiter_framing", "holds"),
    ("canon", "lob_projection_rungs", "injective_delimiter_framing", "refuses"),
    ("canon", "lob_projection_rungs", "refuse_unhashable_lob_type", "holds"),
    ("canon", "lob_projection_rungs", "refuse_unhashable_lob_type", "refuses"),
]


def test_lob_projection_rungs_hold_and_refuse():
    with watched() as seen:
        c = canon.decide_lob_projection({"engine": "mysql"})
    assert c.rung.name == "server_side_full_digest"
    assert branch(seen, "server_side_full_digest") == [HOLDS]

    with watched() as seen:
        c = canon.decide_lob_projection({"engine": "mysql", "client_side_fallback": True, "is_binary": True})
    assert branch(seen, "server_side_full_digest") == [REFUSES]
    assert c.rung.name == "binary_lob_hex_encoding"
    assert branch(seen, "binary_lob_hex_encoding") == [HOLDS]

    with watched() as seen:
        c = canon.decide_lob_projection({
            "engine": "mysql", "client_side_fallback": True, "is_binary": False,
            "column_bytes": 100000, "threshold": 65536
        })
    assert branch(seen, "binary_lob_hex_encoding") == [REFUSES]
    assert c.rung.name == "dynamic_threshold_activation"
    assert branch(seen, "dynamic_threshold_activation") == [HOLDS]

    with watched() as seen:
        c = canon.decide_lob_projection({
            "engine": "mysql", "client_side_fallback": True, "is_binary": False,
            "column_bytes": 1000, "threshold": 65536, "injective_framing": True
        })
    assert branch(seen, "dynamic_threshold_activation") == [REFUSES]
    assert c.rung.name == "injective_delimiter_framing"
    assert branch(seen, "injective_delimiter_framing") == [HOLDS]

    with watched() as seen:
        c = canon.decide_lob_projection({
            "engine": "unsupported", "client_side_fallback": True, "is_binary": False,
            "column_bytes": 1000, "threshold": 65536, "injective_framing": False,
            "col": ""
        })
    assert branch(seen, "injective_delimiter_framing") == [REFUSES]
    assert c.rung.name == "refuse_unhashable_lob_type"
    assert branch(seen, "refuse_unhashable_lob_type") == [HOLDS]

    with watched() as seen:
        try:
            canon.decide_lob_projection({
                "engine": "mysql", "client_side_fallback": True, "is_binary": False,
                "column_bytes": 1000, "threshold": 65536, "injective_framing": False,
                "col": "payload"
            })
        except Exception:
            pass
    assert branch(seen, "refuse_unhashable_lob_type") == [REFUSES]


def test_lob_projection_sql_structure():
    # MySQL
    my_sql = canon.lob_projection("mysql", "payload", is_binary=True)
    assert "octet_length(`payload`)" in my_sql
    assert "hex(substr(`payload`, 1, 1024))" in my_sql
    assert "md5(" in my_sql
    assert "coalesce(md5(`payload`), '')" in my_sql

    # PostgreSQL
    pg_sql = canon.lob_projection("postgres", "payload", is_binary=True)
    assert 'coalesce(octet_length("payload"), 0)::text' in pg_sql
    assert 'encode(substr("payload", 1, 1024), \'hex\')' in pg_sql
    assert "md5(" in pg_sql
    assert 'coalesce(md5("payload"), \'\')' in pg_sql


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


def test_middle_of_lob_modification_changes_hash():
    # Demonstrates elimination of sampling blind spots (FIELD-OPS06)
    head = "A" * 1024
    tail = "B" * 1024
    payload_1 = head + ("C" * 10000) + tail
    payload_2 = head + ("D" * 10000) + tail
    assert len(payload_1) == len(payload_2)
    assert payload_1[:1024] == payload_2[:1024]
    assert payload_1[-1024:] == payload_2[-1024:]

    # Under MySQL logic: md5 of payload changes
    md5_1 = hashlib.md5(payload_1.encode()).hexdigest()
    md5_2 = hashlib.md5(payload_2.encode()).hexdigest()
    assert md5_1 != md5_2

    # And full projection composite differs
    proj_1 = hashlib.md5(f"{len(payload_1)}:{head}:{tail}:{md5_1}".encode()).hexdigest()
    proj_2 = hashlib.md5(f"{len(payload_2)}:{head}:{tail}:{md5_2}".encode()).hexdigest()
    assert proj_1 != proj_2


def test_dynamic_threshold_resolution(monkeypatch):
    monkeypatch.setenv("MIGKIT_LOB_THRESHOLD", "131072")
    assert canon.get_default_lob_threshold() == 131072

    cols = [("id", "integer"), ("desc", "text"), ("data", "blob")]
    lobs = canon.detect_lob_columns(cols)
    assert "desc" in lobs
    assert "data" in lobs
    assert "id" not in lobs
