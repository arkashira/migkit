import sqlite3
import pytest
from migkit import sizing
from migkit import checkpoint


def test_aimd_halve_on_large_row_bytes():
    sizer = sizing.AIMDChunkSizer(initial=1000)
    assert sizer.chunk == 1000

    # 70 KB exceeds 64 KB threshold
    c = sizer.adjust(latency=0.5, row_bytes=70 * 1024)
    assert c == 500
    assert sizer.chunk == 500

    # continue halving
    c = sizer.adjust(latency=0.2, row_bytes=80 * 1024)
    assert c == 250
    c = sizer.adjust(latency=0.1, row_bytes=90 * 1024)
    assert c == 125
    c = sizer.adjust(latency=0.1, row_bytes=100 * 1024)
    assert c == 100
    # floor at 100
    c = sizer.adjust(latency=0.1, row_bytes=100 * 1024)
    assert c == 100


def test_aimd_halve_on_query_latency():
    sizer = sizing.AIMDChunkSizer(initial=1000)
    # latency > 5.0s causes halving even with small rows
    c = sizer.adjust(latency=5.1, row_bytes=200)
    assert c == 500
    c = sizer.adjust(latency=6.0, row_bytes=200)
    assert c == 250


def test_aimd_expand_on_subsecond_healthy():
    sizer = sizing.AIMDChunkSizer(initial=100)
    # subsecond < 1.0s and memory healthy
    c = sizer.adjust(latency=0.4, row_bytes=500, memory_healthy=True)
    assert c == 150
    c = sizer.adjust(latency=0.3, row_bytes=500, memory_healthy=True)
    assert c == 225

    # test ceiling at 50000
    sizer = sizing.AIMDChunkSizer(initial=40000)
    c = sizer.adjust(latency=0.5, row_bytes=500, memory_healthy=True)
    assert c == 50000
    c = sizer.adjust(latency=0.5, row_bytes=500, memory_healthy=True)
    assert c == 50000


def test_aimd_no_expand_when_memory_unhealthy():
    sizer = sizing.AIMDChunkSizer(initial=1000)
    c = sizer.adjust(latency=0.2, row_bytes=500, memory_healthy=False)
    assert c == 1000


def test_aimd_stable_within_nominal_latency():
    sizer = sizing.AIMDChunkSizer(initial=1000)
    c = sizer.adjust(latency=2.5, row_bytes=1024, memory_healthy=True)
    assert c == 1000


def test_aimd_adjust_chunk_helper():
    assert sizing.aimd_adjust_chunk(1000, latency=5.5) == 500
    assert sizing.aimd_adjust_chunk(1000, row_bytes=70000) == 500
    assert sizing.aimd_adjust_chunk(100, latency=0.5, memory_healthy=True) == 150
    assert sizing.aimd_adjust_chunk(100, latency=0.5, memory_healthy=False) == 100
    assert sizing.aimd_adjust_chunk(1000, latency=2.0) == 1000


def test_lob_column_detection_and_predicates():
    assert sizing.is_lob_type("blob")
    assert sizing.is_lob_type("jsonb")
    assert sizing.is_lob_type("text")
    assert not sizing.is_lob_type("integer")
    assert not sizing.is_lob_type("bigint")

    cols = [("id", "integer"), ("meta", "json"), ("data", "blob")]
    preds = sizing.auto_slice_lob_predicates(cols, threshold_bytes=64 * 1024, engine="postgres")
    assert "small" in preds and "large" in preds
    assert "octet_length" in preds["small"]
    assert "octet_length" in preds["large"]
    assert "<=" in preds["small"]
    assert ">" in preds["large"]

    my_preds = sizing.auto_slice_lob_predicates(cols, threshold_bytes=64 * 1024, engine="mysql")
    assert "`meta`" in my_preds["small"] or "`data`" in my_preds["small"]

    sq_preds = sizing.auto_slice_lob_predicates(cols, threshold_bytes=64 * 1024, engine="sqlite")
    assert "length(" in sq_preds["small"]


def test_checkpoint_rate_with_aimd():
    rate = checkpoint.Rate(aimd=True, initial_chunk=1000)
    assert rate.chunk_rows() == 1000

    # Latency spike > 5s
    rate.observe(100, 5.5)
    assert rate.chunk_rows() == 500

    # Subsecond healthy
    rate.observe(100, 0.4)
    assert rate.chunk_rows() == 750


def test_checkpoint_where_with_lob():
    w = checkpoint.where('"id"', 1, 100, lob_pred='length("data") <= 65536')
    assert w == '"id" >= 1 and "id" < 100 and (length("data") <= 65536)'


def test_acceptance_heterogeneous_table_aimd_verification():
    con = sqlite3.connect(":memory:")
    con.execute("create table orders (id integer primary key, data blob)")

    small_blob = b"x" * 100
    for i in range(1, 501):
        con.execute("insert into orders values (?, ?)", (i, small_blob))

    large_blob = b"y" * (500 * 1024)
    for i in range(501, 701):
        con.execute("insert into orders values (?, ?)", (i, large_blob))

    for i in range(701, 1501):
        con.execute("insert into orders values (?, ?)", (i, small_blob))
    con.commit()

    sizer = sizing.AIMDChunkSizer(initial=100)
    cur_id = 1
    max_id = 1500
    verified_rows = 0
    expanded_on_small = False
    halved_on_large = False

    while cur_id <= max_id:
        chunk = sizer.chunk
        hi_id = min(max_id + 1, cur_id + chunk)
        rows = con.execute("select id, data from orders where id >= ? and id < ?", (cur_id, hi_id)).fetchall()
        if not rows:
            break
        n_rows = len(rows)
        total_bytes = sum(len(r[1]) for r in rows)
        avg_row_bytes = total_bytes / n_rows

        simulated_latency = 0.05 if avg_row_bytes < 64 * 1024 else 5.2

        old_chunk = sizer.chunk
        sizer.adjust(latency=simulated_latency, row_bytes=avg_row_bytes, memory_healthy=True)

        if avg_row_bytes < 64 * 1024 and sizer.chunk > old_chunk:
            expanded_on_small = True
        if (avg_row_bytes >= 64 * 1024 or simulated_latency > 5.0) and sizer.chunk < old_chunk:
            halved_on_large = True

        verified_rows += n_rows
        cur_id = hi_id

    assert verified_rows == 1500
    assert expanded_on_small
    assert halved_on_large

