import shutil
import tempfile
import threading
import time
from pathlib import Path

import pytest

from migkit.ring_buffer import (
    RecordBatch,
    StoreAndForwardRingBuffer,
    lsn_le,
    parse_lsn,
)


@pytest.fixture
def temp_spool():
    d = Path(tempfile.mkdtemp(prefix="test_sfrb_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


def test_basic_push_and_poll(temp_spool):
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=1024 * 1024)
    assert len(buf) == 0
    assert buf.is_empty

    pushed = buf.push([{"id": 1, "op": "I"}, {"id": 2, "op": "I"}], lsn=100)
    assert pushed == 2
    assert len(buf) == 2
    assert not buf.is_empty

    buf.push({"id": 3, "op": "U"}, lsn=101)
    assert len(buf) == 3

    batch1 = buf.poll(batch_size=2, timeout=0.5)
    assert len(batch1) == 2
    assert batch1[0] == {"id": 1, "op": "I"}
    assert batch1[1] == {"id": 2, "op": "I"}
    assert batch1.min_lsn == 100
    assert batch1.max_lsn == 100
    assert batch1.lsn == 100
    assert len(batch1.items) == 2

    batch2 = buf.poll(batch_size=2, timeout=0.5)
    assert len(batch2) == 1
    assert batch2[0] == {"id": 3, "op": "U"}
    assert batch2.lsn == 101

    batch3 = buf.poll(batch_size=2, timeout=0.05)
    assert len(batch3) == 0
    buf.close()


def test_watermark_commit_and_persistence(temp_spool):
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=1024 * 1024)
    buf.push([{"key": "a"}], lsn=50)
    buf.push([{"key": "b"}], lsn=60)

    b1 = buf.poll(batch_size=1)
    assert b1.max_lsn == 50
    w1 = b1.commit()
    assert w1 == 50
    assert buf.committed_watermark == 50
    assert buf.get_watermark() == 50

    b2 = buf.poll(batch_size=1)
    assert b2.max_lsn == 60
    w2 = buf.commit(b2.max_lsn)
    assert w2 == 60
    assert buf.committed_watermark == 60

    watermark_file = temp_spool / "watermark.json"
    assert watermark_file.exists()
    buf.close()

    buf2 = StoreAndForwardRingBuffer(spool_dir=temp_spool)
    assert buf2.committed_watermark == 60
    buf2.close()


def test_bounded_memory_and_disk_spooling(temp_spool):
    max_mem = 2048
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=max_mem, segment_size=4096)

    total_records = 150
    for i in range(total_records):
        buf.push({"idx": i, "payload": "x" * 80}, lsn=1000 + i)

    assert buf.in_memory_bytes <= max_mem
    assert buf.spooled_segments_count > 0
    assert buf.disk_bytes > 0

    polled = []
    while len(polled) < total_records:
        b = buf.poll(batch_size=30, timeout=1.0)
        assert len(b) > 0
        polled.extend(b)
        b.commit()

    assert len(polled) == total_records
    assert [r["idx"] for r in polled] == list(range(total_records))
    assert buf.committed_watermark == 1000 + total_records - 1
    buf.close()


def test_mmap_zstandard_compression(temp_spool):
    buf = StoreAndForwardRingBuffer(
        spool_dir=temp_spool,
        max_memory_bytes=1024,
        segment_size=2048,
        compression="zstd",
    )
    for i in range(80):
        buf.push({"id": i, "desc": "repeated text for high compression " * 4}, lsn=i)

    stats = buf.stats()
    assert stats["compression"] == "zstd"
    assert stats["compression_ratio"] > 1.5

    polled = []
    while len(polled) < 80:
        b = buf.poll(batch_size=20, timeout=1.0)
        assert len(b) > 0
        polled.extend(b)
        b.commit()

    assert len(polled) == 80
    assert [r["id"] for r in polled] == list(range(80))
    buf.close()


def test_zlib_compression_fallback(temp_spool):
    buf = StoreAndForwardRingBuffer(
        spool_dir=temp_spool,
        max_memory_bytes=1024,
        segment_size=2048,
        compression="zlib",
    )
    for i in range(50):
        buf.push({"val": i, "text": "log_entry_" + str(i)}, lsn=i)

    stats = buf.stats()
    assert stats["compression"] == "zlib"

    polled = []
    while len(polled) < 50:
        b = buf.poll(batch_size=25, timeout=1.0)
        polled.extend(b)
        b.commit()

    assert len(polled) == 50
    assert [r["val"] for r in polled] == list(range(50))
    buf.close()


def test_circular_eviction_once_confirmed_applied(temp_spool):
    buf = StoreAndForwardRingBuffer(
        spool_dir=temp_spool,
        max_memory_bytes=400,
        segment_size=1024,
        auto_evict=True,
    )
    for i in range(160):
        buf.push({"seq": i, "val": "abc" * 15}, lsn=2000 + i)

    created_segs = list(temp_spool.glob("seg_*.sf"))
    assert len(created_segs) >= 3

    polled = []
    while len(polled) < 160:
        b = buf.poll(batch_size=20, timeout=1.0)
        assert len(b) > 0
        polled.extend(b)
        b.commit()

    remaining_segs = list(temp_spool.glob("seg_*.sf"))
    assert len(remaining_segs) == 0
    assert buf.spooled_segments_count == 0
    assert [r["seq"] for r in polled] == list(range(160))
    buf.close()


def test_decoupled_producer_under_consumer_latency(temp_spool):
    buf = StoreAndForwardRingBuffer(
        spool_dir=temp_spool,
        max_memory_bytes=2048,
        segment_size=4096,
    )

    producer_done = False
    consumer_records = []

    def producer_worker():
        for i in range(300):
            buf.push({"num": i, "data": "cdc_event_" + str(i)}, lsn=100 + i)
            time.sleep(0.001)
        nonlocal producer_done
        producer_done = True

    def consumer_worker():
        while True:
            b = buf.poll(batch_size=25, timeout=0.1)
            if b:
                consumer_records.extend(b)
                b.commit()
                time.sleep(0.01)
            elif producer_done and buf.is_empty:
                break

    t_prod = threading.Thread(target=producer_worker)
    t_cons = threading.Thread(target=consumer_worker)

    t_prod.start()
    t_cons.start()

    t_prod.join(timeout=5.0)
    assert not t_prod.is_alive()
    assert producer_done

    t_cons.join(timeout=10.0)
    assert not t_cons.is_alive()

    assert len(consumer_records) == 300
    assert [r["num"] for r in consumer_records] == list(range(300))
    assert buf.committed_watermark == 100 + 299
    buf.close()


def test_crash_recovery_and_uncommitted_replay(temp_spool):
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=300, segment_size=1024)
    for i in range(100):
        buf.push({"id": i}, lsn=i)

    polled = []
    while len(polled) < 50:
        b = buf.poll(batch_size=25)
        polled.extend(b)
        b.commit()

    assert buf.committed_watermark == 49
    buf.close()

    buf2 = StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=300, segment_size=1024)
    assert buf2.committed_watermark == 49

    polled_after = []
    while len(polled_after) < 50:
        b = buf2.poll(batch_size=25, timeout=1.0)
        if not b:
            break
        polled_after.extend(b)
        b.commit()

    assert len(polled_after) == 50
    assert [r["id"] for r in polled_after] == list(range(50, 100))
    assert buf2.committed_watermark == 99
    buf2.close()


def test_postgres_and_mysql_lsn_formats(temp_spool):
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool)

    buf.push({"wal": 1}, lsn="0/16B3748")
    buf.push({"wal": 2}, lsn="0/16B3800")
    buf.push({"wal": 3}, lsn="0/16B3900")

    b = buf.poll(batch_size=3)
    assert len(b) == 3
    assert b.min_lsn == "0/16B3748"
    assert b.max_lsn == "0/16B3900"
    b.commit()
    assert buf.committed_watermark == "0/16B3900"
    buf.close()

    assert lsn_le("0/16B3748", "0/16B3800")
    assert not lsn_le("0/16B3800", "0/16B3748")
    assert lsn_le("binlog.000001:100", "binlog.000001:200")


def test_empty_poll_timeout_and_close(temp_spool):
    buf = StoreAndForwardRingBuffer(spool_dir=temp_spool)
    start = time.monotonic()
    batch = buf.poll(batch_size=10, timeout=0.15)
    elapsed = time.monotonic() - start
    assert len(batch) == 0
    assert elapsed >= 0.1

    def closer():
        time.sleep(0.05)
        buf.close()

    threading.Thread(target=closer).start()
    start = time.monotonic()
    batch2 = buf.poll(batch_size=10, timeout=2.0)
    elapsed2 = time.monotonic() - start
    assert len(batch2) == 0
    assert elapsed2 < 1.0

    with pytest.raises(RuntimeError):
        buf.push({"a": 1}, lsn=1)


def test_context_manager_and_stats(temp_spool):
    with StoreAndForwardRingBuffer(spool_dir=temp_spool, max_memory_bytes=4096) as buf:
        buf.push([{"x": 1}, {"x": 2}], lsn=10)
        assert len(buf) == 2
        s = buf.stats()
        assert s["total_pushed"] == 2
        assert s["in_memory_bytes"] > 0
        assert s["max_memory_bytes"] == 4096
        b = buf.poll(batch_size=2)
        assert len(b) == 2
        b.commit()
        assert buf.committed_watermark == 10
