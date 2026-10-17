import collections
import dataclasses
import errno
import fcntl
import json
import mmap
import multiprocessing
import os
import pickle
import struct
import tempfile
import threading
import time
import zlib
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, Iterator, List, Optional, Tuple, Union

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

from . import decide, durable, envkeys, sizing

CARE_ACTION = "ACTION"
CARE_REVIEW = "REVIEW"
CARE_BENIGN = "BENIGN"
CARE_PASS = "PASS"


def buffer_envelope_rungs():
    """Dynamic Buffer Sizing Ladder (Row 565)."""
    from .decide import Need, Rung
    return (
        Rung("configured_custom_envelope",
             "explicit memory ceiling configured via environment or hop options",
             needs=(Need("configured memory ceiling provided",
                         lambda f: bool(f.get("configured_bytes")),
                         "no explicit buffer memory envelope configured"),),
             because="operator or hop explicitly set custom memory bounds"),
        Rung("adaptive_host_scaled_envelope",
             "adaptive memory envelope scaled to 25% of host available memory",
             needs=(Need("host available memory readable and sufficient",
                         lambda f: f.get("host_memory_bytes") is not None and f["host_memory_bytes"] >= 256 * 1024 * 1024,
                         "host memory unreadable or constrained under 256 MiB"),),
             because="dynamic host RAM scaling balances throughput and cgroup safety"),
        Rung("constrained_minimal_envelope",
             "minimal safe memory envelope for low-memory container environments",
             because="fallback baseline memory envelope for constrained hosts"),
    )


def decide_buffer_envelope(hop: Any = None, max_memory_bytes: Optional[int] = None, segment_size: Optional[int] = None) -> Tuple[int, int]:
    opts = {}
    if hop is not None and hasattr(hop, "options") and isinstance(hop.options, dict):
        opts = hop.options

    configured = None
    if max_memory_bytes is not None:
        configured = int(max_memory_bytes)
    elif envkeys.text("MIGKIT_RING_BUFFER_BYTES"):
        try:
            configured = int(envkeys.text("MIGKIT_RING_BUFFER_BYTES"))
        except ValueError:
            pass
    elif "ring_buffer_max_memory" in opts:
        configured = int(opts["ring_buffer_max_memory"])
    elif "spool_memory_bytes" in opts:
        configured = int(opts["spool_memory_bytes"])
    elif "max_memory_bytes" in opts:
        configured = int(opts["max_memory_bytes"])

    try:
        host_mem = sizing._memory()
    except Exception:
        host_mem = None

    facts = {
        "configured_bytes": configured,
        "host_memory_bytes": host_mem,
    }
    choice = decide.climb("how store-and-forward memory envelope is sized",
                          buffer_envelope_rungs(), facts)
    if choice.path == "configured_custom_envelope":
        max_mem = configured
    elif choice.path == "adaptive_host_scaled_envelope":
        scaled = host_mem // 4
        max_mem = max(128 * 1024 * 1024, min(scaled, 1024 * 1024 * 1024))
    else:
        max_mem = 64 * 1024 * 1024

    if segment_size is None:
        if "ring_buffer_segment_size" in opts:
            seg_size = int(opts["ring_buffer_segment_size"])
        elif "segment_size" in opts:
            seg_size = int(opts["segment_size"])
        else:
            seg_size = max(1024 * 1024, min(max_mem // 4, 64 * 1024 * 1024))
    else:
        seg_size = int(segment_size)

    return max_mem, seg_size

CHUNK_MAGIC = b"SF01"
SEG_MAGIC = b"SFRBSEG1"
SEG_HDR_FMT = ">8sId"
CHUNK_HDR_FMT = ">4sBBIIIIHH"

CODEC_NONE = 0
CODEC_ZSTD = 1
CODEC_ZLIB = 2
CODEC_GZIP = 3


def parse_lsn(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        if "/" in val:
            parts = val.split("/")
            if len(parts) == 2:
                try:
                    return (int(parts[0], 16) << 32) + int(parts[1], 16)
                except ValueError:
                    pass
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            pass
        return val
    return val


def lsn_le(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return True
    na = parse_lsn(a)
    nb = parse_lsn(b)
    if isinstance(na, (int, float)) and isinstance(nb, (int, float)):
        return na <= nb
    if type(na) is type(nb):
        return na <= nb
    return str(a) <= str(b)


class ChunkCompressor:
    def __init__(self, method: str = "auto", level: int = 3):
        self.method = method
        self.level = level
        if method == "auto":
            self.codec = CODEC_ZSTD if HAS_ZSTD else CODEC_ZLIB
        elif method == "zstd":
            if not HAS_ZSTD:
                self.codec = CODEC_ZLIB
            else:
                self.codec = CODEC_ZSTD
        elif method == "zlib":
            self.codec = CODEC_ZLIB
        elif method == "none":
            self.codec = CODEC_NONE
        else:
            self.codec = CODEC_ZLIB

        self._zstd_compressor = zstd.ZstdCompressor(level=self.level) if (HAS_ZSTD and self.codec == CODEC_ZSTD) else None
        self._zstd_decompressor = zstd.ZstdDecompressor() if (HAS_ZSTD and self.codec == CODEC_ZSTD) else None

    @property
    def codec_name(self) -> str:
        if self.codec == CODEC_ZSTD:
            return "zstd"
        if self.codec == CODEC_ZLIB:
            return "zlib"
        if self.codec == CODEC_NONE:
            return "none"
        return "unknown"

    def compress(self, raw: bytes) -> Tuple[bytes, int]:
        if self.codec == CODEC_ZSTD:
            if not self._zstd_compressor and HAS_ZSTD:
                self._zstd_compressor = zstd.ZstdCompressor(level=self.level)
            if self._zstd_compressor:
                return self._zstd_compressor.compress(raw), CODEC_ZSTD
        if self.codec == CODEC_ZLIB:
            return zlib.compress(raw, level=min(9, max(1, self.level))), CODEC_ZLIB
        return raw, CODEC_NONE

    def decompress(self, comp: bytes, codec: int) -> bytes:
        if codec == CODEC_ZSTD:
            if not HAS_ZSTD:
                raise RuntimeError("Zstandard decompression requested but zstandard is not installed")
            if not self._zstd_decompressor:
                self._zstd_decompressor = zstd.ZstdDecompressor()
            return self._zstd_decompressor.decompress(comp)
        if codec == CODEC_ZLIB:
            return zlib.decompress(comp)
        if codec == CODEC_NONE:
            return comp
        raise ValueError(f"Unknown chunk compression codec {codec}")

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_zstd_compressor"] = None
        state["_zstd_decompressor"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if HAS_ZSTD and self.codec == CODEC_ZSTD:
            self._zstd_compressor = zstd.ZstdCompressor(level=self.level)
            self._zstd_decompressor = zstd.ZstdDecompressor()


class RecordBatch(list):
    def __init__(
        self,
        items: List[Tuple[Any, Any]],
        buffer: Optional["StoreAndForwardRingBuffer"] = None,
    ):
        records = [it[0] for it in items]
        super().__init__(records)
        self.records = records
        self._items = items
        self._buffer = buffer
        if items:
            self.min_lsn = items[0][1]
            self.max_lsn = items[-1][1]
            self.lsn = self.max_lsn
        else:
            self.min_lsn = None
            self.max_lsn = None
            self.lsn = None

    def __getstate__(self):
        return {"records": self.records, "items": self._items}

    def __setstate__(self, state):
        records = state.get("records", [])
        items = state.get("items", [])
        super().__init__(records)
        self.records = records
        self._items = items
        self._buffer = None
        if items:
            self.min_lsn = items[0][1]
            self.max_lsn = items[-1][1]
            self.lsn = self.max_lsn
        else:
            self.min_lsn = None
            self.max_lsn = None
            self.lsn = None

    @property
    def items(self) -> List[Tuple[Any, Any]]:
        return list(self._items)

    def commit(self) -> Any:
        if self._buffer is not None and self.max_lsn is not None:
            return self._buffer.commit(self.max_lsn)
        return self.max_lsn


class DiskSegment:
    def __init__(self, segment_id: int, path: Path, max_size: int, compressor: ChunkCompressor):
        self.segment_id = segment_id
        self.path = path
        self.max_size = max_size
        self.compressor = compressor
        self.write_pos = 0
        self.read_pos = 0
        self.min_lsn = None
        self.max_lsn = None
        self.record_count = 0
        self.uncompressed_bytes = 0
        self.compressed_bytes = 0
        self.is_sealed = False
        self.is_consumed = False
        self.is_evicted = False

        self._write_file = None
        self._write_mmap = None
        self._read_file = None
        self._read_mmap = None

    def _ensure_write_open(self, initial_alloc: Optional[int] = None):
        if self._write_file is None:
            alloc_size = max(self.max_size, initial_alloc or self.max_size)
            self._write_file = open(self.path, "w+b")
            self._write_file.truncate(alloc_size)
            self._write_mmap = mmap.mmap(self._write_file.fileno(), alloc_size, access=mmap.ACCESS_WRITE)
            hdr = struct.pack(SEG_HDR_FMT, SEG_MAGIC, self.segment_id, time.time())
            self._write_mmap[0:len(hdr)] = hdr
            self.write_pos = len(hdr)

    def can_fit(self, chunk_len: int) -> bool:
        if self.is_sealed:
            return False
        if self.write_pos == 0:
            return True
        return (self.write_pos + chunk_len) <= self.max_size

    def append_chunk(self, items: List[Tuple[Any, Any]]) -> bool:
        if self.is_sealed:
            return False

        raw = pickle.dumps(items, protocol=pickle.HIGHEST_PROTOCOL)
        comp, codec = self.compressor.compress(raw)
        crc = zlib.crc32(comp)

        chunk_min_lsn = items[0][1]
        chunk_max_lsn = items[-1][1]
        min_lsn_bytes = str(chunk_min_lsn if chunk_min_lsn is not None else "").encode("utf-8")
        max_lsn_bytes = str(chunk_max_lsn if chunk_max_lsn is not None else "").encode("utf-8")

        hdr_fixed_len = struct.calcsize(CHUNK_HDR_FMT)
        total_chunk_len = hdr_fixed_len + len(min_lsn_bytes) + len(max_lsn_bytes) + len(comp)

        if self.write_pos > 0 and (self.write_pos + total_chunk_len) > self.max_size:
            return False

        if self._write_file is None:
            self._ensure_write_open(initial_alloc=max(self.max_size, struct.calcsize(SEG_HDR_FMT) + total_chunk_len))
        elif (self.write_pos + total_chunk_len) > len(self._write_mmap):
            new_size = max(len(self._write_mmap) * 2, self.write_pos + total_chunk_len)
            self._write_mmap.flush()
            self._write_mmap.close()
            self._write_file.truncate(new_size)
            self._write_mmap = mmap.mmap(self._write_file.fileno(), new_size, access=mmap.ACCESS_WRITE)

        chunk_hdr = struct.pack(
            CHUNK_HDR_FMT,
            CHUNK_MAGIC,
            codec,
            0,
            len(items),
            len(raw),
            len(comp),
            crc,
            len(min_lsn_bytes),
            len(max_lsn_bytes),
        )

        wp = self.write_pos
        self._write_mmap[wp:wp + hdr_fixed_len] = chunk_hdr
        wp += hdr_fixed_len

        if min_lsn_bytes:
            self._write_mmap[wp:wp + len(min_lsn_bytes)] = min_lsn_bytes
            wp += len(min_lsn_bytes)

        if max_lsn_bytes:
            self._write_mmap[wp:wp + len(max_lsn_bytes)] = max_lsn_bytes
            wp += len(max_lsn_bytes)

        self._write_mmap[wp:wp + len(comp)] = comp
        wp += len(comp)

        self.write_pos = wp
        self.record_count += len(items)
        self.uncompressed_bytes += len(raw)
        self.compressed_bytes += len(comp)

        if self.min_lsn is None:
            self.min_lsn = chunk_min_lsn
        self.max_lsn = chunk_max_lsn
        return True

    def seal(self):
        if self.is_sealed:
            return
        if self._write_mmap is not None:
            try:
                self._write_mmap.flush()
            except Exception:
                pass
            self._write_mmap.close()
            self._write_mmap = None
        if self._write_file is not None:
            try:
                self._write_file.truncate(self.write_pos)
                self._write_file.flush()
                os.fsync(self._write_file.fileno())
            except Exception:
                pass
            self._write_file.close()
            self._write_file = None
        self.is_sealed = True

    def _ensure_read_open(self):
        if self._read_mmap is None:
            if not self.path.exists():
                return
            size = self.path.stat().st_size
            if size == 0:
                return
            self._read_file = open(self.path, "rb")
            self._read_mmap = mmap.mmap(self._read_file.fileno(), 0, access=mmap.ACCESS_READ)
            if self.read_pos == 0:
                self.read_pos = struct.calcsize(SEG_HDR_FMT)

    def read_next_chunk(self) -> Optional[List[Tuple[Any, Any]]]:
        if self.is_consumed:
            return None
        self._ensure_read_open()
        if self._read_mmap is None:
            self.is_consumed = True
            return None

        hdr_fixed_len = struct.calcsize(CHUNK_HDR_FMT)
        file_len = len(self._read_mmap)
        rp = self.read_pos

        if (rp + hdr_fixed_len) > file_len:
            self._finish_reading()
            return None

        magic, codec, flags, rec_cnt, u_len, c_len, crc, min_l, max_l = struct.unpack_from(CHUNK_HDR_FMT, self._read_mmap, rp)
        if magic != CHUNK_MAGIC:
            self._finish_reading()
            return None

        rp += hdr_fixed_len
        if (rp + min_l + max_l + c_len) > file_len:
            self._finish_reading()
            return None

        min_lsn_bytes = bytes(self._read_mmap[rp:rp + min_l])
        rp += min_l
        max_lsn_bytes = bytes(self._read_mmap[rp:rp + max_l])
        rp += max_l

        comp = bytes(self._read_mmap[rp:rp + c_len])
        rp += c_len

        if zlib.crc32(comp) != crc:
            raise ValueError(f"Corrupt chunk in segment {self.segment_id} at offset {self.read_pos}: CRC mismatch")

        self.read_pos = rp
        raw = self.compressor.decompress(comp, codec)
        items = pickle.loads(raw)

        if self.read_pos >= file_len:
            self._finish_reading()

        return items

    def _finish_reading(self):
        self.is_consumed = True
        self.close_read()

    def close_read(self):
        if self._read_mmap is not None:
            self._read_mmap.close()
            self._read_mmap = None
        if self._read_file is not None:
            self._read_file.close()
            self._read_file = None

    def close(self):
        self.seal()
        self.close_read()

    def evict(self):
        self.close()
        if not self.is_evicted:
            try:
                if self.path.exists():
                    os.unlink(self.path)
            except OSError:
                pass
            self.is_evicted = True


class StoreAndForwardRingBuffer:
    def __init__(
        self,
        spool_dir: Optional[Union[str, Path]] = None,
        max_memory_bytes: Optional[int] = None,
        segment_size: Optional[int] = None,
        compression: str = "auto",
        compression_level: int = 3,
        auto_evict: bool = True,
        watermark_file: Optional[Union[str, Path]] = None,
        hop: Any = None,
    ):
        derived_max_mem, derived_seg_size = decide_buffer_envelope(
            hop=hop, max_memory_bytes=max_memory_bytes, segment_size=segment_size
        )
        self.max_memory_bytes = derived_max_mem
        self.segment_size = derived_seg_size
        self.auto_evict = auto_evict
        self.compressor = ChunkCompressor(method=compression, level=compression_level)

        if spool_dir is not None:
            self.spool_dir = Path(spool_dir)
            self.spool_dir.mkdir(parents=True, exist_ok=True)
            self._owns_spool_dir = False
        else:
            self.spool_dir = Path(tempfile.mkdtemp(prefix="migkit_sfrb_"))
            self._owns_spool_dir = True

        self.watermark_file = Path(watermark_file) if watermark_file else (self.spool_dir / "watermark.json")

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._closed = False

        self._mem_queue: Deque[Tuple[List[Tuple[Any, Any]], int, Any]] = collections.deque()
        self._mem_bytes = 0

        self._disk_read_staging: Deque[Tuple[Any, Any]] = collections.deque()
        self._disk_segments: Deque[DiskSegment] = collections.deque()
        self._active_write_segment: Optional[DiskSegment] = None
        self._read_segment_idx = 0
        self._next_segment_id = 1

        self._committed_watermark = None
        self._highest_pushed_lsn = None
        self._highest_polled_lsn = None

        self._total_pushed = 0
        self._total_polled = 0
        self._total_raw_spooled = 0
        self._total_comp_spooled = 0

        self._recover_state()

    def _recover_state(self):
        if self.watermark_file.exists():
            try:
                with open(self.watermark_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._committed_watermark = data.get("committed_watermark")
                    self._highest_polled_lsn = data.get("highest_polled_lsn")
                    self._highest_pushed_lsn = data.get("highest_pushed_lsn")
                    self._total_polled = data.get("total_polled", 0)
                    self._total_pushed = data.get("total_pushed", 0)
            except Exception:
                pass

        existing_segments = sorted(self.spool_dir.glob("seg_*.sf"))
        for p in existing_segments:
            try:
                seg_id = int(p.stem.split("_")[1])
                seg = DiskSegment(seg_id, p, self.segment_size, self.compressor)
                seg.seal()
                self._inspect_existing_segment(seg)

                if self._committed_watermark is not None and seg.max_lsn is not None and lsn_le(seg.max_lsn, self._committed_watermark):
                    seg.evict()
                else:
                    self._disk_segments.append(seg)
                    if seg_id >= self._next_segment_id:
                        self._next_segment_id = seg_id + 1
            except Exception:
                pass

    def _inspect_existing_segment(self, seg: DiskSegment):
        if not seg.path.exists() or seg.path.stat().st_size == 0:
            return
        with open(seg.path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            hdr_len = struct.calcsize(SEG_HDR_FMT)
            if len(mm) < hdr_len:
                mm.close()
                return
            rp = hdr_len
            chunk_hdr_len = struct.calcsize(CHUNK_HDR_FMT)
            while (rp + chunk_hdr_len) <= len(mm):
                magic, codec, flags, rec_cnt, u_len, c_len, crc, min_l, max_l = struct.unpack_from(CHUNK_HDR_FMT, mm, rp)
                if magic != CHUNK_MAGIC:
                    break
                rp += chunk_hdr_len
                min_lsn = mm[rp:rp + min_l].decode("utf-8") if min_l else None
                rp += min_l
                max_lsn = mm[rp:rp + max_l].decode("utf-8") if max_l else None
                rp += max_l + c_len

                if seg.min_lsn is None:
                    seg.min_lsn = min_lsn
                seg.max_lsn = max_lsn
                seg.record_count += rec_cnt
                seg.uncompressed_bytes += u_len
                seg.compressed_bytes += c_len
            mm.close()

    def _discover_new_segments(self):
        known_paths = {seg.path for seg in self._disk_segments}
        existing_segments = sorted(self.spool_dir.glob("seg_*.sf"))
        for p in existing_segments:
            if p not in known_paths:
                try:
                    seg_id = int(p.stem.split("_")[1])
                    seg = DiskSegment(seg_id, p, self.segment_size, self.compressor)
                    seg.seal()
                    self._inspect_existing_segment(seg)
                    if self._committed_watermark is not None and seg.max_lsn is not None and lsn_le(seg.max_lsn, self._committed_watermark):
                        seg.evict()
                    else:
                        self._disk_segments.append(seg)
                        known_paths.add(p)
                        if seg_id >= self._next_segment_id:
                            self._next_segment_id = seg_id + 1
                except Exception:
                    pass

    def push(self, records: Any, lsn: Any = None) -> int:
        with self._lock:
            if self._closed:
                raise RuntimeError("StoreAndForwardRingBuffer is closed")

            if isinstance(records, (list, tuple)):
                if not records:
                    return 0
                if lsn is not None:
                    items = [(r, lsn) for r in records]
                else:
                    if records and isinstance(records[0], tuple) and len(records[0]) == 2:
                        items = list(records)
                    else:
                        items = [(r, None) for r in records]
            else:
                items = [(records, lsn)]

            raw = pickle.dumps(items, protocol=pickle.HIGHEST_PROTOCOL)
            raw_len = len(raw)

            chunk_lsn = items[-1][1] if items else lsn
            self._mem_queue.append((items, raw_len, chunk_lsn))
            self._mem_bytes += raw_len

            if chunk_lsn is not None:
                self._highest_pushed_lsn = chunk_lsn
            self._total_pushed += len(items)

            if self._mem_bytes >= self.max_memory_bytes:
                self._spool_to_disk()

            self._cond.notify_all()
            return len(items)

    def _get_or_create_write_segment(self, needed_bytes: int) -> DiskSegment:
        if self._active_write_segment is not None and self._active_write_segment.can_fit(needed_bytes):
            return self._active_write_segment

        if self._active_write_segment is not None:
            self._active_write_segment.seal()
            self._active_write_segment = None

        seg_path = self.spool_dir / f"seg_{self._next_segment_id:08d}.sf"
        seg = DiskSegment(self._next_segment_id, seg_path, max(self.segment_size, needed_bytes + 4096), self.compressor)
        self._next_segment_id += 1
        self._disk_segments.append(seg)
        self._active_write_segment = seg
        return seg

    def _spool_to_disk(self):
        target_mem = int(self.max_memory_bytes * 0.75) if self.max_memory_bytes > 0 else 0
        while self._mem_queue and self._mem_bytes > target_mem:
            items, raw_len, _ = self._mem_queue.popleft()
            self._mem_bytes = max(0, self._mem_bytes - raw_len)

            est_chunk_len = raw_len + struct.calcsize(CHUNK_HDR_FMT) + 128
            seg = self._get_or_create_write_segment(est_chunk_len)
            success = seg.append_chunk(items)
            if not success:
                seg.seal()
                if self._active_write_segment is seg:
                    self._active_write_segment = None
                seg = self._get_or_create_write_segment(est_chunk_len)
                seg.append_chunk(items)

            self._total_raw_spooled += seg.uncompressed_bytes
            self._total_comp_spooled += seg.compressed_bytes

    def flush(self):
        with self._lock:
            while self._mem_queue:
                items, raw_len, _ = self._mem_queue.popleft()
                self._mem_bytes = max(0, self._mem_bytes - raw_len)
                est_chunk_len = raw_len + struct.calcsize(CHUNK_HDR_FMT) + 128
                seg = self._get_or_create_write_segment(est_chunk_len)
                seg.append_chunk(items)
            if self._active_write_segment is not None:
                self._active_write_segment.seal()
                self._active_write_segment = None

    def poll(self, batch_size: int = 100, timeout: Optional[float] = None) -> RecordBatch:
        with self._lock:
            if batch_size <= 0:
                return RecordBatch([], buffer=self)

            collected: List[Tuple[Any, Any]] = []

            start_t = time.monotonic()
            while not self._has_data_ready() and not self._closed:
                if timeout is not None:
                    elapsed = time.monotonic() - start_t
                    rem = timeout - elapsed
                    if rem <= 0:
                        break
                    self._cond.wait(rem)
                else:
                    self._cond.wait()

            if not self._has_data_ready() and (self._closed or (timeout is not None and (time.monotonic() - start_t) >= timeout)):
                return RecordBatch([], buffer=self)

            while len(collected) < batch_size:
                if self._disk_read_staging:
                    take = min(batch_size - len(collected), len(self._disk_read_staging))
                    for _ in range(take):
                        collected.append(self._disk_read_staging.popleft())
                    continue

                if self._read_segment_idx < len(self._disk_segments):
                    seg = self._disk_segments[self._read_segment_idx]
                    if seg is self._active_write_segment:
                        seg.seal()
                        self._active_write_segment = None

                    chunk_items = seg.read_next_chunk()
                    if chunk_items is not None:
                        if self._committed_watermark is not None:
                            chunk_items = [it for it in chunk_items if not lsn_le(it[1], self._committed_watermark)]
                        if chunk_items:
                            self._disk_read_staging.extend(chunk_items)
                        continue
                    else:
                        seg.is_consumed = True
                        seg.close_read()
                        self._read_segment_idx += 1
                        if self.auto_evict and self._committed_watermark is not None and seg.max_lsn is not None and lsn_le(seg.max_lsn, self._committed_watermark):
                            self._evict_applied_segments()
                        continue

                if self._mem_queue:
                    batch_items, raw_len, lsn = self._mem_queue.popleft()
                    needed = batch_size - len(collected)
                    if len(batch_items) <= needed:
                        collected.extend(batch_items)
                        self._mem_bytes = max(0, self._mem_bytes - raw_len)
                    else:
                        taken = batch_items[:needed]
                        remain = batch_items[needed:]
                        collected.extend(taken)
                        rem_ratio = len(remain) / len(batch_items)
                        rem_bytes = int(raw_len * rem_ratio)
                        self._mem_bytes = max(0, self._mem_bytes - (raw_len - rem_bytes))
                        self._mem_queue.appendleft((remain, rem_bytes, lsn))
                    continue

                break

            if collected:
                self._highest_polled_lsn = collected[-1][1]
                self._total_polled += len(collected)

            return RecordBatch(collected, buffer=self)

    def _has_data_ready(self) -> bool:
        if self._disk_read_staging:
            return True
        if self._read_segment_idx < len(self._disk_segments):
            return True
        self._discover_new_segments()
        if self._read_segment_idx < len(self._disk_segments):
            return True
        if self._mem_queue:
            return True
        return False

    def commit(self, lsn: Optional[Any] = None) -> Any:
        with self._lock:
            if lsn is None:
                lsn = self._highest_polled_lsn
            if lsn is None:
                lsn = self._highest_pushed_lsn
            if lsn is None:
                return self._committed_watermark

            self._committed_watermark = lsn

            watermark_payload = {
                "committed_watermark": lsn,
                "highest_polled_lsn": self._highest_polled_lsn,
                "highest_pushed_lsn": self._highest_pushed_lsn,
                "total_polled": self._total_polled,
                "total_pushed": self._total_pushed,
                "updated_at": time.time(),
            }
            durable.write_json(self.watermark_file, watermark_payload)

            if self.auto_evict:
                self._evict_applied_segments()

            return self._committed_watermark

    def commit_watermark(self, lsn: Any) -> Any:
        return self.commit(lsn)

    def ack(self, lsn: Optional[Any] = None) -> Any:
        return self.commit(lsn)

    def get_watermark(self) -> Any:
        with self._lock:
            return self._committed_watermark

    def _evict_applied_segments(self) -> int:
        evicted = 0
        if self._committed_watermark is None:
            return 0

        while self._disk_segments:
            seg = self._disk_segments[0]
            if seg.is_consumed and seg.max_lsn is not None and lsn_le(seg.max_lsn, self._committed_watermark):
                self._disk_segments.popleft()
                seg.evict()
                self._read_segment_idx = max(0, self._read_segment_idx - 1)
                evicted += 1
            else:
                break
        return evicted

    def evict_applied(self) -> int:
        with self._lock:
            return self._evict_applied_segments()

    @property
    def in_memory_bytes(self) -> int:
        with self._lock:
            return self._mem_bytes

    @property
    def disk_bytes(self) -> int:
        with self._lock:
            total = 0
            for seg in self._disk_segments:
                if seg.path.exists():
                    total += seg.path.stat().st_size
            return total

    @property
    def spooled_segments_count(self) -> int:
        with self._lock:
            return len(self._disk_segments)

    @property
    def pending_records_count(self) -> int:
        with self._lock:
            count = len(self._disk_read_staging)
            for seg in list(self._disk_segments)[self._read_segment_idx:]:
                count += seg.record_count
            for items, _, _ in self._mem_queue:
                count += len(items)
            return count

    @property
    def committed_watermark(self) -> Any:
        with self._lock:
            return self._committed_watermark

    @property
    def highest_pushed_lsn(self) -> Any:
        with self._lock:
            return self._highest_pushed_lsn

    @property
    def highest_polled_lsn(self) -> Any:
        with self._lock:
            return self._highest_polled_lsn

    @property
    def compression_ratio(self) -> float:
        with self._lock:
            raw = self._total_raw_spooled
            comp = self._total_comp_spooled
            if comp == 0:
                return 1.0
            return raw / comp

    @property
    def is_empty(self) -> bool:
        with self._lock:
            return not self._has_data_ready()

    @property
    def care_badge(self) -> str:
        with self._lock:
            if self._closed and self.pending_records_count > 0:
                return CARE_ACTION
            if self.disk_bytes > (self.max_memory_bytes * 8):
                return CARE_REVIEW
            if self.disk_bytes > 0 or self._mem_bytes > (self.max_memory_bytes * 0.5):
                return CARE_BENIGN
            return CARE_PASS

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "in_memory_bytes": self.in_memory_bytes,
                "max_memory_bytes": self.max_memory_bytes,
                "disk_bytes": self.disk_bytes,
                "spooled_segments_count": self.spooled_segments_count,
                "pending_records_count": self.pending_records_count,
                "total_pushed": self._total_pushed,
                "total_polled": self._total_polled,
                "committed_watermark": self._committed_watermark,
                "highest_pushed_lsn": self._highest_pushed_lsn,
                "highest_polled_lsn": self._highest_polled_lsn,
                "compression": self.compressor.codec_name,
                "compression_ratio": self.compression_ratio,
                "care_badge": self.care_badge,
            }

    def close(self):
        with self._lock:
            if self._closed:
                return
            try:
                self.flush()
            except Exception:
                pass
            self._closed = True
            if self._active_write_segment is not None:
                self._active_write_segment.seal()
                self._active_write_segment = None
            for seg in self._disk_segments:
                seg.close()
            self._cond.notify_all()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self) -> int:
        return self.pending_records_count

    def __bool__(self) -> bool:
        return not self._closed


def _mp_context():
    try:
        return multiprocessing.get_context("fork")
    except (ValueError, AttributeError):
        return multiprocessing


class StoreAndForwardReaderProcess:
    def __init__(
        self,
        buffer: StoreAndForwardRingBuffer,
        source_records: Optional[Iterable[Any]] = None,
        source_callable: Optional[Callable[[], Optional[Any]]] = None,
        ipc_queue: Optional[Any] = None,
        stop_event: Optional[Any] = None,
        poll_interval: float = 0.001,
        as_process: bool = True,
    ):
        self.buffer = buffer
        self.source_records = list(source_records) if source_records is not None else None
        self.source_callable = source_callable
        self.ipc_queue = ipc_queue
        self.poll_interval = poll_interval
        self.as_process = as_process

        ctx = _mp_context()
        self._stop_event = stop_event or (ctx.Event() if as_process else threading.Event())
        self._done_event = ctx.Event() if as_process else threading.Event()
        self._total_pushed = ctx.Value("i", 0) if as_process else None
        self._total_pushed_counter = 0
        self._proc = None

    def _worker_loop(self):
        try:
            if self.source_records is not None:
                for item in self.source_records:
                    if self._stop_event.is_set():
                        break
                    if isinstance(item, tuple) and len(item) == 2:
                        rec, lsn = item
                    elif isinstance(item, dict) and "lsn" in item:
                        rec, lsn = item, item.get("lsn")
                    else:
                        rec, lsn = item, None

                    pushed = False
                    if self.ipc_queue is not None:
                        try:
                            self.ipc_queue.put_nowait((rec, lsn))
                            pushed = True
                        except Exception:
                            pushed = False
                    if not pushed:
                        self.buffer.push(rec, lsn=lsn)

                    self._total_pushed_counter += 1
                    if self._total_pushed is not None:
                        self._total_pushed.value = self._total_pushed_counter
                    if self.poll_interval > 0:
                        time.sleep(self.poll_interval)
            elif self.source_callable is not None:
                while not self._stop_event.is_set():
                    got = self.source_callable()
                    if got is None:
                        if self._stop_event.is_set():
                            break
                        time.sleep(self.poll_interval)
                        continue
                    if isinstance(got, tuple) and len(got) == 2:
                        rec, lsn = got
                    elif isinstance(got, dict) and "lsn" in got:
                        rec, lsn = got, got.get("lsn")
                    else:
                        rec, lsn = got, None

                    pushed = False
                    if self.ipc_queue is not None:
                        try:
                            self.ipc_queue.put_nowait((rec, lsn))
                            pushed = True
                        except Exception:
                            pushed = False
                    if not pushed:
                        self.buffer.push(rec, lsn=lsn)

                    self._total_pushed_counter += 1
                    if self._total_pushed is not None:
                        self._total_pushed.value = self._total_pushed_counter
                    if self.poll_interval > 0:
                        time.sleep(self.poll_interval)
        finally:
            self.buffer.flush()
            self._done_event.set()

    def start(self):
        ctx = _mp_context()
        if self.as_process:
            self._proc = ctx.Process(target=self._worker_loop, daemon=True)
            self._proc.start()
        else:
            self._proc = threading.Thread(target=self._worker_loop, daemon=True)
            self._proc.start()
        return self

    def stop(self, timeout: float = 5.0):
        self._stop_event.set()
        if self._proc is not None:
            self._proc.join(timeout=timeout)
            if hasattr(self._proc, "is_alive") and self._proc.is_alive():
                if hasattr(self._proc, "terminate"):
                    self._proc.terminate()
                    self._proc.join(timeout=1.0)

    @property
    def pid(self) -> Optional[int]:
        if self._proc is not None and hasattr(self._proc, "pid"):
            return self._proc.pid
        return None

    @property
    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.is_alive()

    @property
    def is_done(self) -> bool:
        return self._done_event.is_set()

    @property
    def total_pushed(self) -> int:
        if self._total_pushed is not None:
            return self._total_pushed.value
        return self._total_pushed_counter


class StoreAndForwardApplierWorker:
    def __init__(
        self,
        buffer: StoreAndForwardRingBuffer,
        applier_callable: Optional[Callable[[RecordBatch], Any]] = None,
        ipc_queue: Optional[Any] = None,
        batch_size: int = 100,
        poll_timeout: float = 0.5,
        stop_event: Optional[Any] = None,
        as_process: bool = True,
        auto_commit: bool = True,
    ):
        self.buffer = buffer
        self.applier_callable = applier_callable
        self.ipc_queue = ipc_queue
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.as_process = as_process
        self.auto_commit = auto_commit

        ctx = _mp_context()
        self._stop_event = stop_event or (ctx.Event() if as_process else threading.Event())
        self._done_event = ctx.Event() if as_process else threading.Event()
        self._total_applied = ctx.Value("i", 0) if as_process else None
        self._total_applied_counter = 0
        self._proc = None

    def _worker_loop(self):
        try:
            while not self._stop_event.is_set():
                batch = self.buffer.poll(batch_size=self.batch_size, timeout=0.01)
                if not batch and self.ipc_queue is not None:
                    items = []
                    while len(items) < self.batch_size:
                        try:
                            rec, lsn = self.ipc_queue.get(timeout=0.01)
                            items.append((rec, lsn))
                        except Exception:
                            break
                    if items:
                        batch = RecordBatch(items, buffer=self.buffer)

                if not batch:
                    if self._stop_event.is_set():
                        break
                    time.sleep(min(self.poll_timeout, 0.05))
                    continue

                if self.applier_callable is not None:
                    self.applier_callable(batch)
                if self.auto_commit:
                    batch.commit()
                self._total_applied_counter += len(batch)
                if self._total_applied is not None:
                    self._total_applied.value = self._total_applied_counter
        finally:
            self._done_event.set()

    def start(self):
        ctx = _mp_context()
        if self.as_process:
            self._proc = ctx.Process(target=self._worker_loop, daemon=True)
            self._proc.start()
        else:
            self._proc = threading.Thread(target=self._worker_loop, daemon=True)
            self._proc.start()
        return self

    def stop(self, timeout: float = 5.0):
        self._stop_event.set()
        if self._proc is not None:
            self._proc.join(timeout=timeout)
            if hasattr(self._proc, "is_alive") and self._proc.is_alive():
                if hasattr(self._proc, "terminate"):
                    self._proc.terminate()
                    self._proc.join(timeout=1.0)

    @property
    def pid(self) -> Optional[int]:
        if self._proc is not None and hasattr(self._proc, "pid"):
            return self._proc.pid
        return None

    @property
    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.is_alive()

    @property
    def is_done(self) -> bool:
        return self._done_event.is_set()

    @property
    def total_applied(self) -> int:
        if self._total_applied is not None:
            return self._total_applied.value
        return self._total_applied_counter


class StoreAndForwardPipeline:
    def __init__(
        self,
        buffer: Optional[StoreAndForwardRingBuffer] = None,
        spool_dir: Optional[Union[str, Path]] = None,
        source_records: Optional[Iterable[Any]] = None,
        source_callable: Optional[Callable] = None,
        applier_callable: Optional[Callable[[RecordBatch], Any]] = None,
        max_memory_bytes: Optional[int] = None,
        segment_size: Optional[int] = None,
        batch_size: int = 100,
        poll_timeout: float = 0.5,
        compression: str = "auto",
        hop: Any = None,
        as_process: bool = True,
    ):
        if buffer is not None:
            self.buffer = buffer
        else:
            self.buffer = StoreAndForwardRingBuffer(
                spool_dir=spool_dir,
                max_memory_bytes=max_memory_bytes,
                segment_size=segment_size,
                compression=compression,
                hop=hop,
            )
        self.as_process = as_process
        ctx = _mp_context()
        self.ipc_queue = ctx.Queue(maxsize=50) if as_process else None
        self.reader = StoreAndForwardReaderProcess(
            buffer=self.buffer,
            source_records=source_records,
            source_callable=source_callable,
            ipc_queue=self.ipc_queue,
            as_process=as_process,
        )
        self.applier = StoreAndForwardApplierWorker(
            buffer=self.buffer,
            applier_callable=applier_callable,
            ipc_queue=self.ipc_queue,
            batch_size=batch_size,
            poll_timeout=poll_timeout,
            as_process=as_process,
        )

    def start(self):
        self.reader.start()
        self.applier.start()
        return self

    def drain(self, timeout: float = 10.0):
        start = time.monotonic()
        while not self.reader.is_done:
            if (time.monotonic() - start) >= timeout:
                break
            time.sleep(0.01)
        self.buffer.flush()
        while (not self.buffer.is_empty) or (self.ipc_queue is not None and not self.ipc_queue.empty()):
            if (time.monotonic() - start) >= timeout:
                break
            time.sleep(0.01)
        self.applier.stop(timeout=1.0)
        self.reader.stop(timeout=1.0)

    def stop(self, timeout: float = 5.0):
        self.reader.stop(timeout=timeout)
        self.applier.stop(timeout=timeout)
        self.buffer.close()

    @property
    def care_badge(self) -> str:
        return self.buffer.care_badge

    def stats(self) -> Dict[str, Any]:
        s = self.buffer.stats()
        s["reader_pid"] = self.reader.pid
        s["applier_pid"] = self.applier.pid
        s["reader_alive"] = self.reader.is_alive
        s["applier_alive"] = self.applier.is_alive
        s["reader_pushed"] = self.reader.total_pushed
        s["applier_applied"] = self.applier.total_applied
        return s

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def store_and_forward_buffer(hop=None, db=None, **kwargs):
    """Factory helper for StoreAndForwardRingBuffer."""
    spool_dir = kwargs.pop("spool_dir", None)
    if spool_dir is None and hop is not None and db:
        spool_dir = hop.report_dir(db) / "store_forward"
    return StoreAndForwardRingBuffer(spool_dir=spool_dir, hop=hop, **kwargs)
