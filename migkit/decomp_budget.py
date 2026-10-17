import bz2
import collections
from contextlib import contextmanager
from dataclasses import dataclass, field
import enum
import io
import os
from pathlib import Path
import shutil
import tempfile
import threading
from typing import Any, BinaryIO, Dict, Iterable, Iterator, List, Optional, Union
import uuid
import zlib

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False


class DecompressionBudgetError(Exception):
    pass


class DecompressionLimitExceeded(DecompressionBudgetError):
    def __init__(
        self,
        message: str,
        limit_type: str,
        limit_bytes: int,
        observed_bytes: int,
        action: str = "refuse",
        transaction_id: Optional[str] = None,
        reseed_required: bool = False,
        partial_effects_acknowledged: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.limit_type = limit_type
        self.limit_bytes = limit_bytes
        self.observed_bytes = observed_bytes
        self.action = action
        self.transaction_id = transaction_id
        self.reseed_required = reseed_required
        self.partial_effects_acknowledged = partial_effects_acknowledged


class DecoderLimitExceeded(DecompressionLimitExceeded):
    pass


class TransactionAtomicityError(DecompressionBudgetError):
    pass


class LimitAction(str, enum.Enum):
    REFUSE = "refuse"
    RESEED = "reseed"


class SpillPolicy(str, enum.Enum):
    AUTO = "auto"
    MEMORY_ONLY = "memory_only"
    ALWAYS_SPILL = "always_spill"


class TransactionStatus(str, enum.Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    REFUSED = "refused"
    RESEEDED = "reseeded"


@dataclass
class BudgetLimits:
    max_memory_bytes: int = 64 * 1024 * 1024
    max_uncompressed_bytes: int = 2 * 1024 * 1024 * 1024
    max_compressed_bytes: int = 1 * 1024 * 1024 * 1024
    spill_threshold_bytes: int = 8 * 1024 * 1024
    decoder_chunk_size: int = 64 * 1024
    max_decoder_buffer_bytes: int = 16 * 1024 * 1024
    max_nested_depth: int = 8
    max_nested_allocations: int = 32
    spill_policy: SpillPolicy = SpillPolicy.AUTO
    default_limit_action: LimitAction = LimitAction.REFUSE


@dataclass
class GovernorMetrics:
    compressed_bytes: int = 0
    expanded_bytes: int = 0
    active_memory_bytes: int = 0
    peak_memory_bytes: int = 0
    spill_bytes: int = 0
    spill_count: int = 0
    nested_allocations: int = 0
    peak_nested_depth: int = 0
    transactions_active: int = 0
    transactions_completed: int = 0
    transactions_refused: int = 0
    transactions_reseeded: int = 0
    decoder_limit_violations: int = 0


class _StreamingDecoder:
    def decode_chunk(self, data: bytes, max_out: int) -> Iterator[bytes]:
        raise NotImplementedError

    def flush(self, max_out: int) -> Iterator[bytes]:
        return iter([])


class _ZlibStreamingDecoder(_StreamingDecoder):
    def __init__(self, wbits: int = zlib.MAX_WBITS):
        self._obj = zlib.decompressobj(wbits)

    def decode_chunk(self, data: bytes, max_out: int) -> Iterator[bytes]:
        in_buf = data
        while in_buf:
            out = self._obj.decompress(in_buf, max_out)
            if out:
                yield out
            in_buf = self._obj.unconsumed_tail

    def flush(self, max_out: int) -> Iterator[bytes]:
        out = self._obj.flush()
        if out:
            for i in range(0, len(out), max_out):
                yield out[i:i + max_out]


class _Bz2StreamingDecoder(_StreamingDecoder):
    def __init__(self):
        self._obj = bz2.BZ2Decompressor()

    def decode_chunk(self, data: bytes, max_out: int) -> Iterator[bytes]:
        out = self._obj.decompress(data, max_out)
        if out:
            yield out
        while not self._obj.needs_input and not self._obj.eof:
            out = self._obj.decompress(b"", max_out)
            if not out:
                break
            yield out

    def flush(self, max_out: int) -> Iterator[bytes]:
        return iter([])


class _ZstdStreamingDecoder(_StreamingDecoder):
    def __init__(self):
        if not HAS_ZSTD:
            raise DecompressionBudgetError("zstandard module not installed")
        self._obj = zstd.ZstdDecompressor().decompressobj()

    def decode_chunk(self, data: bytes, max_out: int) -> Iterator[bytes]:
        out = self._obj.decompress(data)
        if out:
            for i in range(0, len(out), max_out):
                yield out[i:i + max_out]

    def flush(self, max_out: int) -> Iterator[bytes]:
        out = self._obj.flush()
        if out:
            for i in range(0, len(out), max_out):
                yield out[i:i + max_out]


class _IdentityStreamingDecoder(_StreamingDecoder):
    def decode_chunk(self, data: bytes, max_out: int) -> Iterator[bytes]:
        for i in range(0, len(data), max_out):
            yield data[i:i + max_out]


def _create_decoder(codec: str) -> _StreamingDecoder:
    norm = codec.strip().lower()
    if norm in ("zlib", "deflate_zlib"):
        return _ZlibStreamingDecoder(zlib.MAX_WBITS)
    if norm == "gzip":
        return _ZlibStreamingDecoder(zlib.MAX_WBITS | 16)
    if norm in ("deflate", "raw_deflate"):
        return _ZlibStreamingDecoder(-zlib.MAX_WBITS)
    if norm == "bz2":
        return _Bz2StreamingDecoder()
    if norm == "zstd":
        return _ZstdStreamingDecoder()
    if norm in ("none", "raw", "identity"):
        return _IdentityStreamingDecoder()
    raise DecompressionBudgetError(f"unsupported decompression codec: {codec}")


class TransactionPayload:
    def __init__(
        self,
        tx_id: str,
        compressed_bytes: int,
        expanded_bytes: int,
        governor: "DecompressionBudgetGovernor",
        is_spilled: bool,
        spill_path: Optional[str] = None,
        memory_chunks: Optional[List[bytes]] = None,
    ):
        self.tx_id = tx_id
        self.compressed_bytes = compressed_bytes
        self.expanded_bytes = expanded_bytes
        self.governor = governor
        self.is_spilled = is_spilled
        self.spill_path = spill_path
        self._memory_chunks = memory_chunks or []
        self._memory_bytes_held = sum(len(c) for c in self._memory_chunks)
        self.status = TransactionStatus.ACTIVE
        self.partial_effects_acknowledged = False
        self._file: Optional[BinaryIO] = None
        self._mem_stream: Optional[io.BytesIO] = None
        self._closed = False

    @property
    def size(self) -> int:
        return self.expanded_bytes

    def _ensure_stream(self):
        if self._closed:
            raise DecompressionBudgetError("payload has been closed")
        if self.is_spilled:
            if self._file is None:
                if self.spill_path and os.path.exists(self.spill_path):
                    self._file = open(self.spill_path, "rb")
                else:
                    raise DecompressionBudgetError("spill file not found")
        else:
            if self._mem_stream is None:
                self._mem_stream = io.BytesIO(b"".join(self._memory_chunks))

    def read(self, size: Optional[int] = None) -> bytes:
        self._ensure_stream()
        if self.is_spilled:
            assert self._file is not None
            return self._file.read(-1 if size is None else size)
        assert self._mem_stream is not None
        return self._mem_stream.read(-1 if size is None else size)

    def iter_chunks(self, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        self.seek(0)
        while True:
            chunk = self.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def seek(self, offset: int, whence: int = 0) -> int:
        self._ensure_stream()
        if self.is_spilled:
            assert self._file is not None
            return self._file.seek(offset, whence)
        assert self._mem_stream is not None
        return self._mem_stream.seek(offset, whence)

    def tell(self) -> int:
        self._ensure_stream()
        if self.is_spilled:
            assert self._file is not None
            return self._file.tell()
        assert self._mem_stream is not None
        return self._mem_stream.tell()

    def commit(self) -> None:
        if self.status == TransactionStatus.ROLLED_BACK:
            raise TransactionAtomicityError("cannot commit rolled back transaction")
        if self.status == TransactionStatus.ACTIVE:
            self.status = TransactionStatus.COMMITTED

    def rollback(self) -> None:
        if self.status == TransactionStatus.COMMITTED and self.partial_effects_acknowledged:
            raise TransactionAtomicityError("cannot rollback already acknowledged transaction")
        self.status = TransactionStatus.ROLLED_BACK
        self.partial_effects_acknowledged = False
        self._cleanup_resources()

    def acknowledge(self) -> None:
        if self.status != TransactionStatus.COMMITTED:
            raise TransactionAtomicityError(f"cannot acknowledge transaction in status {self.status}")
        self.partial_effects_acknowledged = True

    def _cleanup_resources(self) -> None:
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        if self._mem_stream is not None:
            try:
                self._mem_stream.close()
            except Exception:
                pass
            self._mem_stream = None
        if self.spill_path and os.path.exists(self.spill_path):
            try:
                os.unlink(self.spill_path)
            except OSError:
                pass
            self.spill_path = None
        if self._memory_bytes_held > 0:
            self.governor.release_memory(self._memory_bytes_held, tx_id=self.tx_id)
            self._memory_bytes_held = 0
            self._memory_chunks.clear()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._cleanup_resources()

    def __enter__(self) -> "TransactionPayload":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        self.close()


class DecompressionBudgetGovernor:
    def __init__(
        self,
        limits: Optional[BudgetLimits] = None,
        spill_dir: Optional[Union[str, Path]] = None,
    ):
        self.limits = limits or BudgetLimits()
        if spill_dir is not None:
            self.spill_dir = Path(spill_dir)
            self.spill_dir.mkdir(parents=True, exist_ok=True)
            self._owns_spill_dir = False
        else:
            self.spill_dir = Path(tempfile.mkdtemp(prefix="migkit_decomp_spool_"))
            self._owns_spill_dir = True

        self._lock = threading.Lock()
        self.metrics = GovernorMetrics()
        self._tx_nested_depth: Dict[str, int] = collections.defaultdict(int)

    @property
    def compressed_bytes(self) -> int:
        return self.metrics.compressed_bytes

    @property
    def expanded_bytes(self) -> int:
        return self.metrics.expanded_bytes

    @property
    def spilled_bytes(self) -> int:
        return self.metrics.spill_bytes

    @property
    def spill_count(self) -> int:
        return self.metrics.spill_count

    @property
    def nested_allocations(self) -> int:
        return self.metrics.nested_allocations

    @property
    def active_memory_bytes(self) -> int:
        return self.metrics.active_memory_bytes

    def acquire_memory(self, nbytes: int, tx_id: Optional[str] = None) -> bool:
        with self._lock:
            if self.metrics.active_memory_bytes + nbytes > self.limits.max_memory_bytes:
                return False
            self.metrics.active_memory_bytes += nbytes
            if self.metrics.active_memory_bytes > self.metrics.peak_memory_bytes:
                self.metrics.peak_memory_bytes = self.metrics.active_memory_bytes
            return True

    def release_memory(self, nbytes: int, tx_id: Optional[str] = None) -> None:
        with self._lock:
            self.metrics.active_memory_bytes = max(0, self.metrics.active_memory_bytes - nbytes)

    def record_spill(self, nbytes: int) -> None:
        with self._lock:
            self.metrics.spill_bytes += nbytes

    def enter_nested_allocation(self, tx_id: Optional[str] = None) -> int:
        tid = tx_id or "global"
        with self._lock:
            if self.metrics.nested_allocations + 1 > self.limits.max_nested_allocations:
                raise DecompressionLimitExceeded(
                    message=f"nested allocations limit exceeded: {self.metrics.nested_allocations + 1} > {self.limits.max_nested_allocations}",
                    limit_type="nested_allocations",
                    limit_bytes=self.limits.max_nested_allocations,
                    observed_bytes=self.metrics.nested_allocations + 1,
                    action=self.limits.default_limit_action.value,
                    transaction_id=tx_id,
                    reseed_required=(self.limits.default_limit_action == LimitAction.RESEED),
                    partial_effects_acknowledged=False,
                )
            new_depth = self._tx_nested_depth[tid] + 1
            if new_depth > self.limits.max_nested_depth:
                raise DecompressionLimitExceeded(
                    message=f"nested depth limit exceeded: {new_depth} > {self.limits.max_nested_depth}",
                    limit_type="nested_depth",
                    limit_bytes=self.limits.max_nested_depth,
                    observed_bytes=new_depth,
                    action=self.limits.default_limit_action.value,
                    transaction_id=tx_id,
                    reseed_required=(self.limits.default_limit_action == LimitAction.RESEED),
                    partial_effects_acknowledged=False,
                )
            self._tx_nested_depth[tid] = new_depth
            self.metrics.nested_allocations += 1
            if new_depth > self.metrics.peak_nested_depth:
                self.metrics.peak_nested_depth = new_depth
            return self.metrics.nested_allocations

    def exit_nested_allocation(self, tx_id: Optional[str] = None) -> None:
        tid = tx_id or "global"
        with self._lock:
            if self.metrics.nested_allocations > 0:
                self.metrics.nested_allocations -= 1
            if self._tx_nested_depth[tid] > 0:
                self._tx_nested_depth[tid] -= 1
                if self._tx_nested_depth[tid] == 0:
                    del self._tx_nested_depth[tid]

    @contextmanager
    def nested_allocation(self, tx_id: Optional[str] = None):
        self.enter_nested_allocation(tx_id)
        try:
            yield
        finally:
            self.exit_nested_allocation(tx_id)

    def decompress(
        self,
        source: Union[bytes, bytearray, BinaryIO, Iterable[bytes]],
        codec: str = "zlib",
        tx_id: Optional[str] = None,
        limit_action: Optional[LimitAction] = None,
        spill_policy: Optional[SpillPolicy] = None,
    ) -> TransactionPayload:
        tid = tx_id or f"tx_{uuid.uuid4().hex[:12]}"
        action = limit_action or self.limits.default_limit_action
        policy = spill_policy or self.limits.spill_policy

        decoder = _create_decoder(codec)
        input_stream: BinaryIO
        if isinstance(source, (bytes, bytearray, memoryview)):
            input_stream = io.BytesIO(source)
        elif hasattr(source, "read"):
            input_stream = source
        else:
            input_stream = io.BytesIO(b"".join(source))

        mem_chunks: List[bytes] = []
        mem_held = 0
        is_spilled = False
        spill_fp: Optional[BinaryIO] = None
        spill_path: Optional[str] = None

        tx_comp = 0
        tx_exp = 0

        with self._lock:
            self.metrics.transactions_active += 1

        def _cleanup_partial():
            nonlocal spill_fp, spill_path, mem_held, mem_chunks
            if spill_fp is not None:
                try:
                    spill_fp.close()
                except Exception:
                    pass
                spill_fp = None
            if spill_path and os.path.exists(spill_path):
                try:
                    os.unlink(spill_path)
                except OSError:
                    pass
                spill_path = None
            if mem_held > 0:
                self.release_memory(mem_held, tx_id=tid)
                mem_held = 0
            mem_chunks.clear()

        def _handle_limit_fail(limit_type: str, limit_bytes: int, observed_bytes: int, err_cls=DecompressionLimitExceeded):
            _cleanup_partial()
            with self._lock:
                self.metrics.transactions_active = max(0, self.metrics.transactions_active - 1)
                if limit_type == "decoder_buffer":
                    self.metrics.decoder_limit_violations += 1
                if action == LimitAction.RESEED:
                    self.metrics.transactions_reseeded += 1
                else:
                    self.metrics.transactions_refused += 1

            raise err_cls(
                message=f"decompression {limit_type} limit exceeded: {observed_bytes} > {limit_bytes}",
                limit_type=limit_type,
                limit_bytes=limit_bytes,
                observed_bytes=observed_bytes,
                action=action.value,
                transaction_id=tid,
                reseed_required=(action == LimitAction.RESEED),
                partial_effects_acknowledged=False,
            )

        try:
            if policy == SpillPolicy.ALWAYS_SPILL:
                spill_path = str(self.spill_dir / f"tx_{tid}_{uuid.uuid4().hex}.spool")
                spill_fp = open(spill_path, "wb")
                is_spilled = True
                with self._lock:
                    self.metrics.spill_count += 1

            while True:
                in_chunk = input_stream.read(self.limits.decoder_chunk_size)
                if not in_chunk:
                    break
                tx_comp += len(in_chunk)
                if tx_comp > self.limits.max_compressed_bytes:
                    _handle_limit_fail("compressed_bytes", self.limits.max_compressed_bytes, tx_comp)

                try:
                    decoded_chunks = list(decoder.decode_chunk(in_chunk, self.limits.decoder_chunk_size))
                except Exception as e:
                    _cleanup_partial()
                    with self._lock:
                        self.metrics.transactions_active = max(0, self.metrics.transactions_active - 1)
                    raise DecompressionBudgetError(f"decoder error: {e}") from e

                for out_chunk in decoded_chunks:
                    chunk_len = len(out_chunk)
                    if chunk_len == 0:
                        continue
                    if chunk_len > self.limits.max_decoder_buffer_bytes:
                        _handle_limit_fail("decoder_buffer", self.limits.max_decoder_buffer_bytes, chunk_len, DecoderLimitExceeded)

                    tx_exp += chunk_len
                    if tx_exp > self.limits.max_uncompressed_bytes:
                        _handle_limit_fail("uncompressed_bytes", self.limits.max_uncompressed_bytes, tx_exp)

                    if not is_spilled:
                        should_spill = False
                        if policy == SpillPolicy.AUTO:
                            if tx_exp > self.limits.spill_threshold_bytes:
                                should_spill = True
                            elif not self.acquire_memory(chunk_len, tx_id=tid):
                                should_spill = True
                        elif policy == SpillPolicy.MEMORY_ONLY:
                            if not self.acquire_memory(chunk_len, tx_id=tid):
                                _handle_limit_fail("memory_budget", self.limits.max_memory_bytes, self.metrics.active_memory_bytes + chunk_len)

                        if should_spill:
                            spill_path = str(self.spill_dir / f"tx_{tid}_{uuid.uuid4().hex}.spool")
                            spill_fp = open(spill_path, "wb")
                            is_spilled = True
                            with self._lock:
                                self.metrics.spill_count += 1
                            for prev in mem_chunks:
                                spill_fp.write(prev)
                                self.record_spill(len(prev))
                            if mem_held > 0:
                                self.release_memory(mem_held, tx_id=tid)
                                mem_held = 0
                            mem_chunks.clear()
                            spill_fp.write(out_chunk)
                            self.record_spill(chunk_len)
                        else:
                            mem_held += chunk_len
                            mem_chunks.append(out_chunk)
                    else:
                        assert spill_fp is not None
                        spill_fp.write(out_chunk)
                        self.record_spill(chunk_len)

            try:
                flush_chunks = list(decoder.flush(self.limits.decoder_chunk_size))
            except Exception as e:
                _cleanup_partial()
                with self._lock:
                    self.metrics.transactions_active = max(0, self.metrics.transactions_active - 1)
                raise DecompressionBudgetError(f"decoder flush error: {e}") from e

            for out_chunk in flush_chunks:
                chunk_len = len(out_chunk)
                if chunk_len == 0:
                    continue
                if chunk_len > self.limits.max_decoder_buffer_bytes:
                    _handle_limit_fail("decoder_buffer", self.limits.max_decoder_buffer_bytes, chunk_len, DecoderLimitExceeded)

                tx_exp += chunk_len
                if tx_exp > self.limits.max_uncompressed_bytes:
                    _handle_limit_fail("uncompressed_bytes", self.limits.max_uncompressed_bytes, tx_exp)

                if is_spilled:
                    assert spill_fp is not None
                    spill_fp.write(out_chunk)
                    self.record_spill(chunk_len)
                else:
                    should_spill = False
                    if policy == SpillPolicy.AUTO:
                        if tx_exp > self.limits.spill_threshold_bytes:
                            should_spill = True
                        elif not self.acquire_memory(chunk_len, tx_id=tid):
                            should_spill = True
                    elif policy == SpillPolicy.MEMORY_ONLY:
                        if not self.acquire_memory(chunk_len, tx_id=tid):
                            _handle_limit_fail("memory_budget", self.limits.max_memory_bytes, self.metrics.active_memory_bytes + chunk_len)

                    if should_spill:
                        spill_path = str(self.spill_dir / f"tx_{tid}_{uuid.uuid4().hex}.spool")
                        spill_fp = open(spill_path, "wb")
                        is_spilled = True
                        with self._lock:
                            self.metrics.spill_count += 1
                        for prev in mem_chunks:
                            spill_fp.write(prev)
                            self.record_spill(len(prev))
                        if mem_held > 0:
                            self.release_memory(mem_held, tx_id=tid)
                            mem_held = 0
                        mem_chunks.clear()
                        spill_fp.write(out_chunk)
                        self.record_spill(chunk_len)
                    else:
                        mem_held += chunk_len
                        mem_chunks.append(out_chunk)

            if spill_fp is not None:
                spill_fp.flush()
                spill_fp.close()
                spill_fp = None

            with self._lock:
                self.metrics.compressed_bytes += tx_comp
                self.metrics.expanded_bytes += tx_exp
                self.metrics.transactions_completed += 1
                self.metrics.transactions_active = max(0, self.metrics.transactions_active - 1)

            return TransactionPayload(
                tx_id=tid,
                compressed_bytes=tx_comp,
                expanded_bytes=tx_exp,
                governor=self,
                is_spilled=is_spilled,
                spill_path=spill_path,
                memory_chunks=mem_chunks,
            )

        except (DecompressionBudgetError, Exception):
            _cleanup_partial()
            with self._lock:
                self.metrics.transactions_active = max(0, self.metrics.transactions_active - 1)
            raise

    def handle_refusal(self, exc: DecompressionLimitExceeded) -> Dict[str, Any]:
        return {
            "status": "refused",
            "action": "refuse",
            "transaction_id": exc.transaction_id,
            "limit_type": exc.limit_type,
            "observed_bytes": exc.observed_bytes,
            "limit_bytes": exc.limit_bytes,
            "reseed_required": False,
            "partial_effects_acknowledged": False,
        }

    def handle_reseed(
        self,
        exc: DecompressionLimitExceeded,
        watermark: Any = None,
    ) -> Dict[str, Any]:
        return {
            "status": "reseeded",
            "action": "reseed",
            "transaction_id": exc.transaction_id,
            "limit_type": exc.limit_type,
            "observed_bytes": exc.observed_bytes,
            "limit_bytes": exc.limit_bytes,
            "reseed_required": True,
            "watermark": watermark,
            "partial_effects_acknowledged": False,
        }

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "compressed_bytes": self.metrics.compressed_bytes,
                "expanded_bytes": self.metrics.expanded_bytes,
                "active_memory_bytes": self.metrics.active_memory_bytes,
                "peak_memory_bytes": self.metrics.peak_memory_bytes,
                "spill_bytes": self.metrics.spill_bytes,
                "spill_count": self.metrics.spill_count,
                "nested_allocations": self.metrics.nested_allocations,
                "peak_nested_depth": self.metrics.peak_nested_depth,
                "transactions_active": self.metrics.transactions_active,
                "transactions_completed": self.metrics.transactions_completed,
                "transactions_refused": self.metrics.transactions_refused,
                "transactions_reseeded": self.metrics.transactions_reseeded,
                "decoder_limit_violations": self.metrics.decoder_limit_violations,
            }

    def close(self) -> None:
        if self._owns_spill_dir and self.spill_dir.exists():
            shutil.rmtree(self.spill_dir, ignore_errors=True)
