import hashlib
import io
from dataclasses import dataclass
from typing import Callable, Generator, List, Optional, Tuple, Union

from . import pieces


@dataclass
class LobChunk:
    index: int
    offset: int
    data: bytes
    length: int
    is_last: bool
    checksum: str


@dataclass
class LobStreamSummary:
    total_bytes: int
    chunk_count: int
    digest: str


def iter_lob_chunks(
    source: Union[bytes, str, io.IOBase, Callable[[int, int], bytes]],
    chunk_size: Optional[int] = None,
    total_size: Optional[int] = None,
) -> Generator[LobChunk, None, None]:
    step = int(chunk_size or pieces.PIECE)
    if isinstance(source, str):
        source = source.encode("utf-8")
    if isinstance(source, (bytes, bytearray, memoryview)):
        raw = bytes(source)
        size = len(raw) if total_size is None else min(len(raw), int(total_size))
        at, idx = 1, 0
        while at <= size or (size == 0 and idx == 0):
            n = min(step, size - at + 1) if size > 0 else 0
            part = raw[at - 1 : at - 1 + n]
            at += len(part)
            is_last = at > size
            yield LobChunk(
                index=idx,
                offset=at - len(part),
                data=part,
                length=len(part),
                is_last=is_last,
                checksum=hashlib.md5(part, usedforsecurity=False).hexdigest(),
            )
            idx += 1
            if is_last:
                break
        return

    if isinstance(source, io.IOBase) or hasattr(source, "read"):
        at, idx = 1, 0
        read_total = 0
        buf = source.read(step if total_size is None else min(step, total_size))
        if isinstance(buf, str):
            buf = buf.encode("utf-8")
        buf = bytes(buf or b"")
        if not buf:
            yield LobChunk(0, 1, b"", 0, True, hashlib.md5(b"", usedforsecurity=False).hexdigest())
            return
        while buf:
            read_total += len(buf)
            to_read = step if total_size is None else min(step, total_size - read_total)
            nxt = source.read(to_read) if (total_size is None or read_total < total_size) else b""
            if isinstance(nxt, str):
                nxt = nxt.encode("utf-8")
            nxt = bytes(nxt or b"")
            is_last = len(nxt) == 0 or (total_size is not None and read_total >= total_size)
            yield LobChunk(
                index=idx,
                offset=at,
                data=buf,
                length=len(buf),
                is_last=is_last,
                checksum=hashlib.md5(buf, usedforsecurity=False).hexdigest(),
            )
            at += len(buf)
            idx += 1
            buf = nxt
            if is_last:
                break
        return

    if callable(source):
        at, idx = 1, 0
        bound = total_size if total_size is not None else float("inf")
        while at <= bound:
            ask = min(step, int(bound - at + 1)) if bound != float("inf") else step
            part = source(at, ask)
            if isinstance(part, str):
                part = part.encode("utf-8")
            part = bytes(part or b"")
            if not part:
                if idx == 0:
                    yield LobChunk(0, 1, b"", 0, True, hashlib.md5(b"", usedforsecurity=False).hexdigest())
                break
            at += len(part)
            is_last = len(part) < ask or (bound != float("inf") and at > bound)
            yield LobChunk(
                index=idx,
                offset=at - len(part),
                data=part,
                length=len(part),
                is_last=is_last,
                checksum=hashlib.md5(part, usedforsecurity=False).hexdigest(),
            )
            idx += 1
            if is_last:
                break


class LobStreamer:
    def __init__(self, chunk_size: Optional[int] = None):
        self.chunk_size = int(chunk_size or pieces.PIECE)

    def stream_chunks(
        self,
        source: Union[bytes, str, io.IOBase, Callable[[int, int], bytes]],
        total_size: Optional[int] = None,
        chunk_size: Optional[int] = None,
    ) -> Generator[LobChunk, None, None]:
        step = chunk_size or self.chunk_size
        return iter_lob_chunks(source, chunk_size=step, total_size=total_size)

    def stream_between(
        self,
        read_chunk_fn: Callable[[int, int], bytes],
        write_chunk_fn: Callable[[LobChunk], None],
        total_size: Optional[int] = None,
    ) -> LobStreamSummary:
        h = hashlib.md5(usedforsecurity=False)
        total = 0
        count = 0
        for chunk in self.stream_chunks(read_chunk_fn, total_size=total_size):
            write_chunk_fn(chunk)
            h.update(chunk.data)
            total += chunk.length
            count += 1
        return LobStreamSummary(
            total_bytes=total,
            chunk_count=count,
            digest=h.hexdigest(),
        )

    def read_piece_sql(self, eng, column: str, at: int, n: int) -> str:
        way = getattr(eng, "LARGE_VALUES", None)
        q = eng._q(column) if hasattr(eng, "_q") else f'"{column}"'
        if isinstance(way, pieces.Sql) and way.piece:
            return way.piece.format(c=q, at=int(at), n=int(n))
        canon_name = str(getattr(eng, "CANON_ENGINE", getattr(eng, "ENGINE_FAMILY", ""))).lower()
        if canon_name in ("mysql", "tidb"):
            return f"substring({q}, {int(at)}, {int(n)})"
        if canon_name in ("oracle", "oceanbase-oracle"):
            return f"dbms_lob.substr({q}, {int(n)}, {int(at)})"
        if canon_name in ("mssql", "sqlserver"):
            return f"substring({q}, {int(at)}, {int(n)})"
        return f"substr({q}, {int(at)}, {int(n)})"

    def append_piece_sql(self, eng, column: str, param_mark: str = "?") -> str:
        way = getattr(eng, "LARGE_VALUES", None)
        q = eng._q(column) if hasattr(eng, "_q") else f'"{column}"'
        if isinstance(way, pieces.Sql) and way.append:
            return way.append.format(c=q, v=param_mark)
        canon_name = str(getattr(eng, "CANON_ENGINE", getattr(eng, "ENGINE_FAMILY", ""))).lower()
        if canon_name in ("mysql", "tidb"):
            return f"{q} = concat({q}, {param_mark})"
        if canon_name in ("mssql", "sqlserver"):
            return f"{q}.write({param_mark}, null, null)"
        if canon_name in ("oracle", "oceanbase-oracle"):
            return f"{q} = {q} || {param_mark}"
        return f"{q} = {q} || {param_mark}"

    def verify_parity(self, src: LobStreamSummary, dst: LobStreamSummary) -> bool:
        return src.total_bytes == dst.total_bytes and src.digest.lower() == dst.digest.lower()
