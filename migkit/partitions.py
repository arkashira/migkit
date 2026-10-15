from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PartitionInfo:
    table: str
    name: str
    method: str = "RANGE"
    expression: str = ""
    bound: str = ""
    ordinal: int = 1
    rows: int = 0
    bytes_size: int = 0


@dataclass
class PartitionChunk:
    chunk_id: str
    table: str
    partitions: Tuple[str, ...]
    range_filter: Dict[str, Any] = field(default_factory=dict)
    estimated_rows: int = 0
    worker_id: Optional[int] = None


def discover_partitions(eng, side: str, db: str, table: Optional[str] = None) -> Dict[str, List[PartitionInfo]]:
    canon_name = str(getattr(eng, "CANON_ENGINE", getattr(eng, "ENGINE_FAMILY", ""))).lower()
    out: Dict[str, List[PartitionInfo]] = {}

    if canon_name in ("mysql", "tidb") and hasattr(eng, "partition_scheme"):
        try:
            schemes = eng.partition_scheme(side, db, eng._d(side, db) if hasattr(eng, "_d") else db)
            for tbl, info in schemes.items():
                if table and tbl.lower() != table.lower():
                    continue
                parts = []
                for idx, (pname, desc) in enumerate(info.get("p", {}).items(), 1):
                    parts.append(
                        PartitionInfo(
                            table=tbl,
                            name=pname,
                            method=info.get("m", "RANGE"),
                            expression=info.get("e", ""),
                            bound=str(desc or ""),
                            ordinal=idx,
                        )
                    )
                if parts:
                    out[tbl] = parts
            return out
        except Exception:
            pass

    if hasattr(eng, "_deep_partitions"):
        try:
            schemes = getattr(eng, "partition_scheme", None)
            if callable(schemes):
                sc = schemes(side, db)
                if isinstance(sc, dict):
                    for tbl, info in sc.items():
                        if table and tbl.lower() != table.lower():
                            continue
                        parts = [
                            PartitionInfo(
                                table=tbl,
                                name=pname,
                                method=info.get("m", "RANGE"),
                                expression=info.get("e", ""),
                                bound=str(desc or ""),
                                ordinal=idx,
                            )
                            for idx, (pname, desc) in enumerate(info.get("p", {}).items(), 1)
                        ]
                        if parts:
                            out[tbl] = parts
                    if out:
                        return out
        except Exception:
            pass

    if table:
        out.setdefault(table, [])
    return out


def chunk_partitions(
    partitions: List[PartitionInfo],
    max_rows_per_chunk: int = 50_000,
    max_chunks: Optional[int] = None,
) -> List[PartitionChunk]:
    if not partitions:
        return []

    chunks: List[PartitionChunk] = []
    current_parts: List[str] = []
    current_rows = 0
    tbl = partitions[0].table

    for p in partitions:
        p_rows = max(0, int(p.rows or 0))
        if p_rows > max_rows_per_chunk:
            if current_parts:
                cid = f"{tbl}_chunk_{len(chunks)}"
                chunks.append(
                    PartitionChunk(
                        chunk_id=cid,
                        table=tbl,
                        partitions=tuple(current_parts),
                        estimated_rows=current_rows,
                    )
                )
                current_parts, current_rows = [], 0
            cid = f"{tbl}_{p.name}_0"
            chunks.append(
                PartitionChunk(
                    chunk_id=cid,
                    table=tbl,
                    partitions=(p.name,),
                    estimated_rows=p_rows,
                )
            )
        else:
            if current_parts and (current_rows + p_rows > max_rows_per_chunk):
                cid = f"{tbl}_chunk_{len(chunks)}"
                chunks.append(
                    PartitionChunk(
                        chunk_id=cid,
                        table=tbl,
                        partitions=tuple(current_parts),
                        estimated_rows=current_rows,
                    )
                )
                current_parts, current_rows = [], 0
            current_parts.append(p.name)
            current_rows += p_rows

    if current_parts:
        cid = f"{tbl}_chunk_{len(chunks)}"
        chunks.append(
            PartitionChunk(
                chunk_id=cid,
                table=tbl,
                partitions=tuple(current_parts),
                estimated_rows=current_rows,
            )
        )

    if max_chunks and len(chunks) > max_chunks:
        merged: List[PartitionChunk] = []
        step = (len(chunks) + max_chunks - 1) // max_chunks
        for i in range(0, len(chunks), step):
            group = chunks[i : i + step]
            all_parts = tuple(p for c in group for p in c.partitions)
            total_r = sum(c.estimated_rows for c in group)
            merged.append(
                PartitionChunk(
                    chunk_id=f"{tbl}_chunk_{len(merged)}",
                    table=tbl,
                    partitions=all_parts,
                    estimated_rows=total_r,
                )
            )
        return merged

    return chunks


class PartitionRouter:
    def __init__(self, workers: int = 1, routing_strategy: str = "balanced"):
        self.workers = max(1, int(workers))
        self.routing_strategy = routing_strategy

    def route(self, chunks: List[PartitionChunk]) -> List[PartitionChunk]:
        if not chunks:
            return []
        if self.workers <= 1:
            for c in chunks:
                c.worker_id = 0
            return chunks

        if self.routing_strategy == "round_robin":
            for i, c in enumerate(chunks):
                c.worker_id = i % self.workers
            return chunks

        worker_loads = [0] * self.workers
        for c in sorted(chunks, key=lambda x: x.estimated_rows, reverse=True):
            lightest = min(range(self.workers), key=lambda w: worker_loads[w])
            c.worker_id = lightest
            worker_loads[lightest] += max(1, c.estimated_rows)

        return chunks

    def get_worker_chunks(self, worker_id: int, chunks: List[PartitionChunk]) -> List[PartitionChunk]:
        return [c for c in chunks if c.worker_id == worker_id]

    def partition_clause(self, eng, table: str, partition_name: str) -> str:
        q = eng._q if hasattr(eng, "_q") else lambda x: f'"{x}"'
        canon_name = str(getattr(eng, "CANON_ENGINE", getattr(eng, "ENGINE_FAMILY", ""))).lower()
        if canon_name in ("mysql", "tidb"):
            return f"{table} PARTITION ({q(partition_name)})"
        if canon_name in ("oracle", "oceanbase-oracle"):
            return f"{table} PARTITION ({q(partition_name)})"
        if canon_name == "postgres":
            return f"{q(partition_name)}"
        if canon_name in ("mssql", "sqlserver"):
            return f"$PARTITION({q(partition_name)})"
        if canon_name == "clickhouse":
            return f"{table} PREWHERE _partition_id = '{partition_name}'"
        return f"{table} PARTITION ({partition_name})"
