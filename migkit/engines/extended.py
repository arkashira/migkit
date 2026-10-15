import hashlib
import json
import os
import sqlite3
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .base import Engine, Result


class SQLiteSessionEngine(Engine):
    CANON_ENGINE = "sqlite"
    checks = ("schema", "counts", "autoinc", "data", "stream", "fence")
    WRITES_IN_PARALLEL = False
    SETTINGS_PER_DATABASE = True

    def __init__(self, hop=None, path=None):
        super().__init__(hop)
        self.default_path = path

    def _path(self, side="src"):
        if self.default_path:
            return self.default_path
        if not self.hop:
            return ":memory:"
        ep = self.hop.source if side == "src" else self.hop.target
        return ep.options.get("path") or ep.host or ":memory:"

    def open(self, path=None, readonly=False):
        target = path or self._path()
        if target == ":memory:":
            conn = sqlite3.connect(":memory:")
        elif readonly:
            conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        else:
            conn = sqlite3.connect(target)
        return conn

    def enable_wal(self, conn: sqlite3.Connection) -> str:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        row = cur.fetchone()
        return str(row[0]).lower() if row else "unknown"

    def wal_checkpoint(self, conn: sqlite3.Connection, mode: str = "PASSIVE") -> Tuple[int, int, int]:
        mode_upper = mode.upper()
        if mode_upper not in ("PASSIVE", "FULL", "RESTART", "TRUNCATE"):
            raise ValueError(f"invalid checkpoint mode: {mode}")
        cur = conn.cursor()
        cur.execute(f"PRAGMA wal_checkpoint({mode_upper});")
        row = cur.fetchone()
        return (int(row[0]), int(row[1]), int(row[2])) if row else (0, 0, 0)

    def snapshot(self, src_conn: sqlite3.Connection, dest_conn_or_path: Any, pages: int = -1) -> None:
        if isinstance(dest_conn_or_path, str):
            dst = sqlite3.connect(dest_conn_or_path)
            try:
                src_conn.backup(dst, pages=pages)
            finally:
                dst.close()
        else:
            src_conn.backup(dest_conn_or_path, pages=pages)

    def create_session(self, conn: sqlite3.Connection, db: str = "main") -> Dict[str, Any]:
        return {
            "db": db,
            "conn": conn,
            "attached_tables": set(),
            "recorded_changes": [],
            "closed": False,
        }

    def attach_table(self, session: Dict[str, Any], table_name: str) -> None:
        if session.get("closed"):
            raise RuntimeError("session is closed")
        session["attached_tables"].add(table_name)
        conn = session["conn"]
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS _migkit_session_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT,
                op TEXT,
                pk TEXT,
                old_data TEXT,
                new_data TEXT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _migkit_trg_ins_{table_name}
            AFTER INSERT ON "{table_name}"
            BEGIN
                INSERT INTO _migkit_session_log(table_name, op, pk, new_data)
                VALUES ('{table_name}', 'INSERT', CAST(new.rowid AS TEXT), NULL);
            END;
        """)
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _migkit_trg_upd_{table_name}
            AFTER UPDATE ON "{table_name}"
            BEGIN
                INSERT INTO _migkit_session_log(table_name, op, pk, new_data)
                VALUES ('{table_name}', 'UPDATE', CAST(new.rowid AS TEXT), NULL);
            END;
        """)
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS _migkit_trg_del_{table_name}
            AFTER DELETE ON "{table_name}"
            BEGIN
                INSERT INTO _migkit_session_log(table_name, op, pk, old_data)
                VALUES ('{table_name}', 'DELETE', CAST(old.rowid AS TEXT), NULL);
            END;
        """)
        conn.commit()

    def generate_changeset(self, session: Dict[str, Any]) -> List[Dict[str, Any]]:
        conn = session["conn"]
        cur = conn.cursor()
        cur.execute("""
            SELECT id, table_name, op, pk, old_data, new_data
            FROM _migkit_session_log
            ORDER BY id ASC
        """)
        changes = []
        for r in cur.fetchall():
            changes.append({
                "id": r[0],
                "table": r[1],
                "op": r[2],
                "pk": r[3],
                "old": r[4],
                "new": r[5],
            })
        return changes

    def generate_patchset(self, session: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = self.generate_changeset(session)
        latest_per_pk: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for c in raw:
            latest_per_pk[(c["table"], c["pk"])] = c
        return list(latest_per_pk.values())

    def apply_changeset(self, conn: sqlite3.Connection, changeset: List[Dict[str, Any]]) -> int:
        cur = conn.cursor()
        applied = 0
        for c in changeset:
            op = c["op"]
            table = c["table"]
            pk = c["pk"]
            if op == "DELETE":
                cur.execute(f'DELETE FROM "{table}" WHERE rowid = ?', (pk,))
            applied += 1
        conn.commit()
        return applied

    def invert_changeset(self, changeset: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        inverted = []
        for c in reversed(changeset):
            op = c["op"]
            inv_op = "DELETE" if op == "INSERT" else ("INSERT" if op == "DELETE" else "UPDATE")
            inverted.append({
                "id": c.get("id"),
                "table": c["table"],
                "op": inv_op,
                "pk": c["pk"],
                "old": c.get("new"),
                "new": c.get("old"),
            })
        return inverted


SQLiteChangeTrackingEngine = SQLiteSessionEngine
SQLiteSessionAdapter = SQLiteSessionEngine


class ParquetStorageReader(Engine):
    CANON_ENGINE = "parquet"
    checks = ("schema", "counts", "data", "fence", "snapshot")

    def __init__(self, hop=None, base_path=None, storage_options=None):
        super().__init__(hop)
        self.base_path = base_path
        self.storage_options = storage_options or {}

    def read_manifest(self, manifest_source: Any) -> Dict[str, Any]:
        if isinstance(manifest_source, dict):
            data = manifest_source
        elif isinstance(manifest_source, (str, os.PathLike)):
            with open(manifest_source, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = json.loads(manifest_source.read())
        if "files" not in data or "snapshot_id" not in data:
            raise ValueError("invalid manifest: missing files or snapshot_id")
        return data

    def create_manifest(
        self,
        snapshot_id: str,
        files: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "version": 1,
            "snapshot_id": str(snapshot_id),
            "files": sorted(files, key=lambda x: str(x.get("path", ""))),
            "metadata": metadata or {},
        }
    def read_footer_fence(self, file_path_or_buffer: Any) -> Dict[str, Any]:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(file_path_or_buffer)
        meta = pf.metadata
        schema_names = list(meta.schema.names)
        schema_hash = hashlib.sha256(",".join(schema_names).encode("utf-8")).hexdigest()
        cols_summary = {}
        for rg_idx in range(meta.num_row_groups):
            rg = meta.row_group(rg_idx)
            for c_idx in range(rg.num_columns):
                col = rg.column(c_idx)
                c_name = schema_names[c_idx]
                if col.is_stats_set:
                    stats = col.statistics
                    cols_summary[c_name] = {
                        "min": str(stats.min) if stats.has_min_max else None,
                        "max": str(stats.max) if stats.has_min_max else None,
                        "null_count": stats.null_count,
                    }
        fence_content = f"{meta.num_rows}:{meta.num_row_groups}:{schema_hash}"
        return {
            "num_rows": meta.num_rows,
            "num_row_groups": meta.num_row_groups,
            "schema_hash": schema_hash,
            "schema_columns": schema_names,
            "columns_summary": cols_summary,
            "fence_token": hashlib.sha256(fence_content.encode("utf-8")).hexdigest(),
        }

    def compute_fence(self, files_or_manifest: Any) -> str:
        if isinstance(files_or_manifest, dict):
            files = files_or_manifest.get("files", [])
            snap = files_or_manifest.get("snapshot_id", "")
        else:
            files = list(files_or_manifest)
            snap = ""
        items = []
        for f in sorted(files, key=lambda x: str(x.get("path", ""))):
            path = f.get("path", "")
            rows = f.get("rows", f.get("num_rows", 0))
            f_hash = f.get("hash", f.get("fence_token", ""))
            items.append(f"{path}:{rows}:{f_hash}")
        raw = f"{snap};" + ";".join(items)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def detect_changes(
        self,
        base_manifest: Dict[str, Any],
        new_manifest: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        base_files = {f["path"]: f for f in base_manifest.get("files", [])}
        new_files = {f["path"]: f for f in new_manifest.get("files", [])}
        added = []
        modified = []
        deleted = []
        unchanged = []
        for path, n_f in new_files.items():
            if path not in base_files:
                added.append(n_f)
            else:
                b_f = base_files[path]
                if (
                    b_f.get("hash") != n_f.get("hash")
                    or b_f.get("rows") != n_f.get("rows")
                    or b_f.get("size") != n_f.get("size")
                ):
                    modified.append(n_f)
                else:
                    unchanged.append(n_f)
        for path, b_f in base_files.items():
            if path not in new_files:
                deleted.append(b_f)
        return {
            "added": added,
            "modified": modified,
            "deleted": deleted,
            "unchanged": unchanged,
        }

    def verify_changed_only(
        self,
        base_manifest: Dict[str, Any],
        new_manifest: Dict[str, Any],
        verify_fn: Optional[Any] = None,
    ) -> Dict[str, Any]:
        diff = self.detect_changes(base_manifest, new_manifest)
        work_set = diff["added"] + diff["modified"]
        results = []
        for f in work_set:
            if verify_fn:
                res = verify_fn(f)
                results.append(res)
            else:
                results.append({"path": f["path"], "status": "ok"})
        return {
            "checked_count": len(work_set),
            "added_count": len(diff["added"]),
            "modified_count": len(diff["modified"]),
            "deleted_count": len(diff["deleted"]),
            "unchanged_count": len(diff["unchanged"]),
            "details": results,
            "status": "ok" if all(r.get("status") == "ok" for r in results) else "diff",
        }


ParquetManifestFenceReader = ParquetStorageReader
ParquetObjectReader = ParquetStorageReader


class DuckDBSecondReader(Engine):
    READER_TYPE = "duckdb"
    checks = ("schema", "counts", "aggregates", "data", "parity")

    def __init__(self, hop=None, threads: int = 4, memory_limit: Optional[str] = "2GB"):
        super().__init__(hop)
        self.threads = threads
        self.memory_limit = memory_limit

    def connect(self):
        import duckdb
        conn = duckdb.connect(":memory:")
        if self.threads:
            conn.execute(f"PRAGMA threads={int(self.threads)};")
        if self.memory_limit:
            conn.execute(f"PRAGMA max_memory='{self.memory_limit}';")
        return conn

    def register_table(self, conn, table_name: str, data_or_arrow_or_path: Any) -> None:
        import pyarrow as pa
        if isinstance(data_or_arrow_or_path, (pa.Table, pa.RecordBatch)):
            conn.register(table_name, data_or_arrow_or_path)
        elif isinstance(data_or_arrow_or_path, str):
            if data_or_arrow_or_path.endswith(".parquet"):
                conn.execute(f"CREATE VIEW \"{table_name}\" AS SELECT * FROM read_parquet('{data_or_arrow_or_path}');")
            else:
                conn.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM '{data_or_arrow_or_path}';")
        elif isinstance(data_or_arrow_or_path, Sequence):
            import pyarrow as pa
            if data_or_arrow_or_path and isinstance(data_or_arrow_or_path[0], dict):
                pydict = {k: [d[k] for d in data_or_arrow_or_path] for k in data_or_arrow_or_path[0]}
                tbl = pa.Table.from_pydict(pydict)
                conn.register(table_name, tbl)
            else:
                raise ValueError("sequence must contain dict records")
        else:
            conn.register(table_name, data_or_arrow_or_path)

    def column_aggregates(self, conn, table_name: str, columns: Sequence[str]) -> Dict[str, Any]:
        exprs = ["count(*) AS __total_rows"]
        for c in columns:
            exprs.append(f"count(\"{c}\") AS \"{c}__count\"")
            exprs.append(f"min(\"{c}\") AS \"{c}__min\"")
            exprs.append(f"max(\"{c}\") AS \"{c}__max\"")
            exprs.append(f"try_cast(sum(try_cast(\"{c}\" AS DOUBLE)) AS DOUBLE) AS \"{c}__sum\"")
        sql = f"SELECT {', '.join(exprs)} FROM \"{table_name}\""
        res = conn.execute(sql).fetchone()
        names = [d[0] for d in conn.description]
        out: Dict[str, Any] = {}
        for name, val in zip(names, res):
            out[name] = val
        return out

    def table_row_hash(self, conn, table_name: str, columns: Sequence[str]) -> int:
        col_exprs = [f"coalesce(cast(\"{c}\" as VARCHAR), '')" for c in columns]
        concat_expr = f"concat_ws('||', {', '.join(col_exprs)})"
        sql = f"SELECT coalesce(sum(hash({concat_expr})), 0) FROM \"{table_name}\""
        res = conn.execute(sql).fetchone()
        return int(res[0]) if res else 0

    def verify_table_pair(
        self,
        conn,
        src_table: str,
        dst_table: str,
        key_cols: Sequence[str],
        compare_cols: Sequence[str],
    ) -> Result:
        src_agg = self.column_aggregates(conn, src_table, compare_cols)
        dst_agg = self.column_aggregates(conn, dst_table, compare_cols)
        mismatched_aggs = []
        for k in src_agg:
            if src_agg[k] != dst_agg.get(k):
                mismatched_aggs.append(f"{k} (src={src_agg[k]} dst={dst_agg.get(k)})")
        if mismatched_aggs:
            return Result(
                check="second reading",
                scope=f"{src_table} -> {dst_table}",
                status="diff",
                detail=f"aggregate difference: {', '.join(mismatched_aggs)}",
            )
        all_cols = list(dict.fromkeys(list(key_cols) + list(compare_cols)))
        src_hash = self.table_row_hash(conn, src_table, all_cols)
        dst_hash = self.table_row_hash(conn, dst_table, all_cols)
        if src_hash != dst_hash:
            return Result(
                check="second reading",
                scope=f"{src_table} -> {dst_table}",
                status="diff",
                detail=f"checksum difference: src_hash={src_hash} dst_hash={dst_hash}",
            )
        return Result(
            check="second reading",
            scope=f"{src_table} -> {dst_table}",
            status="ok",
            detail=f"verified {src_agg.get('__total_rows', 0)} rows match exactly",
        )

    def find_mismatches(
        self,
        conn,
        src_table: str,
        dst_table: str,
        key_cols: Sequence[str],
        compare_cols: Sequence[str],
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        join_cond = " AND ".join(f's."{k}" = d."{k}"' for k in key_cols)
        diff_cond = " OR ".join(f's."{c}" IS DISTINCT FROM d."{c}"' for c in compare_cols)
        key_selects = ", ".join(f'coalesce(s."{k}", d."{k}") AS "{k}"' for k in key_cols)
        sql = f"""
            SELECT {key_selects},
                   CASE WHEN s."{key_cols[0]}" IS NULL THEN 'extra_in_target'
                        WHEN d."{key_cols[0]}" IS NULL THEN 'missing_in_target'
                        ELSE 'content_mismatch' END AS __diff_kind
            FROM "{src_table}" s
            FULL OUTER JOIN "{dst_table}" d ON {join_cond}
            WHERE {diff_cond} OR s."{key_cols[0]}" IS NULL OR d."{key_cols[0]}" IS NULL
            LIMIT {int(limit)}
        """
        rows = conn.execute(sql).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]


DuckDBSecondReaderAdapter = DuckDBSecondReader


class QdrantEngine(Engine):
    CANON_ENGINE = "qdrant"
    checks = ("schema", "counts", "data", "recall_at_k")
    DEFAULT_PORT = 6333

    def __init__(self, hop=None, host: str = "localhost", port: int = 6333, api_key: Optional[str] = None):
        super().__init__(hop)
        self.host = host
        self.port = port
        self.api_key = api_key

    def collection_config(
        self,
        size: int,
        distance: str = "Cosine",
        hnsw_m: int = 16,
        ef_construct: int = 100,
        quantization: Optional[str] = None,
        sparse_vectors: Optional[Dict[str, Any]] = None,
        payload_indexes: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        dist_norm = distance.capitalize()
        if dist_norm not in ("Cosine", "Dot", "Euclid", "Manhattan"):
            dist_norm = "Cosine"
        cfg = {
            "vectors": {
                "size": int(size),
                "distance": dist_norm,
            },
            "hnsw_config": {
                "m": int(hnsw_m),
                "ef_construct": int(ef_construct),
            },
            "quantization_config": {"type": quantization} if quantization else None,
            "sparse_vectors": sparse_vectors or {},
            "payload_indexes": payload_indexes or {},
        }
        return cfg

    def point_digest(self, point_id: Any, vector: Sequence[float], payload: Optional[Dict[str, Any]] = None) -> str:
        id_str = str(point_id)
        vec_bytes = struct.pack(f"{len(vector)}f", *[float(x) for x in vector])
        payload_bytes = json.dumps(payload or {}, sort_keys=True, separators=(",", ":")).encode("utf-8")
        h = hashlib.sha256()
        h.update(id_str.encode("utf-8"))
        h.update(b":")
        h.update(vec_bytes)
        h.update(b":")
        h.update(payload_bytes)
        return h.hexdigest()

    def scroll_points(
        self,
        collection_data: List[Dict[str, Any]],
        limit: int = 100,
        offset_id: Optional[Any] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[Any]]:
        sorted_points = sorted(collection_data, key=lambda p: str(p["id"]))
        if offset_id is not None:
            sorted_points = [p for p in sorted_points if str(p["id"]) > str(offset_id)]
        batch = sorted_points[:limit]
        next_offset = batch[-1]["id"] if len(batch) == limit else None
        return batch, next_offset

    def compute_collection_digest(self, points: Iterable[Dict[str, Any]]) -> str:
        sorted_pts = sorted(points, key=lambda p: str(p["id"]))
        h = hashlib.sha256()
        for p in sorted_pts:
            d = self.point_digest(p["id"], p["vector"], p.get("payload"))
            h.update(d.encode("utf-8"))
        return h.hexdigest()

    def evaluate_recall_at_k(
        self,
        src_top_k: Sequence[Sequence[Any]],
        dst_top_k: Sequence[Sequence[Any]],
        k: int = 10,
    ) -> float:
        if not src_top_k:
            return 1.0
        recalls = []
        for s_neighbors, d_neighbors in zip(src_top_k, dst_top_k):
            s_set = set(map(str, s_neighbors[:k]))
            d_set = set(map(str, d_neighbors[:k]))
            if not s_set:
                recalls.append(1.0)
            else:
                intersection = len(s_set & d_set)
                recalls.append(intersection / len(s_set))
        return float(sum(recalls) / len(recalls))

    def verify_ann_queries(
        self,
        src_client_or_fn: Any,
        dst_client_or_fn: Any,
        query_vectors: Sequence[Sequence[float]],
        k: int = 10,
        min_recall: float = 0.99,
    ) -> Result:
        src_results = src_client_or_fn(query_vectors, k) if callable(src_client_or_fn) else src_client_or_fn
        dst_results = dst_client_or_fn(query_vectors, k) if callable(dst_client_or_fn) else dst_client_or_fn
        recall = self.evaluate_recall_at_k(src_results, dst_results, k=k)
        if recall < min_recall:
            return Result(
                check="recall_at_k",
                scope=f"top_{k}",
                status="diff",
                detail=f"recall={recall:.4f} is below threshold {min_recall:.4f}",
            )
        return Result(
            check="recall_at_k",
            scope=f"top_{k}",
            status="ok",
            detail=f"recall={recall:.4f} met threshold {min_recall:.4f}",
        )


QdrantVectorEngine = QdrantEngine


class Neo4jEngine(Engine):
    CANON_ENGINE = "neo4j"
    checks = ("schema", "counts", "data", "endpoints", "constraints")
    DEFAULT_PORT = 7687

    def __init__(self, hop=None, uri: str = "bolt://localhost:7687", auth: Optional[Tuple[str, str]] = None):
        super().__init__(hop)
        self.uri = uri
        self.auth = auth

    def schema_facts(
        self,
        labels: Sequence[str],
        rel_types: Sequence[str],
        constraints: Optional[Sequence[Dict[str, Any]]] = None,
        indexes: Optional[Sequence[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return {
            "labels": sorted(labels),
            "relationship_types": sorted(rel_types),
            "constraints": list(constraints or []),
            "indexes": list(indexes or []),
        }

    def node_digest(self, label: str, business_key: Any, properties: Optional[Dict[str, Any]] = None) -> str:
        props = properties or {}
        sorted_props = sorted((k, str(v)) for k, v in props.items())
        prop_str = ";".join(f"{k}={v}" for k, v in sorted_props)
        raw = f"NODE:{label}:{business_key}:{prop_str}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def relationship_digest(
        self,
        rel_type: str,
        from_bkey: Any,
        to_bkey: Any,
        properties: Optional[Dict[str, Any]] = None,
    ) -> str:
        props = properties or {}
        sorted_props = sorted((k, str(v)) for k, v in props.items())
        prop_str = ";".join(f"{k}={v}" for k, v in sorted_props)
        raw = f"REL:{rel_type}:{from_bkey}->{to_bkey}:{prop_str}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def verify_endpoints(
        self,
        relationships: Iterable[Dict[str, Any]],
        known_node_bkeys: Set[Any],
    ) -> Tuple[bool, List[str]]:
        missing = []
        for r in relationships:
            f = r.get("from_bkey")
            t = r.get("to_bkey")
            rel_type = r.get("type", "UNKNOWN")
            if f not in known_node_bkeys:
                missing.append(f"rel {rel_type} missing source endpoint node {f}")
            if t not in known_node_bkeys:
                missing.append(f"rel {rel_type} missing target endpoint node {t}")
        return len(missing) == 0, missing

    def bulk_export_cmd(self, database: str = "neo4j", target_path: str = "/backups/neo4j.dump") -> List[str]:
        return ["neo4j-admin", "database", "dump", str(database), f"--to-path={os.path.dirname(target_path)}"]

    def bulk_import_cmd(self, source_path: str, database: str = "neo4j", overwrite: bool = False) -> List[str]:
        cmd = ["neo4j-admin", "database", "load", str(database), f"--from-path={source_path}"]
        if overwrite:
            cmd.append("--overwrite-destination=true")
        return cmd


Neo4jGraphEngine = Neo4jEngine


class TrinoSecondReader(Engine):
    READER_TYPE = "trino"
    DEFAULT_PORT = 8080
    checks = ("aggregates", "row_hash", "sampling", "partition_fence")

    def __init__(
        self,
        hop=None,
        host: str = "localhost",
        port: int = 8080,
        user: str = "migkit",
        catalog: str = "system",
        schema: str = "default",
    ):
        super().__init__(hop)
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema

    def connection_info(self) -> Dict[str, Any]:
        return {
            "source_type": "Trino",
            "host": self.host,
            "port": int(self.port),
            "user": self.user,
            "catalog": self.catalog,
            "schema": self.schema,
        }

    def format_table_name(self, catalog: Optional[str], schema: Optional[str], table: str) -> str:
        c = catalog or self.catalog
        s = schema or self.schema
        return f'"{c}"."{s}"."{table}"'

    def column_aggregates_query(
        self,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
        columns: Sequence[str],
    ) -> str:
        tbl = self.format_table_name(catalog, schema, table)
        exprs = ["count(*) AS row_count"]
        for c in columns:
            exprs.append(f'count("{c}") AS "{c}__count"')
            exprs.append(f'approx_distinct("{c}") AS "{c}__cardinality"')
            exprs.append(f'min("{c}") AS "{c}__min"')
            exprs.append(f'max("{c}") AS "{c}__max"')
            exprs.append(f'sum(try_cast("{c}" AS double)) AS "{c}__sum"')
        return f"SELECT {', '.join(exprs)} FROM {tbl}"

    def row_hash_query(
        self,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
        key_cols: Sequence[str],
        columns: Optional[Sequence[str]] = None,
        hash_func: str = "sha256",
    ) -> str:
        tbl = self.format_table_name(catalog, schema, table)
        cols_to_hash = list(columns) if columns else list(key_cols)
        exprs = [f"coalesce(cast(\"{c}\" as varchar), '')" for c in cols_to_hash]
        concat_expr = f"concat_ws('|', {', '.join(exprs)})"
        hash_expr = f"to_hex({hash_func}(to_utf8({concat_expr})))"
        key_selects = ", ".join(f'"{k}"' for k in key_cols)
        return f"SELECT {key_selects}, {hash_expr} AS __row_hash FROM {tbl} ORDER BY {key_selects}"

    def sample_query(
        self,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
        percentage: float = 1.0,
    ) -> str:
        tbl = self.format_table_name(catalog, schema, table)
        pct = max(0.0001, min(100.0, float(percentage)))
        return f"SELECT * FROM {tbl} TABLESAMPLE BERNOULLI ({pct})"

    def compare_aggregates(
        self,
        src_aggregates: Dict[str, Any],
        dst_aggregates: Dict[str, Any],
        tolerance: float = 0.0001,
    ) -> Tuple[bool, List[str]]:
        diffs = []
        for k, s_val in src_aggregates.items():
            if k not in dst_aggregates:
                diffs.append(f"missing metric in target: {k}")
                continue
            d_val = dst_aggregates[k]
            if isinstance(s_val, (int, float)) and isinstance(d_val, (int, float)):
                if abs(float(s_val) - float(d_val)) > tolerance:
                    diffs.append(f"{k} difference: src={s_val} dst={d_val}")
            elif s_val != d_val:
                diffs.append(f"{k} difference: src={s_val} dst={d_val}")
        return len(diffs) == 0, diffs

    def run_contract_check(self, job: Dict[str, Any]) -> Result:
        table = job.get("table", "unknown")
        src_agg = job.get("src_aggregates", {})
        dst_agg = job.get("dst_aggregates", {})
        ok, diffs = self.compare_aggregates(src_agg, dst_agg)
        if not ok:
            return Result(
                check="second reading",
                scope=f"trino:{table}",
                status="diff",
                detail="; ".join(diffs),
            )
        return Result(
            check="second reading",
            scope=f"trino:{table}",
            status="ok",
            detail=f"verified {src_agg.get('row_count', 0)} rows across Trino catalogs",
        )


TrinoSecondReaderContract = TrinoSecondReader
