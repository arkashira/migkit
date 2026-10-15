"""Change capture parity, replica-level decoding, and standalone verifiers.

Supports MySQL statement/mixed invalidations, PostgreSQL replica WAL parsing,
MongoDB standalone dbHash/raw-BSON bucket digests, mongosync change stream
rebuilding with resume tokens, and lossless live Redis replication stream parsing.
"""
from __future__ import annotations

import enum
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import bson
from bson.raw_bson import RawBSONDocument
import sqlglot
from sqlglot import exp


# ---------------------------------------------------------------------------
# Item 272 (LT.R8): MySQL STATEMENT / MIXED Binlog Invalidation
# ---------------------------------------------------------------------------

class InvalidationScope(enum.Enum):
    KEY = "key"
    RANGE = "range"
    TABLE = "table"
    UNSUPPORTED = "unsupported"


@dataclass
class StatementInvalidation:
    schema: str
    table: str
    scope: InvalidationScope
    keys: List[Any] = field(default_factory=list)
    key_column: Optional[str] = None
    range_bounds: Optional[Tuple[Any, Any]] = None
    reason: str = ""
    sql: str = ""


class MySQLStatementInvalidator:
    _LOAD_DATA_RE = re.compile(
        r"LOAD\s+DATA\s+.*INTO\s+TABLE\s+(?:`?([A-Za-z0-9_]+)`?\.)?`?([A-Za-z0-9_]+)`?",
        re.IGNORECASE | re.DOTALL,
    )

    def __init__(self, default_schema: str = "app", primary_keys: Optional[Dict[str, str]] = None):
        self.default_schema = default_schema
        self.primary_keys = primary_keys or {}

    def parse_statement(self, sql: str, schema: Optional[str] = None) -> StatementInvalidation:
        curr_schema = schema or self.default_schema
        clean_sql = sql.strip()

        if not clean_sql:
            return StatementInvalidation(
                schema=curr_schema,
                table="",
                scope=InvalidationScope.UNSUPPORTED,
                reason="empty statement",
                sql=sql,
            )

        load_match = self._LOAD_DATA_RE.search(clean_sql)
        if load_match:
            db_name = load_match.group(1) or curr_schema
            tbl_name = load_match.group(2)
            return StatementInvalidation(
                schema=db_name,
                table=tbl_name,
                scope=InvalidationScope.TABLE,
                reason="load_data_infile",
                sql=sql,
            )

        try:
            parsed = sqlglot.parse_one(clean_sql, read="mysql")
        except Exception as err:
            return StatementInvalidation(
                schema=curr_schema,
                table="",
                scope=InvalidationScope.UNSUPPORTED,
                reason=f"syntax parse failure: {err}",
                sql=sql,
            )

        if isinstance(parsed, exp.Command):
            return StatementInvalidation(
                schema=curr_schema,
                table="",
                scope=InvalidationScope.UNSUPPORTED,
                reason="procedural command or side-effect cannot be isolated to table",
                sql=sql,
            )

        tables = list(parsed.find_all(exp.Table))
        if not tables:
            return StatementInvalidation(
                schema=curr_schema,
                table="",
                scope=InvalidationScope.UNSUPPORTED,
                reason="no target table identified in statement",
                sql=sql,
            )

        unique_tables = {(t.db or curr_schema, t.name) for t in tables}
        if len(unique_tables) > 1:
            target_schema, target_table = next(iter(unique_tables))
            return StatementInvalidation(
                schema=target_schema,
                table=target_table,
                scope=InvalidationScope.TABLE,
                reason="multi-table statement forces full table invalidation",
                sql=sql,
            )

        target_schema, target_table = next(iter(unique_tables))
        pk_col = self.primary_keys.get(target_table, "id")

        where_clause = parsed.find(exp.Where)
        if not where_clause:
            return StatementInvalidation(
                schema=target_schema,
                table=target_table,
                scope=InvalidationScope.TABLE,
                reason="statement without where clause",
                sql=sql,
            )

        condition = where_clause.this

        if isinstance(condition, exp.EQ):
            col_part = condition.this if isinstance(condition.this, exp.Column) else condition.expression
            val_part = condition.expression if isinstance(condition.this, exp.Column) else condition.this
            if isinstance(col_part, exp.Column) and col_part.name.lower() == pk_col.lower() and isinstance(val_part, exp.Literal):
                key_val = val_part.this if val_part.is_string else (int(val_part.this) if val_part.this.isdigit() else float(val_part.this))
                return StatementInvalidation(
                    schema=target_schema,
                    table=target_table,
                    scope=InvalidationScope.KEY,
                    keys=[key_val],
                    key_column=pk_col,
                    sql=sql,
                )

        if isinstance(condition, exp.In):
            col_part = condition.this
            if isinstance(col_part, exp.Column) and col_part.name.lower() == pk_col.lower():
                keys = []
                all_literals = True
                for expr in condition.expressions:
                    if isinstance(expr, exp.Literal):
                        k_val = expr.this if expr.is_string else (int(expr.this) if expr.this.isdigit() else float(expr.this))
                        keys.append(k_val)
                    else:
                        all_literals = False
                        break
                if all_literals and keys:
                    return StatementInvalidation(
                        schema=target_schema,
                        table=target_table,
                        scope=InvalidationScope.KEY,
                        keys=keys,
                        key_column=pk_col,
                        sql=sql,
                    )

        if isinstance(condition, exp.Between):
            col_part = condition.this
            if isinstance(col_part, exp.Column) and col_part.name.lower() == pk_col.lower():
                if isinstance(condition.args.get("low"), exp.Literal) and isinstance(condition.args.get("high"), exp.Literal):
                    low_val = condition.args["low"].this
                    high_val = condition.args["high"].this
                    low = int(low_val) if low_val.isdigit() else low_val
                    high = int(high_val) if high_val.isdigit() else high_val
                    return StatementInvalidation(
                        schema=target_schema,
                        table=target_table,
                        scope=InvalidationScope.RANGE,
                        key_column=pk_col,
                        range_bounds=(low, high),
                        sql=sql,
                    )

        return StatementInvalidation(
            schema=target_schema,
            table=target_table,
            scope=InvalidationScope.TABLE,
            reason="complex or non-key where predicate",
            sql=sql,
        )

    def classify_binlog_event(
        self,
        event_type: str,
        payload: Any,
        format_mode: str = "MIXED",
    ) -> Tuple[str, Optional[StatementInvalidation]]:
        norm_type = event_type.upper()
        if norm_type in ("WRITE_ROWS", "UPDATE_ROWS", "DELETE_ROWS", "PARTIAL_UPDATE_ROWS"):
            return "ROW", None

        if norm_type in ("QUERY", "STATEMENT"):
            sql_text = payload.get("query", "") if isinstance(payload, dict) else str(payload)
            inv = self.parse_statement(sql_text)
            return "STATEMENT", inv

        return "CONTROL", None

    def plan_range_checksum_fallback(
        self,
        schema: str,
        table: str,
        key_column: str,
        min_key: int,
        max_key: int,
        num_ranges: int = 10,
    ) -> List[Tuple[int, int]]:
        if min_key > max_key:
            return []
        span = max_key - min_key + 1
        num_ranges = max(1, min(num_ranges, span))
        step = span // num_ranges
        ranges = []
        curr = min_key
        for i in range(num_ranges):
            next_bound = curr + step - 1 if i < num_ranges - 1 else max_key
            ranges.append((curr, next_bound))
            curr = next_bound + 1
        return ranges

    def generate_range_checksum_sql(
        self,
        schema: str,
        table: str,
        key_column: str,
        range_min: Any,
        range_max: Any,
        columns: List[str],
    ) -> str:
        cols_csv = ", ".join(f"`{c}`" for c in columns)
        return (
            f"SELECT COALESCE(BIT_XOR(CRC32(CONCAT_WS('#', {cols_csv}))), 0) AS checksum, "
            f"COUNT(*) AS row_count "
            f"FROM `{schema}`.`{table}` "
            f"WHERE `{key_column}` >= {range_min} AND `{key_column}` <= {range_max}"
        )

    def diff_range_checksums(
        self,
        source_checksums: Dict[Tuple[Any, Any], Tuple[int, int]],
        target_checksums: Dict[Tuple[Any, Any], Tuple[int, int]],
    ) -> List[Tuple[Any, Any]]:
        mismatched = []
        all_ranges = sorted(set(source_checksums.keys()) | set(target_checksums.keys()))
        for r in all_ranges:
            s_val = source_checksums.get(r)
            t_val = target_checksums.get(r)
            if s_val != t_val:
                mismatched.append(r)
        return mismatched


# ---------------------------------------------------------------------------
# Item 273 (LT.R9): PostgreSQL at wal_level=replica Decoder Contract
# ---------------------------------------------------------------------------

@dataclass
class ReplicaWalChange:
    lsn: str
    xid: int
    table: str
    change_type: str
    ctid: Tuple[int, int]
    resolved_key: Optional[Any] = None
    commit_time: Optional[str] = None
    is_table_rewrite: bool = False


class PostgresReplicaWalDecoder:
    _WALDUMP_HEAP_RE = re.compile(
        r"rmgr:\s+Heap(?:2)?\s+.*len\(rec\):\s+(\d+),\s+tx:\s+(\d+),\s+lsn:\s+([0-9A-Fa-f/]+),.*desc:\s+([A-Z_+]+)\s+(?:.*blkref\s+#0:\s+rel\s+(\d+/\d+/(\d+)).*blk\s+(\d+)(?:.*(?:offset|offsets:)\s*\[?(\d+)\]?)?)?",
        re.IGNORECASE,
    )
    _WALDUMP_TX_RE = re.compile(
        r"rmgr:\s+Transaction\s+.*tx:\s+(\d+),\s+lsn:\s+([0-9A-Fa-f/]+),.*desc:\s+(COMMIT|ABORT)\s*(.*)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        relfilenode_to_table: Optional[Dict[int, str]] = None,
        ctid_key_map: Optional[Dict[Tuple[int, int, int], Any]] = None,
    ):
        self.relfilenode_to_table = dict(relfilenode_to_table or {})
        self.ctid_key_map = dict(ctid_key_map or {})
        self._tx_buffer: Dict[int, List[Dict[str, Any]]] = {}
        self._active_rewrites: Set[str] = set()

    def register_table(self, relfilenode: int, table_name: str) -> None:
        old_filenode = None
        for fn, t in self.relfilenode_to_table.items():
            if t == table_name and fn != relfilenode:
                old_filenode = fn
                break
        if old_filenode is not None:
            self._active_rewrites.add(table_name)
        self.relfilenode_to_table[relfilenode] = table_name

    def record_ctid_mapping(self, relfilenode: int, block: int, offset: int, key: Any) -> None:
        self.ctid_key_map[(relfilenode, block, offset)] = key

    def generate_ctid_scan_sql(self, table: str, block: int) -> str:
        return f"SELECT * FROM {table} WHERE ctid >= '({block}, 0)' AND ctid < '({block + 1}, 0)'"

    def parse_walinspect_records(self, records: List[Dict[str, Any]]) -> List[ReplicaWalChange]:
        emitted: List[ReplicaWalChange] = []
        for rec in records:
            rmgr = str(rec.get("resource_manager", "") or rec.get("rmgr", "")).upper()
            rec_type = str(rec.get("record_type", "")).upper()
            xid = int(rec.get("xid", 0))
            lsn = str(rec.get("lsn", ""))

            if "TRANSACTION" in rmgr or rec_type in ("COMMIT", "ABORT"):
                if rec_type == "COMMIT":
                    commit_ts = rec.get("commit_timestamp") or rec.get("description", "")
                    emitted.extend(self._flush_transaction(xid, str(commit_ts), lsn))
                elif rec_type == "ABORT":
                    self._tx_buffer.pop(xid, None)
                continue

            if rec_type in ("INSERT", "UPDATE", "HOT_UPDATE", "DELETE", "MULTI_INSERT"):
                relfilenode = int(rec.get("relfilenode", 0))
                block = int(rec.get("block", 0) or rec.get("relblocknumber", 0))
                offsets = rec.get("item_offsets") or [int(rec.get("offset", 0))]
                if isinstance(offsets, int):
                    offsets = [offsets]

                for off in offsets:
                    entry = {
                        "lsn": lsn,
                        "xid": xid,
                        "change_type": "UPDATE" if "UPDATE" in rec_type else rec_type,
                        "relfilenode": relfilenode,
                        "block": block,
                        "offset": off,
                    }
                    self._tx_buffer.setdefault(xid, []).append(entry)

        return emitted

    def parse_waldump_text(self, dump_text: str) -> List[ReplicaWalChange]:
        emitted: List[ReplicaWalChange] = []
        for line in dump_text.strip().splitlines():
            line = line.strip()
            if not line:
                continue

            tx_m = self._WALDUMP_TX_RE.search(line)
            if tx_m:
                xid = int(tx_m.group(1))
                lsn = tx_m.group(2)
                action = tx_m.group(3).upper()
                desc = tx_m.group(4)
                if action == "COMMIT":
                    emitted.extend(self._flush_transaction(xid, desc, lsn))
                else:
                    self._tx_buffer.pop(xid, None)
                continue

            heap_m = self._WALDUMP_HEAP_RE.search(line)
            if heap_m:
                xid = int(heap_m.group(2))
                lsn = heap_m.group(3)
                desc = heap_m.group(4).upper()
                relfilenode_str = heap_m.group(6)
                blk_str = heap_m.group(7)
                off_str = heap_m.group(8)

                if not (relfilenode_str and blk_str):
                    continue

                relfilenode = int(relfilenode_str)
                block = int(blk_str)
                offset = int(off_str) if off_str else 0

                change_type = "INSERT" if "INSERT" in desc else ("DELETE" if "DELETE" in desc else "UPDATE")
                entry = {
                    "lsn": lsn,
                    "xid": xid,
                    "change_type": change_type,
                    "relfilenode": relfilenode,
                    "block": block,
                    "offset": offset,
                }
                self._tx_buffer.setdefault(xid, []).append(entry)

        return emitted

    def _flush_transaction(self, xid: int, commit_time: str, commit_lsn: str) -> List[ReplicaWalChange]:
        buffered = self._tx_buffer.pop(xid, [])
        changes = []
        for item in buffered:
            rfn = item["relfilenode"]
            blk = item["block"]
            off = item["offset"]
            table = self.relfilenode_to_table.get(rfn, f"unknown_rel_{rfn}")
            is_rewrite = table in self._active_rewrites

            resolved_key = self.ctid_key_map.get((rfn, blk, off))
            if item["change_type"] == "DELETE":
                self.ctid_key_map.pop((rfn, blk, off), None)

            changes.append(
                ReplicaWalChange(
                    lsn=item["lsn"] or commit_lsn,
                    xid=xid,
                    table=table,
                    change_type=item["change_type"],
                    ctid=(blk, off),
                    resolved_key=resolved_key,
                    commit_time=commit_time.strip(),
                    is_table_rewrite=is_rewrite,
                )
            )
        return changes


# ---------------------------------------------------------------------------
# Item 274 (LT.R10): MongoDB Standalone Verification using dbHash & Raw-BSON
# ---------------------------------------------------------------------------

class MongoStandaloneVerifier:
    @staticmethod
    def compare_db_hashes(source_dbhash: Dict[str, Any], target_dbhash: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
        src_colls = source_dbhash.get("collections", {})
        dst_colls = target_dbhash.get("collections", {})
        all_names = set(src_colls.keys()) | set(dst_colls.keys())

        matching = set()
        differing = set()
        for name in all_names:
            s_hash = src_colls.get(name)
            d_hash = dst_colls.get(name)
            if s_hash and d_hash and s_hash == d_hash:
                matching.add(name)
            else:
                differing.add(name)
        return matching, differing

    @staticmethod
    def partition_id_buckets(ids: List[Any], num_buckets: int = 4) -> List[Tuple[Any, Any]]:
        if not ids:
            return []
        sorted_ids = sorted(ids)
        num_buckets = max(1, min(num_buckets, len(sorted_ids)))
        step = max(1, len(sorted_ids) // num_buckets)
        buckets = []
        start_idx = 0
        for i in range(num_buckets):
            end_idx = min(len(sorted_ids) - 1, start_idx + step - 1) if i < num_buckets - 1 else len(sorted_ids) - 1
            buckets.append((sorted_ids[start_idx], sorted_ids[end_idx]))
            start_idx = end_idx + 1
            if start_idx >= len(sorted_ids):
                break
        return buckets

    @staticmethod
    def compute_raw_bson_bucket_digest(documents: List[Dict[str, Any]]) -> str:
        sorted_docs = sorted(documents, key=lambda d: d.get("_id"))
        hasher = hashlib.sha256()
        for doc in sorted_docs:
            if isinstance(doc, RawBSONDocument):
                hasher.update(doc.raw)
            else:
                hasher.update(bson.encode(doc))
        return hasher.hexdigest()

    @classmethod
    def verify_collection_buckets(
        cls,
        source_buckets: Dict[Tuple[Any, Any], List[Dict[str, Any]]],
        target_buckets: Dict[Tuple[Any, Any], List[Dict[str, Any]]],
    ) -> Tuple[Set[Tuple[Any, Any]], Set[Tuple[Any, Any]]]:
        all_bounds = set(source_buckets.keys()) | set(target_buckets.keys())
        matching = set()
        differing = set()
        for bounds in all_bounds:
            s_docs = source_buckets.get(bounds, [])
            t_docs = target_buckets.get(bounds, [])
            s_digest = cls.compute_raw_bson_bucket_digest(s_docs)
            t_digest = cls.compute_raw_bson_bucket_digest(t_docs)
            if s_digest == t_digest and s_docs and t_docs:
                matching.add(bounds)
            else:
                differing.add(bounds)
        return matching, differing

    @staticmethod
    def detect_deletions_and_updates(
        source_docs: List[Dict[str, Any]],
        target_docs: List[Dict[str, Any]],
    ) -> Tuple[Set[Any], Set[Any], Set[Any]]:
        s_map = {d["_id"]: d for d in source_docs}
        t_map = {d["_id"]: d for d in target_docs}

        to_insert = set()
        to_update = set()
        to_delete = set(t_map.keys()) - set(s_map.keys())

        for doc_id, s_doc in s_map.items():
            if doc_id not in t_map:
                to_insert.add(doc_id)
            else:
                s_bytes = s_doc.raw if isinstance(s_doc, RawBSONDocument) else bson.encode(s_doc)
                t_bytes = t_map[doc_id].raw if isinstance(t_map[doc_id], RawBSONDocument) else bson.encode(t_map[doc_id])
                if s_bytes != t_bytes:
                    to_update.add(doc_id)

        return to_insert, to_update, to_delete

    @staticmethod
    def narrow_candidates_by_updated_at(
        documents: List[Dict[str, Any]],
        since_timestamp: float,
        field_name: str = "updatedAt",
    ) -> List[Dict[str, Any]]:
        candidates = []
        for d in documents:
            val = d.get(field_name)
            if val is not None and val >= since_timestamp:
                candidates.append(d)
        return candidates


# ---------------------------------------------------------------------------
# Item 275 (LT.C1): Mongosync Change Streams Rebuilder from Raw BSON
# ---------------------------------------------------------------------------

@dataclass
class MongoResumeToken:
    data: Any
    cluster_time: Optional[Any] = None
    txn_number: Optional[int] = None
    lsid: Optional[str] = None

    def to_watch_args(self) -> Dict[str, Any]:
        args: Dict[str, Any] = {}
        if self.data is not None:
            args["resumeAfter"] = self.data
        elif self.cluster_time is not None:
            args["startAtOperationTime"] = self.cluster_time
        return args


class MongosyncRebuilder:
    def __init__(self):
        self._tx_buffer: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        self._latest_resume_token: Optional[MongoResumeToken] = None
        self._generation_ranges: Dict[int, Set[Tuple[Any, Any]]] = {}

    @property
    def latest_resume_token(self) -> Optional[MongoResumeToken]:
        return self._latest_resume_token

    def buffer_change_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        token_data = event.get("_id")
        c_time = event.get("clusterTime")
        lsid = event.get("lsid", {}).get("id") if isinstance(event.get("lsid"), dict) else event.get("lsid")
        txn_num = event.get("txnNumber")

        self._latest_resume_token = MongoResumeToken(
            data=token_data,
            cluster_time=c_time,
            txn_number=txn_num,
            lsid=str(lsid) if lsid is not None else None,
        )

        if lsid is not None and txn_num is not None:
            tx_key = (str(lsid), int(txn_num))
            self._tx_buffer.setdefault(tx_key, []).append(event)
            return None

        return event

    def commit_transaction(self, lsid: str, txn_number: int) -> List[Dict[str, Any]]:
        tx_key = (str(lsid), int(txn_number))
        return self._tx_buffer.pop(tx_key, [])

    def abort_transaction(self, lsid: str, txn_number: int) -> None:
        tx_key = (str(lsid), int(txn_number))
        self._tx_buffer.pop(tx_key, None)

    @staticmethod
    def apply_d3_update(target_doc: Dict[str, Any], update_desc: Dict[str, Any]) -> Dict[str, Any]:
        res = dict(target_doc)
        updated_fields = update_desc.get("updatedFields", {})
        removed_fields = update_desc.get("removedFields", [])
        truncated_arrays = update_desc.get("truncatedArrays", {})

        for path, val in updated_fields.items():
            if "." in path:
                parts = path.split(".")
                curr = res
                for part in parts[:-1]:
                    if part not in curr or not isinstance(curr[part], dict):
                        curr[part] = {}
                    curr = curr[part]
                curr[parts[-1]] = val
            else:
                res[path] = val

        for path in removed_fields:
            if "." in path:
                parts = path.split(".")
                curr = res
                for part in parts[:-1]:
                    if part not in curr or not isinstance(curr[part], dict):
                        break
                    curr = curr[part]
                else:
                    curr.pop(parts[-1], None)
            else:
                res.pop(path, None)

        for arr_field, new_len in truncated_arrays.items():
            if arr_field in res and isinstance(res[arr_field], list):
                res[arr_field] = res[arr_field][:new_len]

        return res

    def track_touched_range(
        self,
        doc_id: Any,
        generation: int,
        range_bounds: List[Tuple[Any, Any]],
    ) -> Optional[Tuple[Any, Any]]:
        for low, high in range_bounds:
            if low <= doc_id <= high:
                self._generation_ranges.setdefault(generation, set()).add((low, high))
                return (low, high)
        return None

    def get_generation_dirty_ranges(self, generation: int) -> Set[Tuple[Any, Any]]:
        return self._generation_ranges.get(generation, set())

    @staticmethod
    def audit_write_blocking(users: List[Dict[str, Any]]) -> Dict[str, Any]:
        bypassing_users = []
        privileged_users = []
        safe_users = []

        for u in users:
            username = u.get("user", "")
            roles = u.get("roles", [])
            custom_privileges = u.get("inheritedPrivileges", [])

            has_bypass = False
            has_set_block = False

            for r in roles:
                r_name = r.get("role", "")
                if r_name in ("restore", "root", "__system"):
                    has_bypass = True
                if r_name in ("clusterAdmin", "root"):
                    has_set_block = True

            for priv in custom_privileges:
                actions = priv.get("actions", [])
                if "bypassWriteBlockingMode" in actions:
                    has_bypass = True
                if "setUserWriteBlockMode" in actions:
                    has_set_block = True

            if has_bypass:
                bypassing_users.append(username)
            elif has_set_block:
                privileged_users.append(username)
            else:
                safe_users.append(username)

        return {
            "bypassing_users": bypassing_users,
            "privileged_users": privileged_users,
            "safe_users": safe_users,
            "fence_secure": len(bypassing_users) == 0,
        }

    @staticmethod
    def validate_and_convert_unique_index(
        existing_docs: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> Tuple[bool, List[Any]]:
        seen = set()
        duplicates = []
        for d in existing_docs:
            val = tuple(d.get(k) for k in key_fields)
            if any(v is None for v in val):
                continue
            if val in seen:
                duplicates.append(val)
            seen.add(val)
        return len(duplicates) == 0, duplicates


# ---------------------------------------------------------------------------
# Item 280 (LT.C6): Lossless Live Redis Replication Stream Parser
# ---------------------------------------------------------------------------

class RedisPsyncParser:
    @staticmethod
    def build_psync_request(replid: str = "?", offset: int = -1) -> bytes:
        return f"PSYNC {replid} {offset}\r\n".encode("utf-8")

    @staticmethod
    def parse_handshake_response(stream: bytes) -> Tuple[str, str, int, bytes, int]:
        if stream.startswith(b"+CONTINUE"):
            line_end = stream.find(b"\r\n")
            if line_end == -1:
                return "INCOMPLETE", "", 0, b"", 0
            line = stream[:line_end].decode("utf-8", errors="replace")
            parts = line.split()
            replid = parts[1] if len(parts) > 1 else ""
            return "CONTINUE", replid, 0, b"", line_end + 2

        if stream.startswith(b"+FULLRESYNC"):
            line_end = stream.find(b"\r\n")
            if line_end == -1:
                return "INCOMPLETE", "", 0, b"", 0
            line = stream[:line_end].decode("utf-8", errors="replace")
            parts = line.split()
            replid = parts[1]
            offset = int(parts[2])

            payload_start = line_end + 2
            if payload_start >= len(stream):
                return "INCOMPLETE", replid, offset, b"", 0

            if stream[payload_start:payload_start + 1] == b"$":
                rdb_len_end = stream.find(b"\r\n", payload_start)
                if rdb_len_end == -1:
                    return "INCOMPLETE", replid, offset, b"", 0
                rdb_len = int(stream[payload_start + 1:rdb_len_end])
                rdb_data_start = rdb_len_end + 2
                if len(stream) < rdb_data_start + rdb_len:
                    return "INCOMPLETE", replid, offset, b"", 0
                rdb_payload = stream[rdb_data_start:rdb_data_start + rdb_len]
                consumed = rdb_data_start + rdb_len
                return "FULLRESYNC", replid, offset, rdb_payload, consumed

            return "FULLRESYNC", replid, offset, b"", payload_start

        return "UNKNOWN", "", 0, b"", 0

    @staticmethod
    def parse_replication_stream(
        stream: bytes,
        initial_offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int, bytes]:
        commands = []
        curr_offset = initial_offset
        idx = 0
        total_len = len(stream)

        while idx < total_len:
            if stream[idx:idx + 1] == b"*":
                line_end = stream.find(b"\r\n", idx)
                if line_end == -1:
                    break
                try:
                    num_args = int(stream[idx + 1:line_end])
                except ValueError:
                    idx = line_end + 2
                    continue

                cmd_start_idx = idx
                cur_arg_idx = line_end + 2
                args = []
                malformed = False

                for _ in range(num_args):
                    if cur_arg_idx >= total_len or stream[cur_arg_idx:cur_arg_idx + 1] != b"$":
                        malformed = True
                        break
                    arg_len_end = stream.find(b"\r\n", cur_arg_idx)
                    if arg_len_end == -1:
                        malformed = True
                        break
                    arg_len = int(stream[cur_arg_idx + 1:arg_len_end])
                    data_start = arg_len_end + 2
                    if data_start + arg_len > total_len:
                        malformed = True
                        break
                    arg_val = stream[data_start:data_start + arg_len]
                    args.append(arg_val)
                    cur_arg_idx = data_start + arg_len + 2

                if malformed:
                    break

                consumed_bytes = cur_arg_idx - cmd_start_idx
                curr_offset += consumed_bytes
                idx = cur_arg_idx

                cmd_name = args[0].decode("utf-8", errors="replace").upper() if args else ""
                commands.append({
                    "command": cmd_name,
                    "args": [a.decode("utf-8", errors="replace") for a in args[1:]],
                    "raw_args": args[1:],
                    "offset": curr_offset,
                })
            elif stream[idx:idx + 1] in (b"+", b"-", b":"):
                line_end = stream.find(b"\r\n", idx)
                if line_end == -1:
                    break
                consumed_bytes = line_end + 2 - idx
                curr_offset += consumed_bytes
                idx = line_end + 2
            else:
                idx += 1
                curr_offset += 1

        unparsed = stream[idx:]
        return commands, curr_offset, unparsed

    @staticmethod
    def format_replconf_ack(offset: int) -> bytes:
        off_bytes = str(offset).encode("utf-8")
        return (
            b"*3\r\n"
            b"$8\r\nREPLCONF\r\n"
            b"$3\r\nACK\r\n"
            + f"${len(off_bytes)}\r\n".encode("utf-8")
            + off_bytes
            + b"\r\n"
        )


class RedisIdletimeSweep:
    @staticmethod
    def check_client_no_touch_supported(redis_version: str) -> bool:
        clean = re.sub(r"[^\d.]", "", redis_version.split("-")[0])
        parts = [int(p) for p in clean.split(".") if p.isdigit()]
        if not parts:
            return False
        return parts[0] > 7 or (parts[0] == 7 and len(parts) > 1 and parts[1] >= 2)

    @staticmethod
    def is_lfu_policy(maxmemory_policy: str) -> bool:
        return "lfu" in maxmemory_policy.lower()

    @classmethod
    def execute_lossless_sweep(
        cls,
        scan_keys_idletime: Dict[str, int],
        previous_keys: Set[str],
        window_seconds: float,
        maxmemory_policy: str = "noeviction",
    ) -> Dict[str, Any]:
        if cls.is_lfu_policy(maxmemory_policy):
            return {
                "fallback_to_digest_scan": True,
                "reason": "LFU policy makes OBJECT IDLETIME return frequency counters instead of elapsed seconds",
                "dirty_keys": list(scan_keys_idletime.keys()),
                "deleted_keys": list(previous_keys - set(scan_keys_idletime.keys())),
            }

        dirty = []
        untouched = []
        for key, idletime in scan_keys_idletime.items():
            if idletime <= window_seconds:
                dirty.append(key)
            else:
                untouched.append(key)

        current_keys = set(scan_keys_idletime.keys())
        deleted = list(previous_keys - current_keys)

        return {
            "fallback_to_digest_scan": False,
            "dirty_keys": dirty,
            "untouched_keys": untouched,
            "deleted_keys": deleted,
            "client_no_touch_required": True,
        }
