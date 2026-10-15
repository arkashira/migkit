import base64
import contextlib
import copy
import hashlib
import json
import time
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sequence, Set, Tuple

from .. import changetopic, pieces
from .base import Engine, Result


def _mongo_missing_grant(action: str, exc: Optional[Exception] = None, fallback: Any = None) -> Any:
    handler = MongoGrantFallbackHandler()
    return handler.handle_missing_grant(action, exc=exc, fallback=fallback)


def _hetero_missing_grant(action: str, exc: Optional[Exception] = None, fallback: Any = None) -> Any:
    handler = PairGrantFallbackHandler()
    return handler.handle_missing_grant(action, exc=exc, fallback=fallback)


class MongoGrantFallbackHandler:
    GRANT_ERROR_CODES = {13, 18, 31, 33, 251}

    @classmethod
    def is_grant_error(cls, exc: Exception) -> bool:
        if exc is None:
            return False
        code = getattr(exc, "code", None)
        if code in cls.GRANT_ERROR_CODES:
            return True
        msg = str(exc).lower()
        return any(
            t in msg
            for t in (
                "not authorized",
                "unauthorized",
                "requires authentication",
                "insufficient privilege",
                "command not found",
                "no such command",
            )
        )

    def handle_missing_grant(self, action: str, exc: Optional[Exception] = None, fallback: Any = None) -> Any:
        if fallback is not None:
            return fallback
        if action == "listDatabases":
            return ["admin", "local"]
        if action == "serverStatus":
            return {"ok": 1.0, "version": "fallback", "process": "mongod", "fallback": True}
        if action == "replSetGetStatus":
            return {"ok": 0.0, "fallback": True, "info": "replSetGetStatus unauthorized"}
        if action == "collStats":
            return {"ok": 1.0, "count": 0, "size": 0, "fallback": True}
        if action == "changeStream":
            return []
        return None

    def safe_list_databases(self, client: Any, configured_dbs: Optional[List[str]] = None) -> List[str]:
        if configured_dbs:
            return configured_dbs
        try:
            return list(client.list_database_names())
        except Exception as e:
            if self.is_grant_error(e):
                opts = getattr(client, "options", None)
                if opts and opts.get("database"):
                    return [opts["database"]]
                return ["main"]
            raise

    def safe_server_status(self, db: Any) -> Dict[str, Any]:
        try:
            return db.command("serverStatus")
        except Exception as e:
            if self.is_grant_error(e):
                return {"ok": 1.0, "version": "unknown", "storageEngine": {"name": "wiredTiger"}, "fallback": True}
            raise

    def safe_collection_stats(self, db: Any, coll_name: str) -> Dict[str, Any]:
        try:
            return db.command("collStats", coll_name)
        except Exception as e:
            if self.is_grant_error(e):
                try:
                    cnt = db[coll_name].count_documents({})
                except Exception:
                    cnt = 0
                return {"ok": 1.0, "count": cnt, "size": cnt * 1024, "fallback": True}
            raise

    def safe_change_stream(self, db_or_coll: Any, resume_token: Optional[Any] = None) -> Any:
        try:
            kwargs = {}
            if resume_token:
                kwargs["resume_after"] = resume_token
            return db_or_coll.watch(**kwargs)
        except Exception as e:
            if self.is_grant_error(e):
                return MongoPollingFallback(db_or_coll, resume_token)
            raise


class MongoPollingFallback:
    def __init__(self, target_collection: Any, resume_token: Optional[Any] = None):
        self.coll = target_collection
        self.last_id = resume_token
        self.is_fallback = True

    def poll_next(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        query = {}
        if self.last_id:
            query["_id"] = {"$gt": self.last_id}
        docs = list(self.coll.find(query).sort("_id", 1).limit(batch_size))
        events = []
        for d in docs:
            self.last_id = d.get("_id")
            events.append({
                "operationType": "insert",
                "documentKey": {"_id": self.last_id},
                "fullDocument": d,
                "_id": self.last_id,
            })
        return events


class PairGrantFallbackHandler:
    GRANT_INDICATORS = (
        "permission denied",
        "must be superuser",
        "access denied",
        "insufficient privilege",
        "session_replication_role",
        "super privilege",
    )

    @classmethod
    def is_pair_grant_error(cls, exc: Exception) -> bool:
        if exc is None:
            return False
        msg = str(exc).lower()
        return any(ind in msg for ind in cls.GRANT_INDICATORS)

    def handle_missing_grant(self, action: str, exc: Optional[Exception] = None, fallback: Any = None) -> Any:
        if fallback is not None:
            return fallback
        if action == "disable_triggers":
            return False
        if action == "information_schema":
            return []
        if action == "snapshot_isolation":
            return "READ COMMITTED"
        return None

    def topological_sort_tables(self, tables: Sequence[str], foreign_keys: Dict[str, Sequence[str]]) -> List[str]:
        deps: Dict[str, Set[str]] = {t: set() for t in tables}
        for tbl, refs in foreign_keys.items():
            if tbl in deps:
                for ref in refs:
                    if ref in deps and ref != tbl:
                        deps[tbl].add(ref)

        result: List[str] = []
        visited: Set[str] = set()
        visiting: Set[str] = set()

        def dfs(node: str):
            if node in visiting:
                return
            if node in visited:
                return
            visiting.add(node)
            for parent in sorted(deps.get(node, [])):
                dfs(parent)
            visiting.remove(node)
            visited.add(node)
            result.append(node)

        for t in sorted(tables):
            if t not in visited:
                dfs(t)
        return result


class PairTriggerFallbackWindow:
    def __init__(self, engine: Any, tables: Sequence[str], foreign_keys: Optional[Dict[str, Sequence[str]]] = None):
        self.engine = engine
        self.tables = list(tables)
        self.foreign_keys = foreign_keys or {}
        self.fallback_active = False
        self.ordered_tables: List[str] = []

    def __enter__(self):
        try:
            if hasattr(self.engine, "_quiet_triggers_enter"):
                self.engine._quiet_triggers_enter()
        except Exception as e:
            if PairGrantFallbackHandler.is_pair_grant_error(e):
                self.fallback_active = True
                handler = PairGrantFallbackHandler()
                self.ordered_tables = handler.topological_sort_tables(self.tables, self.foreign_keys)
            else:
                raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.fallback_active and hasattr(self.engine, "_quiet_triggers_exit"):
            try:
                self.engine._quiet_triggers_exit()
            except Exception:
                pass


TiDBLargeValues = pieces.Sql(
    "TiDB large values: sized by length(), read in pieces via substring(), appended via concat(), hashed via md5()",
    length={
        "blob": "length({c})",
        "longblob": "length({c})",
        "mediumblob": "length({c})",
        "tinyblob": "length({c})",
        "text": "length({c})",
        "longtext": "length({c})",
        "mediumtext": "length({c})",
        "tinytext": "length({c})",
        "varchar": "length({c})",
        "char": "length({c})",
        "binary": "length({c})",
        "varbinary": "length({c})",
        "json": "length({c})",
    },
    types=(
        "blob", "longblob", "mediumblob", "tinyblob", "text", "longtext",
        "mediumtext", "tinytext", "varchar", "char", "binary", "varbinary", "json"
    ),
    piece="substring({c}, {at}, {n})",
    append="{c} = concat({c}, {v})",
    appends=("longtext", "mediumtext", "text", "tinytext", "longblob", "mediumblob", "blob", "tinyblob"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)

CockroachDBLargeValues = pieces.Sql(
    "CockroachDB large values: sized by octet_length/length, read via substr, appended via ||, hashed via md5",
    length={
        "bytes": "octet_length({c})",
        "bytea": "octet_length({c})",
        "string": "length({c})",
        "text": "length({c})",
        "varchar": "length({c})",
        "jsonb": "octet_length({c}::text)",
    },
    types=("bytes", "bytea", "string", "text", "varchar", "jsonb"),
    piece="substr({c}, {at}, {n})",
    append="{c} = {c} || {v}",
    appends=("string", "text", "varchar", "bytes", "bytea"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)

OceanBaseLargeValues = pieces.Sql(
    "OceanBase large values: MySQL-mode LOB handling with length, substring, concat, and md5",
    length={
        "blob": "length({c})",
        "longblob": "length({c})",
        "mediumblob": "length({c})",
        "text": "length({c})",
        "longtext": "length({c})",
        "varchar": "length({c})",
        "varbinary": "length({c})",
    },
    types=("blob", "longblob", "mediumblob", "text", "longtext", "varchar", "varbinary"),
    piece="substring({c}, {at}, {n})",
    append="{c} = concat({c}, {v})",
    appends=("longtext", "text", "longblob", "blob"),
    md5="md5({c})",
    md5_classes=("bytes", "text"),
)


class CassandraLargeValuesAdapter(pieces.Way):
    def __init__(self, chunk_size: int = pieces.PIECE):
        super().__init__("Cassandra large values: sized by driver bytes, chunked in pieces, hashed client-side")
        self.chunk_size = chunk_size

    def size_value(self, val: Any) -> int:
        if val is None:
            return 1
        if isinstance(val, (bytes, bytearray, memoryview)):
            return len(val)
        if isinstance(val, str):
            return len(val.encode("utf-8"))
        return 8

    def size_row(self, row: Sequence[Any]) -> int:
        return pieces.row_bytes(row)

    def chunk_value(self, val: Any, chunk_size: Optional[int] = None) -> List[Any]:
        step = chunk_size or self.chunk_size
        if val is None:
            return [None]
        if isinstance(val, (bytes, bytearray)):
            raw = bytes(val)
            if not raw:
                return [b""]
            return [raw[i:i + step] for i in range(0, len(raw), step)]
        if isinstance(val, str):
            if not val:
                return [""]
            return [val[i:i + step] for i in range(0, len(val), step)]
        return [val]

    def read_piece(self, val: Any, offset: int, length: int) -> Any:
        if val is None:
            return None
        idx = max(0, offset - 1)
        return val[idx:idx + length]

    def append_piece(self, current: Any, piece: Any) -> Any:
        if current is None:
            return piece
        if piece is None:
            return current
        return current + piece

    def hash_value(self, val: Any) -> pieces.Hashed:
        if val is None:
            return pieces.Hashed(0, "")
        if isinstance(val, str):
            b = val.encode("utf-8")
        elif isinstance(val, (bytes, bytearray, memoryview)):
            b = bytes(val)
        else:
            b = str(val).encode("utf-8")
        return pieces.Hashed(len(b), hashlib.md5(b, usedforsecurity=False).hexdigest())

    def by_bytes(self, rows: Iterable[Sequence[Any]], most_bytes: Optional[int] = None) -> Generator[List[Sequence[Any]], None, None]:
        batch_limit = most_bytes or pieces.BATCH_BYTES
        current_batch: List[Sequence[Any]] = []
        current_bytes = 0
        for r in rows:
            rb = self.size_row(r)
            if current_batch and (current_bytes + rb > batch_limit):
                yield current_batch
                current_batch = [r]
                current_bytes = rb
            else:
                current_batch.append(r)
                current_bytes += rb
        if current_batch:
            yield current_batch


CassandraLargeValues = CassandraLargeValuesAdapter()


class UnifiedChangeEvent:
    __slots__ = ("engine", "db", "table", "op", "key", "values", "version", "lane", "mark", "txn")

    def __init__(
        self,
        engine: str,
        db: Optional[str],
        table: str,
        op: str,
        key: Dict[str, Any],
        values: Optional[Dict[str, Any]],
        version: int,
        lane: Optional[str] = None,
        mark: bool = False,
        txn: Optional[Any] = None,
    ):
        self.engine = engine
        self.db = db
        self.table = table
        self.op = op
        self.key = key
        self.values = values
        self.version = version
        self.lane = lane
        self.mark = mark
        self.txn = txn

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine": self.engine,
            "db": self.db,
            "table": self.table,
            "op": self.op,
            "key": self.key,
            "values": self.values,
            "version": self.version,
            "lane": self.lane,
            "mark": self.mark,
            "txn": self.txn,
        }


class UnifiedChangeTopicReader:
    SUPPORTED_ENGINES = ("tidb", "couchbase", "dsql", "cockroachdb")

    def __init__(self, engine: str, envelope_name: Optional[str] = None):
        eng = engine.lower()
        if eng not in self.SUPPORTED_ENGINES:
            raise ValueError(f"unsupported engine: {engine}; expected one of {self.SUPPORTED_ENGINES}")
        self.engine = eng
        if envelope_name:
            self.envelope = changetopic.envelope(envelope_name)
        elif self.engine == "tidb":
            self.envelope = changetopic.TicdcCanalJson()
        elif self.engine == "couchbase":
            self.envelope = changetopic.CouchbaseDcp()
        elif self.engine == "dsql":
            self.envelope = changetopic.DsqlCdc()
        elif self.engine == "cockroachdb":
            self.envelope = changetopic.CrdbChangefeed()
        self.lanes_position: Dict[str, int] = {}
        self.watermark: int = 0
        self.seen_keys: Set[Tuple[str, str, int]] = set()

    def decode_message(self, key: Any, value: Any, topic: Optional[str] = None, lane: str = "default") -> List[UnifiedChangeEvent]:
        decoded = self.envelope.decode(key, value, topic=topic)
        out: List[UnifiedChangeEvent] = []
        for item in decoded:
            if isinstance(item, changetopic.Mark):
                self.watermark = max(self.watermark, item.version)
                self.lanes_position[lane] = max(self.lanes_position.get(lane, 0), item.version)
                out.append(UnifiedChangeEvent(
                    engine=self.engine,
                    db=None,
                    table=topic or "mark",
                    op="mark",
                    key={},
                    values=None,
                    version=item.version,
                    lane=lane,
                    mark=True,
                ))
            elif isinstance(item, changetopic.Row):
                sub_lane = item.sub or lane
                self.lanes_position[sub_lane] = max(self.lanes_position.get(sub_lane, 0), item.version)
                pk_hashable = str(sorted(item.key.items()))
                dedup_key = (item.table, pk_hashable, item.version)
                if dedup_key in self.seen_keys:
                    continue
                self.seen_keys.add(dedup_key)
                out.append(UnifiedChangeEvent(
                    engine=self.engine,
                    db=item.db,
                    table=item.table,
                    op=item.op,
                    key=item.key,
                    values=item.values,
                    version=item.version,
                    lane=sub_lane,
                    mark=False,
                    txn=item.txn,
                ))
        return out

    def get_position(self) -> Dict[str, int]:
        return dict(self.lanes_position)

    def commit_position(self, lane: str, version: int):
        self.lanes_position[lane] = max(self.lanes_position.get(lane, 0), version)

    def calculate_lag(self, current_ts_ms: Optional[int] = None) -> int:
        now_ms = current_ts_ms or int(time.time() * 1000)
        if not self.lanes_position:
            return 0
        min_pos = min(self.lanes_position.values())
        if self.engine == "tidb":
            commit_time_ms = min_pos >> 18
            return max(0, now_ms - commit_time_ms)
        if self.engine == "cockroachdb":
            commit_time_ms = (min_pos // (10 ** 10)) // 1_000_000
            return max(0, now_ms - commit_time_ms)
        if self.engine == "dsql":
            commit_time_ms = min_pos // 1_000_000
            return max(0, now_ms - commit_time_ms)
        return 0


class CosmosChangeFeedAdapter:
    COSMOS_SYSTEM_KEYS = {"_rid", "_self", "_etag", "_attachments", "_ts", "_lsn"}

    def __init__(self, mode: str = "AllVersionsAndDeletes", partition_key_fields: Optional[List[str]] = None):
        self.mode = mode
        self.partition_key_fields = partition_key_fields or []
        self.continuations: Dict[str, str] = {}
        self.checkpoint_history: List[Dict[str, str]] = []

    def get_continuation(self, feed_range: str) -> Optional[str]:
        return self.continuations.get(feed_range)

    def set_continuation(self, feed_range: str, token: str):
        self.continuations[feed_range] = token

    def handle_range_split(self, parent_range: str, child_ranges: List[str]):
        token = self.continuations.pop(parent_range, None)
        if token is not None:
            for cr in child_ranges:
                self.continuations[cr] = token

    def strip_system_fields(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in doc.items() if k not in self.COSMOS_SYSTEM_KEYS}

    def process_raw_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        metadata = item.get("metadata")
        if isinstance(metadata, dict) and "operationType" in metadata:
            op_type = str(metadata["operationType"]).lower()
            lsn = int(metadata.get("lsn", 0))
            if op_type == "delete":
                ident = {"id": metadata.get("id")}
                pk = metadata.get("partitionKey")
                if isinstance(pk, dict):
                    ident.update(pk)
                elif pk is not None:
                    pk_field = self.partition_key_fields[0] if self.partition_key_fields else "partitionKey"
                    ident[pk_field] = pk
                return {
                    "op": "delete",
                    "id": metadata.get("id"),
                    "key": ident,
                    "body": None,
                    "lsn": lsn,
                }
            cur = self.strip_system_fields(item.get("current") or {})
            ident = {"id": cur.get("id")}
            for f in self.partition_key_fields:
                if f in cur:
                    ident[f] = cur[f]
            return {
                "op": "replace" if op_type == "replace" else "create",
                "id": cur.get("id"),
                "key": ident,
                "body": cur,
                "lsn": lsn,
            }
        cleaned = self.strip_system_fields(item)
        lsn = int(item.get("_lsn") or 0)
        ident = {"id": cleaned.get("id")}
        for f in self.partition_key_fields:
            if f in cleaned:
                ident[f] = cleaned[f]
        return {
            "op": "upsert",
            "id": cleaned.get("id"),
            "key": ident,
            "body": cleaned,
            "lsn": lsn,
        }

    def process_page(self, feed_range: str, raw_items: List[Dict[str, Any]], continuation_token: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        events = [self.process_raw_item(i) for i in raw_items]
        if continuation_token:
            self.set_continuation(feed_range, continuation_token)
        return events, continuation_token

    def checkpoint(self) -> str:
        snapshot = dict(self.continuations)
        self.checkpoint_history.append(snapshot)
        return json.dumps(snapshot, sort_keys=True)

    def restore_checkpoint(self, checkpoint_json: str):
        self.continuations = json.loads(checkpoint_json)


class SapRuntimeLicenseRestriction(RuntimeError):
    pass


class SapOdpDataExtractor:
    RESTRICTED_SAP_TABLES = frozenset({
        "T000", "DD02L", "DD02T", "DD03L", "DD08L", "USR02", "AGR_USERS", "TADIR"
    })

    def __init__(self, service_url: str, full_use_license: bool = False):
        self.service_url = service_url.rstrip("/")
        self.full_use_license = full_use_license
        self.active_subscriptions: Dict[str, str] = {}

    def check_runtime_license(self, table_or_view: str):
        tbl = table_or_view.upper().split(".")[-1]
        if tbl in self.RESTRICTED_SAP_TABLES and not self.full_use_license:
            raise SapRuntimeLicenseRestriction(
                f"direct query of SAP system table '{tbl}' is forbidden under SAP runtime license (SAP Note 581312). "
                "Use ODP-based extraction via OData or declare a full-use database license."
            )

    def build_initial_extract_request(self, entity_set: str, max_page_size: int = 1000) -> Dict[str, Any]:
        return {
            "url": f"{self.service_url}/{entity_set}",
            "headers": {
                "Prefer": f"odata.track-changes, odata.maxpagesize={max_page_size}",
                "Accept": "application/json",
            },
            "params": {"$format": "json"},
        }

    def parse_page_response(self, response_body: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
        d = response_body.get("d", response_body)
        results = d.get("results", d if isinstance(d, list) else [])
        next_link = d.get("__next") or response_body.get("@odata.nextLink")
        delta_token = (
            d.get("__delta")
            or response_body.get("@odata.deltaLink")
            or d.get("deltaToken")
            or response_body.get("deltaToken")
        )
        return results, next_link, delta_token

    def process_delta_record(self, record: Dict[str, Any], key_fields: Sequence[str]) -> Dict[str, Any]:
        mode = str(record.get("ODQ_CHANGEMODE", "")).upper()
        cntr = int(record.get("ODQ_ENTITYCNTR", 1))
        key = {k: record.get(k) for k in key_fields}
        if mode == "D":
            return {"op": "delete", "key": key, "body": None, "mode": mode}
        if mode == "U" and cntr < 0:
            return {"op": "before_image", "key": key, "body": record, "mode": mode}
        if mode == "U":
            return {"op": "update", "key": key, "body": record, "mode": mode}
        return {"op": "create", "key": key, "body": record, "mode": mode or "C"}

    def register_subscription(self, entity_set: str, subscription_id: str):
        self.active_subscriptions[entity_set] = subscription_id

    def terminate_subscription(self, entity_set: str) -> Dict[str, Any]:
        sub_id = self.active_subscriptions.pop(entity_set, None)
        return {
            "action": f"TerminateDeltasFor{entity_set}",
            "url": f"{self.service_url}/TerminateDeltasFor{entity_set}",
            "subscription_id": sub_id,
            "status": "terminated",
        }

    def verify_extract_proof(
        self,
        extracted_rows: List[Dict[str, Any]],
        expected_count: Optional[int] = None,
        aggregate_field: Optional[str] = None,
        expected_sum: Optional[Decimal] = None,
    ) -> Result:
        count = len(extracted_rows)
        if expected_count is not None and count != expected_count:
            return Result(
                check="sapodp_proof",
                scope="count",
                status="diff",
                detail=f"extracted count {count} != expected count {expected_count}",
            )
        if aggregate_field and expected_sum is not None:
            actual_sum = sum((Decimal(str(r.get(aggregate_field, 0))) for r in extracted_rows), Decimal(0))
            if actual_sum != expected_sum:
                return Result(
                    check="sapodp_proof",
                    scope=f"sum({aggregate_field})",
                    status="diff",
                    detail=f"extracted sum {actual_sum} != expected sum {expected_sum}",
                )
        return Result(
            check="sapodp_proof",
            scope="extract",
            status="ok",
            detail=f"verified {count} rows successfully",
        )
