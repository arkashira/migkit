"""Kafka fabric integrations: offset alignment, transactional outbox offset map,
schema registry wire validation, quota enforcement & config reconciliation,
and cluster linking & MirrorMaker topic shadowing.
"""
from __future__ import annotations

import base64
import collections
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import re
import struct
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple


# ============================================================================
# Item 276 (LT.C2): Gap-Free Partition Offset Alignment
# ============================================================================

class OffsetAlignmentError(Exception):
    """Raised when partition offset alignment cannot be achieved or a gap is found."""


@dataclass
class PaddingRecord:
    topic: str
    partition: int
    offset: int
    key: bytes
    value: Optional[bytes]
    headers: List[Tuple[str, bytes]]
    is_tombstone: bool = False


@dataclass
class AlignmentPlan:
    topic: str
    partition: int
    current_offset: int
    desired_offset: int
    padding_needed: int
    is_aligned: bool
    tombstones: bool


class PartitionOffsetAligner:
    """Calculates and executes gap-free partition offset alignment with dummy padding

    and tombstone records so source and target offsets remain identical.
    """

    def __init__(self, pad_limit: int = 100_000, key_prefix: str = "__migkit_pad__"):
        self.pad_limit = pad_limit
        self.key_prefix = key_prefix

    def plan_alignment(
        self,
        topic: str,
        partition: int,
        current_target_offset: int,
        desired_start_offset: int,
        use_tombstones: bool = True,
    ) -> AlignmentPlan:
        if current_target_offset > desired_start_offset:
            raise OffsetAlignmentError(
                f"Target partition {topic}:{partition} is at offset {current_target_offset}, "
                f"which is already ahead of desired start offset {desired_start_offset}."
            )

        gap = desired_start_offset - current_target_offset
        if gap > self.pad_limit:
            raise OffsetAlignmentError(
                f"Alignment gap of {gap} records for {topic}:{partition} "
                f"exceeds pad limit of {self.pad_limit}."
            )

        return AlignmentPlan(
            topic=topic,
            partition=partition,
            current_offset=current_target_offset,
            desired_offset=desired_start_offset,
            padding_needed=gap,
            is_aligned=(gap == 0),
            tombstones=use_tombstones,
        )

    def generate_padding_records(
        self,
        plan: AlignmentPlan,
    ) -> List[PaddingRecord]:
        records: List[PaddingRecord] = []
        for i in range(plan.padding_needed):
            offset = plan.current_offset + i
            key = f"{self.key_prefix}{plan.partition}_{offset}".encode("utf-8")
            val = None if plan.tombstones else b""
            headers = [
                ("migkit-pad", b"true"),
                ("migkit-offset", str(offset).encode("utf-8")),
            ]
            records.append(
                PaddingRecord(
                    topic=plan.topic,
                    partition=plan.partition,
                    offset=offset,
                    key=key,
                    value=val,
                    headers=headers,
                    is_tombstone=plan.tombstones,
                )
            )
        return records

    def align_partition(
        self,
        producer_fn: Callable[[str, int, bytes, Optional[bytes], List[Tuple[str, bytes]]], int],
        topic: str,
        partition: int,
        current_offset: int,
        desired_offset: int,
        use_tombstones: bool = True,
    ) -> Dict[str, Any]:
        plan = self.plan_alignment(
            topic=topic,
            partition=partition,
            current_target_offset=current_offset,
            desired_start_offset=desired_offset,
            use_tombstones=use_tombstones,
        )
        if plan.is_aligned:
            return {
                "topic": topic,
                "partition": partition,
                "aligned": True,
                "padded_count": 0,
                "start_offset": current_offset,
                "end_offset": current_offset,
            }

        padding = self.generate_padding_records(plan)
        landed_offsets: List[int] = []
        for rec in padding:
            off = producer_fn(rec.topic, rec.partition, rec.key, rec.value, rec.headers)
            landed_offsets.append(off)

        if landed_offsets and landed_offsets[-1] + 1 != desired_offset:
            raise OffsetAlignmentError(
                f"Padding ended at offset {landed_offsets[-1]}, expected {desired_offset - 1}."
            )

        return {
            "topic": topic,
            "partition": partition,
            "aligned": True,
            "padded_count": len(padding),
            "start_offset": current_offset,
            "end_offset": desired_offset,
            "tombstones": use_tombstones,
        }

    @staticmethod
    def verify_gap_free(records: Sequence[Any], expected_start: Optional[int] = None) -> bool:
        if not records:
            return True

        def get_offset(r: Any) -> int:
            if hasattr(r, "offset"):
                return r.offset
            if isinstance(r, dict):
                return r["offset"]
            return int(r[0])

        first_offset = get_offset(records[0])
        if expected_start is not None and first_offset != expected_start:
            raise OffsetAlignmentError(
                f"Expected start offset {expected_start}, found {first_offset}."
            )

        prev_offset = first_offset
        for i in range(1, len(records)):
            curr_offset = get_offset(records[i])
            if curr_offset != prev_offset + 1:
                raise OffsetAlignmentError(
                    f"Offset gap detected: offset {prev_offset} jumped to {curr_offset} "
                    f"at index {i}."
                )
            prev_offset = curr_offset
        return True


# ============================================================================
# Item 277 (LT.C3): Transactional Outbox Pattern for Kafka Offset Map
# ============================================================================

@dataclass
class OutboxOffsetRecord:
    id: int
    topic: str
    partition: int
    source_offset: int
    target_offset: int
    payload: Optional[bytes]
    headers: Optional[Dict[str, str]]
    status: str
    created_at: float


class TransactionalOutbox:
    """Manages transactional outbox persistence for Kafka offset maps inside

    database transactions, guaranteeing atomic mapping commits with data changes.
    """

    TABLE_NAME = "__migkit_offset_outbox"

    @classmethod
    def init_schema(cls, conn: Any) -> None:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {cls.TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                source_offset INTEGER NOT NULL,
                target_offset INTEGER NOT NULL,
                payload BLOB,
                headers TEXT,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at REAL NOT NULL
            );
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{cls.TABLE_NAME}_lookup "
            f"ON {cls.TABLE_NAME} (topic, partition, source_offset);"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{cls.TABLE_NAME}_status "
            f"ON {cls.TABLE_NAME} (status);"
        )
        conn.commit()

    @classmethod
    def write_offset_map(
        cls,
        conn: Any,
        topic: str,
        partition: int,
        source_offset: int,
        target_offset: int,
        payload: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        now: Optional[float] = None,
    ) -> int:
        cur = conn.cursor()
        headers_json = json.dumps(headers) if headers is not None else None
        created = time.time() if now is None else now
        cur.execute(
            f"""
            INSERT INTO {cls.TABLE_NAME}
            (topic, partition, source_offset, target_offset, payload, headers, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
            """,
            (topic, partition, source_offset, target_offset, payload, headers_json, created),
        )
        return cur.lastrowid

    @classmethod
    def transactional_write(
        cls,
        conn: Any,
        business_fn: Callable[[Any], Any],
        topic: str,
        partition: int,
        source_offset: int,
        target_offset: int,
        payload: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[Any, int]:
        try:
            res = business_fn(conn)
            outbox_id = cls.write_offset_map(
                conn=conn,
                topic=topic,
                partition=partition,
                source_offset=source_offset,
                target_offset=target_offset,
                payload=payload,
                headers=headers,
            )
            conn.commit()
            return res, outbox_id
        except Exception:
            conn.rollback()
            raise

    @classmethod
    def fetch_pending(cls, conn: Any, limit: int = 100) -> List[OutboxOffsetRecord]:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT id, topic, partition, source_offset, target_offset, payload, headers, status, created_at
            FROM {cls.TABLE_NAME}
            WHERE status = 'PENDING'
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        out = []
        for r in rows:
            h = json.loads(r[6]) if r[6] else None
            out.append(
                OutboxOffsetRecord(
                    id=r[0],
                    topic=r[1],
                    partition=r[2],
                    source_offset=r[3],
                    target_offset=r[4],
                    payload=r[5],
                    headers=h,
                    status=r[7],
                    created_at=r[8],
                )
            )
        return out

    @classmethod
    def mark_dispatched(cls, conn: Any, record_ids: Sequence[int]) -> int:
        if not record_ids:
            return 0
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in record_ids)
        cur.execute(
            f"UPDATE {cls.TABLE_NAME} SET status = 'DISPATCHED' WHERE id IN ({placeholders})",
            tuple(record_ids),
        )
        conn.commit()
        return cur.rowcount

    @classmethod
    def read_offset_map(cls, conn: Any, topic: str, partition: int) -> Dict[int, int]:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT source_offset, target_offset
            FROM {cls.TABLE_NAME}
            WHERE topic = ? AND partition = ?
            ORDER BY source_offset ASC
            """,
            (topic, partition),
        )
        result: Dict[int, int] = {}
        for src, tgt in cur.fetchall():
            if src in result and result[src] != tgt:
                raise ValueError(
                    f"Conflicting offset map for {topic}:{partition} at source {src}: "
                    f"previously {result[src]}, now {tgt}"
                )
            result[src] = tgt
        return result

    @classmethod
    def get_last_mapped_position(
        cls, conn: Any, topic: str, partition: int
    ) -> Optional[Tuple[int, int]]:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT source_offset, target_offset
            FROM {cls.TABLE_NAME}
            WHERE topic = ? AND partition = ?
            ORDER BY source_offset DESC
            LIMIT 1
            """,
            (topic, partition),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else None


# ============================================================================
# Item 314 (R4.formats): Confluent/Apicurio Schema Registry Wire Formats
# ============================================================================

class RegistryFlavor(str, Enum):
    CONFLUENT = "confluent"
    APICURIO = "apicurio"


class SchemaValidationError(Exception):
    """Raised when payload does not conform to the schema or registry wire format."""


class RegistryWireFormat:
    """Encodes and decodes Confluent and Apicurio wire protocol envelopes."""

    CONFLUENT_MAGIC = b"\x00"
    APICURIO_MAGIC = b"\x00"

    @classmethod
    def encode_confluent(
        cls,
        schema_id: int,
        payload_bytes: bytes,
        is_protobuf: bool = False,
        message_indexes: Sequence[int] = (0,),
    ) -> bytes:
        header = bytearray(cls.CONFLUENT_MAGIC)
        header.extend(struct.pack(">I", schema_id))
        if is_protobuf:
            if not message_indexes or message_indexes == (0,):
                header.append(0)
            else:
                header.extend(cls._encode_varint(len(message_indexes)))
                for idx in message_indexes:
                    zigzag = (idx << 1) ^ (idx >> 31)
                    header.extend(cls._encode_varint(zigzag))
        header.extend(payload_bytes)
        return bytes(header)

    @classmethod
    def encode_apicurio(
        cls,
        artifact_id: int,
        payload_bytes: bytes,
        as_global_id: bool = False,
    ) -> bytes:
        header = bytearray(cls.APICURIO_MAGIC)
        if as_global_id:
            header.extend(struct.pack(">Q", artifact_id))
        else:
            header.extend(struct.pack(">I", artifact_id))
        header.extend(payload_bytes)
        return bytes(header)

    @classmethod
    def decode_wire(
        cls,
        data: bytes,
        flavor: RegistryFlavor = RegistryFlavor.CONFLUENT,
        is_protobuf: bool = False,
    ) -> Tuple[int, bytes, Dict[str, Any]]:
        if len(data) < 5:
            raise SchemaValidationError(f"Wire message is too short ({len(data)} bytes).")
        if data[0:1] != cls.CONFLUENT_MAGIC:
            raise SchemaValidationError(f"Invalid magic byte {data[0:1]!r}, expected \\x00.")

        meta: Dict[str, Any] = {"flavor": flavor.value}
        if flavor == RegistryFlavor.APICURIO and len(data) >= 9 and data[1:5] == b"\x00\x00\x00\x00":
            schema_id = struct.unpack(">Q", data[1:9])[0]
            body = data[9:]
            meta["id_type"] = "global_id"
            return schema_id, body, meta

        schema_id = struct.unpack(">I", data[1:5])[0]
        meta["id_type"] = "schema_id"
        offset = 5

        if is_protobuf:
            if offset >= len(data):
                raise SchemaValidationError("Truncated Protobuf wire payload.")
            first_byte = data[offset]
            if first_byte == 0:
                meta["message_indexes"] = [0]
                offset += 1
            else:
                count, read_bytes = cls._decode_varint(data[offset:])
                offset += read_bytes
                indexes = []
                for _ in range(count):
                    zigzag, r = cls._decode_varint(data[offset:])
                    offset += r
                    idx = (zigzag >> 1) ^ -(zigzag & 1)
                    indexes.append(idx)
                meta["message_indexes"] = indexes

        body = data[offset:]
        return schema_id, body, meta

    @staticmethod
    def _encode_varint(val: int) -> bytes:
        res = bytearray()
        while val > 0x7F:
            res.append((val & 0x7F) | 0x80)
            val >>= 7
        res.append(val & 0x7F)
        return bytes(res)

    @staticmethod
    def _decode_varint(data: bytes) -> Tuple[int, int]:
        res = 0
        shift = 0
        read = 0
        for b in data:
            read += 1
            res |= (b & 0x7F) << shift
            if not (b & 0x80):
                return res, read
            shift += 7
        raise SchemaValidationError("Premature end of varint stream.")


class SchemaFormatValidator:
    """Validates JSON Schema and Protobuf payloads against schema specifications."""

    _TYPE_MAP = {
        "null": lambda v: v is None,
        "boolean": lambda v: isinstance(v, bool),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "string": lambda v: isinstance(v, str),
        "array": lambda v: isinstance(v, list),
        "object": lambda v: isinstance(v, dict),
    }

    @classmethod
    def validate_json_schema(
        cls,
        schema: Any,
        data: Any,
    ) -> Dict[str, Any]:
        if isinstance(schema, str):
            schema = json.loads(schema)
        if isinstance(data, (bytes, bytearray)):
            try:
                data = json.loads(data.decode("utf-8"))
            except Exception as e:
                raise SchemaValidationError(f"Invalid JSON payload: {e}")

        if not isinstance(schema, dict):
            raise SchemaValidationError("JSON Schema must be an object.")
        if not isinstance(data, dict):
            raise SchemaValidationError(f"Expected JSON object row, got {type(data).__name__}.")

        expected_type = schema.get("type")
        if expected_type:
            types = [expected_type] if isinstance(expected_type, str) else expected_type
            if "object" not in types:
                raise SchemaValidationError(f"Schema type must allow 'object', got {expected_type}.")

        for req in schema.get("required", []):
            if req not in data:
                raise SchemaValidationError(f"Missing required property: {req!r}.")

        props = schema.get("properties", {})
        for key, prop_schema in props.items():
            if key not in data:
                continue
            val = data[key]
            prop_type = prop_schema.get("type")
            if prop_type:
                p_types = [prop_type] if isinstance(prop_type, str) else prop_type
                if not any(cls._TYPE_MAP.get(t, lambda _: False)(val) for t in p_types):
                    raise SchemaValidationError(
                        f"Field {key!r} has invalid type {type(val).__name__}, expected {prop_type}."
                    )

        if schema.get("additionalProperties") is False:
            allowed = set(props.keys())
            extras = set(data.keys()) - allowed
            if extras:
                raise SchemaValidationError(f"Disallowed additional properties present: {extras!r}.")

        return data

    @classmethod
    def parse_proto3_fields(cls, proto_text: str, message_name: str) -> Dict[int, Tuple[str, str, int]]:
        pat = rf"message\s+{re.escape(message_name)}\s*\{{(.*?)\}}"
        m = re.search(pat, proto_text, re.DOTALL)
        if not m:
            raise SchemaValidationError(f"Message {message_name!r} not found in proto schema.")

        body = m.group(1)
        fields: Dict[int, Tuple[str, str, int]] = {}
        for line in body.splitlines():
            line = re.sub(r"//.*$", "", line).strip()
            if not line:
                continue
            field_match = re.match(r"(?:optional\s+)?([A-Za-z0-9_.]+)\s+([A-Za-z0-9_]+)\s*=\s*(\d+)\s*;", line)
            if field_match:
                ftype, fname, ftag = field_match.groups()
                tag = int(ftag)
                wire_type = cls._proto_wire_type(ftype)
                fields[tag] = (fname, ftype, wire_type)
        return fields

    @staticmethod
    def _proto_wire_type(ftype: str) -> int:
        if ftype in ("int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool", "enum"):
            return 0
        if ftype in ("fixed64", "sfixed64", "double"):
            return 1
        if ftype in ("string", "bytes", "message"):
            return 2
        if ftype in ("fixed32", "sfixed32", "float"):
            return 5
        return 2

    @classmethod
    def validate_protobuf_wire(
        cls,
        proto_text: str,
        message_name: str,
        wire_bytes: bytes,
    ) -> Dict[str, Any]:
        fields = cls.parse_proto3_fields(proto_text, message_name)
        offset = 0
        parsed: Dict[str, Any] = {}

        while offset < len(wire_bytes):
            tag_wire, read = RegistryWireFormat._decode_varint(wire_bytes[offset:])
            offset += read
            field_tag = tag_wire >> 3
            wire_type = tag_wire & 0x07

            if field_tag not in fields:
                raise SchemaValidationError(f"Encountered unknown Protobuf field tag: {field_tag}.")

            fname, ftype, expected_wire = fields[field_tag]
            if wire_type != expected_wire:
                raise SchemaValidationError(
                    f"Wire type mismatch for field {fname} (tag {field_tag}): "
                    f"got {wire_type}, expected {expected_wire}."
                )

            if wire_type == 0:
                val, r = RegistryWireFormat._decode_varint(wire_bytes[offset:])
                offset += r
                parsed[fname] = val
            elif wire_type == 1:
                if offset + 8 > len(wire_bytes):
                    raise SchemaValidationError("Truncated fixed64 value.")
                val = struct.unpack("<d" if ftype == "double" else "<Q", wire_bytes[offset : offset + 8])[0]
                offset += 8
                parsed[fname] = val
            elif wire_type == 2:
                length, r = RegistryWireFormat._decode_varint(wire_bytes[offset:])
                offset += r
                if offset + length > len(wire_bytes):
                    raise SchemaValidationError(f"Truncated length-delimited field for {fname}.")
                val_bytes = wire_bytes[offset : offset + length]
                offset += length
                parsed[fname] = val_bytes.decode("utf-8") if ftype == "string" else val_bytes
            elif wire_type == 5:
                if offset + 4 > len(wire_bytes):
                    raise SchemaValidationError("Truncated fixed32 value.")
                val = struct.unpack("<f" if ftype == "float" else "<I", wire_bytes[offset : offset + 4])[0]
                offset += 4
                parsed[fname] = val
            else:
                raise SchemaValidationError(f"Unsupported Protobuf wire type: {wire_type}.")

        return parsed


class SchemaRegistryClient:
    """Multi-flavor Schema Registry client for Confluent and Apicurio APIs."""

    def __init__(self, default_flavor: RegistryFlavor = RegistryFlavor.CONFLUENT):
        self.default_flavor = default_flavor
        self._schemas: Dict[int, Dict[str, Any]] = {}
        self._subjects: Dict[str, List[int]] = {}
        self._next_id = 1

    def register(
        self,
        subject: str,
        schema: Any,
        schema_type: str = "JSON",
        flavor: Optional[RegistryFlavor] = None,
    ) -> int:
        sid = self._next_id
        self._next_id += 1
        entry = {
            "id": sid,
            "subject": subject,
            "schema": schema if isinstance(schema, str) else json.dumps(schema),
            "schemaType": schema_type.upper(),
            "flavor": (flavor or self.default_flavor).value,
        }
        self._schemas[sid] = entry
        self._subjects.setdefault(subject, []).append(sid)
        return sid

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        if schema_id not in self._schemas:
            raise KeyError(f"Schema ID {schema_id} not found in registry.")
        return self._schemas[schema_id]

    def serialize(
        self,
        subject: str,
        schema_id: int,
        data: Any,
        flavor: Optional[RegistryFlavor] = None,
        is_protobuf: bool = False,
        message_name: Optional[str] = None,
    ) -> bytes:
        entry = self.get_schema(schema_id)
        flv = flavor or self.default_flavor
        stype = entry["schemaType"]

        if stype == "JSON":
            val = SchemaFormatValidator.validate_json_schema(entry["schema"], data)
            body = json.dumps(val).encode("utf-8")
            if flv == RegistryFlavor.CONFLUENT:
                return RegistryWireFormat.encode_confluent(schema_id, body, is_protobuf=False)
            return RegistryWireFormat.encode_apicurio(schema_id, body)

        if stype == "PROTOBUF":
            if not isinstance(data, (bytes, bytearray)):
                raise SchemaValidationError("Protobuf payload must be encoded bytes for wire transmission.")
            if message_name:
                SchemaFormatValidator.validate_protobuf_wire(entry["schema"], message_name, bytes(data))
            if flv == RegistryFlavor.CONFLUENT:
                return RegistryWireFormat.encode_confluent(schema_id, bytes(data), is_protobuf=True)
            return RegistryWireFormat.encode_apicurio(schema_id, bytes(data))

        raise SchemaValidationError(f"Unsupported schema type: {stype}")

    def deserialize_and_validate(
        self,
        raw_bytes: bytes,
        flavor: Optional[RegistryFlavor] = None,
        is_protobuf: bool = False,
        message_name: Optional[str] = None,
    ) -> Tuple[int, Any, Dict[str, Any]]:
        flv = flavor or self.default_flavor
        schema_id, body, meta = RegistryWireFormat.decode_wire(raw_bytes, flavor=flv, is_protobuf=is_protobuf)
        entry = self.get_schema(schema_id)
        stype = entry["schemaType"]

        if stype == "JSON":
            parsed = SchemaFormatValidator.validate_json_schema(entry["schema"], body)
            return schema_id, parsed, meta

        if stype == "PROTOBUF":
            if message_name:
                parsed = SchemaFormatValidator.validate_protobuf_wire(entry["schema"], message_name, body)
                return schema_id, parsed, meta
            return schema_id, body, meta

        raise SchemaValidationError(f"Unsupported schema type: {stype}")


# ============================================================================
# Item 355 (R13.kafka): Quota Enforcement & Config Reconciliation
# ============================================================================

class QuotaExceededError(Exception):
    """Raised when topic or client throughput exceeds configured quota thresholds."""


@dataclass
class TopicQuota:
    producer_byte_rate: Optional[float] = None
    consumer_byte_rate: Optional[float] = None
    request_percentage: Optional[float] = None


class TopicQuotaEnforcer:
    """Enforces producer and consumer quotas per topic and client with sliding windows."""

    def __init__(self, window_size_seconds: float = 1.0):
        self.window_size = window_size_seconds
        self.quotas: Dict[str, TopicQuota] = {}
        self.produce_history: Dict[str, List[Tuple[float, int]]] = collections.defaultdict(list)
        self.consume_history: Dict[str, List[Tuple[float, int]]] = collections.defaultdict(list)

    def set_quota(self, key: str, quota: TopicQuota) -> None:
        self.quotas[key] = quota

    def _prune(self, records: List[Tuple[float, int]], now: float) -> None:
        cutoff = now - self.window_size
        while records and records[0][0] < cutoff:
            records.pop(0)

    def _rate(self, records: List[Tuple[float, int]], now: float) -> float:
        self._prune(records, now)
        total = sum(b for _, b in records)
        return total / self.window_size

    def check_and_record_produce(
        self,
        topic: str,
        byte_count: int,
        client_id: Optional[str] = None,
        now: Optional[float] = None,
        raise_on_exceed: bool = True,
    ) -> float:
        current_time = time.time() if now is None else now
        quota = self.quotas.get(topic) or (self.quotas.get(client_id) if client_id else None)
        if quota and quota.producer_byte_rate is not None:
            curr_rate = self._rate(self.produce_history[topic], current_time)
            proj_rate = curr_rate + (byte_count / self.window_size)
            if proj_rate > quota.producer_byte_rate:
                excess = proj_rate - quota.producer_byte_rate
                throttle_ms = (excess / quota.producer_byte_rate) * 1000.0
                if raise_on_exceed:
                    raise QuotaExceededError(
                        f"Produce quota of {quota.producer_byte_rate} B/s exceeded for {topic} "
                        f"(projected {proj_rate:.1f} B/s). Throttle {throttle_ms:.1f}ms."
                    )
                return throttle_ms

        self.produce_history[topic].append((current_time, byte_count))
        return 0.0

    def check_and_record_consume(
        self,
        topic: str,
        byte_count: int,
        client_id: Optional[str] = None,
        now: Optional[float] = None,
        raise_on_exceed: bool = True,
    ) -> float:
        current_time = time.time() if now is None else now
        quota = self.quotas.get(topic) or (self.quotas.get(client_id) if client_id else None)
        if quota and quota.consumer_byte_rate is not None:
            curr_rate = self._rate(self.consume_history[topic], current_time)
            proj_rate = curr_rate + (byte_count / self.window_size)
            if proj_rate > quota.consumer_byte_rate:
                excess = proj_rate - quota.consumer_byte_rate
                throttle_ms = (excess / quota.consumer_byte_rate) * 1000.0
                if raise_on_exceed:
                    raise QuotaExceededError(
                        f"Consume quota of {quota.consumer_byte_rate} B/s exceeded for {topic} "
                        f"(projected {proj_rate:.1f} B/s). Throttle {throttle_ms:.1f}ms."
                    )
                return throttle_ms

        self.consume_history[topic].append((current_time, byte_count))
        return 0.0


@dataclass
class ConfigReconciliationPlan:
    topic: str
    to_add: Dict[str, str] = field(default_factory=dict)
    to_update: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    to_delete: List[str] = field(default_factory=list)
    read_only_violations: Dict[str, Tuple[str, str]] = field(default_factory=dict)

    @property
    def is_noop(self) -> bool:
        return not (self.to_add or self.to_update or self.to_delete)


class TopicConfigReconciler:
    """Reconciles topic configuration state against desired configuration,

    handling read-only restrictions and dynamic configuration alterations.
    """

    READ_ONLY_CONFIGS: Set[str] = {
        "partitions",
        "replication_factor",
    }

    @classmethod
    def diff(
        cls,
        topic: str,
        current_configs: Mapping[str, Any],
        desired_configs: Mapping[str, Any],
        read_only_keys: Optional[Set[str]] = None,
    ) -> ConfigReconciliationPlan:
        ro = cls.READ_ONLY_CONFIGS if read_only_keys is None else read_only_keys
        plan = ConfigReconciliationPlan(topic=topic)

        current = {k: str(v) for k, v in current_configs.items()}
        desired = {k: str(v) for k, v in desired_configs.items()}

        for k, des_val in desired.items():
            if k not in current:
                if k in ro:
                    plan.read_only_violations[k] = ("<absent>", des_val)
                else:
                    plan.to_add[k] = des_val
            elif current[k] != des_val:
                if k in ro:
                    plan.read_only_violations[k] = (current[k], des_val)
                else:
                    plan.to_update[k] = (current[k], des_val)

        return plan

    @classmethod
    def reconcile(
        cls,
        topic: str,
        current_configs: Dict[str, Any],
        desired_configs: Dict[str, Any],
        alter_callback: Optional[Callable[[str, Dict[str, str]], None]] = None,
        read_only_keys: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        plan = cls.diff(topic, current_configs, desired_configs, read_only_keys)
        if plan.read_only_violations:
            raise ValueError(
                f"Cannot reconcile read-only configurations for {topic}: {plan.read_only_violations}"
            )

        changes: Dict[str, str] = {}
        for k, v in plan.to_add.items():
            changes[k] = v
        for k, (_, new_v) in plan.to_update.items():
            changes[k] = new_v

        if changes and alter_callback:
            alter_callback(topic, changes)

        for k, v in changes.items():
            current_configs[k] = v

        return {
            "topic": topic,
            "applied_count": len(changes),
            "changes": changes,
            "is_noop": plan.is_noop,
        }


# ============================================================================
# Item 181 (S3.link): Cluster Linking & MirrorMaker Topic Shadowing
# ============================================================================

class LinkState(str, Enum):
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class ShadowTopicStatus(str, Enum):
    SYNCING = "SYNCING"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    PROMOTED = "PROMOTED"


@dataclass
class ShadowTopic:
    source_topic: str
    shadow_topic: str
    link_name: str
    partitions: int
    replication_factor: int
    configs: Dict[str, str]
    status: ShadowTopicStatus = ShadowTopicStatus.SYNCING
    offsets: Dict[int, int] = field(default_factory=dict)
    lag: Dict[int, int] = field(default_factory=dict)


@dataclass
class ClusterLink:
    name: str
    source_cluster: str
    target_cluster: str
    state: LinkState = LinkState.CREATING
    shadow_topics: Dict[str, ShadowTopic] = field(default_factory=dict)
    consumer_offsets: Dict[str, Dict[str, Dict[int, int]]] = field(default_factory=dict)


class ClusterLinkController:
    """Controls Kafka Cluster Linking and MirrorMaker topic shadowing,

    managing mirror topic lifecycles, offset translation, and promotion failovers.
    """

    def __init__(self):
        self.links: Dict[str, ClusterLink] = {}

    def create_link(
        self,
        name: str,
        source_cluster: str,
        target_cluster: str,
    ) -> ClusterLink:
        if name in self.links:
            raise ValueError(f"Link {name!r} already exists.")
        link = ClusterLink(
            name=name,
            source_cluster=source_cluster,
            target_cluster=target_cluster,
            state=LinkState.ACTIVE,
        )
        self.links[name] = link
        return link

    def create_shadow_topic(
        self,
        link_name: str,
        source_topic: str,
        shadow_topic_name: Optional[str] = None,
        partitions: int = 1,
        replication_factor: int = 1,
        configs: Optional[Dict[str, str]] = None,
    ) -> ShadowTopic:
        link = self._get_link(link_name)
        target_name = shadow_topic_name or f"shadow.{source_topic}"
        if target_name in link.shadow_topics:
            raise ValueError(f"Shadow topic {target_name!r} already defined under link {link_name}.")

        shadow = ShadowTopic(
            source_topic=source_topic,
            shadow_topic=target_name,
            link_name=link_name,
            partitions=partitions,
            replication_factor=replication_factor,
            configs=configs or {},
            status=ShadowTopicStatus.ACTIVE if link.state == LinkState.ACTIVE else ShadowTopicStatus.PAUSED,
        )
        link.shadow_topics[target_name] = shadow
        return shadow

    def pause_link(self, link_name: str) -> None:
        link = self._get_link(link_name)
        link.state = LinkState.PAUSED
        for shadow in link.shadow_topics.values():
            if shadow.status != ShadowTopicStatus.PROMOTED:
                shadow.status = ShadowTopicStatus.PAUSED

    def resume_link(self, link_name: str) -> None:
        link = self._get_link(link_name)
        link.state = LinkState.ACTIVE
        for shadow in link.shadow_topics.values():
            if shadow.status != ShadowTopicStatus.PROMOTED:
                shadow.status = ShadowTopicStatus.ACTIVE

    def sync_record(
        self,
        link_name: str,
        shadow_topic_name: str,
        partition: int,
        source_offset: int,
        target_offset: int,
    ) -> None:
        link = self._get_link(link_name)
        if link.state != LinkState.ACTIVE:
            raise RuntimeError(f"Cannot sync records while link {link_name} is in state {link.state}.")

        shadow = link.shadow_topics.get(shadow_topic_name)
        if not shadow:
            raise KeyError(f"Shadow topic {shadow_topic_name} not registered in link {link_name}.")
        if shadow.status == ShadowTopicStatus.PROMOTED:
            raise RuntimeError(f"Shadow topic {shadow_topic_name} is already PROMOTED; writes are locked from mirror.")

        shadow.offsets[partition] = target_offset
        shadow.lag[partition] = max(0, source_offset - target_offset)

    def sync_consumer_offset(
        self,
        link_name: str,
        group_id: str,
        topic: str,
        partition: int,
        source_offset: int,
    ) -> int:
        link = self._get_link(link_name)
        if link.state != LinkState.ACTIVE:
            raise RuntimeError(f"Cannot translate consumer offsets while link {link_name} is {link.state}.")

        groups = link.consumer_offsets.setdefault(group_id, {})
        topic_offsets = groups.setdefault(topic, {})
        topic_offsets[partition] = source_offset
        return source_offset

    def promote_shadow_topic(
        self,
        link_name: str,
        shadow_topic_name: str,
    ) -> Dict[str, Any]:
        link = self._get_link(link_name)
        shadow = link.shadow_topics.get(shadow_topic_name)
        if not shadow:
            raise KeyError(f"Shadow topic {shadow_topic_name} not found in link {link_name}.")

        shadow.status = ShadowTopicStatus.PROMOTED
        return {
            "link": link_name,
            "topic": shadow_topic_name,
            "promoted": True,
            "final_offsets": dict(shadow.offsets),
            "final_lag": dict(shadow.lag),
        }

    def get_link_status(self, link_name: str) -> Dict[str, Any]:
        link = self._get_link(link_name)
        return {
            "name": link.name,
            "state": link.state.value,
            "source_cluster": link.source_cluster,
            "target_cluster": link.target_cluster,
            "shadow_topics_count": len(link.shadow_topics),
            "shadows": {
                name: {
                    "source_topic": s.source_topic,
                    "status": s.status.value,
                    "partitions": s.partitions,
                    "offsets": s.offsets,
                    "lag": s.lag,
                }
                for name, s in link.shadow_topics.items()
            },
        }

    def _get_link(self, name: str) -> ClusterLink:
        if name not in self.links:
            raise KeyError(f"Cluster link {name!r} not found.")
        return self.links[name]
