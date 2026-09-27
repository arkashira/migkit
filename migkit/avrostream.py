"""Changes as Avro messages through a schema registry, in Debezium's
envelope: `before`, `after`, `source`, `op` and `ts_ms`, the key a record
of its own, and a tombstone after each delete so a compacted topic lets
the row go.

A table's schema follows what its changes carry: each field a union of
null and every type it has been seen holding, in the order seen. A column
added, or one seen with a type it had not held, is a new version of the
schema - one the registry's usual rule (BACKWARD) takes, since every field
has a default of null and a union only grows. A number of fixed precision
goes as its exact text, as Debezium's `decimal.handling.mode=string` sends
it: its scale can differ row by row, and a float would round it.
"""
import datetime
import decimal
import io
import json
import re
import uuid

from . import registry as _registry

TIMESTAMP = {"type": "long", "logicalType": "timestamp-micros"}
DATE = {"type": "int", "logicalType": "date"}
TIME = {"type": "long", "logicalType": "time-micros"}


def avro_type(value):
    """The Avro type a value is written as, and the value as written."""
    if isinstance(value, bool):
        return "boolean", value
    if isinstance(value, int):
        return "long", value
    if isinstance(value, float):
        return "double", value
    if isinstance(value, decimal.Decimal):
        return "string", format(value, "f")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "bytes", bytes(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return TIMESTAMP, value
    if isinstance(value, datetime.date):
        return DATE, value
    if isinstance(value, datetime.time):
        return TIME, value
    if isinstance(value, datetime.timedelta):
        return "double", value.total_seconds()
    if isinstance(value, (dict, list)):
        return "string", json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, uuid.UUID):
        return "string", str(value)
    return "string", str(value)


def _name(text):
    """A name Avro takes: letters, digits and _, not starting with a
    digit (Debezium's `avro` adjustment)."""
    out = re.sub(r"[^A-Za-z0-9_]", "_", str(text))
    return "_" + out if not out or out[0].isdigit() else out


class Encoder:
    """Per topic, what each field has been seen holding, and the schemas
    registered for it."""

    def __init__(self, registry):
        self.registry = registry
        self.fields = {}      # (topic, part) -> {field: [types]}
        self.names = {}       # (topic, part) -> {field: avro name}

    def _record(self, topic, part, row):
        seen = self.fields.setdefault((topic, part), {})
        names = self.names.setdefault((topic, part), {})
        out = {}
        for k, v in row.items():
            names.setdefault(k, _name(k))
            if v is None:
                seen.setdefault(k, [])
                out[names[k]] = None
                continue
            t, written = avro_type(v)
            kinds = seen.setdefault(k, [])
            if t not in kinds:
                kinds.append(t)
            out[names[k]] = written
        fields = [{"name": names[k], "type": ["null", *seen[k]],
                   "default": None} for k in seen]
        return fields, out

    def encode(self, topic, db, change, now_ms, tombstones=True):
        """[(key bytes or None, value bytes or None)] for one change: the
        change, and a tombstone after a delete."""
        from fastavro import parse_schema, schemaless_writer
        op, table = change["op"], change["table"]
        key = dict(change.get("key") or {})
        after = {k: v for k, v in (change.get("values") or {}).items()
                 if v is not _absent()}
        ns = f"migkit.{_name(db)}.{_name(table)}"
        value_fields, after_row = self._record(topic, "value",
                                               {**key, **after}
                                               if op != "delete"
                                               else dict(key))
        key_fields, key_row = self._record(topic, "key", key)
        value_rec = {"type": "record", "name": "Value", "namespace": ns,
                     "fields": value_fields}
        before_row = ({f["name"]: key_row.get(f["name"])
                       for f in value_fields} if op != "insert" else None)
        envelope = {
            "type": "record", "name": "Envelope", "namespace": ns,
            "fields": [
                {"name": "before", "type": ["null", value_rec],
                 "default": None},
                {"name": "after", "type": ["null", f"{ns}.Value"],
                 "default": None},
                {"name": "source", "type": {
                    "type": "record", "name": "Source", "namespace": ns,
                    "fields": [{"name": "connector", "type": "string"},
                               {"name": "db", "type": "string"},
                               {"name": "table", "type": "string"},
                               {"name": "ts_ms", "type": "long"}]}},
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": "long"}]}
        key_schema = {"type": "record", "name": "Key", "namespace": ns,
                      "fields": key_fields}
        value_id = self.registry.register(f"{topic}-value", envelope)
        key_id = self.registry.register(f"{topic}-key", key_schema)
        full = {f["name"]: None for f in value_fields}
        body = {"before": ({**full, **before_row} if before_row is not None
                           else None),
                "after": ({**full, **after_row} if op != "delete"
                          else None),
                "source": {"connector": "migkit", "db": str(db),
                           "table": str(table), "ts_ms": int(now_ms)},
                "op": {"insert": "c", "update": "u", "delete": "d"}[op],
                "ts_ms": int(now_ms)}
        vbuf, kbuf = io.BytesIO(), io.BytesIO()
        schemaless_writer(vbuf, parse_schema(envelope), body)
        schemaless_writer(kbuf, parse_schema(key_schema),
                          {f["name"]: key_row.get(f["name"])
                           for f in key_fields})
        k = _registry.framed(key_id, kbuf.getvalue())
        out = [(k, _registry.framed(value_id, vbuf.getvalue()))]
        if op == "delete" and tombstones:
            out.append((k, None))
        return out


def _absent():
    from . import canon
    return canon.ABSENT


def decode(registry, raw):
    """A framed message read back through the registry: the record, or
    None for a tombstone."""
    from fastavro import parse_schema, schemaless_reader
    if raw is None:
        return None
    frame = _registry.frame_of(raw)
    if frame is None:
        raise ValueError("not a message a schema registry framed")
    sid, body = frame
    return schemaless_reader(io.BytesIO(body),
                             parse_schema(registry.schema(sid)))
