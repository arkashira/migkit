"""Messages framed by a schema registry that carry JSON Schema or Protobuf
rather than Avro (backlog R4.formats), read and - for JSON Schema - written.

The frame is the one `registry` defines: a zero byte, the schema's id in
four bytes, then the body.

* JSON Schema: the body is JSON text. Read, it is checked against the
  schema's `type`, `required`, `additionalProperties: false` and each
  property's simple `type`; the other keywords (`$ref`, `oneOf`, `pattern`,
  bounds ...) are not evaluated and the values pass through as the message
  holds them.
* Protobuf: after the id come the message indexes - the path to the message
  type among the file's messages (the n-th message of the file, then the
  m-th message nested in it ...), each a zigzag varint, led by their count;
  a single zero byte stands for the path `[0]`. Then the protobuf-encoded
  message. The `.proto` text is parsed here, for proto3 only and without
  imports; whatever it does not cover (`oneof`, maps, groups, extensions,
  imports - `Any` and the well-known types among them -, proto2, a schema
  with references) is refused with its reason, and so is a message that
  does not fit the schema. Nothing is guessed, and nothing is skipped.

Read as a row, a field a proto3 message leaves out holds its default (zero,
empty text, false, an empty list), a field marked `optional` or of a
message type holds None; an enum holds its name, or its number where the
schema does not name it.

Only JSON Schema is written (`JsonEncoder`, `format: jsonschema` on a Kafka
target). Protobuf is not, and says why (`WHY_NOT_WRITTEN`).
"""
import base64
import datetime
import functools
import json
import re
import struct
import uuid

from . import registry as _registry

WHY_NOT_WRITTEN = (
    "a Protobuf message is written against field numbers that must stay the"
    " same for as long as the topic lives, and migkit keeps none for a"
    " table's columns - numbering them by their order would have the next"
    " column added, dropped or reordered change what every earlier message"
    " means to its readers. Protobuf topics are read and copied, not"
    " written; use format: avro or jsonschema")


class Undecodable(Exception):
    """A message that cannot be read, and why. Never to be skipped: the
    caller stops with the reason."""


# --- reading, by the schema's kind ------------------------------------------

def decode(registry, raw):
    """A framed message read back through the registry: the record (a
    dict), or None for a tombstone. `Undecodable` for a message whose
    schema cannot be had, is of a kind not read here, or does not fit it."""
    if raw is None:
        return None
    frame = _registry.frame_of(raw)
    if frame is None:
        raise Undecodable("not a message a schema registry framed (no zero"
                          " byte and four bytes of schema id lead it)")
    sid, body = frame
    try:
        kind, text, refs = registry.entry(sid)
    except (KeyError, _registry.Missing) as e:
        raise Undecodable(f"schema {sid} is not in the registry: {e}")
    except Exception as e:  # noqa: BLE001 - the registry could not be asked
        raise Undecodable(f"schema {sid} could not be read from the"
                          f" registry: {e}")
    if kind == "AVRO":
        from . import avrostream
        try:
            return avrostream.decode(registry, raw)
        except Exception as e:  # noqa: BLE001 - whatever fastavro raises
            raise Undecodable(f"schema {sid} (Avro): {type(e).__name__}:"
                              f" {e}")
    if refs:
        raise Undecodable(
            f"schema {sid} refers to {len(refs)} other schema"
            f"{'' if len(refs) == 1 else 's'} in the registry"
            f" ({', '.join(str(r.get('name', '?')) for r in refs[:3])}),"
            " which are not resolved here")
    if kind == "JSON":
        return _json_record(sid, text, body)
    if kind == "PROTOBUF":
        return _proto_record(sid, text, body)
    raise Undecodable(f"schema {sid} is of kind {kind}, which is not one of"
                      " AVRO, JSON or PROTOBUF")


# --- JSON Schema ------------------------------------------------------------

_JSON_TYPES = {
    "null": lambda v: v is None,
    "boolean": lambda v: isinstance(v, bool),
    "integer": lambda v: (isinstance(v, int) and not isinstance(v, bool))
    or (isinstance(v, float) and v.is_integer()),
    "number": lambda v: isinstance(v, (int, float))
    and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _types_of(schema):
    """The simple types a (sub)schema names, or None where it names none
    or names one this reading does not know."""
    t = schema.get("type") if isinstance(schema, dict) else None
    names = [t] if isinstance(t, str) else t
    if not isinstance(names, list) or not names \
            or any(n not in _JSON_TYPES for n in names):
        return None
    return names


def _json_record(sid, schema_text, body):
    try:
        schema = json.loads(schema_text)
    except ValueError as e:
        raise Undecodable(f"schema {sid} is not JSON: {e}")
    if not isinstance(schema, dict):
        raise Undecodable(f"schema {sid} is not a JSON Schema object")
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise Undecodable(f"schema {sid} (JSON Schema): the body is not"
                          f" JSON: {e}")
    if not isinstance(value, dict):
        raise Undecodable(f"schema {sid} (JSON Schema): the body is not a"
                          " JSON object, so not a row"
                          f" ({type(value).__name__})")
    top = _types_of(schema)
    if top is not None and "object" not in top:
        raise Undecodable(f"schema {sid} (JSON Schema) is of type"
                          f" {'/'.join(top)}, not object - not a row")
    for need in schema.get("required") or []:
        if need not in value:
            raise Undecodable(f"schema {sid} (JSON Schema): the required"
                              f" property {need!r} is missing")
    props = schema.get("properties") or {}
    for name, sub in props.items():
        if name not in value:
            continue
        names = _types_of(sub)
        if names is not None and not any(_JSON_TYPES[n](value[name])
                                         for n in names):
            raise Undecodable(
                f"schema {sid} (JSON Schema): {name!r} should be"
                f" {'/'.join(names)}, the message holds a"
                f" {type(value[name]).__name__}")
    if schema.get("additionalProperties") is False:
        extra = sorted(set(value) - set(props))
        if extra:
            raise Undecodable(f"schema {sid} (JSON Schema) allows no"
                              " property but its own; the message holds"
                              f" {extra[0]!r}")
    return value


def json_type(value):
    """(JSON Schema type, the value as JSON holds it) for one value."""
    from .streamout import jsonable
    if isinstance(value, bool):
        return "boolean", value
    if isinstance(value, int):
        return "integer", value
    if isinstance(value, float):
        return "number", value
    if isinstance(value, dict):
        return "object", value
    if isinstance(value, list):
        return "array", value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "string", base64.b64encode(bytes(value)).decode()
    if isinstance(value, datetime.timedelta):
        return "number", value.total_seconds()
    if isinstance(value, uuid.UUID):
        return "string", str(value)
    return "string", jsonable(value)


class JsonEncoder:
    """Changes as JSON Schema messages through a registry, in the same
    Debezium envelope the Avro writer uses (`avrostream.Encoder`): `before`,
    `after`, `source`, `op`, `ts_ms`, the key a record of its own, a
    tombstone after each delete. A table's schema follows what its changes
    carry: each property the types it has been seen holding, and null; a
    new column or type is a new version, which the registry's rule for the
    subject takes or refuses (`registry.Refused`).

    Not run against a real registry: whether its JSON Schema compatibility
    rule takes these versions is what this does not prove."""

    def __init__(self, registry):
        self.registry = registry
        self.fields = {}      # (topic, part) -> {property: [types]}

    def _record(self, topic, part, row):
        seen = self.fields.setdefault((topic, part), {})
        out = {}
        for k, v in row.items():
            kinds = seen.setdefault(str(k), [])
            if v is None:
                out[str(k)] = None
                continue
            t, written = json_type(v)
            if t not in kinds:
                kinds.append(t)
            out[str(k)] = written
        props = {k: {"type": ["null", *kinds]} for k, kinds in seen.items()}
        return props, out

    def encode(self, topic, db, change, now_ms, tombstones=True):
        """[(key bytes or None, value bytes or None)] for one change: the
        change, and a tombstone after a delete."""
        from . import canon
        from .streamout import jsonable
        op, table = change["op"], change["table"]
        key = dict(change.get("key") or {})
        after = {k: v for k, v in (change.get("values") or {}).items()
                 if v is not canon.ABSENT}
        props, after_row = self._record(
            topic, "value", {**key, **after} if op != "delete" else key)
        key_props, key_row = self._record(topic, "key", key)
        row_schema = {"type": ["null", "object"], "properties": props}
        envelope = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": f"migkit.{db}.{table}.Envelope", "type": "object",
            "properties": {
                "before": row_schema, "after": row_schema,
                "source": {"type": "object", "properties": {
                    "connector": {"type": "string"},
                    "db": {"type": "string"}, "table": {"type": "string"},
                    "ts_ms": {"type": "integer"}}},
                "op": {"type": "string"}, "ts_ms": {"type": "integer"}}}
        key_schema = {"$schema": "http://json-schema.org/draft-07/schema#",
                      "title": f"migkit.{db}.{table}.Key",
                      "type": "object", "properties": key_props}
        value_id = self.registry.register(f"{topic}-value", envelope, "JSON")
        key_id = self.registry.register(f"{topic}-key", key_schema, "JSON")
        full = {k: None for k in props}
        body = {"before": ({**full, **key_row} if op != "insert" else None),
                "after": ({**full, **after_row} if op != "delete" else None),
                "source": {"connector": "migkit", "db": str(db),
                           "table": str(table), "ts_ms": int(now_ms)},
                "op": {"insert": "c", "update": "u", "delete": "d"}[op],
                "ts_ms": int(now_ms)}
        try:
            vtext = json.dumps(body, sort_keys=True, allow_nan=False,
                               default=jsonable).encode()
            ktext = json.dumps(key_row, sort_keys=True, allow_nan=False,
                               default=jsonable).encode()
        except ValueError as e:
            raise ValueError(f"{topic}: a value JSON cannot hold ({e})")
        k = _registry.framed(key_id, ktext)
        out = [(k, _registry.framed(value_id, vtext))]
        if op == "delete" and tombstones:
            out.append((k, None))
        return out


# --- Protobuf: the schema ---------------------------------------------------

SCALARS = {
    "double": 1, "float": 5, "int32": 0, "int64": 0, "uint32": 0,
    "uint64": 0, "sint32": 0, "sint64": 0, "fixed32": 5, "fixed64": 1,
    "sfixed32": 5, "sfixed64": 1, "bool": 0, "string": 2, "bytes": 2}
_PACKABLE = set(SCALARS) - {"string", "bytes"}
_REFUSED_WORDS = {
    "oneof": "a oneof", "extensions": "extension ranges",
    "extend": "extensions", "group": "a group",
    "required": "a required field (proto2)"}

_TOKEN = re.compile(r"""
    (?P<skip>\s+|//[^\n]*|/\*.*?\*/)
  | (?P<str>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')
  | (?P<id>\.?[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*)
  | (?P<num>-?(?:0[xX][0-9a-fA-F]+|[0-9]+))
  | (?P<p>[{}\[\]()=;<>,:])
""", re.X | re.S)


def _tokens(text):
    out, at = [], 0
    while at < len(text):
        m = _TOKEN.match(text, at)
        if m is None:
            raise Undecodable("the .proto text cannot be read at"
                              f" {text[at:at + 20]!r}")
        at = m.end()
        if m.lastgroup != "skip":
            out.append((m.lastgroup, m.group()))
    return out


class _Message:
    def __init__(self, name, full):
        self.name, self.full = name, full
        self.fields = []           # [_Field] in declaration order
        self.nested = []           # nested messages, in order
        self.by_number = {}


class _Field:
    def __init__(self, name, number, kind, repeated, optional):
        self.name, self.number, self.kind = name, number, kind
        self.repeated, self.optional = repeated, optional
        self.target = None         # the _Message or _Enum, once resolved


class _Enum:
    def __init__(self, name, full):
        self.name, self.full, self.values = name, full, {}


class ProtoFile:
    def __init__(self):
        self.package = ""
        self.messages = []         # top-level messages, in order
        self.symbols = {}          # full name -> _Message | _Enum


class _Parser:
    def __init__(self, text):
        self.t = _tokens(text)
        self.i = 0
        self.file = ProtoFile()

    def peek(self, at=0):
        return self.t[self.i + at][1] if self.i + at < len(self.t) else None

    def take(self, want=None):
        if self.i >= len(self.t):
            raise Undecodable("the .proto text ends in the middle of a"
                              " definition")
        tok = self.t[self.i][1]
        self.i += 1
        if want is not None and tok != want:
            raise Undecodable(f"the .proto text has {tok!r} where {want!r}"
                              " belongs")
        return tok

    def name(self):
        kind, tok = self.t[self.i] if self.i < len(self.t) else (None, None)
        if kind != "id" or "." in tok:
            raise Undecodable(f"the .proto text has {tok!r} where a name"
                              " belongs")
        self.i += 1
        return tok

    def skip_to(self, stop):
        depth = 0
        while True:
            tok = self.take()
            if tok in ("[", "(", "{"):
                depth += 1
            elif tok in ("]", ")", "}"):
                depth -= 1
            if tok == stop and depth <= 0:
                return

    def skip_block(self):
        self.take("{")
        depth = 1
        while depth:
            tok = self.take()
            depth += (tok == "{") - (tok == "}")

    def parse(self):
        saw_syntax = False
        while self.i < len(self.t):
            tok = self.take()
            if tok == "syntax":
                self.take("=")
                said = self.take().strip("\"'")
                self.take(";")
                if said != "proto3":
                    raise Undecodable(
                        f"the schema is {said}, not proto3: field presence"
                        " and defaults differ, and only proto3 is read")
                saw_syntax = True
            elif tok == "package":
                self.file.package = self.take()
                self.take(";")
            elif tok == "import":
                raise Undecodable("the schema imports another .proto file"
                                  " (the well-known types such as Timestamp"
                                  " and Any among them), which is not"
                                  " resolved here")
            elif tok == "option":
                self.skip_to(";")
            elif tok == "message":
                self.file.messages.append(self.message([]))
            elif tok == "enum":
                self.enum([])
            elif tok == "service":
                self.name()
                self.skip_block()
            elif tok == ";":
                continue
            elif tok in _REFUSED_WORDS:
                raise Undecodable(f"the schema uses {_REFUSED_WORDS[tok]},"
                                  " which is not read")
            else:
                raise Undecodable(f"the .proto text has {tok!r} where a"
                                  " definition belongs")
        if not saw_syntax:
            raise Undecodable("the schema names no syntax, so it is proto2,"
                              " which is not read")
        self.resolve()
        return self.file

    def full(self, scope, name):
        parts = ([self.file.package] if self.file.package else []) \
            + scope + [name]
        return ".".join(parts)

    def enum(self, scope):
        name = self.name()
        full = self.full(scope, name)
        enum = _Enum(name, full)
        self.file.symbols[full] = enum
        self.take("{")
        while self.peek() != "}":
            tok = self.take()
            if tok in ("option", "reserved"):
                self.skip_to(";")
            elif tok == ";":
                continue
            else:
                self.take("=")
                try:
                    number = int(self.take(), 0)
                except ValueError:
                    raise Undecodable(f"{full}.{tok} has no number")
                if self.peek() == "[":
                    self.skip_to("]")
                self.take(";")
                enum.values.setdefault(number, tok)
        self.take("}")

    def message(self, scope):
        name = self.name()
        full = self.full(scope, name)
        msg = _Message(name, full)
        self.file.symbols[full] = msg
        inner = scope + [name]
        self.take("{")
        while self.peek() != "}":
            tok = self.peek()
            if tok == "message" and self.peek(2) == "{":
                self.take()
                msg.nested.append(self.message(inner))
            elif tok == "enum" and self.peek(2) == "{":
                self.take()
                self.enum(inner)
            elif tok in ("option", "reserved") and self.peek(1) != "=":
                self.take()
                self.skip_to(";")
            elif tok == ";":
                self.take()
            elif tok == "map" and self.peek(1) == "<":
                raise Undecodable(f"{full} has a map field, which is not"
                                  " read")
            elif tok in _REFUSED_WORDS and self.peek(1) != "=":
                raise Undecodable(f"{full} uses {_REFUSED_WORDS[tok]},"
                                  " which is not read")
            else:
                self.field(msg)
        self.take("}")
        return msg

    def field(self, msg):
        repeated = optional = False
        if self.peek() == "repeated" and self.peek(1) != "=":
            self.take()
            repeated = True
        elif self.peek() == "optional" and self.peek(1) != "=":
            self.take()
            optional = True
        kind = self.take()
        if not re.fullmatch(r"\.?[A-Za-z_][\w.]*", kind):
            raise Undecodable(f"{msg.full} has {kind!r} where a field type"
                              " belongs")
        fname = self.name()
        self.take("=")
        try:
            number = int(self.take(), 0)
        except ValueError:
            raise Undecodable(f"{msg.full}.{fname} has no field number")
        if not 1 <= number <= 536870911 or 19000 <= number <= 19999:
            raise Undecodable(f"{msg.full}.{fname}: {number} is not a field"
                              " number")
        if self.peek() == "[":
            self.skip_to("]")
        if self.peek() == "{":
            raise Undecodable(f"{msg.full}.{fname} is a group, which is not"
                              " read")
        self.take(";")
        if number in msg.by_number:
            raise Undecodable(f"{msg.full} gives field number {number} to"
                              " two fields")
        f = _Field(fname, number, kind, repeated, optional)
        msg.fields.append(f)
        msg.by_number[number] = f

    def lookup(self, scope_full, kind):
        """The message or enum `kind` names, seen from inside `scope_full`:
        the innermost scope that has the name's first part wins, as the
        language resolves it."""
        if kind.startswith("."):
            got = self.file.symbols.get(kind[1:])
            if got is None:
                raise Undecodable(f"the type {kind} is not defined in the"
                                  " schema")
            return got
        first = kind.split(".")[0]
        scope = scope_full.split(".") if scope_full else []
        while True:
            head = ".".join(scope + [first])
            if head in self.file.symbols or any(
                    s.startswith(head + ".") for s in self.file.symbols):
                got = self.file.symbols.get(".".join(scope + kind.split(".")))
                if got is None:
                    raise Undecodable(f"the type {kind} is not defined in"
                                      " the schema")
                return got
            if not scope:
                raise Undecodable(f"the type {kind} is not defined in the"
                                  " schema (imports are not read)")
            scope.pop()

    def resolve(self):
        for sym in list(self.file.symbols.values()):
            if not isinstance(sym, _Message):
                continue
            for f in sym.fields:
                if f.kind not in SCALARS:
                    f.target = self.lookup(sym.full, f.kind)


@functools.lru_cache(maxsize=256)
def parse_proto(text):
    """The `.proto` text as a `ProtoFile`; `Undecodable` for what it does
    not cover."""
    return _Parser(text).parse()


# --- Protobuf: the message --------------------------------------------------

def read_indexes(body):
    """(message index path, where the message begins) of a Protobuf body.
    The count and each index are zigzag varints; a count of zero (the single
    zero byte) is the path [0]."""
    n, at = _varint(body, 0)
    count = _unzig(n)
    if count < 0:
        raise Undecodable("the message indexes lead with a negative count")
    if count == 0:
        return [0], at
    if count > 32:
        raise Undecodable(f"the message indexes name a path {count} deep,"
                          " which no schema is")
    path = []
    for _ in range(count):
        v, at = _varint(body, at)
        v = _unzig(v)
        if v < 0:
            raise Undecodable("a message index is negative")
        path.append(v)
    return path, at


def _varint(buf, at):
    shift = value = 0
    while True:
        if at >= len(buf):
            raise Undecodable("the message ends in the middle of a varint")
        b = buf[at]
        at += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, at
        shift += 7
        if shift >= 70:
            raise Undecodable("a varint is longer than ten bytes")


def _unzig(n):
    return (n >> 1) ^ -(n & 1)


def _signed(n, bits):
    n &= (1 << 64) - 1
    if n >= 1 << 63:
        n -= 1 << 64
    if not -(1 << (bits - 1)) <= n < 1 << (bits - 1):
        raise Undecodable(f"{n} does not fit an int{bits}")
    return n


def _scalar(kind, v, where):
    """One value of scalar `kind` from its raw content (an int for varints
    and fixed widths, bytes for the length-delimited kinds)."""
    if kind in ("int32", "int64"):
        return _signed(v, int(kind[3:]))
    if kind in ("uint32", "uint64"):
        if v >> int(kind[4:]):
            raise Undecodable(f"{where}: {v} does not fit a {kind}")
        return v
    if kind in ("sint32", "sint64"):
        bits = int(kind[4:])
        out = _unzig(v)
        if not -(1 << (bits - 1)) <= out < 1 << (bits - 1):
            raise Undecodable(f"{where}: {out} does not fit a {kind}")
        return out
    if kind == "bool":
        return v != 0
    if kind in ("fixed32", "fixed64"):
        return v
    if kind == "sfixed32":
        return v - (1 << 32) if v >> 31 else v
    if kind == "sfixed64":
        return v - (1 << 64) if v >> 63 else v
    if kind == "float":
        return struct.unpack("<f", v.to_bytes(4, "little"))[0]
    if kind == "double":
        return struct.unpack("<d", v.to_bytes(8, "little"))[0]
    if kind == "string":
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError as e:
            raise Undecodable(f"{where}: text that is not UTF-8 ({e})")
    return bytes(v)


def _enum_value(f, v):
    number = _signed(v, 32)
    return f.target.values.get(number, number)


def _default(f):
    if f.repeated:
        return []
    if f.optional or isinstance(f.target, _Message):
        return None
    if isinstance(f.target, _Enum):
        return f.target.values.get(0, 0)
    return {"string": "", "bytes": b"", "bool": False, "float": 0.0,
            "double": 0.0}.get(f.kind, 0)


def _fixed(buf, at, width):
    if at + width > len(buf):
        raise Undecodable("the message ends in the middle of a fixed-width"
                          " value")
    return int.from_bytes(buf[at:at + width], "little"), at + width


def _decode_into(msg, data, into, depth):
    if depth > 64:
        raise Undecodable("messages nested more than 64 deep")
    at = 0
    while at < len(data):
        tag, at = _varint(data, at)
        number, wire = tag >> 3, tag & 7
        if wire in (3, 4):
            raise Undecodable(f"{msg.full}: a group on the wire, which is"
                              " not read")
        if wire not in (0, 1, 2, 5):
            raise Undecodable(f"{msg.full}: wire type {wire} is not one")
        f = msg.by_number.get(number)
        if f is None:
            raise Undecodable(f"{msg.full}: the message holds field"
                              f" {number}, which the schema does not name"
                              " - not the schema it was written with")
        where = f"{msg.full}.{f.name}"
        if wire == 0:
            raw, at = _varint(data, at)
        elif wire == 1:
            raw, at = _fixed(data, at, 8)
        elif wire == 5:
            raw, at = _fixed(data, at, 4)
        else:
            n, at = _varint(data, at)
            if at + n > len(data):
                raise Undecodable(f"{where}: its length runs past the end"
                                  " of the message")
            raw, at = data[at:at + n], at + n
        kind = f.kind if f.target is None else (
            "enum" if isinstance(f.target, _Enum) else "message")
        want = SCALARS.get(kind, 0 if kind == "enum" else 2)
        if wire == want:
            if kind == "message":
                if f.repeated:
                    sub = {}
                    _decode_into(f.target, raw, sub, depth + 1)
                    into.setdefault(f.name, []).append(sub)
                else:
                    sub = into.setdefault(f.name, {})
                    _decode_into(f.target, raw, sub, depth + 1)
                continue
            val = (_enum_value(f, raw) if kind == "enum"
                   else _scalar(kind, raw, where))
            if f.repeated:
                into.setdefault(f.name, []).append(val)
            else:
                into[f.name] = val
        elif wire == 2 and f.repeated and (kind in _PACKABLE
                                           or kind == "enum"):
            vals = into.setdefault(f.name, [])
            pos = 0
            while pos < len(raw):
                if kind == "enum" or SCALARS[kind] == 0:
                    v, pos = _varint(raw, pos)
                else:
                    v, pos = _fixed(raw, pos, 4 if SCALARS[kind] == 5 else 8)
                vals.append(_enum_value(f, v) if kind == "enum"
                            else _scalar(kind, v, where))
        else:
            raise Undecodable(f"{where}: wire type {wire} where {kind} is"
                              " written")


def _filled(msg, got):
    out = {}
    for f in msg.fields:
        if f.name in got:
            v = got[f.name]
            if isinstance(f.target, _Message):
                v = ([_filled(f.target, x) for x in v] if f.repeated
                     else _filled(f.target, v))
            out[f.name] = v
        else:
            out[f.name] = _default(f)
    return out


def _proto_record(sid, text, body):
    try:
        file = parse_proto(text)
        if not file.messages:
            raise Undecodable("the schema defines no message")
        path, at = read_indexes(body)
        level, msg = file.messages, None
        for i in path:
            if i >= len(level):
                raise Undecodable(
                    f"the message index path {path} names a message the"
                    " schema does not have")
            msg = level[i]
            level = msg.nested
        got = {}
        _decode_into(msg, body[at:], got, 0)
        return _filled(msg, got)
    except Undecodable as e:
        raise Undecodable(f"schema {sid} (Protobuf): {e}")
