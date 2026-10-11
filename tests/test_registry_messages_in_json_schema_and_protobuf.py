"""Messages a schema registry framed that carry JSON Schema or Protobuf
(backlog R4.formats): read to rows, JSON Schema written, Protobuf refused
as a write, and an unreadable message stopping with its reason.

No broker and no registry: the registry is a dict behind the same calls
(`entry`, `schema`, `register`), the wire bytes are made by hand. Whether a
real registry takes the JSON Schema versions written here, and whether a
real Protobuf serializer's bytes read as these do, is not tried.
"""
import json
import struct

import pytest

from migkit import registry, schemaformats
from migkit.schemaformats import Undecodable


class FakeRegistry:
    """The registry's calls, over a dict of id -> (kind, text, refs)."""

    def __init__(self, schemas=None):
        self.schemas = dict(schemas or {})
        self.by_text = {}

    def entry(self, sid):
        if sid not in self.schemas:
            raise registry.Missing(f"the schema registry answered 404:"
                                   f" no schema {sid}")
        return self.schemas[sid]

    def schema(self, sid):
        kind, text, _ = self.entry(sid)
        if kind != "AVRO":
            raise ValueError(f"schema {sid} is a {kind} schema")
        return json.loads(text)

    def register(self, subject, schema, kind="AVRO"):
        text = schema if isinstance(schema, str) else json.dumps(
            schema, sort_keys=True)
        key = (subject, kind, text)
        if key not in self.by_text:
            sid = 100 + len(self.by_text)
            self.by_text[key] = sid
            self.schemas[sid] = (kind, text, [])
        return self.by_text[key]


def framed(sid, body):
    return b"\x00" + struct.pack(">I", sid) + body


def varint(n):
    out = b""
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out


def zig(n):
    return varint(((n << 1) ^ (n >> 63)) & ((1 << 64) - 1))


def tag(number, wire):
    return varint(number << 3 | wire)


def ld(number, payload):
    return tag(number, 2) + varint(len(payload)) + payload


def indexes(*path):
    if path == (0,):
        return b"\x00"
    return zig(len(path)) + b"".join(zig(i) for i in path)


# --- JSON Schema ---------------------------------------------------------

ORDERS = {"type": "object", "required": ["id"],
          "properties": {"id": {"type": "integer"},
                         "note": {"type": ["null", "string"]},
                         "total": {"type": "number"}}}


def _json_registry(schema=ORDERS, refs=()):
    return FakeRegistry({7: ("JSON", json.dumps(schema), list(refs))})


def test_a_json_schema_message_is_read_as_its_row():
    reg = _json_registry()
    row = {"id": 5, "note": None, "total": 12.5, "extra": [1]}
    assert schemaformats.decode(
        reg, framed(7, json.dumps(row).encode())) == row


def test_a_tombstone_is_read_as_none():
    assert schemaformats.decode(_json_registry(), None) is None


@pytest.mark.parametrize("body, why", [
    (b'{"note": "x"}', "required property 'id' is missing"),
    (b'{"id": "five"}', "'id' should be integer"),
    (b'[1, 2]', "not a JSON object"),
    (b'{"id": ', "not JSON"),
    (b'\xff\xfe', "not JSON"),
])
def test_a_json_message_that_does_not_fit_stops_with_its_reason(body, why):
    with pytest.raises(Undecodable, match=why):
        schemaformats.decode(_json_registry(), framed(7, body))


def test_a_closed_json_schema_refuses_a_property_it_does_not_name():
    reg = _json_registry({**ORDERS, "additionalProperties": False})
    with pytest.raises(Undecodable, match="allows no property but its own"):
        schemaformats.decode(reg, framed(7, b'{"id": 1, "x": 2}'))


def test_a_json_schema_with_references_is_refused_not_guessed():
    reg = _json_registry(refs=[{"name": "address.json", "subject": "a",
                                "version": 1}])
    with pytest.raises(Undecodable, match="refers to 1 other schema"):
        schemaformats.decode(reg, framed(7, b'{"id": 1}'))


def test_json_schema_changes_are_written_and_read_back_round_trip():
    import datetime
    import decimal
    reg = FakeRegistry()
    enc = schemaformats.JsonEncoder(reg)
    at = datetime.datetime(2024, 5, 1, 12, 30, 0, 123456)
    out = enc.encode("app.orders", "app", {
        "op": "insert", "table": "orders", "key": {"id": 1},
        "values": {"id": 1, "total": decimal.Decimal("12.50"),
                   "raw": b"\x00\xff", "at": at, "doc": {"b": 1},
                   "note": None}}, 1700000000000)
    (key, value), = out
    assert key[:1] == b"\x00" and value[:1] == b"\x00"
    got = schemaformats.decode(reg, value)
    assert got["op"] == "c" and got["before"] is None
    assert got["after"] == {"id": 1, "total": "12.50", "raw": "AP8=",
                            "at": "2024-05-01T12:30:00.123456",
                            "doc": {"b": 1}, "note": None}
    assert got["source"]["table"] == "orders"
    assert schemaformats.decode(reg, key) == {"id": 1}
    # the registry holds them as JSON Schema, under topic-name subjects
    kinds = {(s, k) for s, k, _ in reg.by_text}
    assert kinds == {("app.orders-value", "JSON"), ("app.orders-key", "JSON")}


def test_a_json_delete_has_a_tombstone_and_a_new_column_a_new_schema():
    reg = FakeRegistry()
    enc = schemaformats.JsonEncoder(reg)
    first = enc.encode("t", "d", {"op": "insert", "table": "t",
                                  "key": {"id": 1},
                                  "values": {"id": 1}}, 1)
    second = enc.encode("t", "d", {"op": "update", "table": "t",
                                   "key": {"id": 1},
                                   "values": {"id": 1, "extra": "x"}}, 2)
    gone = enc.encode("t", "d", {"op": "delete", "table": "t",
                                 "key": {"id": 1}}, 3)
    assert registry.frame_of(first[0][1])[0] != \
        registry.frame_of(second[0][1])[0]
    assert len(gone) == 2 and gone[1][1] is None and gone[0][0] == gone[1][0]
    assert schemaformats.decode(reg, gone[0][1])["after"] is None
    assert len(enc.encode("t", "d", {"op": "delete", "table": "t",
                                     "key": {"id": 1}}, 4,
                          tombstones=False)) == 1


def test_a_value_json_cannot_hold_stops_the_write():
    enc = schemaformats.JsonEncoder(FakeRegistry())
    with pytest.raises(ValueError, match="a value JSON cannot hold"):
        enc.encode("t", "d", {"op": "insert", "table": "t",
                              "key": {"id": 1},
                              "values": {"id": 1, "x": float("nan")}}, 1)


# --- Protobuf ------------------------------------------------------------

PROTO = '''
// an order
syntax = "proto3";
package shop.v1;
option java_package = "com.example";

enum Status { UNKNOWN = 0; PAID = 1; SHIPPED = 2; }

message Order {
  int64 id = 1;
  string note = 2;
  sint32 delta = 3;
  repeated int32 qty = 4;
  repeated Line lines = 5;
  Status status = 6;
  optional double price = 7 [deprecated = true];
  bytes raw = 8;
  bool paid = 9;
  fixed32 f32 = 10;
  sfixed64 sf64 = 11;
  float ratio = 12;
  repeated string tags = 13;
  Order.Gift gift = 14;
  message Line {
    string sku = 1;
    uint32 n = 2;
    message Detail { string text = 1; }
    Detail detail = 3;
  }
  message Gift { string to = 1; Line line = 2; }
}

message Other { string name = 1; }
'''


def _proto_registry(text=PROTO, refs=()):
    return FakeRegistry({9: ("PROTOBUF", text, list(refs))})


def line(sku, n, detail=None):
    out = ld(1, sku.encode()) + tag(2, 0) + varint(n)
    if detail is not None:
        out += ld(3, ld(1, detail.encode()))
    return out


def test_a_protobuf_message_with_nested_and_repeated_fields_is_read():
    body = (tag(1, 0) + varint(300)
            + ld(2, "héllo".encode())
            + tag(3, 0) + zig(-3)               # sint32
            + ld(4, varint(1) + varint(2)
                 + varint((-1) & ((1 << 64) - 1)))   # packed, one negative
            + ld(5, line("a", 2, "boxed"))
            + ld(5, line("b", 7))
            + tag(6, 0) + varint(2)
            + tag(7, 1) + struct.pack("<d", 2.5)
            + ld(8, b"\x00\xff")
            + tag(9, 0) + varint(1)
            + tag(10, 5) + struct.pack("<I", 4000000000)
            + tag(11, 1) + struct.pack("<q", -5)
            + tag(12, 5) + struct.pack("<f", 0.5)
            + ld(13, b"x") + ld(13, b"y"))
    got = schemaformats.decode(_proto_registry(),
                               framed(9, indexes(0) + body))
    assert got == {
        "id": 300, "note": "héllo", "delta": -3, "qty": [1, 2, -1],
        "lines": [{"sku": "a", "n": 2, "detail": {"text": "boxed"}},
                  {"sku": "b", "n": 7, "detail": None}],
        "status": "SHIPPED", "price": 2.5, "raw": b"\x00\xff", "paid": True,
        "f32": 4000000000, "sf64": -5, "ratio": 0.5, "tags": ["x", "y"],
        "gift": None}


def test_a_proto3_message_that_leaves_fields_out_reads_their_defaults():
    got = schemaformats.decode(_proto_registry(),
                               framed(9, indexes(0) + tag(1, 0) + varint(1)))
    assert got["id"] == 1 and got["note"] == "" and got["qty"] == []
    assert got["delta"] == 0 and got["status"] == "UNKNOWN"
    assert got["price"] is None and got["gift"] is None
    assert got["raw"] == b"" and got["paid"] is False and got["ratio"] == 0.0
    assert schemaformats.decode(
        _proto_registry(), framed(9, b"\x00"))["id"] == 0


@pytest.mark.parametrize("n", [-1, -2, -2 ** 31, 2 ** 31 - 1])
def test_negative_and_extreme_int32_and_sint32_values_are_exact(n):
    text = ('syntax = "proto3"; message M { int32 a = 1; sint32 b = 2;'
            ' sint64 c = 3; int64 d = 4; }')
    mask = (1 << 64) - 1
    body = (tag(1, 0) + varint(n & mask) + tag(2, 0) + zig(n)
            + tag(3, 0) + zig(n) + tag(4, 0) + varint(n & mask))
    reg = FakeRegistry({1: ("PROTOBUF", text, [])})
    assert schemaformats.decode(reg, framed(1, b"\x00" + body)) == \
        {"a": n, "b": n, "c": n, "d": n}


def test_an_int32_that_does_not_fit_is_refused_not_cut():
    text = 'syntax = "proto3"; message M { int32 a = 1; }'
    reg = FakeRegistry({1: ("PROTOBUF", text, [])})
    with pytest.raises(Undecodable, match="does not fit an int32"):
        schemaformats.decode(reg, framed(
            1, b"\x00" + tag(1, 0) + varint(2 ** 31)))


def test_the_message_index_path_picks_a_nested_or_a_later_message():
    reg = _proto_registry()
    # [1]: the second top-level message
    assert schemaformats.decode(reg, framed(
        9, indexes(1) + ld(1, b"bo"))) == {"name": "bo"}
    # [0, 1]: Order.Gift, the second message nested in Order (Line is first)
    assert schemaformats.decode(reg, framed(
        9, indexes(0, 1) + ld(1, b"ann") + ld(2, line("k", 1)))) == {
        "to": "ann", "line": {"sku": "k", "n": 1, "detail": None}}
    # [0, 0, 0]: Order.Line.Detail
    assert schemaformats.decode(reg, framed(
        9, indexes(0, 0, 0) + ld(1, b"deep"))) == {"text": "deep"}


def test_the_indexes_are_read_as_zigzag_varints_led_by_their_count():
    # a count of 2 is the byte 4, the indexes 1 and 2 are 2 and 4
    assert indexes(1, 2) == b"\x04\x02\x04"
    assert schemaformats.read_indexes(b"\x04\x02\x04rest") == ([1, 2], 3)
    assert schemaformats.read_indexes(b"\x00rest") == ([0], 1)
    # an index that takes more than one byte
    assert schemaformats.read_indexes(
        zig(1) + zig(200) + b"z") == ([200], len(zig(1) + zig(200)))


def test_a_path_to_a_message_the_schema_does_not_have_is_refused():
    with pytest.raises(Undecodable, match=r"index path \[5\] names a message"
                                          " the schema does not have"):
        schemaformats.decode(_proto_registry(), framed(9, indexes(5)))


def test_a_message_that_holds_a_field_the_schema_does_not_name_is_refused():
    with pytest.raises(Undecodable, match="holds field 99"):
        schemaformats.decode(_proto_registry(), framed(
            9, b"\x00" + tag(99, 0) + varint(1)))


def test_a_truncated_or_mistyped_protobuf_message_is_refused():
    reg = _proto_registry()
    with pytest.raises(Undecodable, match="runs past the end"):
        schemaformats.decode(reg, framed(9, b"\x00" + tag(2, 2)
                                         + varint(50) + b"ab"))
    with pytest.raises(Undecodable, match="wire type 0 where string"):
        schemaformats.decode(reg, framed(9, b"\x00" + tag(2, 0)
                                         + varint(1)))
    with pytest.raises(Undecodable, match="not UTF-8"):
        schemaformats.decode(reg, framed(9, b"\x00" + ld(2, b"\xff\xfe")))
    with pytest.raises(Undecodable, match="middle of a varint"):
        schemaformats.decode(reg, framed(9, b"\x00" + b"\x08\x80"))


@pytest.mark.parametrize("text, why", [
    ('syntax = "proto3"; message M { oneof x { int32 a = 1; } }', "a oneof"),
    ('syntax = "proto3"; message M { map<string, int32> m = 1; }',
     "a map field"),
    ('syntax = "proto3"; import "google/protobuf/any.proto";'
     ' message M { string a = 1; }', "imports another .proto"),
    ('syntax = "proto2"; message M { optional int32 a = 1; }',
     "not proto3"),
    ('message M { int32 a = 1; }', "names no syntax"),
    ('syntax = "proto3"; message M { extensions 100 to 199; }',
     "extension ranges"),
    ('syntax = "proto3"; extend M { int32 a = 100; }', "extensions"),
    ('syntax = "proto3"; message M { Missing a = 1; }',
     "type Missing is not defined"),
    ('syntax = "proto3"; message M { int32 a = 1; int32 b = 1; }',
     "two fields"),
    ('syntax = "proto3"; message M { int32 a = 1', "ends in the middle"),
])
def test_what_the_proto_reader_does_not_cover_is_refused_with_the_reason(
        text, why):
    reg = FakeRegistry({1: ("PROTOBUF", text, [])})
    with pytest.raises(Undecodable, match=why) as e:
        schemaformats.decode(reg, framed(1, b"\x00"))
    assert "schema 1 (Protobuf)" in str(e.value)


def test_a_protobuf_schema_with_references_is_refused():
    reg = _proto_registry(refs=[{"name": "a.proto", "subject": "a",
                                 "version": 1}])
    with pytest.raises(Undecodable, match="refers to 1 other schema"):
        schemaformats.decode(reg, framed(9, b"\x00"))


def test_an_unpacked_repeated_scalar_and_an_unknown_enum_number_read():
    text = ('syntax = "proto3"; enum E { Z = 0; A = 1; }'
            ' message M { repeated int32 n = 1; E e = 2; repeated E es = 3;'
            ' reserved 5; }')
    reg = FakeRegistry({1: ("PROTOBUF", text, [])})
    body = (tag(1, 0) + varint(4) + tag(1, 0) + varint(5)
            + tag(2, 0) + varint(77) + ld(3, varint(1) + varint(0)))
    assert schemaformats.decode(reg, framed(1, b"\x00" + body)) == {
        "n": [4, 5], "e": 77, "es": ["A", "Z"]}


# --- what is neither -----------------------------------------------------

def test_an_unknown_schema_id_stops_with_the_reason():
    with pytest.raises(Undecodable,
                       match="schema 4242 is not in the registry"):
        schemaformats.decode(FakeRegistry(), framed(4242, b"{}"))


def test_a_schema_type_not_supported_is_refused():
    reg = FakeRegistry({3: ("THRIFT", "struct X {}", [])})
    with pytest.raises(Undecodable, match="kind THRIFT"):
        schemaformats.decode(reg, framed(3, b"{}"))


def test_bytes_a_registry_did_not_frame_are_refused_not_passed():
    with pytest.raises(Undecodable, match="not a message a schema registry"):
        schemaformats.decode(_json_registry(), b'{"id": 1}')


def test_an_avro_message_is_still_read_through_the_same_door():
    import io

    fastavro = pytest.importorskip("fastavro")
    parse_schema, schemaless_writer = (fastavro.parse_schema,
                                       fastavro.schemaless_writer)
    schema = {"type": "record", "name": "R",
              "fields": [{"name": "a", "type": "long"}]}
    buf = io.BytesIO()
    schemaless_writer(buf, parse_schema(schema), {"a": 41})
    reg = FakeRegistry({2: ("AVRO", json.dumps(schema), [])})
    assert schemaformats.decode(reg, framed(2, buf.getvalue())) == {"a": 41}
    with pytest.raises(Undecodable, match="schema 2 \\(Avro\\)"):
        schemaformats.decode(reg, framed(2, b""))


# --- the registry client -------------------------------------------------

def test_the_registry_client_reads_the_schema_type_and_registers_by_it(
        monkeypatch):
    reg = registry.Registry("http://registry.example.com:8081")
    asked = []

    def ask(method, path, body=None):
        asked.append((method, path, body))
        if method == "POST":
            return {"id": 12}
        if path.endswith("/1"):
            return {"schemaType": "PROTOBUF", "schema": "syntax=...;"}
        if path.endswith("/2"):
            return {"schema": '{"type": "string"}'}
        if path.endswith("/5"):
            return {"schemaType": "JSON", "schema": '{"type":"object"}'}
        raise registry.Missing("the schema registry answered 404: gone")

    monkeypatch.setattr(reg, "_ask", ask)
    assert reg.entry(1) == ("PROTOBUF", "syntax=...;", [])
    assert reg.entry(2)[0] == "AVRO" and reg.schema(2) == {"type": "string"}
    with pytest.raises(ValueError, match="PROTOBUF schema, not an Avro"):
        reg.schema(1)
    with pytest.raises(registry.Missing):
        reg.entry(3)
    assert reg.register("s-value", {"type": "object"}, "JSON") == 12
    assert asked[-1][2]["schemaType"] == "JSON"
    reg.register("s-value", {"type": "object"}, "JSON")      # remembered
    assert [a for a in asked if a[0] == "POST"] == [asked[-1]]
    reg.register("s2-value", {"type": "string"})
    assert "schemaType" not in asked[-1][2]
    # a schema another registry numbered compares by what it says
    assert reg.fingerprint(1) != reg.fingerprint(5)


# --- the Kafka engine ----------------------------------------------------

def _hop(**options):
    from migkit.config import Endpoint, Hop
    ep = Endpoint(host="h", port=1, user="", password="",
                  options={"schema_registry": "http://registry.example.com"})
    return Hop(name="x", engine="kafka", source=ep, target=ep,
               databases=["d"], db_map={"d": "d"}, options=options)


def test_format_protobuf_is_refused_as_a_write_with_the_reason():
    from migkit.engines.kafka import KafkaEngine
    with pytest.raises(SystemExit, match="protobuf is not written: a"
                                         " Protobuf message is written"
                                         " against field numbers"):
        KafkaEngine(_hop(format="protobuf"))._stream_options()


def test_format_jsonschema_needs_a_registry_on_the_target():
    from migkit.config import Endpoint, Hop
    from migkit.engines.kafka import KafkaEngine
    bare = Endpoint(host="h", port=1, user="", password="")
    hop = Hop(name="x", engine="kafka", source=bare, target=bare,
              databases=["d"], db_map={"d": "d"},
              options={"format": "jsonschema"})
    with pytest.raises(SystemExit, match="format: jsonschema needs the"
                                         " target's `schema_registry`"):
        KafkaEngine(hop).neutral_apply("dst", "d", [])


class _Producer:
    def __init__(self, sent):
        self.sent = sent

    def send(self, topic, value=None, key=None):
        self.sent.append((topic, key, value))

    def flush(self):
        pass

    def close(self):
        pass


def test_format_jsonschema_writes_through_the_registry(monkeypatch):
    from migkit.engines.kafka import KafkaEngine
    eng = KafkaEngine(_hop(format="jsonschema"))
    reg = FakeRegistry()
    eng.__dict__["_registries"] = {"dst": reg}
    sent = []
    monkeypatch.setattr(eng, "_producer", lambda **kw: _Producer(sent))
    eng.neutral_apply("dst", "d", [
        {"op": "insert", "table": "t", "key": {"id": 1},
         "values": {"id": 1, "v": "a"}},
        {"op": "delete", "table": "t", "key": {"id": 1}}])
    assert [t for t, _, _ in sent] == ["d.t"] * 3
    assert sent[2][2] is None                       # the tombstone
    assert schemaformats.decode(reg, sent[0][2])["after"] == \
        {"id": 1, "v": "a"}


def test_a_refused_json_schema_stops_the_stream_and_says_so(monkeypatch):
    from migkit.engines.kafka import KafkaEngine
    eng = KafkaEngine(_hop(format="jsonschema"))

    class Strict(FakeRegistry):
        def register(self, subject, schema, kind="AVRO"):
            raise registry.Refused("incompatible")

    eng.__dict__["_registries"] = {"dst": Strict()}
    sent = []
    monkeypatch.setattr(eng, "_producer", lambda **kw: _Producer(sent))
    with pytest.raises(SystemExit, match="registry refused the table's"):
        eng.neutral_apply("dst", "d", [
            {"op": "insert", "table": "t", "key": {"id": 1},
             "values": {"id": 1}}])
    assert sent == []


def test_a_copy_registers_a_json_or_protobuf_schema_under_its_own_kind():
    from migkit.engines.kafka import KafkaEngine
    eng = KafkaEngine(_hop())
    src = FakeRegistry({5: ("JSON", '{"type": "object"}', []),
                        6: ("PROTOBUF", 'syntax = "proto3";', [])})
    dst = FakeRegistry()
    dst.register("pad", {"type": "string"})        # ids differ across clusters
    eng.__dict__["_registries"] = {"src": src, "dst": dst}
    j = eng._translated("t", "value", framed(5, b'{"a": 1}'))
    p = eng._translated("t", "value", framed(6, b"\x00body"))
    assert j[5:] == b'{"a": 1}' and p[5:] == b"\x00body"
    assert dst.schemas[registry.frame_of(j)[0]][0] == "JSON"
    assert dst.schemas[registry.frame_of(p)[0]][0] == "PROTOBUF"
    assert registry.frame_of(j)[0] != 5


def test_a_copy_stops_at_a_message_whose_schema_cannot_be_had():
    from migkit.engines.kafka import KafkaEngine
    eng = KafkaEngine(_hop())
    src = FakeRegistry({8: ("JSON", "{}", [{"name": "x", "subject": "s",
                                            "version": 1}])})
    eng.__dict__["_registries"] = {"src": src, "dst": FakeRegistry()}
    with pytest.raises(SystemExit, match="schema 77, which the source's"
                                         " registry does not hold"):
        eng._translated("t", "value", framed(77, b"{}"))
    with pytest.raises(SystemExit, match="refers to other schemas"):
        eng._translated("t", "value", framed(8, b"{}"))
