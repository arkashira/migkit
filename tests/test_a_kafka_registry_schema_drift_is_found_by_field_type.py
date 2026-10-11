"""A field whose type differs between the source's schema registry and the
target's is found, topic by topic (P0-PARITY.verify, value.type-drift).

NOT in `PROVES`: the registry's `GET /subjects/<s>/versions/latest` has never
been asked of a real registry from here (no registry in this sandbox), so
`capabilities.UNPROVED` carries the cell until a docker test does. What is
held here is the reading: the answer's shape as the registry's API
documents it, the field-by-field comparison, and what the check says for a
subject one side lacks.
"""
import json

import pytest

from migkit import registry
from tests import kafka_fake as fake
from tests.kafka_fake import Cluster


def _avro(*fields, name="Order"):
    return {"type": "record", "name": name, "fields": [
        {"name": n, "type": t} for n, t in fields]}


def _have(schema, kind=None):
    return {"type": kind or "AVRO", "schema": schema}


def test_a_field_that_is_an_int_on_one_side_and_a_long_on_the_other():
    a = _have(_avro(("id", "int"), ("name", "string")))
    b = _have(_avro(("id", "long"), ("name", "string")))
    assert registry.field_drift(a, b) == [
        "field id is int on the source and long on the target"]
    assert registry.field_drift(a, a) == []


def test_a_field_only_one_side_has_is_named():
    a = _have(_avro(("id", "int"), ("note", "string")))
    b = _have(_avro(("id", "int")))
    assert registry.field_drift(a, b) == [
        "field note is only on the source"]
    assert registry.field_drift(b, a) == [
        "field note is only on the target"]


def test_a_nullable_field_is_not_the_same_type_as_a_required_one():
    a = _have(_avro(("id", "int"), ("note", ["null", "string"])))
    b = _have(_avro(("id", "int"), ("note", "string")))
    assert registry.field_drift(a, b) == [
        "field note is union<null|string> on the source and string on the"
        " target"]


def test_a_logical_type_is_part_of_a_fields_type():
    a = _have(_avro(("at", {"type": "long", "logicalType":
                            "timestamp-millis"})))
    b = _have(_avro(("at", "long")))
    assert registry.field_drift(a, b) == [
        "field at is long(timestamp-millis) on the source and long on the"
        " target"]


def test_a_nested_record_is_walked_to_its_fields():
    inner = lambda t: {"type": "record", "name": "Address", "fields": [  # noqa
        {"name": "zip", "type": t}]}
    a = _have(_avro(("address", inner("int"))))
    b = _have(_avro(("address", inner("string"))))
    assert registry.field_drift(a, b) == [
        "field address.zip is int on the source and string on the target"]


def test_the_items_of_an_array_are_walked_too():
    line = lambda t: {"type": "array", "items": {  # noqa
        "type": "record", "name": "Line", "fields": [
            {"name": "qty", "type": t}]}}
    a = _have(_avro(("lines", line("int"))))
    b = _have(_avro(("lines", line("double"))))
    assert registry.field_drift(a, b) == [
        "field lines[].qty is int on the source and double on the target"]


def test_a_schema_kept_as_text_is_compared_whole():
    a = _have('syntax = "proto3"; message A { int32 id = 1; }', "PROTOBUF")
    b = _have('syntax = "proto3"; message A { int64 id = 1; }', "PROTOBUF")
    (said,) = registry.field_drift(a, b)
    assert "PROTOBUF" in said and "as a whole" in said
    assert registry.field_drift(a, a) == []


def test_a_schema_of_another_kind_is_said():
    a = _have(_avro(("id", "int")))
    b = _have({"type": "object"}, "JSON")
    assert registry.field_drift(a, b) == [
        "is AVRO on the source and JSON on the target"]


# ---- the registry's answer ------------------------------------------------

def test_the_latest_version_is_read_from_the_registrys_answer(monkeypatch):
    reg = registry.Registry("http://10.0.0.5:8081")
    seen = []

    def ask(method, path, body=None):
        seen.append((method, path))
        if "orders-value" in path:
            return {"subject": "orders-value", "version": 3, "id": 7,
                    "schema": json.dumps(_avro(("id", "int")))}
        if "payments-value" in path:
            return {"subject": "payments-value", "version": 1, "id": 8,
                    "schemaType": "PROTOBUF", "schema": "message A {}"}
        raise registry.Missing("Subject not found")
    monkeypatch.setattr(reg, "_ask", ask)
    assert reg.latest("orders-value") == {
        "type": "AVRO", "schema": _avro(("id", "int"))}
    assert reg.latest("payments-value") == {
        "type": "PROTOBUF", "schema": "message A {}"}
    assert reg.latest("nothing-value") is None
    assert seen[0] == ("GET", "/subjects/orders-value/versions/latest")


# ---- the check -------------------------------------------------------------

class _Registry:
    def __init__(self, subjects):
        self.subjects = subjects

    def latest(self, subject):
        return self.subjects.get(subject)


@pytest.fixture
def pair(monkeypatch, tmp_path):
    fake.install(monkeypatch)
    src, dst = Cluster("src"), Cluster("dst")
    for cluster in (src, dst):
        cluster.topic("orders")
        cluster.topic("payments")
    eng = fake.engine(src, dst)
    eng.hop.report_dir = lambda db=None: tmp_path
    return eng


def _registries(eng, src, dst):
    eng._registries = {"src": _Registry(src) if src is not None else None,
                       "dst": _Registry(dst) if dst is not None else None}


def test_the_check_names_the_subject_and_the_field(pair):
    eng = pair
    _registries(eng, {"orders-value": _have(_avro(("id", "int"))),
                      "payments-value": _have(_avro(("amount", "long")))},
                {"orders-value": _have(_avro(("id", "long"))),
                 "payments-value": _have(_avro(("amount", "long")))})
    (one,) = eng._schema_types("cluster")
    assert one.status == "diff"
    assert ("orders-value field id is int on the source and long on the"
            " target") in one.detail
    assert one.category == "value.type-drift"
    assert one.table == "orders"
    assert one.tables == {"orders": "diff", "payments": "ok"}


def test_equal_schemas_pass_and_say_how_many(pair):
    eng = pair
    same = {"orders-value": _have(_avro(("id", "int"))),
            "orders-key": _have("string")}
    _registries(eng, same, dict(same))
    (one,) = eng._schema_types("cluster")
    assert one.status == "ok" and "2 schemas" in one.detail


def test_a_subject_the_target_lacks_is_a_finding(pair):
    eng = pair
    _registries(eng, {"orders-value": _have(_avro(("id", "int")))}, {})
    (one,) = eng._schema_types("cluster")
    assert one.status == "diff"
    assert "orders-value is not registered on the target" in one.detail


def test_a_subject_only_the_target_has_is_its_own(pair):
    eng = pair
    _registries(eng, {}, {"orders-value": _have(_avro(("id", "int")))})
    (one,) = eng._schema_types("cluster")
    assert one.status == "ok"


def test_one_side_without_a_registry_is_said_not_passed(pair):
    eng = pair
    _registries(eng, {"orders-value": _have(_avro(("id", "int")))}, None)
    (one,) = eng._schema_types("cluster")
    assert one.status == "warn" and "only the source" in one.detail


def test_no_registry_on_either_side_is_no_finding(pair):
    eng = pair
    _registries(eng, None, None)
    assert eng._schema_types("cluster") == []


def test_a_registry_that_will_not_answer_is_an_error(pair):
    eng = pair

    class Down:
        def latest(self, subject):
            raise RuntimeError("the schema registry answered 503")
    eng._registries = {"src": Down(), "dst": Down()}
    (one,) = eng._schema_types("cluster")
    assert one.status == "error" and "503" in one.detail


def test_a_topic_the_hop_leaves_out_has_its_schemas_left_out(pair):
    eng = pair
    eng.hop.exclude = ["payments"]
    _registries(eng, {"payments-value": _have(_avro(("a", "int")))},
                {"payments-value": _have(_avro(("a", "long")))})
    (one,) = eng._schema_types("cluster")
    assert one.status == "ok"
