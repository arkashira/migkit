"""The classes type-fidelity left open after G1-G29 (docs/research/
type-fidelity-2026-09-28.md, sections 11-12), decided before any server
is asked: an interval as its three parts, a vector as its floats, a shape
as its SRID and longitude-first binary, a MongoDB value with its BSON
type, a collection told apart from NULL, and the digits past the sixth
of Snowflake and Db2. What the servers answer is measured in
`test_mongo_types_survive_sql_and_back.py`,
`test_shapes_and_vectors_are_carried_exactly.py`,
`test_cassandra_durations_and_collections_are_carried.py`,
`test_clickhouse_arrays_are_carried_exactly.py` and
`test_sql_server_writes_its_seventh_digit.py`.
"""
import datetime
import struct
from decimal import Decimal

import pytest

from migkit import bsontype, canon, commercial, geo, intervals, vectors


# ---- intervals ------------------------------------------------------------

@pytest.mark.parametrize("text,parts", [
    ("1 year 2 mons 3 days 04:05:06.5", (14, 3, 14706500000000)),
    ("-1 mons +2 days -00:00:01", (-1, 2, -10 ** 9)),
    ("100:00:00", (0, 0, 360000 * 10 ** 9)),
    ("1 year 2 months 3 days 04:05:06.5", (14, 3, 14706500000000)),
    ("P1Y2M3DT4H5M6.5S", (14, 3, 14706500000000)),
    ("P-1M2DT-1S", (-1, 2, -10 ** 9)),
    ("P14M3DT3723.5000000000000000S", (14, 3, 3723500000000)),
    ("14 months 3 days 3723500000 microseconds", (14, 3, 3723500000000)),
    ("P0M0DT0.000000001S", (0, 0, 1)),
])
def test_an_interval_is_its_three_parts(text, parts):
    assert tuple(intervals.parse(text)) == parts


def test_a_month_is_not_thirty_days():
    """What psycopg2 and DuckDB's library hand over, a `timedelta`, made
    one of the other."""
    assert intervals.text("1 mon") != intervals.text("30 days")
    assert intervals.text("1 day") != intervals.text("24:00:00")
    assert intervals.text("1 mon") == "P1M0DT0S"
    assert intervals.text("P0M0DT0.0000015S") == "P0M0DT0.0000015S"
    # Oracle's day-to-second keeps days and time of one sign
    assert tuple(intervals.of(datetime.timedelta(days=-1, hours=-2))) \
        == (0, -1, -7200 * 10 ** 9)


def test_a_duration_and_a_year_to_month_are_read_as_intervals():
    from cassandra.util import Duration
    assert tuple(intervals.of(Duration(14, 3, 5))) == (14, 3, 5)

    class YM:
        years, months = 1, 2
    assert tuple(intervals.of(YM())) == (14, 0, 0)


def test_an_interval_goes_to_each_engine_in_its_own_form():
    iv = intervals.parse("P14M3DT3723.5S")
    assert canon.write_value("postgres", "interval", iv) \
        == "14 months 3 days 3723500000 microseconds"
    from cassandra.util import Duration
    got = canon.write_value("cassandra", "interval", iv)
    assert isinstance(got, Duration)
    assert (got.months, got.days, got.nanoseconds) == (14, 3, 3723500000000)
    # a nanosecond PostgreSQL cannot hold is no microsecond text at all
    assert intervals.postgres_text("P0M0DT0.000000001S") is None


def test_digits_past_the_microsecond_of_an_interval_are_counted():
    assert canon.unfit("cassandra", "duration", "postgres", "interval") \
        == [("fraction", 6)]
    assert canon.unfit_value("fraction", intervals.parse("P0M0DT1.5S"), 6) \
        is False
    assert canon.unfit_value("fraction", intervals.parse(
        "P0M0DT0.000000001S"), 6) is True
    assert canon.unfit_value("fraction", "P1M0DT0.0000001S", 6) is True


def test_oracle_keeps_an_interval_as_one_of_two_types():
    """Mappings only (the Oracle engine is read and written through its
    driver): a year-to-month has no days or time, a day-to-second no
    months, and what one has no room for is counted before the move."""
    import oracledb
    assert canon.unfit("postgres", "interval", "oracle",
                       "INTERVAL DAY(2) TO SECOND(6)") \
        == [("interval-part", "months")]
    assert canon.unfit("postgres", "interval", "oracle",
                       "INTERVAL YEAR(2) TO MONTH") \
        == [("interval-part", "days or a time")]
    assert canon.unfit_value("interval-part", "P1M0DT0S", "months") is True
    assert canon.unfit_value("interval-part", "P0M2DT1S", "months") is False
    assert canon.unfit_value("interval-part", "P-14M0DT0S",
                             "days or a time") is False
    assert canon.write_value("oracle", "interval", "P-14M0DT0S") \
        == oracledb.IntervalYM(-1, -2)
    assert canon.write_value("oracle", "interval", "P0M2DT1.5S") \
        == datetime.timedelta(days=2, seconds=1.5)
    got = canon.write_value("oracle", "vector", "[0.5,1,2]")
    assert got.typecode == "f" and list(got) == [0.5, 1.0, 2.0]


def test_an_interval_has_no_type_on_mysql_and_says_so():
    with pytest.raises(ValueError, match="interval"):
        canon.ddl_type("mysql", "interval")
    assert canon.ddl_type("cassandra", "interval") == "duration"
    assert canon.ddl_type("duckdb", "interval") == "INTERVAL"
    assert canon.type_class("oracle", "INTERVAL DAY(2) TO SECOND(6)") \
        == "interval"
    assert canon.type_class("oracle", "INTERVAL YEAR(2) TO MONTH") \
        == "interval"


# ---- vectors --------------------------------------------------------------

def test_a_vector_read_as_text_is_read_as_the_floats_it_printed():
    """pgvector prints the shortest decimal of each 32-bit float: read as
    a double, `0.1` is another number."""
    got = vectors.of("[0.1,1e-07,3.4028235e+38]")
    assert got[0] == struct.unpack("<f", struct.pack("<f", 0.1))[0]
    assert got[0] != 0.1
    assert vectors.text("[0.1]") == "[0.10000000149011612]"
    # a halfvec's text read as the 16-bit floats it printed
    assert vectors.of("[0.3333]", 16) == [struct.unpack(
        "<e", struct.pack("<e", 0.3333))[0]]


def test_a_vector_is_the_same_whichever_form_carried_it():
    raw = struct.pack("<3f", 0.1, -0.0, 2.5)
    assert vectors.text(raw) == vectors.text("[0.1,-0,2.5]") \
        == vectors.text([struct.unpack("<f", struct.pack("<f", 0.1))[0],
                         -0.0, 2.5])
    assert vectors.text("{1:0.5,3:2}/4") == "[0.5,0.0,2.0,0.0]"
    # the sign of a zero, a different element, a different length
    assert vectors.text("[0,1]") != vectors.text("[-0,1]")
    assert vectors.text("[1,2]") != vectors.text("[1,2,0]")
    assert vectors.packed("[0.1,-0,2.5]") == raw


def test_a_vector_is_counted_where_the_target_would_change_it():
    assert canon.unfit("postgres", "vector(3)", "postgres", "halfvec(3)") \
        == [("vector-width", 16)]
    assert ("vector-dims", 4) in canon.unfit("mysql", "vector(3)",
                                             "postgres", "vector(4)")
    assert ("vector-nonfinite", None) in canon.unfit(
        "clickhouse", "Array(Float32)", "postgres", "vector(3)")
    assert canon.unfit_value("vector-width", [0.1], 32) is True
    assert canon.unfit_value("vector-width", [0.5, 2.0], 32) is False
    assert canon.unfit_value("vector-dims", "[1,2]", 3) is True
    assert canon.unfit_value("vector-dims", "[1,2,3]", 3) is False
    assert canon.unfit_value("vector-nonfinite", [1.0, float("nan")],
                             None) is True
    assert canon.unfit_value("vector-nonfinite", [1.0], None) is False


def test_vector_types_by_engine():
    assert canon.type_class("postgres", "vector(3)") == "vector"
    assert canon.type_class("postgres", "halfvec(3)") == "vector"
    assert canon.type_class("mysql", "vector(3)") == "vector"
    assert canon.type_class("clickhouse", "Array(Float32)") == "vector"
    # an array whose elements may be NULL is an array, not an embedding
    assert canon.type_class("clickhouse", "Array(Nullable(Float32))") \
        == "json"
    assert canon.type_class("cassandra", "vector<float, 3>") == "vector"
    assert canon.type_class("duckdb", "FLOAT[3]") == "vector"
    assert canon.ddl_type("postgres", "vector", (3,)) == "vector(3)"
    assert canon.ddl_type("cassandra", "vector", (3,)) == "vector<float, 3>"
    with pytest.raises(ValueError, match="how many"):
        canon.ddl_type("cassandra", "vector")
    assert canon.read_expr("postgres", '"v"', "vector") == \
        '"v"::vector::text'


# ---- shapes ---------------------------------------------------------------

POINT = "0101000020E610000000000000002059400000000000802B40"  # 100.5 13.75


def test_a_shape_is_one_text_whatever_binary_carried_it():
    big = "00000000014059200000000000402B800000000000"  # WKB, big-endian
    assert geo.text(POINT) == geo.text(geo.from_wkb(big, 4326)) \
        == "SRID=4326;010100000000000000002059400000000000802B40"
    assert geo.text({"type": "Point", "coordinates": [100.5, 13.75]}) \
        == geo.text(POINT)
    assert geo.text("4326:010100000000000000002059400000000000802B40") \
        == geo.text(POINT)
    # the SRID is part of it, and so is the order of the two numbers
    assert geo.text(geo.from_wkb(big, 0)) != geo.text(POINT)
    assert geo.text(geo.from_wkb(big, 4326, lat_first=True)) \
        != geo.text(POINT)


def test_mysql_keeps_a_geographic_point_latitude_first():
    raw = geo.to_mysql(POINT)
    assert raw[:4] == struct.pack("<I", 4326)
    # stored: latitude 13.75, then longitude 100.5
    assert struct.unpack_from("<2d", raw, 9) == (13.75, 100.5)
    assert geo.text(geo.from_mysql(raw)) == geo.text(POINT)
    # a projected SRID is stored as it is given
    flat = geo.to_mysql(geo.from_wkb(POINT[:2] + "01000000" + POINT[18:],
                                     3857))
    assert struct.unpack_from("<2d", flat, 9) == (100.5, 13.75)


def test_a_shape_with_depth_and_parts_survives_geojson():
    doc = {"type": "MultiPolygon", "coordinates": [
        [[[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 0, 1]]]]}
    shape = geo.from_geojson(doc)
    assert geo.to_geojson(shape) == {"type": "MultiPolygon", "coordinates": [
        [[[0.0, 0.0, 1.0], [1.0, 0.0, 1.0], [1.0, 1.0, 1.0],
          [0.0, 0.0, 1.0]]]]}
    assert geo.text(geo.from_wkb(geo.ewkb_hex(shape))) == geo.text(shape)
    with pytest.raises(ValueError, match="4326"):
        geo.to_geojson(geo.from_wkb(POINT[:2] + "01000000" + POINT[18:],
                                    3857))
    assert canon.unfit("mongodb", "object", "postgres", "geometry") \
        == [("geometry-form", None)]
    assert canon.unfit_value("geometry-form", {"type": "Point",
                                               "coordinates": [1, 2]},
                             None) is False
    assert canon.unfit_value("geometry-form", {"type": "Circle"}, None) \
        is True
    assert canon.unfit_value("geometry-geojson", POINT, None) is False
    assert canon.unfit_value("geometry-geojson", "3857:" + POINT[:2]
                             + "01000000" + POINT[18:], None) is True


def test_shape_types_by_engine():
    assert canon.type_class("postgres", "geometry(Point,4326)") == "geometry"
    assert canon.type_class("postgres", "geography") == "geometry"
    assert canon.type_class("mysql", "point") == "geometry"
    assert canon.type_class("mssql", "geography") == "geometry"
    assert canon.ddl_numbers("postgres", "geography(Point,4326)",
                             "geometry") == ("geography",)
    assert canon.ddl_type("postgres", "geometry", ("geography",)) \
        == "geography"
    # SQL Server builds both, and is written through each type's own
    # constructor (`commercial.mssql_shape_mark`); its geography reads a
    # polygon by the order of its rings' points, which is asked first
    assert canon.ddl_type("mssql", "geometry") == "geometry"
    assert canon.ddl_type("mssql", "geometry", ("geography",)) \
        == "geography"
    assert canon.unfit("postgres", "geometry", "mssql", "geometry") == []
    assert canon.unfit("postgres", "geometry", "mssql", "geography") \
        == [("ring-orientation", commercial.RING_RULES[("mssql",
                                                        "geography")])]
    assert canon.unfit_value("no-writer", POINT, "shape") is True
    assert canon.unfit_value("no-writer", None, "shape") is False


# ---- BSON types -----------------------------------------------------------

def test_a_bson_value_renders_with_its_type():
    import uuid

    from bson import Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
    from bson.binary import Binary
    oid = ObjectId("65a1b2c3d4e5f6a7b8c9d0e1")
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    got = {bsontype.render(v) for v in (
        5, Int64(5), 5.0, oid, str(oid), Binary.from_uuid(u),
        Binary(u.bytes, 3), u.bytes, datetime.datetime(2024, 1, 1),
        Timestamp(1704067200, 1), Regex("a.*", "i"), MinKey(), MaxKey())}
    # thirteen values, thirteen texts: none of them passes for another
    assert len(got) == 13, got
    sep = bsontype.SEP
    assert bsontype.render(5) == f"int{sep}5"
    assert bsontype.render(Int64(5)) == f"long{sep}5"
    assert bsontype.render(Binary.from_uuid(u)) == f"uuid{sep}{u}"
    # a report shows the value alone, and a text key with a colon in it
    # is not taken for a type
    assert bsontype.shown(bsontype.render(oid)) == str(oid)
    assert bsontype.shown("string:x") == "string:x"


def test_another_engines_value_renders_as_mongodb_holds_it():
    from bson import Int64, ObjectId
    oid = ObjectId("65a1b2c3d4e5f6a7b8c9d0e1")
    assert canon.render_value("bson:long", 5) == \
        canon.render_value("bson/long", Int64(5))
    assert canon.render_value("bson:long", 5) != \
        canon.render_value("bson/long", 5)
    assert canon.render_value("bson:json", {"$oid": str(oid)}) \
        == canon.render_value("bson/json", oid)
    # the hex digits of an ObjectId are a string, not the ObjectId
    assert canon.render_value("bson:string", str(oid)) \
        != canon.render_value("bson/string", oid)
    # a value that has no form of the column's type never matches
    assert canon.render_value("bson:bool", 2).startswith(canon.UNCOMPARABLE)


def test_which_bson_type_a_column_is_held_as():
    tag = bsontype.column_tag
    assert tag("postgres", "integer", "integer") == "int"
    assert tag("postgres", "bigint", "integer") == "long"
    assert tag("mysql", "int unsigned", "integer") == "long"
    assert tag("mysql", "tinyint(1)", "integer") == "bool"
    assert tag("mysql", "tinyint(4)", "integer") == "int"
    assert tag("postgres", "uuid", "uuid") == "uuid"
    assert tag("postgres", "jsonb", "json") == "json"
    assert tag("postgres", "time", "time") is None
    assert canon.bson_pair("mongodb", "int", "postgres", "bigint") \
        == ("bson/long", "bson:long")
    assert canon.bson_pair("postgres", "numeric(10,2)", "mongodb",
                           "decimal") == ("bson:number", "bson/number")
    assert canon.bson_pair("postgres", "bigint", "mysql", "bigint") is None
    assert canon.bson_pair("mongodb", "array", "postgres", "vector(3)") \
        == ("vector", "vector")


def test_two_mongodb_sides_are_compared_with_each_values_type():
    from bson import Int64

    from migkit.config import Endpoint, Hop
    from migkit.engines.base import Engine
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    eng = HeteroEngine(Hop(name="h", engine="hetero", source=ep, target=ep,
                           options={"source_engine": "mongodb",
                                    "target_engine": "mongodb"}))
    got = Engine._classify_columns(eng.src_engine, {"n": "int"},
                                   eng.dst_engine, {"n": "int|long"})
    assert got["pairs"] == [("n", "bson", "bson")], got["pairs"]
    assert canon.render_value("bson", 5) \
        != canon.render_value("bson", Int64(5))
    # and a document written to MongoDB goes as it is
    assert canon.write_value("mongodb", "bson", {"a": Int64(1)}) \
        == {"a": Int64(1)}


def test_the_schema_check_holds_a_field_to_the_type_its_column_keeps():
    """A field of `long`s beside a `bigint` is one kind of value; a field
    of `int`s beside one is not - it comes back a `long`."""
    from migkit.engines.hetero import HeteroEngine
    got = {"pairs": [("l", "bson/long", "bson:long"),
                     ("i", "bson/long", "bson:long"),
                     ("o", "bson/json", "bson:json")],
           "only_src": [], "only_dst": [], "unreadable": [],
           "src_types": {"l": "long", "i": "int", "o": "objectId"},
           "dst_types": {"l": "bigint", "i": "bigint", "o": "jsonb"}}
    said = HeteroEngine._schema_result(None, "cx.t", got)
    assert said.status == "diff", said.detail
    assert "i is int on the source and bigint" in said.detail, said.detail
    assert "l is" not in said.detail and "o is" not in said.detail
    got["pairs"] = got["pairs"][::2]
    assert HeteroEngine._schema_result(None, "cx.t", got).status == "ok"


def test_a_value_goes_into_mongodb_as_its_columns_type():
    import uuid

    from bson import Int64, ObjectId
    from bson.binary import Binary
    got = canon.write_value("mongodb", "bson/long", 5)
    assert isinstance(got, Int64) and got == 5
    assert type(canon.write_value("mongodb", "bson/int", 5)) is int
    u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    got = canon.write_value("mongodb", "bson/uuid", u)
    assert isinstance(got, Binary) and got.subtype == 4
    assert canon.write_value("mongodb", "bson/json",
                             '{"$oid": "65a1b2c3d4e5f6a7b8c9d0e1"}') \
        == ObjectId("65a1b2c3d4e5f6a7b8c9d0e1")
    assert canon.write_value("mongodb", "bson/bool", 1) is True
    with pytest.raises(ValueError):
        canon.write_value("mongodb", "bson/bool", 2)
    # and out of MongoDB into a JSON column, as its Extended JSON
    assert canon.write_value("postgres", "bson:json", Int64(5)) \
        == '{"$numberLong": "5"}'
    assert canon.write_value("mysql", "json", {"a": Int64(5)}) \
        == '{"a": {"$numberLong": "5"}}'
    assert canon.write_value("postgres", "bson:uuid", Binary(
        uuid.UUID(u).bytes, 4)) == u


def test_what_would_not_come_back_as_it_left_is_counted():
    from bson import Int64, ObjectId
    assert canon.unfit("mongodb", "int", "postgres", "bigint") \
        == [("bson-type", "long")]
    # a field holding only the column's type has nothing to count
    assert canon.unfit("mongodb", "int", "postgres", "integer") == []
    assert ("bson-type", "int") in canon.unfit("mongodb", "int|long",
                                               "postgres", "integer")
    # into MongoDB a value is written by its source column's width, whatever
    # the field holds now
    assert canon.unfit("postgres", "bigint", "mongodb", "int") == []
    assert canon.unfit_value("bson-type", 5, "long") is True
    assert canon.unfit_value("bson-type", Int64(5), "long") is False
    assert canon.unfit_value("bson-type", ObjectId(), "string") is True
    assert canon.unfit_value("bson-type", "x", "string") is False
    assert canon.unfit("mongodb", "object", "mysql", "json") \
        == [("bson-order", None)]
    assert canon.unfit_value("bson-order", {"b": 1, "a": 2}, None) is True
    assert canon.unfit_value("bson-order", {"a": [{"b": 1}]}, None) is False
    assert ("bson-none", "time") in canon.unfit(
        "postgres", "time", "mongodb", None)
    assert canon.unfit_value("bson-form", 2, "bool") is True
    assert canon.unfit_value("bson-form", 1, "bool") is False


def test_mongodb_field_types_and_what_is_built_for_them():
    assert canon.type_class("mongodb", "objectId") == "bson"
    assert canon.type_class("mongodb", "uuid") == "uuid"
    assert canon.type_class("mongodb", "binData3") == "bson"
    assert canon.type_class("mongodb", "double|int") == "bson"
    assert canon.type_class("mongodb", "int|long") == "integer"
    assert canon.ddl_numbers("mongodb", "int", "integer") == (32,)
    assert canon.ddl_numbers("mongodb", "int|long", "integer") == ()
    assert canon.ddl_type("postgres", "integer", (32,)) == "integer"
    assert canon.ddl_numbers("mongodb", "object", "bson") == ("document",)
    assert canon.ddl_type("postgres", "bson", ("document",)) == "json"
    assert canon.ddl_type("postgres", "bson", (), key=True) == "jsonb"
    with pytest.raises(ValueError, match="cannot key"):
        canon.ddl_type("mysql", "bson", (), key=True)
    assert canon.mongo_value_class("mongodb", "objectId", "bson") == "text"
    assert canon.by_value("bson/long") == "integer"


def test_a_repair_key_comes_back_out_of_its_typed_text():
    from bson import Int64, ObjectId
    sep = bsontype.SEP
    got = canon.from_text("bson/long", f"long{sep}5")
    assert isinstance(got, Int64) and got == 5
    assert canon.from_text("bson/json",
                           f"objectId{sep}65a1b2c3d4e5f6a7b8c9d0e1") \
        == ObjectId("65a1b2c3d4e5f6a7b8c9d0e1")
    # the other side's JSON column is looked up by what is written there
    got = canon.from_text("bson:json",
                          f"objectId{sep}65a1b2c3d4e5f6a7b8c9d0e1")
    assert canon.write_value("postgres", "bson:json", got) \
        == '{"$oid": "65a1b2c3d4e5f6a7b8c9d0e1"}'
    assert canon.write_value("postgres", "bson:json", "one") == '"one"'


# ---- collections ----------------------------------------------------------

def test_an_empty_collection_is_told_from_null():
    assert canon.type_class("cassandra", "list<int>") == "json"
    assert canon.type_class("cassandra", "frozen<set<text>>") == "json"
    assert canon.type_class("cassandra", "map<text, int>") == "json"
    assert canon.collection_kind("cassandra", "map<text, int>") == "map"
    assert ("empty-collection", "list") in canon.unfit(
        "postgres", "integer[]", "cassandra", "list<int>")
    assert ("empty-collection", "map") in canon.unfit(
        "mongodb", "object", "cassandra", "map<text, int>")
    assert canon.unfit_value("empty-collection", [], "list") is True
    assert canon.unfit_value("empty-collection", "{}", "map") is True
    assert canon.unfit_value("empty-collection", [1], "list") is False
    assert canon.unfit_value("null-element", [1, None], None) is True
    assert canon.unfit_value("null-element", [1, 2], None) is False
    # an empty array and NULL are two renderings
    assert canon.render_value("json", []) == "[]"
    assert canon.render_value("json", None) is None
    assert canon.ddl_type("cassandra", "json", ("array", "integer")) \
        == "list<bigint>"
    # a set keeps its elements sorted and once each
    assert ("set-order", None) in canon.unfit("postgres", "integer[]",
                                              "cassandra", "set<int>")
    assert ("set-order", None) not in canon.unfit("postgres", "integer[]",
                                                  "cassandra", "list<int>")
    assert canon.unfit_value("set-order", [3, 1, 1], None) is True
    assert canon.unfit_value("set-order", [1, 3], None) is False
    # a map keeps its keys sorted: a document's field order is not kept
    assert ("bson-order", None) in canon.unfit(
        "mongodb", "object", "cassandra", "map<text, int>")


def test_a_mongodb_document_goes_into_a_collection_as_the_collection():
    from bson import Int64
    from bson.decimal128 import Decimal128
    got = canon.write_value("cassandra", "bson:json",
                            {"a": Int64(2), "b": [Decimal128("1.50")]})
    assert got == {"a": 2, "b": [Decimal("1.50")]}
    assert type(got["a"]) is int
    # and into a JSON column elsewhere, as its Extended JSON
    assert canon.write_value("postgres", "bson:json", {"a": Int64(2)}) \
        == '{"a": {"$numberLong": "2"}}'


# ---- arrays in ClickHouse -------------------------------------------------

def test_an_array_goes_into_clickhouse_as_the_list_it_is():
    assert canon.write_value("clickhouse", "json", [1, None, 3]) \
        == [1, None, 3]
    # into a JSON column elsewhere it is the JSON text of it
    assert canon.write_value("mysql", "json", [1, None, 3]) == "[1, null, 3]"
    assert canon.ddl_type("clickhouse", "json", ("array", "text")) \
        == "Array(Nullable(String))"
    with pytest.raises(ValueError, match="decimal"):
        canon.ddl_type("clickhouse", "json", ("array", "decimal"))
    assert ("null", None) in canon.unfit(
        "postgres", "integer[]", "clickhouse", "Array(Nullable(Int64))")


def test_clickhouse_reads_an_arrays_text_elements_as_text():
    from migkit.engines.clickhouse import ClickHouseEngine
    assert ClickHouseEngine._value("json", [b"a b", None, b"c"]) \
        == ["a b", None, "c"]
    assert ClickHouseEngine._value("bytes", b"\x00") == b"\x00"


# ---- nine digits ----------------------------------------------------------

def test_sql_server_is_sent_every_digit_it_keeps():
    """Its driver sent a Python time at three digits."""
    from migkit import nanotime
    from pymssql import _mssql
    plain = datetime.datetime(2024, 1, 1, 0, 0, 0, 123456)
    assert _mssql.quote_simple_value(plain) == b"'2024-01-01 00:00:00.123'"
    assert canon.write_value("mssql", "timestamp", plain) \
        == "2024-01-01 00:00:00.123456"
    # a `datetime` column takes three digits and no more in its text
    assert canon.write_value("mssql", "timestamp", datetime.datetime(
        2024, 1, 1, 0, 0, 0, 123000)) == "2024-01-01 00:00:00.123"
    assert canon.write_value("mssql", "timestamp", datetime.datetime(
        2024, 1, 1)) == "2024-01-01 00:00:00"
    seven = nanotime.parse("2024-01-01 00:00:00.1234567")
    assert canon.write_value("mssql", "timestamp", seven) \
        == "2024-01-01 00:00:00.1234567"
    assert canon.write_value("mssql", "time",
                             nanotime.parse("12:00:00.0000001")) \
        == "12:00:00.0000001"
    # another engine's driver takes its own time
    assert canon.write_value("postgres", "timestamp", plain) is plain


def test_db2_reads_twelve_digits_and_marks_what_nine_cannot_hold():
    from migkit.engines.db2 import Db2Engine, _twelve
    eng = Db2Engine.__new__(Db2Engine)
    assert eng._nanos_text('"TS"', "TIMESTAMP(6)") is None
    assert "FF9" in eng._nanos_text('"TS"', "TIMESTAMP(9)")
    assert "FF12" in eng._nanos_text('"TS"', "TIMESTAMP(12)")
    got = _twelve("2024-01-01 00:00:00.123456789000")
    assert canon.render_value("timestamp", got) \
        == "2024-01-01 00:00:00.123456789"
    cut = _twelve("2024-01-01 00:00:00.123456789001")
    assert canon.render_value("timestamp", cut).startswith(
        canon.UNCOMPARABLE)
    assert canon.unfit_value("fraction", cut, 9) is True


def test_snowflake_reads_nine_digits_and_instants_at_utc():
    from migkit.engines.warehouse import SnowflakeEngine
    eng = SnowflakeEngine.__new__(SnowflakeEngine)
    assert eng._nanos_text('"T"', "TIMESTAMP_NTZ") == \
        """to_varchar("T", 'YYYY-MM-DD HH24:MI:SS.FF9')"""
    assert "convert_timezone('UTC'" in eng._nanos_text('"T"', "TIMESTAMP_TZ")
    assert eng._nanos_text('"T"', "TIME") == \
        """to_varchar("T", 'HH24:MI:SS.FF9')"""
    assert eng._nanos_text('"T"', "TIMESTAMP_NTZ(3)") is None
    assert eng._nanos_text('"T"', "NUMBER(38,0)") is None


def test_snowflake_rows_keep_their_ninth_digit(monkeypatch):
    """Through the reader every DB-API engine shares, with the rows as
    the Snowflake driver hands back the selected text (there is no
    Snowflake to run here): nine digits in, nine rendered."""
    from migkit.config import Endpoint, Hop
    from migkit.engines.warehouse import SnowflakeEngine
    eng = SnowflakeEngine(Hop(
        name="sf", engine="hetero",
        options={"source_engine": "snowflake", "target_engine": "postgres"},
        source=Endpoint(host="x", port=1, user="u", password="CHANGE_ME",
                        options={"account": "a", "database": "d"}),
        target=Endpoint(host="x", port=1, user="u", password="CHANGE_ME"),
        databases=["app"]))
    seen = []

    def rows(side, db, sql, args=None):
        seen.append(sql)
        if "information_schema.columns" in sql:
            return [("ID", "NUMBER(38,0)"), ("TS", "TIMESTAMP_NTZ")]
        return [[1, "2024-01-01 00:00:00.123456789"]]
    monkeypatch.setattr(eng, "_rows", rows)
    monkeypatch.setattr(eng, "neutral_key", lambda *a: ["id"])
    got, _ = eng.neutral_read("src", "app", "t", [("id", "integer"),
                                                  ("ts", "timestamp")])
    assert any("to_varchar(\"TS\"" in q for q in seen), seen
    assert canon.render_value("timestamp", got[0][1]) \
        == "2024-01-01 00:00:00.123456789"
