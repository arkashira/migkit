"""The types only the commercial engines have (backlog W6), decided before
any server is asked: what each is compared as, what each target builds for
it, what is counted and refused before a move, and the form each is written
in. What the servers answer is measured in
`test_sql_server_types_only_it_has.py`, `test_oracle_types_only_it_has.py`
and `test_db2_types_only_it_has.py`.

Every rendering here is held to say "different" too: a base type, an
offset, a byte, an SRID that changed is a different text - the false
"equal" is the bug these types invite (an `int` 5 and an `nvarchar` '5'
read through the driver as one 5).
"""
import datetime
import json
import struct
import types
import uuid
from decimal import Decimal

import pytest

from migkit import canon, commercial, geo, nanotime
from migkit.config import Endpoint, Hop


def _hop(src="mssql", dst="postgres"):
    ep = Endpoint(host="10.0.0.5", port=1, user="u", password="CHANGE_ME")
    return Hop(name="w6", engine="hetero", source=ep, target=ep,
               databases=["a"], options={"source_engine": src,
                                         "target_engine": dst})


# ---- what each type is -----------------------------------------------------

@pytest.mark.parametrize("engine,declared,cls", [
    ("mssql", "sql_variant", "variant"),
    ("mssql", "hierarchyid", "text"),
    ("mssql", "datetimeoffset", "timestamp"),
    ("mssql", "xml(dbo.orders)", "xml"),
    ("mssql", "geography", "geometry"),
    ("mssql", "money", "decimal"),
    ("mssql", "timestamp", None),
    ("mssql", "Point", None),
    ("oracle", "FLOAT(126)", "decimal"),
    ("oracle", "NUMBER", "decimal"),
    ("oracle", "BINARY_DOUBLE", "float"),
    ("oracle", "XMLTYPE", "xml"),
    ("oracle", "SDO_GEOMETRY", "geometry"),
    ("oracle", "BFILE", "bytes"),
    ("oracle", "JSON", "json"),
    ("oracle", "BOOLEAN", "boolean"),
    ("oracle", "LONG RAW", "bytes"),
    ("oracle", '"APP"."ADDRESS_T"', "json"),
    ("oracle", "ROWID", None),
    ("oracle", "ANYDATA", None),
    ("db2", "DECFLOAT(34)", "decimal"),
    ("db2", "XML", "xml"),
    ("db2", "VARGRAPHIC(20)", "text"),
    ("db2i", "XML", "xml"),
    ("db2zos", "XML", "xml"),
    ("teradata", "NUMBER", "decimal"),
    ("teradata", "UDT SYSUDTLIB.MONEY_T", None),
    ("ase", "money", "decimal"),
])
def test_each_type_is_one_class_or_none(engine, declared, cls):
    assert canon.type_class(engine, declared) == cls


@pytest.mark.parametrize("engine,declared", [
    ("mssql", "timestamp"), ("ase", "timestamp"), ("oracle", "ROWID"),
    ("oracle", "UROWID"), ("db2", "ROWID"), ("db2i", "ROWID"),
    ("db2zos", "ROWID")])
def test_row_versions_and_addresses_are_not_data(engine, declared):
    assert "not data" in commercial.not_data(engine, declared)
    # never refused or counted: there is nothing of the source's to keep
    assert canon.unfit(engine, declared, "postgres", "bytea") == []
    assert commercial.not_data("postgres", "timestamp") is None
    assert commercial.not_data("oracle", "TIMESTAMP(6)") is None


def _sides(src, dst):
    return (types.SimpleNamespace(CANON_ENGINE=src, TEXT_HOLDS_BYTES=()),
            types.SimpleNamespace(CANON_ENGINE=dst, TEXT_HOLDS_BYTES=()))


def test_a_pair_is_compared_as_the_types_hold_it():
    """A row version is said and left out; a datetimeoffset beside an
    Oracle time with a zone is compared with its offset; a sql_variant
    beside the JSON it is kept in as a sql_variant; sixteen bytes beside a
    UUID as the UUID."""
    from migkit.engines.base import Engine
    s, d = _sides("mssql", "oracle")
    got = Engine._classify_columns(
        s, {"rv": "timestamp", "at": "datetimeoffset", "id": "int"},
        d, {"rv": "RAW(8)", "at": "timestamp with time zone",
            "id": "NUMBER(10,0)"})
    pairs = {n: (a, b) for n, a, b in got["pairs"]}
    assert pairs["at"] == (canon.OFFSET, canon.OFFSET)
    assert "rv" not in pairs
    assert [n for n, _ in got["unreadable"]] == ["rv"]
    assert "row version" in got["unreadable"][0][1]
    s, d = _sides("mssql", "postgres")
    got = Engine._classify_columns(
        s, {"v": "sql_variant", "g": "binary(16)", "at": "datetimeoffset"},
        d, {"v": "jsonb", "g": "uuid", "at": "timestamp with time zone"})
    pairs = {n: (a, b) for n, a, b in got["pairs"]}
    assert pairs["v"] == (canon.VARIANT, canon.VARIANT)
    assert pairs["g"] == ("uuid", "uuid")
    # PostgreSQL keeps the instant alone: compared as the instant
    assert pairs["at"] == ("timestamp", "timestamp")
    # one engine, one type: compared exactly, offset and all
    s, d = _sides("mssql", "mssql")
    got = Engine._classify_columns(s, {"at": "datetimeoffset"},
                                   d, {"at": "datetimeoffset"})
    assert got["pairs"] == [("at", canon.OFFSET, canon.OFFSET)]
    assert got["how"]["at"][0] == "exactly"


# ---- sql_variant -------------------------------------------------------------

def _v(text):
    return commercial.variant_of(text)


def test_a_sql_variant_is_its_base_type_and_its_value():
    five = [_v("int|10|0|4|5"), _v("bigint|19|0|8|5"),
            _v("nvarchar|0|0|2|5"), _v("decimal|10|2|9|5.00")]
    texts = [canon.render_value(canon.VARIANT, v) for v in five]
    assert texts == ["int:5", "bigint:5", "nvarchar(1):5",
                     "decimal(10,2):5.00"]
    # four values, never one: read as the base value alone they were
    assert len(set(texts)) == 4
    assert five[0].value == 5 and five[3].value == Decimal("5.00")
    assert str(five[0]) == "int:5"


@pytest.mark.parametrize("text,declared,value", [
    ("float|53|0|8|0.10000000000000001", "float", 0.1),
    ("real|24|0|4|0.10000000149011612", "real", 0.10000000149011612),
    ("money|19|4|8|1.5000", "money", Decimal("1.5000")),
    ("bit|1|0|1|1", "bit", True),
    ("date|10|0|3|2024-02-29", "date", datetime.date(2024, 2, 29)),
    ("varbinary|0|0|8|00FF41", "varbinary(8)", b"\x00\xffA"),
    ("uniqueidentifier|0|0|16|A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
     "uniqueidentifier", uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")),
    ("varchar|0|0|10|caf|é", "varchar(10)", "caf|é"),
])
def test_each_base_type_is_read_from_the_servers_text(text, declared,
                                                       value):
    got = _v(text)
    assert got.declared == declared
    assert got.value == value


def test_digits_and_offsets_of_a_variant_are_kept():
    t = _v("datetime2|27|7|8|2024-02-29 13:14:15.1234567")
    assert t.declared == "datetime2(7)"
    assert canon.render_value(canon.VARIANT, t) == \
        "datetime2(7):2024-02-29 13:14:15.123456700"
    a = _v("datetimeoffset|34|7|10|2024-01-01 07:00:00.0000000 +07:00")
    b = _v("datetimeoffset|34|7|10|2024-01-01 00:00:00.0000000 +00:00")
    # one instant, two values
    assert canon.render_value(canon.VARIANT, a) != \
        canon.render_value(canon.VARIANT, b)
    assert canon.render_value(canon.VARIANT, a).endswith("+07:00")


def test_a_variant_kept_as_json_elsewhere_is_the_same_variant():
    v = _v("decimal|10|2|9|1.50")
    kept = canon.write_value("postgres", "json", v)
    assert json.loads(kept) == {"type": "decimal(10,2)", "value": "1.50"}
    # what PostgreSQL hands back (a document) and MySQL (its text)
    for back in (json.loads(kept), kept):
        assert canon.render_value(canon.VARIANT, back) == \
            canon.render_value(canon.VARIANT, v)
    # and a variant of another type is another text
    assert canon.render_value(canon.VARIANT, {
        "type": "decimal(10,3)", "value": "1.500"}) != \
        canon.render_value(canon.VARIANT, v)
    assert canon.write_value("mongodb", "bson", v) == {
        "type": "decimal(10,2)", "value": "1.50"}
    # into SQL Server a key is compared by its base value
    assert canon.write_value("mssql", canon.VARIANT, v) == Decimal("1.50")
    assert json.loads(canon.sql_value(v))["value"] == "1.50"
    # what is no variant at all is a difference, never a stop
    assert canon.render_value(canon.VARIANT, "5").startswith(
        canon.UNCOMPARABLE)


def test_sql_server_writes_a_variant_through_its_base_type():
    assert commercial.variant_mark(_v("decimal|10|2|9|1.50")) == \
        ("convert(decimal(10,2), {})", "1.50")
    assert commercial.variant_mark(_v("datetime|23|3|8|2024-01-01"
                                      " 00:00:00.003")) == \
        ("convert(datetime, {}, 121)", "2024-01-01 00:00:00.003")
    assert commercial.variant_mark(_v("varbinary|0|0|8|00FF")) == \
        ("convert(varbinary(8), convert(varchar(max), {}), 2)", "00FF")
    assert commercial.variant_mark(_v("real|24|0|4|0.5"))[0] == \
        "convert(real, convert(float, {}))"
    assert commercial.variant_mark(_v("nvarchar|0|0|2|5")) == \
        ("convert(nvarchar(1), {})", "5")
    # the JSON another engine kept writes back the same
    assert commercial.variant_mark({"type": "int", "value": "5"}) == \
        ("convert(int, {})", "5")
    assert commercial.variant_mark(5) is None


def test_a_variant_goes_as_json_only_where_the_hop_accepts_it():
    assert ("as-json", "a SQL Server sql_variant") in canon.unfit(
        "mssql", "sql_variant", "postgres", "jsonb")
    assert canon.unfit("mssql", "sql_variant", "mssql", "sql_variant") == []
    assert canon.unfit_value("as-json", _v("int|10|0|4|5"), None) is True
    assert canon.unfit_value("as-json", None, None) is False
    # not a value JSON has no form for: it goes as its type and text
    assert canon.unfit_value("json-form", _v(
        "datetime|23|3|8|2024-01-01 00:00:00.000"), None) is False
    assert canon.ddl_type("postgres", canon.VARIANT) == "jsonb"
    assert canon.ddl_type("mssql", canon.VARIANT, (), True) == "sql_variant"
    with pytest.raises(ValueError):
        canon.ddl_type("mysql", canon.VARIANT, (), True)


def test_a_column_of_variants_digests_alike_and_a_base_type_change_does_not():
    read = [[1, _v("int|10|0|4|5")], [2, _v("nvarchar|0|0|20|abc")]]
    kept = [[1, {"type": "int", "value": "5"}],
            [2, json.dumps({"type": "nvarchar(10)", "value": "abc"})]]
    classes = ["integer", canon.VARIANT]
    assert canon.fold_rows(classes, read) == canon.fold_rows(classes, kept)
    kept[0][1] = {"type": "bigint", "value": "5"}
    assert canon.fold_rows(classes, read) != canon.fold_rows(classes, kept)


# ---- hierarchyid, datetimeoffset and the zones -------------------------------

def test_sql_server_reads_its_own_types_whole():
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop())
    assert eng._read_as("[h]", "hierarchyid") == ("[h].ToString()", str)
    sql, parse = eng._read_as("[v]", "sql_variant")
    assert "sql_variant_property([v], 'BaseType')" in sql
    assert parse is commercial.variant_of
    sql, parse = eng._read_as("[t]", "datetimeoffset")
    assert sql == "convert(varchar(34), [t], 121)"
    got = parse("2024-02-29 13:14:15.1234567 +07:00")
    assert got.utcoffset() == datetime.timedelta(hours=7)
    assert got.nanosecond == 700 and got.microsecond == 123456
    assert eng._read_as("[n]", "nvarchar(10)") is None
    # a CLR type by its own name, which its system type id does not say
    assert "user_type_id" in eng.DECLARED
    assert "xml_schema_collections" in eng.DECLARED


def _zoned(utc, zone):
    from migkit.engines.oracle import Zoned
    return Zoned.at(utc, zone)


def test_an_instant_and_its_offset_are_one_value_on_both_engines():
    utc = datetime.datetime(2024, 1, 1, 0, 0, 0, 123456,
                            tzinfo=datetime.timezone.utc)
    mine = commercial.offset_time("2024-01-01 07:00:00.123456 +07:00")
    theirs = _zoned(utc, "+07:00")
    assert canon.render_value(canon.OFFSET, mine) == \
        canon.render_value(canon.OFFSET, theirs) == \
        "2024-01-01 00:00:00.123456 +07:00"
    # the same instant at another offset is another value
    other = commercial.offset_time("2024-01-01 00:00:00.123456 +00:00")
    assert canon.render_value(canon.OFFSET, other) != \
        canon.render_value(canon.OFFSET, mine)
    assert canon.render_value("timestamp", other) == \
        canon.render_value("timestamp", mine)
    # UTC by its names is one zone
    assert canon.render_value(canon.OFFSET, _zoned(utc, "UTC")) == \
        canon.render_value(canon.OFFSET, _zoned(utc, "+00:00"))
    assert commercial.zone_text("+7:00") == "+07:00"
    assert commercial.zone_text("Europe/Paris") == "EUROPE/PARIS"


def test_an_offset_dropped_or_a_zone_cut_is_refused_before_the_move():
    assert ("offset", None) in canon.unfit("mssql", "datetimeoffset",
                                           "postgres", "timestamptz(6)")
    assert ("offset", None) in canon.unfit(
        "oracle", "timestamp with time zone", "oracle",
        "timestamp with local time zone")
    # both keep the offset: nothing to ask of the source
    assert canon.unfit("mssql", "datetimeoffset", "oracle",
                       "TIMESTAMP(9) WITH TIME ZONE") == []
    assert ("zone-region", None) in canon.unfit(
        "oracle", "timestamp with time zone", "mssql", "datetimeoffset")
    utc = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    seven = commercial.offset_time("2024-01-01 07:00:00 +07:00")
    assert canon.unfit_value("offset", seven, None) is True
    assert canon.unfit_value("offset", utc, None) is False
    assert canon.unfit_value("offset", _zoned(utc, "UTC"), None) is False
    assert canon.unfit_value("offset", datetime.datetime(2024, 1, 1),
                             None) is False
    assert canon.unfit_value("zone-region", _zoned(utc, "Europe/Paris"),
                             None) is True
    assert canon.unfit_value("zone-region", _zoned(utc, "+01:00"),
                             None) is False


def test_sql_server_is_written_a_time_at_its_own_offset():
    utc = datetime.datetime(2024, 1, 1, 0, 0, 0, 123456,
                            tzinfo=datetime.timezone.utc)
    nine = nanotime.of(utc, 789)
    assert commercial.mssql_offset_text(_zoned(nine, "+07:00")) == \
        "2024-01-01 07:00:00.1234567 +07:00"
    # a region at the offset it had then: Paris in January is +01:00
    assert commercial.mssql_offset_text(_zoned(utc, "Europe/Paris")) \
        == "2024-01-01 01:00:00.1234560 +01:00"
    assert commercial.mssql_offset_text(datetime.datetime(2024, 1, 1)) \
        == "2024-01-01 00:00:00.0000000 +00:00"
    assert canon.write_value("mssql", canon.OFFSET, _zoned(utc, "+07:00")) \
        == "2024-01-01 07:00:00.1234560 +07:00"
    # into a column of no offset, the instant at UTC as before
    assert canon.write_value("mssql", "timestamp", _zoned(utc, "+07:00")) \
        == "2024-01-01 00:00:00.123456"
    mark = commercial.mssql_mark("datetimeoffset")
    assert mark(_zoned(utc, "+07:00")) == (
        "convert(datetimeoffset(7), {}, 121)",
        "2024-01-01 07:00:00.1234560 +07:00")
    assert mark("2024-01-01") is None


def test_oracle_is_written_a_time_in_the_zone_it_came_with():
    from migkit.engines.oracle import Bind
    b = Bind("at", "TIMESTAMP WITH TIME ZONE")
    seven = commercial.offset_time("2024-01-01 07:00:00 +07:00")
    assert b.zone(seven) == "+07:00"
    utc = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    assert b.zone(utc) == "UTC"
    assert b.zone(_zoned(utc, "Europe/Paris")) == "Europe/Paris"


def test_a_column_that_keeps_its_offset_is_built_as_one():
    assert canon.ddl_numbers("mssql", "datetimeoffset", "timestamp") == \
        ("offset",)
    assert canon.ddl_numbers("oracle", "timestamp with time zone",
                             "timestamp") == ("offset",)
    assert canon.ddl_type("oracle", "timestamp", ("offset",)) == \
        "TIMESTAMP(9) WITH TIME ZONE"
    assert canon.ddl_type("mssql", "timestamp", ("offset",)) == \
        "datetimeoffset(7)"
    assert canon.ddl_type("postgres", "timestamp", ("offset",)) == \
        "timestamptz(6)"
    assert canon.ddl_type("mysql", "timestamp", ("offset",)) == \
        "datetime(6)"


# ---- shapes -----------------------------------------------------------------

SQUARE = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
HOLE = [[2, 2], [2, 4], [4, 4], [4, 2], [2, 2]]


def _poly(*rings, srid=4326):
    return geo.from_geojson({"type": "Polygon",
                             "coordinates": list(rings)}, srid)


def test_a_polygon_drawn_the_wrong_way_round_is_found():
    assert geo.misoriented(_poly(SQUARE)) is False
    assert geo.misoriented(_poly(SQUARE[::-1])) is True
    assert geo.misoriented(_poly(SQUARE, HOLE)) is False
    assert geo.misoriented(_poly(SQUARE, HOLE[::-1])) is True
    multi = geo.from_geojson({"type": "MultiPolygon", "coordinates": [
        [SQUARE], [[[20, 0], [20, 5], [25, 5], [25, 0], [20, 0]]]]})
    assert geo.misoriented(multi) is True
    # around the antimeridian, the short way: a small ring drawn
    # counter-clockwise
    across = [[179, 0], [-179, 0], [-179, 1], [179, 1], [179, 0]]
    assert geo.misoriented(_poly(across)) is False
    assert geo.misoriented(_poly(across[::-1])) is True
    assert geo.misoriented(geo.from_geojson({"type": "Point",
                                             "coordinates": [1, 2]})) \
        is False


def test_a_target_that_reads_ring_order_asks_before_the_move():
    rule = commercial.RING_RULES[("mssql", "geography")]
    assert canon.unfit("postgres", "geography", "mssql", "geography") == \
        [("ring-orientation", rule)]
    assert ("ring-orientation", commercial.RING_RULES[(
        "oracle", "sdo_geometry")]) in canon.unfit(
        "postgres", "geometry", "oracle", "SDO_GEOMETRY")
    assert canon.unfit("postgres", "geometry", "mysql", "geometry") == []
    assert canon.unfit_value("ring-orientation", _poly(SQUARE[::-1]),
                             rule) is True
    assert canon.unfit_value("ring-orientation", _poly(SQUARE), rule) \
        is False
    assert canon.unfit_value("ring-orientation", "no shape", rule) is True


def test_sql_server_is_written_a_shape_through_its_constructor():
    point = geo.from_geojson({"type": "Point", "coordinates": [100.5, 13.75]})
    got = commercial.mssql_shape_mark("geography", point)
    assert got == ("geography::STGeomFromWKB({}, 4326)", geo.wkb(point))
    # Oracle's own number for the system, at EPSG's
    oracle = geo.Shape(8307, "Point", False, False, [100.5, 13.75])
    assert commercial.mssql_shape_mark("geography", oracle) == got
    # Z and M as text, which SQL Server reads them from
    zm = geo.Shape(0, "Point", True, True, [1.5, 2.0, 3.0, 4.0])
    assert commercial.mssql_shape_mark("geometry", zm) == (
        "geometry::STGeomFromText({}, 0)", "POINT (1.5 2.0 3.0 4.0)")
    m = geo.Shape(0, "Point", False, True, [1.5, 2.0, 4.0])
    assert commercial.mssql_shape_mark("geometry", m)[1] == \
        "POINT (1.5 2.0 NULL 4.0)"
    assert commercial.mssql_shape_mark("geometry", "no shape") is None
    assert geo.wkt(geo.Shape(0, "Polygon", True, False, [
        [[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 0, 1]]])) == \
        "POLYGON ((0.0 0.0 1.0, 1.0 0.0 1.0, 1.0 1.0 1.0, 0.0 0.0 1.0))"
    assert geo.wkt(geo.Shape(0, "MultiPoint", True, False, [
        ("Point", True, False, [1, 2, 3])])) == "MULTIPOINT ((1.0 2.0 3.0))"
    assert geo.wkt(geo.Shape(0, "Point", True, False,
                             [float("nan")] * 3)) == "POINT EMPTY"


def test_oracles_own_number_for_a_system_is_epsgs():
    ours = geo.Shape(8307, "Point", False, False, [100.5, 13.75])
    theirs = geo.Shape(4326, "Point", False, False, [100.5, 13.75])
    assert geo.text(ours) == geo.text(theirs)
    # and no other number meets one it is not
    assert geo.text(geo.Shape(3857, "Point", False, False, [100.5, 13.75])) \
        != geo.text(theirs)
    assert canon.write_value("postgres", canon.GEOMETRY, ours) == \
        geo.ewkb_hex(theirs)
    # into Oracle as it was read
    from migkit.engines.oracle import Bind
    b = Bind("g", "SDO_GEOMETRY")
    assert b.values(ours) == [1, geo.wkb(ours), 8307]
    assert b.values(geo.Shape(0, "Point", False, False, [1, 2]))[2] is None
    assert b.values(None) == [None, None, None]
    assert "sdo_geometry(" in b.mark() and b.mark().count("\x00?\x00") == 3


# ---- XML ----------------------------------------------------------------------

def test_xml_a_target_holds_otherwise_is_asked_first():
    assert canon.unfit("postgres", "xml", "oracle", "XMLTYPE") == \
        [("xml-fragment", None)]
    assert ("xml-fragment", None) in canon.unfit("mssql", "xml", "db2",
                                                 "XML")
    assert canon.unfit("oracle", "XMLTYPE", "postgres", "xml") == []
    assert canon.unfit("postgres", "xml", "mssql", "xml(dbo.orders)") == \
        [("xml-typed", "dbo.orders")]
    assert canon.unfit("mssql", "xml(dbo.orders)", "mssql",
                       "xml(dbo.orders)") == []
    assert canon.unfit_value("xml-fragment", "<a/><b/>", None) is True
    assert canon.unfit_value("xml-fragment", "x<b/>", None) is True
    assert canon.unfit_value("xml-fragment", "<a><b/>t</a>", None) is False
    assert canon.ddl_type("oracle", "xml") == "XMLTYPE"
    assert canon.ddl_type("db2", "xml") == "XML"
    assert commercial.built_without("mssql", "xml(dbo.orders)", {},
                                    "postgres").startswith(
        "its documents are checked against the schema collection dbo.orders")
    assert commercial.built_without("mssql", "xml(dbo.orders)", {},
                                    "mssql") is None
    assert "FILESTREAM" in commercial.built_without(
        "mssql", "varbinary(max)", {"filestream": True}, "postgres")
    assert commercial.built_without("mssql", "varbinary(max)",
                                    {"filestream": False}, "postgres") is None


def test_oracle_writes_a_document_from_its_text():
    from migkit.engines.oracle import Bind
    b = Bind("x", "XMLTYPE")
    assert "xmltype(" in b.mark()
    assert b.values("<a/>") == ["<a/>", "<a/>"]


# ---- decimals ---------------------------------------------------------------

def test_a_float_of_oracle_is_a_decimal_and_never_a_double():
    assert commercial.floating("oracle", "FLOAT(126)")[0] == 38
    assert commercial.floating("oracle", "FLOAT(10)")[0] == 3
    assert canon.ddl_numbers("oracle", "FLOAT(126)", "decimal") == ()
    # 38 digits rendered as they are; as a double they were 17
    v = Decimal("0.12345678901234567890123456789012345678")
    assert canon.render_value(canon.NUMBER, v) == str(v)
    assert canon.render_value(canon.NUMBER, v) != canon.render_value(
        canon.NUMBER, Decimal(repr(float(v))))


def test_decfloat_is_held_to_its_digits_and_its_range():
    assert commercial.floating("db2", "DECFLOAT(16)") == \
        (16, ("1e-398", "1e385"))
    got = canon.unfit("postgres", "numeric", "db2", "DECFLOAT(16)")
    assert ("sigdigits", 16) in got
    assert ("magnitude", ("1e-398", "1e385")) in got
    # DECFLOAT holds NaN and the infinities; DECIMAL does not
    assert ("numeric-nonfinite", None) not in got
    assert ("numeric-nonfinite", None) in canon.unfit(
        "db2", "DECFLOAT(34)", "mysql", "decimal(65,30)")
    # nineteen digits of a bigint do not fit sixteen; ten of an int do
    assert ("sigdigits", 16) in canon.unfit("postgres", "bigint", "db2",
                                            "DECFLOAT(16)")
    assert ("sigdigits", 16) not in canon.unfit("postgres", "integer",
                                                "db2", "DECFLOAT(16)")
    assert canon.unfit_value("sigdigits", Decimal("12345678901234567"),
                             16) is True
    assert canon.unfit_value("sigdigits", Decimal("1234567890123456"),
                             16) is False
    assert canon.ddl_numbers("db2", "DECFLOAT(34)", "decimal") == ()


def test_db2_reads_a_decfloat_as_its_own_text():
    from migkit.engines.db2 import DECLARED, Db2Engine, _decfloat
    eng = Db2Engine(_hop("db2", "postgres"))
    assert eng._read_as('"D"', "DECFLOAT(34)") == ('rtrim(char("D"))',
                                                   _decfloat)
    assert eng._read_as('"X"', "XML")[0] == 'xmlserialize("X" as clob(2G))'
    assert _decfloat("1.50E+3 ") == Decimal("1.50E+3")
    assert _decfloat("NaN").is_nan() and _decfloat("-Infinity") < 0
    # by value, as PostgreSQL's numeric is compared beside it
    assert canon.render_value(canon.NUMBER, _decfloat("1.50")) == \
        canon.render_value(canon.NUMBER, Decimal("1.5"))
    assert canon.render_value(canon.NUMBER, _decfloat("1.51")) != \
        canon.render_value(canon.NUMBER, Decimal("1.5"))
    # a distinct type by the built-in type it is made from
    assert "syscat.datatypes" in DECLARED and "DECFLOAT(" in DECLARED


def test_a_double_into_a_number_that_holds_no_nan_is_asked():
    got = canon.unfit("postgres", "double precision", "oracle", "NUMBER")
    assert ("float-nonfinite", None) in got
    assert ("magnitude", ("1e-130", "1e126")) in got
    assert canon.unfit_value("magnitude", 5e-324, ("1e-130", "1e126")) \
        is True
    assert canon.unfit_value("magnitude", 1.5, ("1e-130", "1e126")) is False
    assert ("float-nonfinite", None) not in canon.unfit(
        "postgres", "double precision", "oracle", "BINARY_DOUBLE")
    assert ("float-nonfinite", None) in canon.unfit(
        "postgres", "double precision", "postgres", "bigint")


def test_money_is_built_at_its_places_and_held_to_its_range():
    assert canon.ddl_numbers("mssql", "money", "decimal") == (19, 4)
    assert canon.ddl_numbers("ase", "smallmoney", "decimal") == (10, 4)
    lo, hi = commercial.MONEY["money"]
    assert ("money-range", (lo, hi)) in canon.unfit("postgres", "numeric",
                                                    "mssql", "money")
    assert canon.unfit("mssql", "smallmoney", "mssql", "money") == []
    assert ("money-range", commercial.MONEY["smallmoney"]) in canon.unfit(
        "mssql", "money", "mssql", "smallmoney")
    assert canon.unfit_value("money-range",
                             Decimal("922337203685477.5808"), (lo, hi)) \
        is True
    assert canon.unfit_value("money-range",
                             Decimal("922337203685477.5807"), (lo, hi)) \
        is False
    assert canon.unfit_value("money-range", 1e16, (lo, hi)) is True
    assert canon.unfit_value("money-range", Decimal("NaN"), (lo, hi)) \
        is False


# ---- UUIDs kept as bytes ----------------------------------------------------------

def test_sixteen_bytes_beside_a_uuid_are_the_uuid_in_rfc_order():
    u = uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert canon.pair_classes("mssql", "binary(16)", "bytes", "postgres",
                              "uuid", "uuid") == ("uuid", "uuid")
    assert canon.pair_classes("mssql", "varbinary(max)", "bytes",
                              "postgres", "uuid", "uuid") == ("bytes",
                                                              "uuid")
    assert canon.render_value("uuid", u.bytes) == canon.render_value(
        "uuid", u)
    # SQL Server's own binary form of the same UUID is another value
    assert canon.render_value("uuid", u.bytes_le) != canon.render_value(
        "uuid", u)
    assert canon.write_value("mssql", "bytes", u) == u.bytes
    assert canon.write_value("postgres", "uuid", u.bytes) == str(u)
    assert canon.unfit("mssql", "binary(16)", "postgres", "uuid") == \
        [("uuid-order", "read from")]
    assert canon.unfit("postgres", "uuid", "mssql", "binary(16)") == \
        [("uuid-order", "written as")]
    assert canon.unfit("postgres", "uuid", "mssql", "uniqueidentifier") \
        == []


# ---- refused by name, kept as JSON, a BFILE ------------------------------

def test_a_type_with_no_form_elsewhere_is_refused_by_name():
    assert canon.unfit("mssql", "Point", "postgres", "bytea") == \
        [("refused-type", "SQL Server Point")]
    # carried as its bytes between two SQL Servers
    assert canon.unfit("mssql", "Point", "mssql", "Point") == []
    assert ("refused-type", "Oracle Database ANYDATA") in canon.unfit(
        "oracle", "ANYDATA", "postgres", "jsonb")
    assert canon.unfit("teradata", "UDT SYSUDTLIB.MONEY_T", "postgres",
                       "text") == [("refused-type",
                                    "Teradata UDT SYSUDTLIB.MONEY_T")]
    assert canon.unfit_value("refused-type", b"\x01", "x") is True
    assert canon.unfit_value("refused-type", None, "x") is False
    # an engine of no such types refuses nothing new
    assert canon.unfit("postgres", "int4range", "mysql", "json") == []


def test_an_oracle_object_goes_as_json_only_where_the_hop_accepts_it():
    got = canon.unfit("oracle", '"APP"."ADDRESS_T"', "postgres", "jsonb")
    assert ("as-json", "an Oracle object or collection") in got
    assert ("no-writer", "value of an Oracle object type") in canon.unfit(
        "oracle", '"APP"."ADDRESS_T"', "oracle", '"APP"."ADDRESS_T"')

    class Attr:
        def __init__(self, name):
            self.name = name

    class Kind:
        def __init__(self, names, collection=False):
            self.attributes = [Attr(n) for n in names]
            self.iscollection = collection

    class Obj:
        def __init__(self, kind, **values):
            self.type = kind
            self.__dict__.update(values)

    class Coll:
        def __init__(self, items):
            self.type = Kind([], True)
            self.items = items

        def aslist(self):
            return self.items
    address = Obj(Kind(["STREET", "SINCE", "CODE"]), STREET="1 Main",
                  SINCE=datetime.datetime(2024, 1, 2, 3, 4, 5),
                  CODE=b"\x0a\x0b")
    got = commercial.oracle_value(Coll([address, None]))
    assert got == [{"STREET": "1 Main", "SINCE": "2024-01-02T03:04:05",
                    "CODE": "0A0B"}, None]
    assert canon.json_write(got)
    assert commercial.oracle_value(5) == 5


def test_a_bfile_is_refused_and_built_as_what_it_carries():
    assert ("bfile", None) in canon.unfit("oracle", "BFILE", "postgres",
                                          "bytea")
    assert ("bfile", None) in canon.unfit("oracle", "BFILE", "oracle",
                                          "BLOB")
    assert commercial.own_type("oracle", "BFILE") == "BLOB"
    assert commercial.own_type("oracle", "BLOB") == "BLOB"
    assert canon.unfit_value("bfile", b"%PDF", None) is True


def test_graphic_columns_count_in_utf16_and_varchar_in_bytes():
    assert ("utf16", 4) in canon.unfit("postgres", "text", "db2",
                                       "VARGRAPHIC(4)")
    assert ("bytes", 4) in canon.unfit("postgres", "text", "db2",
                                       "VARCHAR(4)")
    assert canon.unfit_value("utf16", "\U0001D11Eabc", 4) is True
    assert canon.unfit_value("utf16", "abcd", 4) is False
    assert canon.unfit_value("bytes", "café", 4) is True


# ---- the read and the write on the engines --------------------------------

def test_oracle_reads_each_of_its_own_types_as_what_it_holds():
    from migkit.engines.oracle import OracleEngine
    eng = OracleEngine(_hop("oracle", "postgres"))
    facts = [{"name": "x", "kind": "XMLTYPE"},
             {"name": "g", "kind": "SDO_GEOMETRY"},
             {"name": "f", "kind": "BFILE"},
             {"name": "o", "kind": "ADDRESS_T", "object": True}]
    eng._facts = lambda side, db, table: facts
    select, finish = eng._read_plan("src", "app", "t", [
        ("x", "xml"), ("g", "geometry"), ("f", "bytes"), ("o", "json")])
    assert 'xmlserialize(content "X" as clob)' in select
    assert 'treat("G" as mdsys.sdo_geometry).sdo_srid' in select
    assert 'sdo_util.to_wkbgeometry("G")' in select
    assert 'to_blob("F")' in select
    point = geo.from_geojson({"type": "Point", "coordinates": [100.5, 13.75]})

    class Kind:
        attributes = [types.SimpleNamespace(name="CITY")]
        iscollection = False
    obj = types.SimpleNamespace(type=Kind(), CITY="Bangkok")
    row = finish([["<a/>", 8307, geo.wkb(point), b"%PDF", 4, obj,
                   "AAAA"]])[0]
    assert row[0] == "<a/>"
    assert row[1].srid == 8307 and geo.text(row[1]) == geo.text(
        geo.Shape(4326, "Point", False, False, [100.5, 13.75]))
    assert row[2] == b"%PDF"
    assert row[3] == {"CITY": "Bangkok"}
    nulls = finish([[None, None, None, None, None, None, "AAAA"]])[0]
    assert nulls == [None, None, None, None]


class _Cursor:
    def __init__(self, calls):
        self.calls, self.rowcount = calls, 1

    def execute(self, sql, args=None):
        self.calls.append(("one", sql, args))

    def executemany(self, sql, rows):
        self.calls.append(("many", sql, list(rows)))

    def fetchone(self):
        return (0,)


class _Conn:
    def __init__(self, calls):
        self.calls = calls

    def cursor(self):
        return _Cursor(self.calls)

    def commit(self):
        self.calls.append(("commit",))

    def close(self):
        pass


def test_sql_server_is_written_each_value_through_its_columns_type(
        monkeypatch):
    """A batch of variants of two base types is two statements, each
    converting its values from their text into their base type; a shape
    goes through its type's constructor with its SRID."""
    from migkit.engines.dbapi import DbapiRows
    from migkit.engines.mssql import MSSQLEngine
    calls = []
    eng = MSSQLEngine(_hop("postgres", "mssql"))
    shape = {"declared": {"id": "int", "v": "sql_variant",
                          "g": "geography", "at": "datetimeoffset"},
             "key": ["id"], "identity": None, "unwritable": set(),
             "utf8": set()}
    monkeypatch.setattr(MSSQLEngine, "_shape",
                        lambda self, side, db, table: shape)
    monkeypatch.setattr(MSSQLEngine, "_connect",
                        lambda self, side, db: _Conn(calls))
    monkeypatch.setattr(MSSQLEngine, "neutral_key",
                        lambda self, side, db, table: ["id"])
    monkeypatch.setattr(MSSQLEngine, "_before_insert",
                        lambda self, cur, side, db, table: None)
    point = geo.from_geojson({"type": "Point", "coordinates": [100.5, 13.75]})
    at = commercial.offset_time("2024-01-01 07:00:00 +07:00")
    cols = [("id", "integer"), ("v", canon.VARIANT), ("g", canon.GEOMETRY),
            ("at", canon.OFFSET)]
    rows = [[1, {"type": "int", "value": "5"}, point, at],
            [2, {"type": "nvarchar(3)", "value": "abc"}, point, None],
            [3, {"type": "int", "value": "7"}, None, at]]
    assert DbapiRows.neutral_write(eng, "dst", "a", "dbo.t", cols, rows) == 3
    inserts = [c for c in calls if c[0] == "many"]
    assert len(inserts) == 3, inserts
    by = {c[1]: c[2] for c in inserts}
    first = next(s for s in by if "convert(int, %s)" in s
                 and "STGeomFromWKB" in s)
    assert "geography::STGeomFromWKB(%s, 4326)" in first
    assert "convert(datetimeoffset(7), %s, 121)" in first
    assert by[first] == [(1, "5", geo.wkb(point),
                          "2024-01-01 07:00:00.0000000 +07:00")]
    # a NULL goes as a parameter, whatever the column's type
    third = next(s for s in by if "convert(int, %s)" in s and s != first)
    assert by[third] == [(3, "7", None,
                          "2024-01-01 07:00:00.0000000 +07:00")]
    second = next(s for s in by if "convert(nvarchar(3), %s)" in s)
    assert by[second][0][1] == "abc"


def test_a_change_is_applied_through_the_same_expressions(monkeypatch):
    from migkit.engines.mssql import MSSQLEngine
    calls = []
    eng = MSSQLEngine(_hop("postgres", "mssql"))
    shape = {"declared": {"id": "int", "v": "sql_variant"}, "key": ["id"],
             "identity": None, "unwritable": set(), "utf8": set()}
    monkeypatch.setattr(MSSQLEngine, "_shape",
                        lambda self, side, db, table: shape)
    cur = _Cursor(calls)
    eng._upsert_one(cur, "dst", "a", "dbo.t", {"id": 1},
                    {"v": {"type": "bigint", "value": "5"}})
    sql, args = calls[0][1], calls[0][2]
    assert "set [v] = convert(bigint, %s)" in sql
    assert args == ("5", 1)


def test_sql_servers_own_row_hash_reads_a_variant_with_its_base_type(
        monkeypatch):
    """Between two SQL Servers each row is hashed as the server writes it
    (`FOR JSON`), where a `sql_variant` was its base value alone: a
    `bigint` 5 written where the source held an `int` 5 hashed alike."""
    from migkit.engines.mssql import MSSQLEngine
    eng = MSSQLEngine(_hop("mssql", "mssql"))
    monkeypatch.setattr(
        MSSQLEngine, "_rows", lambda self, side, db, sql, args=None: [
            ["id", "int", False, 56], ["rv", "timestamp", False, 189],
            ["v", "sql_variant", False, 98], ["h", "hierarchyid", True, 240]])
    row, left = eng._row_json("cx", "dbo.t")
    assert "sql_variant_property(t.[v], 'BaseType')" in row
    assert "t.[h].ToString()" in row
    assert left == ["dbo.t.rv (rowversion)"]


def test_a_struct_of_shapes_is_no_shape():
    with pytest.raises((ValueError, struct.error, IndexError)):
        geo.of(b"\x01\x02")
