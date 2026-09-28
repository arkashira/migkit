"""The rules behind carrying a value exactly, decided before any server is
asked (docs/research/type-fidelity-2026-09-28.md). What the servers answer
is measured in `test_values_no_rendering_writes_are_never_equal.py`,
`test_values_the_target_would_change_are_refused.py` and
`test_keys_a_target_collation_merges_are_refused.py`.
"""
import datetime
from decimal import Decimal

import pytest

from migkit import canon as c


def test_values_with_no_shared_text_are_each_marked_by_name():
    """One marker for all of them made a NaN and an Infinity one value."""
    got = {c.render_value("float", v) for v in
           (float("nan"), float("inf"), float("-inf"))}
    assert len(got) == 3, got
    assert all(g.startswith(c.UNCOMPARABLE) for g in got), got
    assert c.render_value("float", float("nan")) == c.uncomparable("NaN")
    # never the marker for a real number, and never NULL
    assert not c.render_value("float", 0.5).startswith(c.UNCOMPARABLE)


def test_postgres_marks_infinity_and_years_it_alone_holds():
    sql = c.expr("postgres", "ts", "timestamp")
    assert "isfinite" in sql and "'0001-01-01'" in sql, sql
    assert "'10000-01-01'" in sql and c.UNCOMPARABLE in sql, sql


def test_a_single_precision_float_is_rendered_as_its_double():
    assert '"r"::float8' in c.expr("postgres", "r", "float")
    assert "(`r` * 1e0)" in c.expr("mysql", "r", "float")
    assert c.read_expr("mysql", "`r`", "float") == "(`r` * 1e0)"
    assert c.read_expr("postgres", '"r"', "float") == '"r"::float8'
    assert c.read_expr("mysql", "`t`", "text") == "`t`"


def test_a_single_precision_float_is_built_as_one():
    assert c.ddl_numbers("mysql", "float", "float") == (4,)
    assert c.ddl_numbers("mysql", "double", "float") == ()
    assert c.ddl_numbers("postgres", "real", "float") == (4,)
    assert c.ddl_type("postgres", "float", (4,)) == "real"
    assert c.ddl_type("postgres", "float", ()) == "double precision"
    assert c.ddl_type("mysql", "float", (4,)) == "float"
    assert c.ddl_type("clickhouse", "float", (4,)) == "Float32"
    # an engine with no single precision builds its double
    assert c.ddl_type("bigquery", "float", (4,)) == "FLOAT64"


def test_a_time_renders_its_digits_past_the_microsecond_when_it_has_any():
    """Nine where the value carries a digit past the sixth, six where it
    does not - so what every engine holds renders as it always has, and a
    value a six-digit engine cannot hold never meets one it can."""
    from migkit import nanotime
    full = nanotime.parse("2024-01-01 00:00:00.123456789")
    assert c.render_value("timestamp", full) \
        == "2024-01-01 00:00:00.123456789"
    assert c.render_value("timestamp", full.replace(microsecond=123456)) \
        == "2024-01-01 00:00:00.123456"
    assert c.render_value("time", nanotime.parse("12:00:00.0000001")) \
        == "12:00:00.000000100"
    # a value with nothing past the sixth digit is a plain one
    assert type(nanotime.parse("2024-01-01 00:00:00.123456000")) \
        is datetime.datetime


def test_a_mysql_time_renders_past_a_day_and_below_zero():
    """pymysql hands a TIME over as a timedelta, which had no strftime."""
    td = datetime.timedelta
    assert c.render_value("time", td(hours=-1)) == "-01:00:00.000000"
    assert c.render_value("time", td(hours=100)) == "100:00:00.000000"
    assert c.render_value("time", td(seconds=-1.25)) == "-00:00:01.250000"
    assert c.render_value("time", td(hours=12, microseconds=500000)) \
        == "12:00:00.500000"
    assert c.render_value("time", datetime.time(12, 0, 0, 500000)) \
        == "12:00:00.500000"


@pytest.mark.parametrize("written,normal", [
    ("1.50", "1.5"), ("100", "100"), ("1E+2", "100"), ("-0.00", "0"),
    ("0E-10", "0"), ("123.4500", "123.45"), ("-0.5000", "-0.5"),
    ("NaN", "NaN")])
def test_a_number_is_its_value_not_its_digits(written, normal):
    assert c.render_value(c.NUMBER, Decimal(written)) == normal


def test_which_decimal_pairs_are_compared_by_value():
    assert c.decimal_class("postgres", "numeric", "mysql",
                           "decimal(65,10)") == c.NUMBER
    assert c.decimal_class("postgres", "numeric(10,2)", "mysql",
                           "decimal(12,2)") == "decimal"
    assert c.decimal_class("postgres", "numeric(10,2)", "mysql",
                           "decimal(12,4)") == c.NUMBER
    # both keep each value's own scale: `1.50` against `1.5` is a change
    assert c.decimal_class("postgres", "numeric", "mongodb",
                           "decimal") == "decimal"
    assert c.decimal_class("postgres", "numeric", "oracle",
                           "number") == c.NUMBER


def test_the_kinds_asked_depend_on_the_pair():
    assert c.unfit("postgres", "timestamp without time zone", "mysql",
                   "datetime(6)") == [("nonfinite", None), ("years", None)]
    assert ("fraction", 0) in c.unfit("postgres", "timestamp", "mysql",
                                      "datetime")
    assert c.unfit("postgres", "timestamp", "postgres", "timestamp") == []
    assert c.unfit("mysql", "bigint unsigned", "postgres", "bigint") == \
        [("int-range", (-2 ** 63, 2 ** 63 - 1))]
    assert c.unfit("mysql", "bigint", "postgres", "bigint") == []
    assert ("scale", 10) in c.unfit("postgres", "numeric", "mysql",
                                    "decimal(65,10)")
    assert c.unfit("mysql", "decimal(12,2)", "postgres",
                   "numeric(12,2)") == []
    assert ("supplementary", "utf8mb3") in c.unfit(
        "postgres", "text", "mysql", "varchar(10)", {"charset": "utf8mb3"})
    assert ("charset", "latin1") in c.unfit(
        "postgres", "text", "mysql", "varchar(10)", {"charset": "latin1"})
    assert ("nul", None) in c.unfit("mysql", "varchar(10)", "postgres",
                                    "text")
    assert ("null", None) in c.unfit("postgres", "bigint", "clickhouse",
                                     "Int64", {"null": False})
    assert ("null", None) not in c.unfit("postgres", "bigint", "clickhouse",
                                         "Int64", {"null": True})


@pytest.mark.parametrize("kind,value,arg,hit", [
    ("timeday", datetime.timedelta(hours=-1), None, True),
    ("timeday", datetime.timedelta(hours=23, minutes=59), None, False),
    ("timeday", datetime.timedelta(hours=24), None, True),
    ("fraction", datetime.datetime(2024, 1, 1, 0, 0, 0, 123456), 3, True),
    ("fraction", datetime.datetime(2024, 1, 1, 0, 0, 0, 123000), 3, False),
    ("scale", Decimal("1.2345"), 2, True),
    ("scale", Decimal("1.2300"), 2, False),
    ("digits", Decimal("9999999999.995"), (12, 2), True),
    ("digits", Decimal("9999999999.99"), (12, 2), False),
    ("sigdigits", Decimal("1." + "1" * 38), 38, True),
    ("sigdigits", Decimal("1E+40"), 38, False),
    ("magnitude", Decimal("1E-131"), c.DYNAMO_RANGE, True),
    ("magnitude", 5e-324, c.DYNAMO_RANGE, True),
    ("magnitude", 1.5, c.DYNAMO_RANGE, False),
    ("int-range", 2 ** 63, (-2 ** 63, 2 ** 63 - 1), True),
    ("int-range", 2 ** 63 - 1, (-2 ** 63, 2 ** 63 - 1), False),
    ("numeric-nonfinite", Decimal("NaN"), None, True),
    ("float-nonfinite", float("-inf"), None, True),
    ("float-nonfinite", 1.0, None, False),
    ("nul", "a\x00b", None, True),
    ("supplementary", "a\U0001F600", "utf8mb3", True),
    ("supplementary", "é", "utf8mb3", False),
    ("charset", "ā", "latin1", True),
    ("charset", "é€", "latin1", False),
    ("utf16", "\U0001F600" * 3, 5, True),
    ("chars", "x" * 11, 10, True),
    ("chars", "x" * 10, 10, False),
    ("null", None, None, True),
    ("json-number", '{"n": 123456789012345678901234567890}', None, True),
    ("json-number", '{"n": [1.10, 2, "x"]}', None, False),
    ("json-form", {"t": datetime.datetime(2024, 1, 1)}, None, True),
    ("json-form", {"n": [Decimal("1.50"), None, "x"]}, None, False),
])
def test_a_value_read_here_is_asked_the_same_questions(kind, value, arg,
                                                       hit):
    assert c.unfit_value(kind, value, arg) is hit


def test_every_kind_has_words_and_a_mysql_and_postgres_answer():
    for kind in c.UNFIT_WORDS:
        if kind == "merge":
            continue
        for engine in ("mysql", "postgres"):
            got = c.unfit_sql(engine, kind, "c", 3 if kind in (
                "fraction", "scale", "sigdigits", "chars", "bytes",
                "utf16") else (12, 2) if kind == "digits" else
                c.DYNAMO_RANGE if kind == "magnitude" else
                (0, 9) if kind == "int-range" else "latin1")
            assert got is None or isinstance(got, str), (engine, kind)
        assert c.unfit_words(kind, (12, 2), "mysql")


def test_a_json_number_worth_asking_the_target_about():
    assert c.json_candidate(123456789012345678901234567890)
    assert not c.json_candidate(18446744073709551615)
    assert c.json_candidate(Decimal("9088544342.689999"))
    assert not c.json_candidate(Decimal("1.10"))
    assert not c.json_candidate(True)


def test_a_document_is_written_with_its_numbers():
    """`default=str` wrote a Decimal as the string "1.50"."""
    got = c.sql_value({"b": Decimal("1.50"), "a": [1, 2.5, None, True]})
    assert got == '{"a": [1, 2.5, null, true], "b": 1.50}', got
    with pytest.raises(ValueError, match="datetime"):
        c.sql_value({"t": datetime.datetime(2024, 1, 1)})
    with pytest.raises(ValueError, match="no JSON form"):
        c.sql_value({"n": Decimal("NaN")})


def test_bytes_in_either_of_postgres_text_forms():
    assert c.from_text("bytes", "\\x00ff41") == b"\x00\xffA"
    assert c.from_text("bytes", "\\000\\377A") == b"\x00\xffA"
    assert c.from_text("bytes", "a\\\\b") == b"a\\b"


def test_an_xml_value_is_its_canonical_document():
    """What SQL Server writes back for what PostgreSQL keeps as given."""
    kept = '<?xml version="1.0"?><a  b="1" c=\'2\'> <b/> <!-- n --></a>'
    rewritten = '<a b="1" c="2"><b /><!-- n --></a>'
    assert c.render_value("xml", kept) == c.render_value("xml", rewritten)
    # a changed attribute, text or comment is still a change
    for other in ('<a b="2" c="2"><b /><!-- n --></a>',
                  '<a b="1" c="2"><b>x</b><!-- n --></a>',
                  '<a b="1" c="2"><b /><!-- m --></a>'):
        assert c.render_value("xml", other) != c.render_value("xml", kept)
    # a fragment of several, and text that is not XML at all
    assert c.render_value("xml", "x<b/>y") == "x<b></b>y"
    assert c.render_value("xml", "not <xml") == "not <xml"
    assert "xml" in c.FOLDED_HERE


def test_a_uuid_is_one_text_whatever_case_it_was_kept_in():
    import uuid
    u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert c.render_value("uuid", uuid.UUID(u)) == u
    assert c.render_value("uuid", u.upper()) == u
    # any other text is compared as it is
    assert c.render_value("uuid", "{" + u + "}") == "{" + u + "}"
    assert c.render_value("uuid", "x") == "x"
    assert c.type_class("postgres", "uuid") == "uuid"
    assert c.type_class("mssql", "uniqueidentifier") == "uuid"
    assert c.ddl_type("mysql", "uuid") == "char(36)"


def _pair(src="cassandra", dst="postgres", **options):
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    return HeteroEngine(Hop(name="h", engine="hetero", source=ep, target=ep,
                            options={"source_engine": src,
                                     "target_engine": dst, **options}))


def test_keys_that_differ_by_the_sign_of_a_zero_are_found(monkeypatch):
    eng = _pair()
    monkeypatch.setattr(
        eng.src_engine, "_every_row",
        lambda *a, **k: iter([[[0.0], [1.5], [-0.0]], [[2.0], [-1.5]]]))
    groups, shown = eng._signed_zero_keys("db", "z", ["k"],
                                          {"k": "float"}, None)
    assert groups == 1, shown
    assert [[repr(v) for v in k] for k in shown[0]] == [["0.0"], ["-0.0"]]
    monkeypatch.setattr(eng.src_engine, "_every_row",
                        lambda *a, **k: iter([[[0.0], [1.5], [-1.5]]]))
    assert eng._signed_zero_keys("db", "z", ["k"], {"k": "float"},
                                 None) == (0, [])


def test_a_column_the_hop_accepts_changing_is_named_not_refused():
    eng = _pair(accept_changes=["orders.placed_*", "t.x"])
    assert eng._accepted("public.orders", "placed_at")
    assert eng._accepted("t", "x")
    assert not eng._accepted("public.orders", "total")
    assert not _pair()._accepted("t", "x")


def test_an_array_of_scalars_is_a_json_array_and_no_other_is():
    assert c.type_class("postgres", "integer[]") == "json"
    assert c.type_class("postgres", "character varying(10)[]") == "json"
    assert c.type_class("duckdb", "INTEGER[]") == "json"
    assert c.type_class("clickhouse", "Array(Nullable(Int64))") == "json"
    # a timestamp has no JSON form, so an array of them has no class
    assert c.type_class("postgres", "timestamp without time zone[]") is None
    assert c.type_class("clickhouse", "Array(DateTime)") is None
    assert c.render_value("json", [1, None, "a b", [2.5, True]]) \
        == '[1, null, "a b", [2.5, true]]'


def test_the_change_stream_reads_an_array_as_the_list_it_is():
    """`test_decoding` writes an array's type with brackets of its own
    (`integer[]`), and the first `]` ended the type there."""
    from migkit import pgslot
    got = pgslot.parse_line(
        "table public.t: INSERT: id[integer]:1 a[integer[]]:'{1,NULL,3}'"
        r""" s[text[]]:'{"a b","q\"t",c}' m[integer[]]:'{{1,2},{3,4}}'"""
        " b[integer[]]:'[0:1]={5,6}' n[numeric[]]:'{1.10}'")
    values = {n: pgslot.value(t, v, q) for n, t, v, q in got["new"]}
    assert values == {"id": 1, "a": [1, None, 3], "s": ["a b", 'q"t', "c"],
                      "m": [[1, 2], [3, 4]], "b": [5, 6],
                      "n": [Decimal("1.10")]}, values


def test_an_address_renders_with_its_prefix_always():
    import ipaddress
    assert c.render_value("inet", "10.0.0.1") == "10.0.0.1/32"
    assert c.render_value("inet", "10.0.0.1/24") == "10.0.0.1/24"
    assert c.render_value("inet", ipaddress.IPv4Address("10.0.0.1")) \
        == "10.0.0.1/32"
    assert c.render_value("inet", "2001:DB8:0:0:0:0:0:1") \
        == "2001:db8::1/128"
    assert c.render_value("inet", "::ffff:1.2.3.4") == "::ffff:1.2.3.4/128"
    assert c.render_value("inet", "not one") == "not one"
    assert c.unfit("postgres", "inet", "clickhouse", "IPv4") \
        == [("inet-mask", None)]
    assert c.unfit_value("inet-mask", "10.0.0.1/24", None)
    assert not c.unfit_value("inet-mask", "10.0.0.1", None)


def test_what_is_folded_here_is_folded_only_across_two_engines():
    """Across two engines JSON is compared in this process, as the two
    servers print it differently; within one engine and one type it is
    the server's own form on both sides, rendered where the rows are."""
    from migkit.engines.base import Engine
    across = _pair("postgres", "mysql")
    got = Engine._classify_columns(
        across.src_engine, {"j": "jsonb", "x": "xml", "a": "inet"},
        across.dst_engine, {"j": "json", "x": "longtext", "a": "varchar(49)"})
    assert sorted(got["pairs"]) == [("a", "inet", "inet"),
                                    ("j", "json", "json"),
                                    ("x", "xml", "xml")], got["pairs"]
    assert all(s in c.FOLDED_HERE for _, s, _ in got["pairs"])
    one = _pair("mysql", "mysql")
    got = Engine._classify_columns(one.src_engine, {"j": "json"},
                                   one.dst_engine, {"j": "json"})
    assert got["pairs"] == [("j", c.JSON_ONE, c.JSON_ONE)], got["pairs"]
    assert c.JSON_ONE not in c.FOLDED_HERE
    # one engine, two types: across two forms, so folded here
    got = Engine._classify_columns(one.src_engine, {"j": "json"},
                                   one.dst_engine, {"j": "longtext"})
    assert got["pairs"] == [("j", "json", "text")], got["pairs"]
