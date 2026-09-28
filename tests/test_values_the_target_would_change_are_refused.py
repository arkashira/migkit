"""A value the target would not keep as it is stops the move before
anything is copied, named by column, kind, count and the keys of a few
rows - instead of being rounded, cut, turned into a double or a default,
or refused by the target halfway through the load (type-fidelity G7, G9-G12,
G25, G26, and the fraction of a second a narrower target rounds).

Measured before, PostgreSQL 16 and MySQL 8.4, each through the copier every
pair shares - the target written, then the move stopped part-way:

* `numeric` 0.12345678901234 into the `decimal(65,10)` built for it was
  stored as 0.1234567890 - MySQL rounds in strict mode with Note 1265 only
  - and the batch's read-back found 7 of 7 rows changed;
* a MySQL TIME of -01:00:00 stopped the copy at `invalid input syntax for
  type time: "-1 day 23:00:00"`, and PostgreSQL's `24:00:00` was written as
  00:00:00 and the read-back of it raised;
* `bigint unsigned` 18446744073709551615: `out of range for type bigint`;
* a year before Christ: `year -44 is out of range`, reading the source;
* a JSON number of 30 digits, and `9088544342.689999`, were stored as other
  numbers and found by the read-back;
* a text array written into the `bigint` built for it: `Incorrect integer
  value`.
"""
import pytest

from tests.typepair import (engine, fresh, move, my, pg, servers,  # noqa
                            verdict)

pytestmark = [pytest.mark.docker]


def _refused(eng, table, schema="public"):
    with pytest.raises(SystemExit) as got:
        move(eng, table, schema)
    return " ".join(str(got.value).split())


def _empty_or_absent(table):
    there = my(f"select count(*) from information_schema.tables where"
               f" table_schema = 'cx' and table_name = '{table}'")
    return there == "0" or my(f"select count(*) from {table}") == "0"


def test_infinity_and_years_before_christ_are_refused(fresh, tmp_path):
    pg("create table t (id int primary key, ts timestamp, d date);"
       " insert into t values (1, 'infinity', '2024-01-01'),"
       " (2, '-infinity', '-infinity'), (3, '0044-03-15 BC', '2024-01-01'),"
       " (4, '2024-01-01', '2024-01-01')")
    said = _refused(engine(tmp_path), "t")
    assert "ts: 2 rows hold infinity or -infinity" in said, said
    assert "id 1, 2" in said, said
    assert "ts: 1 row holds a year before 1 AD" in said and "id 3" in said
    assert "d: 1 row holds infinity" in said, said
    assert _empty_or_absent("t")
    # the rows that fit move, once the rest are decided
    pg("delete from t where id < 4")
    move(engine(tmp_path), "t", "public")
    assert verdict(engine(tmp_path), "t").status == "ok"


def test_a_longer_fraction_is_built_for_not_rounded(fresh, tmp_path):
    values = ("0.12345678901234", "1.5", "1.50", "100", "0", "-0.0",
              "12345678901234567890.5")
    pg("create table n (id int primary key, v numeric); insert into n"
       " values " + ", ".join(f"({i}, {v})" for i, v in enumerate(values)))
    eng = engine(tmp_path)
    move(eng, "n", "public")
    assert my("select column_type from information_schema.columns where"
              " table_schema = 'cx' and table_name = 'n' and"
              " column_name = 'v'") == "decimal(65,14)"
    assert my("select cast(v as char) from n where id = 0") \
        == "0.12345678901234"
    # 1.5 against 1.50000000000000 is one number
    assert verdict(eng, "n").status == "ok"
    my("update n set v = 1.51 where id = 1")
    assert verdict(eng, "n").status == "diff"


def test_a_fraction_no_mysql_decimal_holds_is_refused(fresh, tmp_path):
    pg("create table n (id int primary key, v numeric); insert into n"
       " values (1, 0.1234567890123456789012345678901), (2, 1.5)")
    said = _refused(engine(tmp_path), "n")
    assert "v: 1 row holds more than 10 digits after the point, which" \
        " mysql rounds to 10" in said and "id 1" in said, said


def test_a_narrower_target_decimal_is_counted(fresh, tmp_path):
    pg("create table n (id int primary key, v numeric(12,4));"
       " insert into n values (1, 1.2345), (2, 1.5), (3, 99999999.99)")
    my("create table n (id int primary key, v decimal(10,2))")
    said = _refused(engine(tmp_path), "n")
    assert "v: 1 row holds more than 2 digits after the point" in said, said
    assert "id 1" in said and "id 2" not in said, said
    # a value whose rounding carries past the whole part is counted too
    pg("update n set v = 99999999.995 where id = 3")
    said = _refused(engine(tmp_path), "n")
    assert "v: 1 row holds more digits before the point than mysql's" \
        " decimal(10,2) holds - id 3" in said, said


def test_nan_and_infinity_in_a_decimal_are_refused(fresh, tmp_path):
    pg("create table n (id int primary key, v numeric, f float8);"
       " insert into n values (1, 'NaN', 1), (2, 'Infinity', 'NaN'),"
       " (3, 1, '-Infinity')")
    said = _refused(engine(tmp_path), "n")
    assert "v: 2 rows hold NaN or an infinity in a decimal" in said, said
    assert "f: 2 rows hold NaN or an infinity, which mysql refuses" in said


def test_midnight_at_the_end_of_the_day_is_refused(fresh, tmp_path):
    pg("create table t (id int primary key, tm time);"
       " insert into t values (1, '24:00:00'), (2, '12:00:00')")
    said = _refused(engine(tmp_path), "t")
    assert "tm: 1 row holds the time 24:00:00" in said and "id 1" in said


def test_a_mysql_time_outside_a_day_is_refused(fresh, tmp_path):
    my("create table t (id int primary key, tm time(6));"
       " insert into t values (1, '-01:00:00'), (2, '100:00:00'),"
       " (3, '12:00:00.5')")
    eng = engine(tmp_path, "mysql", "postgres")
    said = _refused(eng, "t", "")
    assert "tm: 2 rows hold a TIME outside 00:00:00 to 23:59:59.999999" \
        in said and "id 1, 2" in said, said
    my("delete from t where id < 3")
    move(engine(tmp_path, "mysql", "postgres"), "t")
    assert pg("select tm::text from t") == "12:00:00.5"
    assert verdict(engine(tmp_path, "mysql", "postgres"), "t").status == "ok"


def test_unsigned_64_bit_into_a_bigint_is_refused(fresh, tmp_path):
    my("create table u (id int primary key, n bigint unsigned);"
       " insert into u values (1, 18446744073709551615),"
       " (2, 9223372036854775807), (3, 9223372036854775808)")
    said = _refused(engine(tmp_path, "mysql", "postgres"), "u", "")
    assert "n: 2 rows hold a whole number outside" \
        " -9223372036854775808..9223372036854775807" in said, said
    assert "id 1, 3" in said, said


def test_nul_and_four_byte_characters_are_refused(fresh, tmp_path):
    my("create table s (id int primary key, v varchar(10));"
       " insert into s values (1, concat('a', char(0), 'b')), (2, 'ab')")
    said = _refused(engine(tmp_path, "mysql", "postgres"), "s", "")
    assert "v: 1 row holds the character U+0000" in said, said
    pg("create table s (id int primary key, v text);"
       " insert into s values (1, 'a😀'), (2, 'é')")
    my("drop table s; create table s (id int primary key, v varchar(10))"
       " character set utf8mb3")
    said = _refused(engine(tmp_path), "s")
    assert "v: 1 row holds a character outside the Basic Multilingual" \
        " Plane" in said and "id 1" in said, said


def test_a_longer_text_than_the_column_built_is_refused(fresh, tmp_path):
    pg("create table s (id int primary key, v text);"
       " insert into s values (1, repeat('x', 2000)), (2, 'ok')")
    said = _refused(engine(tmp_path), "s")
    assert "v: 1 row holds more than 1024 characters" in said, said


def test_a_fraction_of_a_second_a_target_rounds_is_refused(fresh, tmp_path):
    pg("create table t (id int primary key, ts timestamp(6));"
       " insert into t values (1, '2024-01-01 00:00:00.123456'),"
       " (2, '2024-01-01 00:00:01')")
    my("create table t (id int primary key, ts datetime)")
    said = _refused(engine(tmp_path), "t")
    assert "ts: 1 row holds more than 0 digits of a second" in said, said


def test_an_array_moves_as_the_json_array_it_is(fresh, tmp_path):
    """An array of numbers, text or booleans was classed as its element
    type: built as a bigint on MySQL, and the first write failed
    (type-fidelity G15). It is the JSON array of its elements."""
    pg("create table a (id int primary key, i int[], s text[],"
       " n numeric[], b boolean[]); insert into a values"
       " (1, '{1,2,NULL}', '{\"a b\",\"é\"}', '{1.10,2}', '{t,f}'),"
       " (2, '{{1,2},{3,4}}', '{}', null, '{}')")
    eng = engine(tmp_path)
    move(eng, "a", "public")
    assert my("select data_type from information_schema.columns where"
              " table_schema = 'cx' and table_name = 'a' and"
              " column_name = 'i'") == "json"
    assert my("select cast(i as char) from a where id = 2") \
        == "[[1, 2], [3, 4]]"
    assert verdict(eng, "a").status == "ok"
    my("update a set s = json_array('a b', 'e') where id = 1")
    assert verdict(eng, "a").status == "diff"


def test_an_array_stays_an_array_between_two_postgresql(fresh, tmp_path):
    """Read as its list and compared as a JSON array, it is still written
    into an array column as the array it is - and into a `jsonb` one as
    the JSON array."""
    pg("create table a (id int primary key, i int[], s text[]);"
       " insert into a values (1, '{1,2,NULL}', '{\"a b\",c}'),"
       " (2, '{{1,2},{3,4}}', '{}')")
    pg("create database cy", db="postgres")
    try:
        pg("create table a (id int primary key, i int[], s jsonb)", db="cy")
        eng = engine(tmp_path, "postgres", "postgres")
        eng.hop.db_map = {"cx": "cy"}
        move(eng, "a", "public")
        assert pg("select i::text, s::text from a order by id", db="cy") \
            == '{1,2,NULL}|["a b", "c"]\n{{1,2},{3,4}}|[]'
        assert verdict(eng, "a").status == "ok"
    finally:
        pg("drop database cy with (force)", db="postgres")


def test_an_array_that_does_not_start_at_one_is_refused(fresh, tmp_path):
    pg("create table a (id int primary key, i int[]); insert into a values"
       " (1, '[0:1]={5,6}'), (2, '{5,6}')")
    said = _refused(engine(tmp_path), "a")
    assert "i: 1 row holds an array that does not start at 1" in said \
        and "id 1" in said, said


def test_json_numbers_mysql_stores_as_another_are_refused(fresh, tmp_path):
    pg("create table j (id int primary key, j jsonb); insert into j values"
       " (1, '{\"n\": 123456789012345678901234567890}'),"
       " (2, '{\"n\": 9088544342.689999}'),"
       " (3, '{\"n\": 1.10}'), (4, '{\"n\": [0.30000000000000004]}'),"
       " (5, '{\"n\": 18446744073709551615}')")
    said = _refused(engine(tmp_path), "j")
    # 0.30000000000000004 is a double MySQL keeps; 2^64-1 an integer it
    # keeps; 1.10 is 1.1
    assert "j: 2 rows hold a JSON number mysql stores as a double that is" \
        " not the same number" in said and "id 1, 2" in said, said
    pg("delete from j where id < 3")
    move(engine(tmp_path), "j", "public")
    assert my("select cast(j as char) from j where id = 4") \
        == '{"n": [0.30000000000000004]}'


def test_assess_names_them_before_a_move_is_asked(fresh, tmp_path):
    pg("create table t (id int primary key, tm time);"
       " insert into t values (1, '24:00:00')")
    got = [i for i in engine(tmp_path).assess()
           if i["item"] == "cx values the target would change"]
    assert len(got) == 1 and got[0]["level"] == "fail", got
    assert "t.tm: 1 row holds the time 24:00:00" in got[0]["detail"], got
    pg("update t set tm = '23:00:00'")
    got = [i for i in engine(tmp_path).assess()
           if i["item"] == "cx values the target would change"]
    assert got[0]["level"] == "pass", got


#: (column, kind, arg) asked of both servers' SQL and of their values read
#: here, over the rows below: every kind a value in this process can be
#: asked about
ASKED = (
    ("ts", "fraction", 3), ("ts", "fraction", 0), ("n", "scale", 2),
    ("n", "digits", (6, 2)), ("n", "sigdigits", 5),
    ("n", "magnitude", ("1e-3", "1e4")), ("i", "int-range", (-5, 100)),
    ("s", "chars", 3), ("s", "bytes", 3), ("s", "supplementary", "x"),
    ("s", "null", None), ("n", "not-null", None))
PG_ONLY = (("n", "numeric-nonfinite", None), ("f", "float-nonfinite", None),
           ("s", "utf16", 3), ("j", "json-number", None))
MY_ONLY = (("tm", "timeday", None), ("s", "nul", None))


def _both_ways(eng, side, table, asked, columns):
    """(counts in SQL, counts over the values read here) for `asked`."""
    from migkit import canon
    sql = [n for n, _ in eng.count_unfit(side, "cx", table, columns,
                                         list(asked), ["id"])]
    names = [n for n, _ in columns]
    here = [0] * len(asked)
    for rows in eng._every_row(side, "cx", table, columns):
        for r in rows:
            for i, (col, kind, arg) in enumerate(asked):
                here[i] += canon.unfit_value(kind, r[names.index(col)], arg)
    return sql, here


def test_each_question_has_one_answer_in_sql_and_here(fresh, tmp_path):
    """`canon.unfit_sql` and `canon.unfit_value` are two spellings of one
    question; a source with SQL answers the first, every other engine the
    second, and the move is refused or not on what they say."""
    pg("create table q (id int primary key, ts timestamp(6), n numeric,"
       " i bigint, s text, f float8, j jsonb); insert into q values"
       " (1, '2024-01-01 00:00:00.123456', 1.234, 5, 'abcd', 1, '{\"a\": 1}'),"
       " (2, '2024-01-01 00:00:00.5', 10000.5, 101, '😀', 'NaN',"
       "  '{\"a\": 123456789012345678901234567890}'),"
       " (3, '2024-01-01 00:00:01', 0.0001, -6, null, 2, '[1.10]'),"
       " (4, null, 'NaN', null, 'é', 'Infinity', null),"
       " (5, '2024-01-01 00:00:00.123', null, 7, 'ab', 3,"
       "  '{\"b\": 0.30000000000000004}')")
    my("create table q (id int primary key, ts datetime(6),"
       " n decimal(20,6), i bigint, s varchar(10), tm time(6));"
       " insert into q values"
       " (1, '2024-01-01 00:00:00.123456', 1.234, 5, 'abcd', '-01:00:00'),"
       " (2, '2024-01-01 00:00:00.5', 10000.5, 101, '😀', '100:00:00'),"
       " (3, '2024-01-01 00:00:01', 0.0001, -6, null, '12:00:00'),"
       " (4, null, null, null, concat('a', char(0)), '23:59:59.999999'),"
       " (5, '2024-01-01 00:00:00.123', 0.1, 7, 'ab', null)")
    eng = engine(tmp_path)
    for side, dialect, table_engine, extra, types in (
            ("src", "postgres", eng.src_engine, PG_ONLY,
             {"ts": "timestamp", "n": "decimal", "i": "integer",
              "s": "text", "f": "float", "j": "json"}),
            ("src", "mysql", engine(tmp_path, "mysql", "postgres").src_engine,
             MY_ONLY, {"ts": "timestamp", "n": "decimal", "i": "integer",
                       "s": "text", "tm": "time"})):
        asked = list(ASKED) + list(extra)
        columns = [("id", "integer")] + sorted(types.items())
        sql, here = _both_ways(table_engine, side, "q", asked, columns)
        assert sql == here, (dialect, list(zip(asked, sql, here)))
        # and the rows asked about hold some of each and not all of any,
        # so an answer of "none" or of "every row" would show
        assert all(0 < n < 5 for n in sql), (dialect,
                                             list(zip(asked, sql)))


def test_an_instant_into_a_wall_clock_is_named(fresh, tmp_path):
    """A PostgreSQL `timestamptz` - an instant - into a MySQL `datetime` -
    a wall clock - changes what the column means, which no digest of the
    values sees (type-fidelity G28); the deep check of a pair says so, as
    it did only within one engine."""
    pg("create table t (id int primary key, at timestamptz, d date)")
    my("create table t (id int primary key, at datetime(6), d date)")
    got = [r for r in engine(tmp_path).check_deep("cx")
           if r.scope == "cx temporal meaning"]
    assert [r.status for r in got] == ["warn"], [r.__dict__ for r in got]
    assert "t.at timestamp with time zone (instant) -> datetime(6)" \
        " (wall clock)" in got[0].detail, got[0].detail
    my("alter table t modify at timestamp(6) null")
    got = [r for r in engine(tmp_path).check_deep("cx")
           if r.scope == "cx temporal meaning"]
    assert [r.status for r in got] == ["ok"], [r.__dict__ for r in got]
