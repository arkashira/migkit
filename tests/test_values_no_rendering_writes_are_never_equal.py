"""A value one engine holds and the other has no text for is never read as
equal to something else - and a value is compared as the number it is, not
as the digits a session happened to print.

Each of these compared equal before, measured on PostgreSQL 16 and MySQL
8.4 (type-fidelity G1, G2, G4):

* `to_char()` of a PostgreSQL `infinity` is NULL, so a target holding NULL
  where the source held `infinity` passed;
* `to_char()` of `0044-03-15 BC` prints `0044-03-15`, so the year before
  Christ and the year after were one text;
* a database set to `extra_float_digits = 0` prints 0.1 + 0.2 as `0.3`, so
  a target holding 0.3 passed - and a move read the 15 digits and wrote
  them, changing the value with nothing to say so;
* NaN, Infinity and -Infinity were one marker, so a NaN and an Infinity
  passed as the same value.

And the other direction, a single-precision float: MySQL sends a FLOAT's
text at six significant digits and PostgreSQL a `real`'s as the shortest
float text, so the same float read as a difference, and a move out of
MySQL wrote 16777216 as 16777200.
"""
import pytest

from tests.typepair import (engine, fresh, move, my, pg, servers,  # noqa
                            verdict)

pytestmark = [pytest.mark.docker]


def test_infinity_is_not_null(fresh, tmp_path):
    pg("create table t (id int primary key, ts timestamp, tz timestamptz);"
       " insert into t values (1, 'infinity', 'infinity'),"
       " (2, '-infinity', '-infinity'), (3, null, null),"
       " (4, '2024-01-01 00:00:00', '2024-01-01 00:00:00+00')")
    my("create table t (id int primary key, ts datetime(6),"
       " tz datetime(6)); insert into t values (1, null, null),"
       " (2, null, null), (3, null, null),"
       " (4, '2024-01-01 00:00:00', '2024-01-01 00:00:00')")
    eng = engine(tmp_path)
    got = verdict(eng, "t")
    assert got.status == "diff", got.detail
    # the finite rows are still compared, and equal: only the two rows
    # with an infinity differ
    pg("delete from t where id in (1, 2)")
    my("delete from t where id in (1, 2)")
    assert verdict(eng, "t").status == "ok"


def test_the_same_infinity_on_both_sides_is_equal(fresh, tmp_path):
    """Marked, not dropped: two servers that both hold `infinity` hold the
    same value, and saying otherwise would be a difference invented by
    the comparison."""
    pg("create table t (id int primary key, ts timestamp);"
       " insert into t values (1, 'infinity'), (2, '-infinity')")
    pg("create database cy", db="postgres")
    pg("create table t (id int primary key, ts timestamp);"
       " insert into t values (1, 'infinity'), (2, '-infinity')", db="cy")
    try:
        eng = engine(tmp_path, "postgres", "postgres")
        eng.hop.db_map = {"cx": "cy"}
        assert verdict(eng, "t").status == "ok"
        pg("update t set ts = '-infinity' where id = 1", db="cy")
        assert verdict(eng, "t").status == "diff"
    finally:
        pg("drop database cy with (force)", db="postgres")


def test_a_year_before_christ_is_not_the_year_after(fresh, tmp_path):
    pg("create table t (id int primary key, ts timestamp);"
       " insert into t values (1, '0044-03-15 12:00:00 BC'),"
       " (2, '2024-01-01')")
    my("create table t (id int primary key, ts datetime(6));"
       " insert into t values (1, '0044-03-15 12:00:00'),"
       " (2, '2024-01-01')")
    # the premise: MySQL holds the AD year the digits spell
    assert my("select ts from t where id = 1") == "0044-03-15 12:00:00.000000"
    got = verdict(engine(tmp_path), "t")
    assert got.status == "diff", got.detail


def test_a_database_that_prints_short_floats_is_read_whole(fresh, tmp_path):
    pg("alter database cx set extra_float_digits = 0")
    pg("create table t (id int primary key, f double precision);"
       " insert into t values (1, 0.1::float8 + 0.2::float8)")
    # the premise: this database's own sessions print it short
    assert pg("select f::text from t") == "0.3"
    my("create table t (id int primary key, f double);"
       " insert into t values (1, 0.3)")
    eng = engine(tmp_path)
    got = verdict(eng, "t")
    assert got.status == "diff", got.detail
    # and the move carries the value, not its short text
    my("delete from t")
    move(eng, "t", "public")
    assert my("select cast(f as char) from t") == "0.30000000000000004"
    assert verdict(eng, "t").status == "ok"


def test_nan_and_infinity_are_not_one_value(fresh, tmp_path):
    pg("create table t (id int primary key, f double precision);"
       " insert into t values (1, 'NaN'), (2, 'Infinity'),"
       " (3, '-Infinity')")
    pg("create database cy", db="postgres")
    pg("create table t (id int primary key, f double precision);"
       " insert into t values (1, 'Infinity'), (2, '-Infinity'),"
       " (3, 'NaN')", db="cy")
    try:
        eng = engine(tmp_path, "postgres", "postgres")
        eng.hop.db_map = {"cx": "cy"}
        assert verdict(eng, "t").status == "diff"
        pg("update t set f = 'NaN' where id = 1; update t set f ="
           " 'Infinity' where id = 2; update t set f = '-Infinity'"
           " where id = 3", db="cy")
        assert verdict(eng, "t").status == "ok"
    finally:
        pg("drop database cy with (force)", db="postgres")


FLOATS = ("0.1", "1.1", "3.4028234e38", "1e-40", "16777217", "-2.5e-8")


def test_a_single_precision_float_is_one_value_on_both(fresh, tmp_path):
    rows = ", ".join(f"({i}, {v})" for i, v in enumerate(FLOATS))
    pg(f"create table t (id int primary key, r real);"
       f" insert into t values {rows}")
    my(f"create table t (id int primary key, r float);"
       f" insert into t values {rows}")
    # the premise: the two print the same floats differently
    assert pg("select r::text from t where id = 0") == "0.1"
    assert my("select cast(r as char) from t where id = 2") == "3.40282e38"
    for src, dst in (("postgres", "mysql"), ("mysql", "postgres")):
        got = verdict(engine(tmp_path, src, dst), "t")
        assert got.status == "ok", (src, got.detail)
    my("update t set r = 0.2 where id = 0")
    assert verdict(engine(tmp_path), "t").status == "diff"


def test_a_float_moved_out_of_mysql_keeps_every_digit(fresh, tmp_path):
    rows = ", ".join(f"({i}, {v}, {v})" for i, v in enumerate(FLOATS))
    my(f"create table t (id int primary key, r float, d double);"
       f" insert into t values {rows}")
    eng = engine(tmp_path, "mysql", "postgres")
    move(eng, "t")
    # built as a float, not widened into a double
    assert pg("select format_type(atttypid, atttypmod) from pg_attribute"
              " where attrelid = 't'::regclass and attname = 'r'") == "real"
    assert pg("select r::float8::text from t where id = 4") == "16777216"
    assert pg("select r::float8::text from t where id = 2") \
        == "3.4028234663852886e+38"
    assert verdict(eng, "t").status == "ok"


def test_money_is_compared_as_its_number(fresh, tmp_path):
    """A `money` printed its locale's text, `$1,234.56`, where every other
    engine's decimal prints 1234.56 (type-fidelity G17)."""
    pg("create table t (id int primary key, m money);"
       " insert into t values (1, 1234.56), (2, -0.5)")
    assert pg("select m::text from t where id = 1") == "$1,234.56"
    my("create table t (id int primary key, m decimal(19,2));"
       " insert into t values (1, 1234.56), (2, -0.5)")
    assert verdict(engine(tmp_path), "t").status == "ok"
    my("update t set m = 1234.57 where id = 1")
    assert verdict(engine(tmp_path), "t").status == "diff"


def test_a_latin1_database_is_hashed_as_utf8(fresh, tmp_path):
    """`md5(text)` hashed the database's own encoding: `é` is one byte in
    a LATIN1 database and two everywhere else (type-fidelity G21)."""
    pg("create database cl encoding 'LATIN1' template template0"
       " lc_collate 'C' lc_ctype 'C'", db="postgres")
    try:
        # psql sends the database's encoding when it is not talking to a
        # terminal, so the UTF-8 of the seed is said to be UTF-8
        pg("set client_encoding to 'UTF8';"
           " create table t (id int primary key, s text);"
           " insert into t values (1, 'café'), (2, 'plain')", db="cl")
        # the premise: one byte for `é` in this database
        assert pg("select octet_length(s), length(s) from t where id = 1",
                  db="cl") == "4|4"
        my("create table t (id int primary key, s varchar(10));"
           " insert into t values (1, 'café'), (2, 'plain')")
        eng = engine(tmp_path)
        eng.hop.databases = ["cl"]
        eng.hop.db_map = {"cl": "cx"}
        got = [r for r in eng.check_data("cl")
               if r.check == "data" and r.scope == "cl.t"]
        assert [r.status for r in got] == ["ok"], [r.detail for r in got]
        my("update t set s = 'cafe' where id = 1")
        got = [r for r in eng.check_data("cl")
               if r.check == "data" and r.scope == "cl.t"]
        assert [r.status for r in got] == ["diff"], [r.detail for r in got]
    finally:
        pg("drop database cl with (force)", db="postgres")


def test_json_is_compared_by_what_it_holds(fresh, tmp_path):
    """MySQL writes `1.10` back as `1.1` and 10^20 as `1e20`; a `json`
    value holding `\\u0000` stopped the whole PostgreSQL digest at
    `::jsonb` (type-fidelity G23, G24)."""
    docs = ('{"é": 1, "z": 2, "aa": 3, "b": {"y": 1.10, "x": 2}}',
            '{"n": 100000000000000000000}', '{"a": "\\u0000"}')
    rows = ", ".join(f"({i}, '{d}')" for i, d in enumerate(docs))
    pg(f"create table t (id int primary key, j json);"
       f" insert into t values {rows}")
    # a MySQL string literal reads a backslash as an escape
    rows = rows.replace("\\", "\\\\")
    my(f"create table t (id int primary key, j json);"
       f" insert into t values {rows}")
    # the premise: MySQL's own text of them is not jsonb's
    assert my("select cast(j as char) from t where id = 1") == '{"n": 1e20}'
    got = verdict(engine(tmp_path), "t")
    assert got.status == "ok", got.detail
    my("""update t set j = '{"a": "b"}' where id = 2""")
    assert verdict(engine(tmp_path), "t").status == "diff"


def test_bytes_print_as_hex_whatever_the_database_says(fresh, tmp_path):
    """The change stream is read as text in a session of migkit's own: a
    database set to `bytea_output = escape` printed `\\000\\377A` there,
    which the reader of it took as nine characters (type-fidelity G13)."""
    pg("alter database cx set bytea_output = 'escape'")
    assert pg("select '\\x00ff41'::bytea::text") == "\\000\\377A"
    src = engine(tmp_path).src_engine
    got = src._psql("src", "cx", "select '\\x00ff41'::bytea::text")
    assert got == "\\x00ff41", got
    from migkit import canon
    assert canon.from_text("bytes", got) == b"\x00\xffA"


def test_an_address_is_compared_as_the_address_it_is(fresh, tmp_path):
    """A PostgreSQL `inet` against the text a MySQL column keeps it as:
    one address in capitals or with its zeros written out is the same
    address, and a mask is part of the value (type-fidelity 8.5)."""
    pg("create table n (id int primary key, a inet); insert into n values"
       " (1, '2001:db8::1'), (2, '10.0.0.1/24'), (3, '10.0.0.1'),"
       " (4, '::ffff:1.2.3.4')")
    my("create table n (id int primary key, a varchar(49));"
       " insert into n values (1, '2001:DB8:0:0:0:0:0:1'),"
       " (2, '10.0.0.1/24'), (3, '10.0.0.1'), (4, '::ffff:1.2.3.4')")
    got = verdict(engine(tmp_path), "n")
    assert got.status == "ok", got.detail
    my("update n set a = '10.0.0.1' where id = 2")
    assert verdict(engine(tmp_path), "n").status == "diff"
