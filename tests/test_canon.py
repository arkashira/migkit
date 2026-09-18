"""Choosing a rendering, and refusing to choose one that was never checked.

The cross-engine agreement itself is measured against running servers in
`test_canon_cross_engine.py`. This file covers the decisions made before any
SQL runs: which neutral class a declared type belongs to, and what happens to
a type or an engine that has no rule.
"""
import pytest

from migkit import canon as c


def test_declared_types_carry_their_length_and_still_map():
    assert c.type_class("mysql", "varchar(255)") == "text"
    assert c.type_class("mysql", "decimal(12,4)") == "decimal"
    assert c.type_class("mysql", "int unsigned") == "integer"
    assert c.type_class("postgres", "character varying(50)") == "text"
    assert c.type_class("postgres", "timestamp without time zone") \
        == "timestamp"
    assert c.type_class("postgres", "numeric(12,4)") == "decimal"


def test_mysql_tinyint_is_an_integer_not_a_boolean():
    """`tinyint(1)` is a convention, not a type: it holds -128..127. Calling
    it boolean would render a column holding 2 as if it were true, and hide
    the difference from a PostgreSQL boolean that cannot hold 2 at all."""
    assert c.type_class("mysql", "tinyint(1)") == "integer"
    assert c.type_class("postgres", "boolean") == "boolean"
    assert "::int::text" in c.expr("postgres", "b", "boolean")


def test_an_unmapped_type_is_refused_rather_than_treated_as_text():
    """Falling back to the engine's own text would compare two renderings
    nobody checked agree, and report correct data as different."""
    cls, why = c.comparable("postgres", "tsvector")
    assert cls is None
    assert "no canonical rendering" in why and "tsvector" in why
    assert c.type_class("mysql", "geometry") is None


def test_an_engine_with_no_rendering_raises_instead_of_guessing():
    with pytest.raises(ValueError):
        c.expr("mongodb", "x", "text")
    with pytest.raises(ValueError):
        c.expr("mysql", "x", "geography")


def test_the_float_rule_is_banded_on_both_engines():
    """Fixed decimal in the middle, the engine's own shortest text outside.
    The bounds are not cosmetic: MySQL's decimal cast saturates above 1e45
    and both engines round to zero below 1e-20, so a single rendering would
    make different numbers compare equal at both ends."""
    for engine in ("mysql", "postgres"):
        sql = c.expr(engine, "d", "float")
        assert c.FLOAT_MAX in sql, (engine, sql)
        assert c.FLOAT_MIN in sql, (engine, sql)


def test_postgres_marks_the_values_mysql_cannot_hold():
    """PostgreSQL stores Infinity and NaN in a double; MySQL rejects them.
    There is no rendering that makes those comparable, so they are marked and
    counted rather than rendered into something that looks like a number."""
    sql = c.expr("postgres", "d", "float")
    assert "'Infinity'::float8" in sql
    assert "'NaN'::float8" in sql
    assert c.UNCOMPARABLE in sql
    # `x <> x` is the usual NaN test and is always false in PostgreSQL, which
    # defines NaN as equal to itself. Measured: a NaN fell through to the
    # renderer and came out as the text `NaN`.
    assert '"d" <> "d"' not in sql


def test_the_uncomparable_marker_survives_being_put_in_sql():
    """The obvious marker is a NUL byte, since no real value contains one.
    PostgreSQL rejects NUL in text, so an expression carrying one fails the
    whole query instead of marking one row."""
    assert "\x00" not in c.UNCOMPARABLE
    assert "\x00" not in c.expr("postgres", "d", "float")


def test_json_crosses_through_the_normalised_form():
    """A PostgreSQL `json` column keeps the text it was handed - spacing,
    key order and all - while `jsonb` and MySQL's JSON both normalise. Going
    through jsonb is what makes the two sides agree."""
    assert "::jsonb::text" in c.expr("postgres", "j", "json")
    assert c.type_class("postgres", "json") == "json"
    assert c.type_class("postgres", "jsonb") == "json"


def test_every_class_has_a_rendering_on_every_engine_that_has_any():
    """A class one engine can render and another cannot is a hole that only
    shows up as a failed query against a customer's table."""
    for engine in c.BUILDERS:
        for cls in c.CLASSES:
            sql = c.expr(engine, "col", cls)
            assert sql and "col" in sql, (engine, cls)
