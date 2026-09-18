"""The encoding a row goes through before it is hashed.

The property under test is injectivity: distinct tuples must produce distinct
strings, whatever the data contains. It is not an abstract concern - joining
values with `#` made MySQL 8 report two tables whose every row differed as
`rows 2==2, checksum 810ced44==810ced44`.
"""
import pytest

from migkit import rowtext


def _enc(*values):
    """What the SQL builds, done in Python so the property can be tested
    without a server. The live tests prove the SQL matches this."""
    parts = []
    for v in values:
        if v is None:
            parts.append(f"{rowtext.NULL_LEN}:")
        else:
            parts.append(f"{len(v)}:{v}")
    return rowtext.SEP.join(parts)


def test_the_separator_inside_a_value_does_not_shift_the_boundary():
    """The collision that started this: ('x#y','z') and ('x','y#z')."""
    assert _enc("x#y", "z") != _enc("x", "y#z")
    # and with the encoding's own separator, which is the harder case
    assert _enc("x|y", "z") != _enc("x", "y|z")


def test_no_literal_can_impersonate_null():
    assert _enc(None, "q") != _enc("~null~", "q")
    assert _enc(None, "q") != _enc("", "q")
    assert _enc("", "q") != _enc("N:", "q")


def test_an_empty_string_is_not_a_missing_value():
    assert _enc("", "") != _enc(None, None)


def test_round_trip_recovers_every_value():
    for values in (["a", "b"], ["x#y", "z"], ["x|y", "z"], [None, "q"],
                   ["", ""], [None, None], ["a:b", "c"], ["1:x", "2"],
                   ["tab\there", "x"], ["multi\nline", "y"]):
        assert rowtext.parse(_enc(*values)) == values


def test_a_value_that_looks_like_an_encoding_is_still_recovered():
    """The reason lengths beat escaping: a value can contain anything at all,
    including a well-formed encoding of something else."""
    inner = _enc("a", "b")
    assert rowtext.parse(_enc(inner, "z")) == [inner, "z"]


def test_garbage_is_refused_rather_than_guessed_at():
    for bad in ("no-colon", "9:ab", "x:ab"):
        with pytest.raises(ValueError):
            rowtext.parse(bad)


def test_the_sql_builders_name_every_column_and_carry_a_length():
    my = rowtext.mysql_row(["a", "b"])
    assert "`a`" in my and "`b`" in my
    assert my.count("char_length") == 2
    assert rowtext.NULL_LEN in my
    pg = rowtext.postgres_row(["a", "b"])
    assert '"a"' in pg and '"b"' in pg
    assert pg.count("length(") == 2
