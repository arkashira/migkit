"""Values SQLite kept that the next database will not take.

SQLite does not enforce a column's declared type on an ordinary table.
Measured:

    create table t (id integer primary key, n integer)
    insert into t values (2, 'not a number')    accepted
    select typeof(n) from t where id = 2        text

and the same insert into a `STRICT` table:

    cannot store TEXT value in INTEGER column s.n

Every other engine migkit moves to behaves like the second one, so those
rows are exactly the ones a move will be refused on - found now, on a
laptop, instead of part-way through a cutover.

What makes this answerable rather than a guess is that **SQLite already
tried**. Affinity is applied on the way in, so a numeric column converts
what it can. Measured by inserting the same text into a column of each
declared type and reading `typeof` back:

    declared        '5' stored as     'abc' stored as
    integer         integer           text
    int             integer           text
    bigint          integer           text
    real            real              text
    double          real              text
    numeric         integer           text
    decimal(10,2)   integer           text
    text            text              text
    varchar(10)     text              text
    blob            text              text
    (no type)       text              text

`'5'` became a number wherever the column had numeric affinity. So a value
still sitting there as `text` is one this database itself could not convert
- migkit contributes no heuristic of its own, and there is none to get
wrong.

The engine had no deep battery at all; the file-level questions it does
ask - integrity, foreign keys, journal mode - are in `assess`.
"""
import os
import pathlib
import sqlite3
import tempfile

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.sqlite import SQLiteEngine


def _engine(src_sql, dst_sql=None):
    d = tempfile.mkdtemp()
    paths = []
    for name, script in (("s.db", src_sql), ("t.db", dst_sql or src_sql)):
        p = os.path.join(d, name)
        c = sqlite3.connect(p)
        c.executescript(script)
        c.commit()
        c.close()
        paths.append(p)
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"path": paths[0]}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"path": paths[1]}),
              databases=["main"])
    hop.report_dir = lambda db=None, _p=pathlib.Path(d): _p
    return SQLiteEngine(hop)


def _side(res, which):
    got = [r for r in res if which in r.scope]
    assert len(got) == 1, [(r.scope, r.status) for r in got]
    return got[0]


def test_the_engine_offers_a_deep_battery_at_all():
    from migkit.engines.base import Engine
    assert SQLiteEngine.check_deep is not Engine.check_deep


def test_sqlite_really_does_keep_a_value_of_the_wrong_type():
    """The premise. If a future SQLite stops accepting it, this file is
    moot and should say so by failing."""
    d = tempfile.mkdtemp()
    c = sqlite3.connect(os.path.join(d, "x.db"))
    c.execute("create table t (id integer primary key, n integer)")
    c.execute("insert into t values (2, 'not a number')")
    assert c.execute("select typeof(n) from t").fetchone()[0] == "text"
    c.execute("create table s (id integer primary key, n integer) strict")
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("insert into s values (1, 'nope')")
    c.close()


def test_affinity_really_does_convert_what_it_can():
    """The other half of the premise, and the reason no heuristic is
    needed: a value that is still text is one SQLite could not convert."""
    d = tempfile.mkdtemp()
    c = sqlite3.connect(os.path.join(d, "x.db"))
    c.execute("create table t (a integer, b decimal(10,2), c real, e text)")
    c.execute("insert into t values ('5','5','5','5')")
    assert list(c.execute("select typeof(a), typeof(b), typeof(c),"
                          " typeof(e) from t")) == [
        ("integer", "integer", "real", "text")]
    c.close()


@pytest.mark.parametrize("declared,kind", [
    ("integer", "numeric"), ("INT", "numeric"), ("bigint", "numeric"),
    ("numeric", "numeric"), ("decimal(10,2)", "numeric"),
    ("real", "numeric"), ("double precision", "numeric"),
    ("text", "text"), ("varchar(10)", "text"), ("CLOB", "text"),
    ("blob", "blob"), ("", "blob"), ("   ", "blob"),
])
def test_the_affinity_rule_matches_what_sqlite_does(declared, kind):
    assert SQLiteEngine._affinity(declared) == kind, declared


def test_a_column_holding_the_wrong_kind_of_value_is_reported():
    eng = _engine(
        "create table t (id integer primary key, n integer);"
        " insert into t values (1,5),(2,'not a number'),(3,7);",
        "create table t (id integer primary key, n integer);"
        " insert into t values (1,5),(3,7);")
    got = _side(eng.check_deep("main"), "source")
    assert got.status == "diff", got.detail
    assert "t.n is declared integer" in got.detail.replace("INTEGER",
                                                           "integer")
    assert "not a number" in got.detail, got.detail
    assert "refused on the way out" in got.detail, got.detail


def test_the_clean_side_passes_and_says_how_much_it_looked_at():
    eng = _engine(
        "create table t (id integer primary key, n integer);"
        " insert into t values (1,5),(2,'bad');",
        "create table t (id integer primary key, n integer);"
        " insert into t values (1,5);")
    res = eng.check_deep("main")
    assert _side(res, "source").status == "diff"
    clean = _side(res, "target")
    assert clean.status == "ok", clean.detail
    assert "2 columns declared as numbers" in clean.detail, clean.detail


def test_text_columns_are_not_reported():
    """A number stored in a text column is what a text column is for -
    flagging it would make the check noise."""
    eng = _engine("create table t (id integer primary key, label text);"
                  " insert into t values (1,'5'),(2,'abc');")
    assert _side(eng.check_deep("main"), "source").status == "ok"


def test_nulls_are_not_a_wrong_type():
    eng = _engine("create table t (id integer primary key, n integer);"
                  " insert into t values (1,5),(2,NULL);")
    assert _side(eng.check_deep("main"), "source").status == "ok"


def test_a_real_in_an_integer_column_is_not_reported():
    """SQLite stores it as a number; the target will take a number. The
    check is about values that are not numbers at all."""
    eng = _engine("create table t (id integer primary key, n integer);"
                  " insert into t values (1,5),(2,1.5);")
    assert _side(eng.check_deep("main"), "source").status == "ok"


def test_one_bad_value_reads_as_one():
    eng = _engine("create table t (id integer primary key, n integer);"
                  " insert into t values (1,'bad');")
    said = _side(eng.check_deep("main"), "source").detail
    assert "1 value that is not a number" in said, said
    eng = _engine("create table t (id integer primary key, n integer);"
                  " insert into t values (1,'bad'),(2,'worse');")
    said = _side(eng.check_deep("main"), "source").detail
    assert "2 values that are not numbers" in said, said


def test_a_file_it_cannot_read_is_an_error_not_a_pass():
    d = tempfile.mkdtemp()
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host="", port=0, user="", password="",
                              options={"path": os.path.join(d, "nope.db")}),
              target=Endpoint(host="", port=0, user="", password="",
                              options={"path": os.path.join(d, "nope.db")}),
              databases=["main"])
    hop.report_dir = lambda db=None, _p=pathlib.Path(d): _p
    res = SQLiteEngine(hop).check_deep("main")
    assert res and all(r.status == "error" for r in res), \
        [(r.scope, r.status) for r in res]
    assert all(r.status != "ok" for r in res)


def test_the_verdicts_name_no_tool():
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    eng = _engine("create table t (id integer primary key, n integer);"
                  " insert into t values (1,'bad');")
    for r in eng.check_deep("main"):
        text = f"{r.detail} {r.fix_hint}".lower()
        assert not [t for t in TOOLS if t in text], text
