"""The rungs a shipped seed's tables are cut and formatted by, each
reached on its branches on fakes (backlog AUD-Z.rungs-outside): a table
goes whole only where it fits one part, by ranges only on one integer key
of a source that answers SQL, in key order only where the source reads on
by a key, and one pass otherwise; its parts are in the binary copy format
or the bulk load's lines only between two servers of that engine whose
columns each format carries, else as rows - each passed over with its own
reason."""
import pytest

from migkit import decide, ship
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("ship", "CUTS", "whole", "holds"),
    ("ship", "CUTS", "whole", "refuses"),
    ("ship", "CUTS", "ranges", "holds"),
    ("ship", "CUTS", "ranges", "refuses"),
    ("ship", "CUTS", "key order", "holds"),
    ("ship", "CUTS", "key order", "refuses"),
    ("ship", "CUTS", "one pass", "holds"),
    ("ship", "FORMATS", "binary copy", "holds"),
    ("ship", "FORMATS", "binary copy", "refuses"),
    ("ship", "FORMATS", "load lines", "holds"),
    ("ship", "FORMATS", "load lines", "refuses"),
    ("ship", "FORMATS", "rows", "holds"),
]

CUT = {"rows": 10, "per": 100, "integer key": True, "sql": True,
       "resumes": True}


def _cut(facts):
    with watched() as seen:
        got = decide.climb("how t is cut", ship.CUTS, facts)
    return got, seen


def test_a_table_that_fits_a_part_goes_whole_and_every_cut_holds():
    got, seen = _cut(CUT)
    assert got.rung.name == "whole"
    for name in ("whole", "ranges", "key order", "one pass"):
        assert branch(seen, name) == [HOLDS], name


def test_a_large_table_with_no_key_falls_to_one_pass():
    got, seen = _cut({"rows": 1000, "per": 100, "integer key": False,
                      "sql": True, "resumes": False})
    assert got.rung.name == "one pass"
    assert "larger than a part" in why(seen, "whole")
    assert "not one integer column" in why(seen, "ranges")
    assert "no key the source reads on by" in why(seen, "key order")
    for name in ("whole", "ranges", "key order"):
        assert branch(seen, name) == [REFUSES], name
    assert branch(seen, "one pass") == [HOLDS]


def test_a_table_of_unknown_size_is_not_taken_whole():
    got, seen = _cut(dict(CUT, rows=None))
    assert got.rung.name == "ranges"
    assert branch(seen, "whole") == [REFUSES]


def test_a_source_that_takes_no_sql_cuts_no_ranges():
    got, seen = _cut(dict(CUT, rows=1000, sql=False))
    assert got.rung.name == "key order"
    assert "takes no SQL to cut the ranges" in why(seen, "ranges")


FMT = {"src engine": "postgres", "dst engine": "postgres", "cut": "whole",
       "folded here": False, "binary": True, "client": True,
       "loads": None, "unfit": ""}


def _fmt(facts):
    with watched() as seen:
        got = decide.climb("the format of t's parts", ship.FORMATS, facts)
    return got, seen


def test_postgres_to_postgres_goes_in_the_binary_copy_format():
    got, seen = _fmt(FMT)
    assert got.rung.name == "binary copy"
    assert branch(seen, "binary copy") == [HOLDS]
    assert "MySQL's own" in why(seen, "load lines")
    assert branch(seen, "rows") == [HOLDS]


def test_mysql_to_mysql_goes_in_the_bulk_loads_lines():
    got, seen = _fmt(dict(FMT, **{"src engine": "mysql",
                                  "dst engine": "mysql", "cut": "ranges"}))
    assert got.rung.name == "load lines"
    assert branch(seen, "load lines") == [HOLDS]
    assert "PostgreSQL's own, on both sides" in why(seen, "binary copy")


@pytest.mark.parametrize("change,rung,said", [
    ({"dst engine": "mysql"}, "binary copy", "PostgreSQL's own"),
    ({"cut": "key order"}, "binary copy", "by ranges of a key or whole"),
    ({"folded here": True}, "binary copy", "digested only in this process"),
    ({"binary": False}, "binary copy", "with a binary form on both sides"),
])
def test_the_binary_copy_refused_by_each_of_its_needs_falls_to_rows(
        change, rung, said):
    got, seen = _fmt(dict(FMT, **change))
    assert got.rung.name == "rows"
    assert branch(seen, rung) == [REFUSES] and said in why(seen, rung)


MY = dict(FMT, **{"src engine": "mysql", "dst engine": "mysql"})


@pytest.mark.parametrize("change,said", [
    ({"cut": "one pass"}, "by ranges of a key or whole"),
    ({"client": False}, "migkit doctor --install"),
    ({"loads": False}, "local_infile on"),
    ({"unfit": "a bit column reads back as text"},
     "a bit column reads back as text"),
])
def test_the_bulk_load_refused_by_each_of_its_needs_falls_to_rows(
        change, said):
    got, seen = _fmt(dict(MY, **change))
    assert got.rung.name == "rows"
    assert branch(seen, "load lines") == [REFUSES]
    assert said in why(seen, "load lines") and said in got.reason
