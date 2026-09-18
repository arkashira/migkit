"""Which rows differ across two engines, not just that the digests do.

Before this, a cross-engine check answered with a pair of numbers:

    diff  postgres.t  rows 4 match but the contents do not:
                      digest src=2517543869784034524 dst=2901291700672583238

and nothing else - no files, and `repair_plan` returned an empty list. The
operator was told the table was wrong and left to find out how.

The pairing here is PostgreSQL to SQLite, and the fixture is built so the row
counts match while three different things are wrong: one row changed, one
missing on the target, one only on the target. A check that compares counts
first, or that trusted the count, would call that clean.

Both sides are asked for the same keys rather than read in step. Two engines do
not agree on the order of a text key, so merging two ordered reads would report
missing and extra rows in equal numbers with neither being true.
"""
import json
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

AWKWARD = 'a:b["x"]'


def _hop(pg_port, lite_path, tmp_path):
    hop = Hop(name="h", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_port,
                              user="postgres", password="test"),
              target=Endpoint(host=str(lite_path), port=0, user="",
                              password=""),
              databases=["postgres"],
              options={"source_engine": "postgres",
                       "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    from migkit.engines.hetero import HeteroEngine
    return HeteroEngine(hop)


def _lite(path, statements):
    conn = sqlite3.connect(path)
    for sql, args in statements:
        if args is None:
            conn.execute(sql)
        else:
            conn.executemany(sql, args)
    conn.commit()
    conn.close()


def _seed_pair(pg_port, lite_path, target_rows):
    psql(pg_port,
         "create table t (id bigint primary key, label text,"
         " n numeric(10,2));"
         f" insert into t values (1, $${AWKWARD}$$, 1.50), (2, 'two', 2.00),"
         " (3, 'three', 3.00), (4, 'four', 4.00);")
    _lite(lite_path, [
        ("create table t (id integer primary key, label text, n text)", None),
        ("insert into t values (?,?,?)", target_rows)])


def _drill_file(tmp_path, kind):
    path = tmp_path / f"data-t.{kind}"
    if not path.exists():
        return None
    return [json.loads(l) for l in path.read_text().splitlines() if l]


def test_a_row_count_that_matches_does_not_mean_the_rows_do(pg_pair,
                                                            tmp_path):
    """One row missing and one row extra leaves the counts equal - and the
    report still has to name all three differences."""
    lite = tmp_path / "t.db"
    _seed_pair(pg_pair["src"], lite,
               [(1, "CHANGED", "1.50"), (2, "two", "2.00"),
                (4, "four", "4.00"), (9, "only here", "9.00")])
    got = _hop(pg_pair["src"], lite, tmp_path).check_data("postgres")
    row = [r for r in got if r.scope.endswith(".t")][0]
    assert row.status == "diff", row.detail
    assert "rows 4 match" in row.detail, row.detail
    assert "1 missing on the target (3)" in row.detail, row.detail
    assert "1 with different values (1)" in row.detail, row.detail
    assert "1 only on the target (9)" in row.detail, row.detail

    assert _drill_file(tmp_path, "missing") == [["3"]]
    assert _drill_file(tmp_path, "changed") == [["1"]]
    assert _drill_file(tmp_path, "extra") == [["9"]]


def test_a_table_that_agrees_says_so_and_clears_the_files(pg_pair, tmp_path):
    """The files are a repair's input, so a clean run has to remove the
    previous run's - otherwise `sync` would act on rows that are now fine."""
    lite = tmp_path / "t.db"
    _seed_pair(pg_pair["src"], lite,
               [(1, "CHANGED", "1.50"), (2, "two", "2.00"),
                (4, "four", "4.00"), (9, "only here", "9.00")])
    eng = _hop(pg_pair["src"], lite, tmp_path)
    eng.check_data("postgres")
    assert (tmp_path / "data-t.missing").exists()

    _lite(lite, [("delete from t", None),
                 ("insert into t values (?,?,?)",
                  [(1, AWKWARD, "1.50"), (2, "two", "2.00"),
                   (3, "three", "3.00"), (4, "four", "4.00")])])
    got = eng.check_data("postgres")
    row = [r for r in got if r.scope.endswith(".t")][0]
    assert row.status == "ok", row.detail
    assert not list(tmp_path.glob("data-t.*")), list(tmp_path.iterdir())


def test_a_value_that_only_differs_in_one_column_is_still_found(pg_pair,
                                                                tmp_path):
    """The digest hides which column; the drilldown at least names the row.
    The awkward label is the one that changes, because punctuation in a value
    has broken a rendering before."""
    lite = tmp_path / "t.db"
    _seed_pair(pg_pair["src"], lite,
               [(1, AWKWARD, "1.50"), (2, "two", "2.00"),
                (3, "three", "3.00"), (4, "four", "9.99")])
    got = _hop(pg_pair["src"], lite, tmp_path).check_data("postgres")
    row = [r for r in got if r.scope.endswith(".t")][0]
    assert "1 with different values (4)" in row.detail, row.detail
    assert _drill_file(tmp_path, "changed") == [["4"]]
    assert _drill_file(tmp_path, "missing") is None
    assert _drill_file(tmp_path, "extra") is None


def test_a_table_with_no_key_says_why_rather_than_writing_nothing(pg_pair,
                                                                   tmp_path):
    """An empty list of differing rows reads as "nothing to repair", which is
    the opposite of what a table nobody can line up means."""
    lite = tmp_path / "t.db"
    psql(pg_pair["src"],
         "create table nokey (a text, b int);"
         " insert into nokey values ('x', 1), ('y', 2);")
    _lite(lite, [("create table nokey (a text, b integer)", None),
                 ("insert into nokey values (?,?)",
                  [("x", 1), ("DIFFERENT", 2)])])
    got = _hop(pg_pair["src"], lite, tmp_path).check_data("postgres")
    row = [r for r in got if r.scope.endswith(".nokey")][0]
    assert row.status == "diff", row.detail
    assert "rows not localised" in row.detail, row.detail
    assert "no key" in row.detail, row.detail
    assert not list(tmp_path.glob("data-nokey.*")), list(tmp_path.iterdir())


def test_a_composite_key_is_matched_on_every_part(pg_pair, tmp_path):
    lite = tmp_path / "c.db"
    psql(pg_pair["src"],
         "create table c (a text, b int, v text, primary key (a, b));"
         f" insert into c values ($${AWKWARD}$$, 1, 'first'),"
         f" ($${AWKWARD}$$, 2, 'second'), ('other', 1, 'third');")
    _lite(lite, [
        ("create table c (a text, b integer, v text, primary key (a, b))",
         None),
        ("insert into c values (?,?,?)",
         [(AWKWARD, 1, "first"), (AWKWARD, 2, "CHANGED")])])
    got = _hop(pg_pair["src"], lite, tmp_path).check_data("postgres")
    row = [r for r in got if r.scope.endswith(".c")][0]
    assert row.status == "diff", row.detail

    changed = [json.loads(l) for l in
               (tmp_path / "data-c.changed").read_text().splitlines()]
    missing = [json.loads(l) for l in
               (tmp_path / "data-c.missing").read_text().splitlines()]
    assert changed == [[AWKWARD, "2"]], changed
    assert missing == [["other", "1"]], missing
    # the row sharing the first key part is not dragged in with its neighbour
    assert [AWKWARD, "1"] not in changed + missing


def test_the_walk_stops_at_the_cap_and_says_it_did(pg_pair, tmp_path):
    """A drilldown that reads a whole production table without saying where
    it stopped is worse than one that admits the limit."""
    lite = tmp_path / "t.db"
    psql(pg_pair["src"],
         "create table t (id bigint primary key, v text);"
         " insert into t select g, 'v' from generate_series(1, 40) g;")
    _lite(lite, [("create table t (id integer primary key, v text)", None),
                 ("insert into t values (?,?)",
                  [(i, "v" if i % 2 else "other") for i in range(1, 41)])])
    eng = _hop(pg_pair["src"], lite, tmp_path)
    eng.DRILL_CAP = 10
    got = eng.check_data("postgres")
    row = [r for r in got if r.scope.endswith(".t")][0]
    assert "stopped after 10 rows" in row.detail, row.detail
    found = _drill_file(tmp_path, "changed")
    assert found and len(found) <= 10, found
    # and what it did find is real: every id it named differs on the target
    for (key,) in [tuple(k) for k in found]:
        assert int(key) % 2 == 0, key
