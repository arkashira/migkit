"""Checks that only ever looked at the source.

`migkit check` is meant to be the same command underneath every engine, and
postgres and mysql have always named a table the target has and the source
does not. SQLite walked the source's tables and nothing walked the target's,
so a target still holding a table from an earlier load came back clean:

    src tables: ['shared']   dst tables: ['leftover_from_an_earlier_load', 'shared']
    counts  ok    rows 5==5
    data    ok    main.shared   rows 5==5, md5 5b034253... both sides

Only the schema check noticed, and it says "2 changed lines" - which is a text
diff, not the sentence "there is a table here that should not be".

The second half of this file is the other way an answer goes quiet: a column
fingerprint that could not run returned an empty list, and an empty list of
differing columns reads as "no column differs". mysql did that; postgres said
so. Same question, two answers, so the wording now lives in one place.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.sqlite import SQLiteEngine


def _engine(src, dst, tmp_path):
    def ep(path):
        return Endpoint(host=str(path), port=0, user="", password="")
    hop = Hop(name="s", engine="sqlite", source=ep(src), target=ep(dst))
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def _seed(path, rows=5):
    conn = sqlite3.connect(path)
    conn.execute("create table shared (id integer primary key, v text)")
    conn.executemany("insert into shared (v) values (?)", [("x",)] * rows)
    conn.commit()
    conn.close()


def _add_table(path, name, rows):
    conn = sqlite3.connect(path)
    conn.execute(f'create table "{name}" (id integer primary key)')
    conn.executemany(f'insert into "{name}" values (?)',
                     [(i,) for i in range(rows)])
    conn.commit()
    conn.close()


def test_a_table_only_the_target_has_is_named(tmp_path):
    """The measurement this file opens with."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    _add_table(dst, "leftover_from_an_earlier_load", 40)

    got = _engine(src, dst, tmp_path).check_counts("main")
    assert [r.status for r in got] == ["diff"], [(r.status, r.detail)
                                                 for r in got]
    assert "leftover_from_an_earlier_load extra on target" in got[0].detail
    # and it does not report the shared table as a problem
    assert "shared src=" not in got[0].detail, got[0].detail


def test_the_wording_is_the_one_the_other_engines_use(tmp_path):
    """postgres says "extra on target" and "missing on target" in this same
    check; a report that reads differently per engine is its own problem."""
    from migkit.engines.postgres import PostgresEngine
    import inspect
    pg = inspect.getsource(PostgresEngine.check_counts)
    assert "extra on target" in pg and "missing on target" in pg

    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    _add_table(src, "only_on_source", 3)
    _add_table(dst, "only_on_target", 3)
    got = _engine(src, dst, tmp_path).check_counts("main")
    assert "only_on_source missing on target" in got[0].detail
    assert "only_on_target extra on target" in got[0].detail


def test_two_files_that_match_still_pass_and_say_how_many(tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    got = _engine(src, dst, tmp_path).check_counts("main")
    assert [r.status for r in got] == ["ok"], [(r.status, r.detail)
                                               for r in got]
    assert "1 tables, rows 5==5" in got[0].detail, got[0].detail


def test_row_counts_are_still_compared_on_the_tables_both_sides_have(
        tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src, rows=5)
    _seed(dst, rows=4)
    got = _engine(src, dst, tmp_path).check_counts("main")
    assert got[0].status == "diff"
    assert "shared src=5 dst=4" in got[0].detail


def test_a_target_that_cannot_be_opened_is_not_matching_counts(tmp_path):
    src = tmp_path / "a.db"
    _seed(src)
    got = _engine(src, tmp_path / "missing.db", tmp_path).check_counts("main")
    assert [r.status for r in got] == ["error"], [(r.status, r.detail)
                                                  for r in got]
    assert "not the same as the counts matching" in got[0].detail


# ---- the fingerprint that used to answer with silence ------------------

@pytest.mark.docker
def test_a_fingerprint_that_could_not_run_says_so(pg_pair, tmp_path):
    """A real failure on a real server: the table is not there, so the
    aggregate cannot be built. What matters is that the answer is not an
    empty list, which the caller prints as no columns differing.

    The mysql half is covered by the two checks below rather than by a
    second container: its change is which helper it returns from, and both
    that helper's wording and mysql's use of it are asserted directly.
    """
    import migkit.config as cfg
    from migkit.engines.postgres import PostgresEngine
    cfg.REPORTS = tmp_path / "reports"
    hop = Hop(name="fp", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"])
    eng = PostgresEngine(hop)
    got = eng._column_fingerprint("postgres", "public.not_there_at_all")
    assert got, "the fingerprint answered with silence"
    assert len(got) == 1 and got[0].startswith("(column fingerprint failed"), got

    # and it still answers with the columns when it can run
    from tests.conftest import psql
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "create table fp (id int primary key, a text, b text);"
                   " insert into fp values (1, 'x', 'y');")
    psql(pg_pair["dst"], "update fp set b = 'changed'")
    assert eng._column_fingerprint("postgres", "public.fp") == ["b"]


def test_both_engines_route_through_the_one_wording():
    from migkit.engines.base import Engine
    said = Engine._fingerprint_failed(RuntimeError("boom\nsecond line"))
    assert said == ["(column fingerprint failed: second line)"], said
    import inspect
    for module in ("mysql", "postgres"):
        src = inspect.getsource(
            __import__(f"migkit.engines.{module}", fromlist=["x"]))
        assert "_fingerprint_failed" in src, module
        assert "column fingerprint failed" not in src, (
            f"{module} still spells the message out itself")
