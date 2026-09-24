"""SQLite's AUTOINCREMENT counters: read honestly, and repaired for real.

Found by running every check against a target nothing answers on. Every
failure to read `sqlite_sequence` came back as "no counters", so a target
file that did not exist was reported as `0 counters, values match`. The
repair had the other half of the problem: a target table that had never
been written to has no row in `sqlite_sequence`, and the UPDATE it planned
changed nothing and reported success - leaving the target to hand out ids
the source had already used and deleted.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop


def _file(path, sql):
    con = sqlite3.connect(path)
    con.executescript(sql)
    con.commit()
    con.close()


def _engine(tmp_path, exclude=(), target=None):
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="c", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db"), port=0, user="",
                              password=""),
              target=Endpoint(host=str(target or tmp_path / "b.db"), port=0,
                              user="", password=""),
              databases=["main"], exclude=list(exclude))
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


TABLES = ("create table orders (id integer primary key autoincrement, v);"
          " create table audit (id integer primary key autoincrement, v);")


@pytest.fixture
def pair(tmp_path):
    # the source has used ids up to 10 on orders and deleted the last ones
    _file(tmp_path / "a.db", TABLES + " insert into orders (id, v)"
          " values (10, 'x'); delete from orders where id = 10;"
          " insert into audit (id, v) values (3, 'a');")
    # the target has the tables and nothing written to orders yet
    _file(tmp_path / "b.db", TABLES + " insert into audit (id, v)"
          " values (50, 'target-owned');")
    return tmp_path


def test_a_target_that_is_not_there_is_not_a_match(pair):
    got = _engine(pair, target=pair / "nowhere" / "gone.db") \
        .check_autoinc("main")
    assert [r.status for r in got] == ["error"], [(r.status, r.detail)
                                                  for r in got]


def test_a_counter_the_target_has_no_row_for_is_repaired(pair):
    eng = _engine(pair, exclude=["audit"])
    got = eng.check_autoinc("main")
    assert got[0].status == "diff" and "orders src=10" in got[0].detail, \
        got[0].detail
    plan = eng.repair_plan("main", "sequences")
    assert plan, "a counter that differs is a finding"
    for action in plan:
        eng.apply("main", action)
    assert eng.check_autoinc("main")[0].status == "ok"
    # and it does what the counter is for: the next id is not a reused one
    con = sqlite3.connect(pair / "b.db")
    con.execute("insert into orders (v) values ('after cutover')")
    got = con.execute("select max(id) from orders").fetchone()[0]
    con.close()
    assert got == 11, got


def test_an_excluded_table_keeps_its_counter(pair):
    eng = _engine(pair, exclude=["audit"])
    said = " ".join(r.detail for r in eng.check_autoinc("main"))
    assert "audit" not in said, said
    for action in eng.repair_plan("main", "sequences"):
        eng.apply("main", action)
    con = sqlite3.connect(pair / "b.db")
    got = con.execute("select seq from sqlite_sequence"
                      " where name = 'audit'").fetchone()[0]
    con.close()
    assert got == 50, got


def test_no_counters_on_either_side_is_a_match(tmp_path):
    _file(tmp_path / "a.db", "create table t (id integer primary key);")
    _file(tmp_path / "b.db", "create table t (id integer primary key);")
    got = _engine(tmp_path).check_autoinc("main")
    assert got[0].status == "ok", got[0].detail
