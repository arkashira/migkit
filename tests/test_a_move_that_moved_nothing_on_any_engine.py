"""The empty-move guard, for every engine that copies through the pair
copier (backlog 0e).

The guard was written for MySQL into PostgreSQL alone; every other pair,
and every engine that copies through the pair copier, answered "cannot
tell". It now asks through each side's own reads: a source table with a
row under the hop's filter, and nothing on the target under the name the
hop gives it.
"""
import sqlite3

from migkit.config import Endpoint, Hop


def _file(path, *statements):
    con = sqlite3.connect(path)
    for s in statements:
        con.execute(s)
    con.commit()
    con.close()


def _engine(tmp_path, **extra):
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(tmp_path / "a.db"), user="x",
                              password="x"),
              target=Endpoint(host=str(tmp_path / "b.db"), user="x",
                              password="x"),
              databases=["main"], **extra)
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


def test_a_table_with_rows_and_an_empty_target_is_named(tmp_path):
    _file(tmp_path / "a.db",
          "create table full_one (id integer primary key, v text)",
          "insert into full_one values (1, 'x')",
          "create table empty_one (id integer primary key)",
          "create table gone (id integer primary key)",
          "insert into gone values (1)",
          "create table arrived (id integer primary key)",
          "insert into arrived values (1)")
    _file(tmp_path / "b.db",
          "create table full_one (id integer primary key, v text)",
          "create table empty_one (id integer primary key)",
          "create table arrived (id integer primary key)",
          "insert into arrived values (1)")
    # an empty source table is not a failure; one the target lacks is
    assert _engine(tmp_path).moved_nothing("main") == ["full_one", "gone"]


def test_what_the_hop_leaves_out_is_not_named(tmp_path):
    _file(tmp_path / "a.db",
          "create table skipped (id integer primary key)",
          "insert into skipped values (1)",
          "create table some (id integer primary key, k text)",
          "insert into some values (1, 'old')")
    _file(tmp_path / "b.db",
          "create table skipped (id integer primary key)",
          "create table some (id integer primary key, k text)")
    eng = _engine(tmp_path, exclude=["skipped"],
                  mapping={"where": {"some": "k = 'new'"}})
    # the one table left is filtered to nothing: nothing was to arrive
    assert eng.moved_nothing("main") == []
    _file(tmp_path / "a.db", "insert into some values (2, 'new')")
    assert eng.moved_nothing("main") == ["some"]
