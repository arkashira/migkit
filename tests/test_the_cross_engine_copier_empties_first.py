"""Emptying a target table, the step the cross-engine copier was missing.

The cross-engine copier only ever wrote. Measured SQLite to SQLite through
it: a row the target already had stayed after the move, and a key-less
table went from 2 rows to 4 when the move ran again. Each engine that
writes neutrally now also empties a target table the same way; this file
pins that part of the contract.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop


def _pair(tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    for path, rows in ((src, "('a'), ('b')"), (dst, "('stale'), ('x')")):
        con = sqlite3.connect(path)
        con.executescript(f"create table log (msg text);"
                          f" insert into log values {rows};")
        con.commit()
        con.close()
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              db_map={"main": "main"})
    return SQLiteEngine(hop), src, dst


def _rows(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select msg from log order by msg").fetchall()
    finally:
        con.close()


def test_the_target_table_is_emptied_and_kept(tmp_path):
    eng, _, dst = _pair(tmp_path)
    assert eng.neutral_empty("dst", "main", "log") == 2
    assert _rows(dst) == []
    assert "log" in eng._all_tables("dst")


def test_a_source_is_never_emptied(tmp_path):
    eng, src, _ = _pair(tmp_path)
    with pytest.raises(SystemExit) as e:
        eng.neutral_empty("src", "main", "log")
    assert "source" in str(e.value)
    assert _rows(src) == [("a",), ("b",)]


def test_every_engine_that_writes_neutrally_can_empty_too():
    """Half the contract is how the copier came to leave strays and
    doubles behind."""
    from migkit.engines import NAMES, _class_for
    from migkit.engines.base import Engine
    for name in NAMES:
        cls = _class_for(name)
        if cls.neutral_write is Engine.neutral_write:
            continue
        assert cls.neutral_empty is not Engine.neutral_empty, name


class _Checkpoint(dict):
    def save(self):
        pass


def _copier(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    con = sqlite3.connect(src)
    con.executescript("create table orders (id integer primary key, v text);"
                      " insert into orders values (1, 'a'), (2, 'b');"
                      " create table log (msg text);"
                      " insert into log values ('a'), ('b');")
    con.commit()
    con.close()
    con = sqlite3.connect(dst)
    con.executescript("create table orders (id integer primary key, v text);"
                      " insert into orders values (9, 'stale');"
                      " create table log (msg text);")
    con.commit()
    con.close()
    hop = Hop(name="x", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"],
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    return HeteroEngine(hop), dst


def _move_all(eng, lines):
    ck = _Checkpoint()
    for sch, t in eng.list_move_tables("main"):
        eng.move_table("main", sch, t, 1000, ck, lines.append)


def test_a_stray_target_row_does_not_survive_the_copy(tmp_path):
    eng, dst = _copier(tmp_path)
    lines = []
    _move_all(eng, lines)
    con = sqlite3.connect(dst)
    try:
        assert con.execute("select id from orders order by id").fetchall() \
            == [(1,), (2,)]
    finally:
        con.close()
    assert any("emptied 1 rows" in ln for ln in lines), lines


def test_a_keyless_table_does_not_double_when_the_move_runs_again(tmp_path):
    eng, dst = _copier(tmp_path)
    _move_all(eng, [])
    _move_all(eng, [])
    con = sqlite3.connect(dst)
    try:
        assert con.execute("select count(*) from log").fetchone()[0] == 2
    finally:
        con.close()


def _fresh_pair(tmp_path, mapping=None, exclude=()):
    """A source with three tables and a target file that is not there."""
    from migkit.engines.hetero import HeteroEngine
    src = tmp_path / "s.db"
    con = sqlite3.connect(src)
    con.executescript("create table orders (id integer primary key, v text);"
                      " insert into orders values (1, 'a'), (2, 'b');"
                      " create table log (msg text);"
                      " insert into log values ('a');"
                      " create table audit (id integer primary key);"
                      " insert into audit values (1);")
    con.commit()
    con.close()
    hop = Hop(name="x", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(tmp_path / "new.db"), port=0,
                              user="", password=""),
              databases=["main"], mapping=mapping or {},
              exclude=list(exclude),
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    return HeteroEngine(hop), tmp_path / "new.db"


def _tables(path):
    con = sqlite3.connect(path)
    try:
        return sorted(r[0] for r in con.execute(
            "select name from sqlite_master where type = 'table'"))
    finally:
        con.close()


def test_a_table_the_target_lacks_is_moved_not_skipped(tmp_path):
    """Only tables already on the target used to be listed, so a fresh
    target got nothing and the rest of a partial one was left out without
    a word - while the copier had the code to create them all along."""
    eng, dst = _fresh_pair(tmp_path, exclude=["audit"])
    _move_all(eng, [])
    assert _tables(dst) == ["log", "orders"]


def test_a_renamed_table_is_created_under_its_new_name(tmp_path):
    eng, dst = _fresh_pair(tmp_path, mapping={"tables": {"orders":
                                                         "orders_v2"}})
    _move_all(eng, [])
    assert "orders_v2" in _tables(dst) and "orders" not in _tables(dst)


def test_the_same_engine_copies_table_by_table_through_the_same_copier(
        tmp_path):
    """SQLite to SQLite, through the one copier: no second implementation
    to drift from the first."""
    from migkit.engines.sqlite import SQLiteEngine
    eng, dst = _fresh_pair(tmp_path, exclude=["audit"])
    lite = SQLiteEngine(Hop(name="l", engine="sqlite",
                            source=eng.hop.source, target=eng.hop.target,
                            databases=["main"], exclude=["audit"]))
    lite.hop.report_dir = lambda db=None, _p=tmp_path: _p
    ck = _Checkpoint()
    for sch, t in lite.list_move_tables("main"):
        lite.move_table("main", sch, t, 1000, ck, lambda m: None)
    assert _tables(dst) == ["log", "orders"]
    got = lite.check_counts("main") + lite.check_data("main")
    assert got and all(r.status == "ok" for r in got), \
        [(r.scope, r.status, r.detail) for r in got]
