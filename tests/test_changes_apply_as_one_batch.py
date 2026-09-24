"""A batch of changes is applied in one transaction, on one connection.

Each applied change used to open a connection of its own and commit on it.
The benchmark (`bench/run.py`) measured what that costs: MySQL into MySQL at
190 rows a second, the tail was still 15 seconds behind when the writer
stopped after ten - 2.9 seconds once a batch shared one connection. And a
batch that failed part of the way left the part before the failure
committed; it is rolled back whole now, and the tail replays it, which the
appliers are idempotent for.
"""
import sqlite3

import pytest


def _eng(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.sqlite import SQLiteEngine
    path = tmp_path / "t.db"
    con = sqlite3.connect(path)
    con.execute("create table t (id integer primary key, v text)")
    con.commit()
    con.close()
    ep = Endpoint(host=str(path), port=0, user="", password="")
    hop = Hop(name="b", engine="sqlite", source=ep, target=ep,
              databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop), path


def _rows(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select id, v from t order by id").fetchall()
    finally:
        con.close()


def test_a_batch_that_fails_leaves_nothing_behind(tmp_path):
    eng, path = _eng(tmp_path)
    changes = [{"op": "insert", "table": "t", "key": {"id": 1},
                "values": {"id": 1, "v": "a"}},
               {"op": "insert", "table": "nowhere", "key": {"id": 2},
                "values": {"id": 2}}]
    with pytest.raises(sqlite3.OperationalError):
        eng.neutral_apply("dst", "main", changes)
    assert _rows(path) == []
    # replayed once the batch can land, it lands once
    eng.neutral_apply("dst", "main", changes[:1])
    eng.neutral_apply("dst", "main", changes[:1])
    assert _rows(path) == [(1, "a")]


def test_a_batch_opens_one_connection(tmp_path, monkeypatch):
    eng, path = _eng(tmp_path)
    opened = []
    real = type(eng)._open_writer

    def counting(self, side, db):
        opened.append(1)
        return real(self, side, db)
    monkeypatch.setattr(type(eng), "_open_writer", counting)
    eng.neutral_apply("dst", "main", [
        {"op": "insert", "table": "t", "key": {"id": i},
         "values": {"id": i, "v": str(i)}} for i in range(50)])
    assert len(opened) == 1, len(opened)
    assert len(_rows(path)) == 50


def test_each_row_is_written_once_with_what_the_batch_left(tmp_path,
                                                           monkeypatch):
    """A row changed three times is written once; a row made and removed in
    one batch is not written at all; a key moved leaves its old address."""
    eng, path = _eng(tmp_path)
    con = sqlite3.connect(path)
    con.executemany("insert into t values (?, ?)", [(3, "old"), (4, "k")])
    con.commit()
    con.close()
    wrote = []
    real = type(eng)._apply_upsert

    def counting(self, side, db, table, key, values):
        wrote.append(dict(key))
        return real(self, side, db, table, key, values)
    monkeypatch.setattr(type(eng), "_apply_upsert", counting)

    def ch(op, key, **values):
        return {"op": op, "table": "t", "key": {"id": key},
                "values": ({"id": values.pop("to", key), **values}
                           if op != "delete" else {})}
    eng.neutral_apply("dst", "main", [
        ch("insert", 1, v="a"), ch("update", 1, v="b"),
        ch("update", 1, v="c"),
        ch("insert", 2, v="gone"), ch("delete", 2),
        ch("delete", 3), ch("insert", 3, v="new"),
        ch("update", 4, to=5, v="k"),
    ])
    assert _rows(path) == [(1, "c"), (3, "new"), (5, "k")]
    assert [k["id"] for k in wrote] == [1, 3, 5], wrote


def test_a_change_that_carries_some_columns_is_merged_not_blanked(tmp_path):
    """An update the log wrote without a column it did not change (an
    unchanged large value) carries only the others; two of them in a batch
    are merged, so neither blanks what the other wrote."""
    from migkit.engines.base import Engine
    got = Engine._collapsed([
        {"op": "update", "table": "t", "key": {"id": 1},
         "values": {"id": 1, "a": "x"}},
        {"op": "update", "table": "t", "key": {"id": 1},
         "values": {"id": 1, "b": "y"}}])
    assert got == [("t", ("upsert", {"id": 1},
                          {"id": 1, "a": "x", "b": "y"}))], got
