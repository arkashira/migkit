"""A statement-level record of a repair, beside its undo (problems C3,
backlog C3.repair).

A row repair left an undo file of the rows it overwrote and nothing that
said which of its writes had landed when it stopped. Every row repair now
writes its record before it writes anything - each statement with the row
the target held and the one it will hold - and marks how far it got; a
rollback puts each row back only where the target still holds what the
repair wrote, and a replay does the same the other way.
"""
import json
import sqlite3

import pytest

from migkit import repairlog
from migkit.config import Endpoint, Hop


def _pair(tmp_path, newer_wins=None):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    con = sqlite3.connect(src)
    con.executescript(
        "create table t (id integer primary key, v text, at integer);"
        " insert into t values (1, 'one', 10), (2, 'two', 10),"
        " (3, 'three', 10), (5, 'five', 10);")
    con.commit()
    con.close()
    con = sqlite3.connect(dst)
    con.executescript(
        "create table t (id integer primary key, v text, at integer);"
        # 2 changed and older, 3 changed and newer, 4 extra, 5 missing
        " insert into t values (1, 'one', 10), (2, 'TWO', 5),"
        " (3, 'THREE', 99), (4, 'four', 10);")
    con.commit()
    con.close()
    opts = {"source_engine": "sqlite", "target_engine": "sqlite"}
    if newer_wins:
        opts["newer_wins"] = newer_wins
    hop = Hop(name="r", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"], options=opts)
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop), dst


def _rows(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select id, v, at from t order by id").fetchall()
    finally:
        con.close()


def _repair(eng):
    [r for r in eng.check_data("main")]
    for action in eng.repair_plan("main", "rows"):
        eng.apply("main", action)


def test_the_record_holds_each_statement_before_and_after(tmp_path):
    eng, dst = _pair(tmp_path)
    _repair(eng)
    assert _rows(dst) == [(1, "one", 10), (2, "two", 10), (3, "three", 10),
                          (5, "five", 10)]
    [path] = repairlog.records(tmp_path / "undo")
    head, stmts, upto = repairlog.read(path)
    assert head["form"] == "canon" and head["key"] == ["id"], head
    by = {tuple(s["key"]): s for s in stmts}
    assert by[("2",)]["op"] == "write"
    assert by[("2",)]["before"][1:] == ["TWO", "5"], by[("2",)]
    assert by[("2",)]["after"][1:] == ["two", "10"], by[("2",)]
    assert by[("5",)]["before"] is None and by[("5",)]["after"], by
    assert by[("4",)]["op"] == "delete" and by[("4",)]["after"] is None
    assert by[("4",)]["before"][1:] == ["four", "10"], by[("4",)]
    assert upto == len(stmts) == 4, (upto, stmts)
    assert "all applied" in repairlog.said(path)


def test_a_repair_that_stops_says_how_far_it_got(tmp_path, monkeypatch):
    """The writes landed and the delete did not: the record says the
    writes were applied and the delete was in flight."""
    from migkit.engines.sqlite import SQLiteEngine
    eng, dst = _pair(tmp_path)

    def cut(*a, **k):
        raise RuntimeError("the connection went away")
    monkeypatch.setattr(SQLiteEngine, "_apply_delete", cut)
    with pytest.raises(RuntimeError):
        _repair(eng)
    [path] = repairlog.records(tmp_path / "undo")
    _, stmts, upto = repairlog.read(path)
    writes = [s for s in stmts if s["op"] == "write"]
    assert upto == len(writes) == 3 and len(stmts) == 4, (upto, stmts)
    assert repairlog.stopped(tmp_path / "undo") == [(path, 3, 4)]
    assert "stopped after 3 of 4" in repairlog.said(path)


def test_a_rollback_puts_back_only_what_still_holds_the_repair(tmp_path):
    eng, dst = _pair(tmp_path)
    _repair(eng)
    # a row the application wrote after the repair is not the repair's to
    # take back
    con = sqlite3.connect(dst)
    con.execute("update t set v = 'written since' where id = 3")
    con.commit()
    con.close()
    [path] = repairlog.to_reverse(tmp_path / "undo")
    said = []
    done, same, left = repairlog.reverse(path, eng.repair_io(
        "main", repairlog.read(path)[0]), said.append)
    assert (done, same, left) == (3, 0, [["3"]]), (done, same, left)
    assert _rows(dst) == [(1, "one", 10), (2, "TWO", 5),
                          (3, "written since", 10), (4, "four", 10)]
    assert "1 left alone - changed since the repair: 3" in said[0], said
    # and again: everything it could put back already is
    done, same, left = repairlog.reverse(path, eng.repair_io(
        "main", repairlog.read(path)[0]))
    assert (done, same) == (0, 3), (done, same)


def test_a_replay_puts_the_repair_back(tmp_path):
    eng, dst = _pair(tmp_path)
    _repair(eng)
    after = _rows(dst)
    [path] = repairlog.records(tmp_path / "undo")
    io = eng.repair_io("main", repairlog.read(path)[0])
    repairlog.reverse(path, io)
    assert _rows(dst) != after
    done, _, left = repairlog.replay(path, io)
    assert done == 4 and not left, (done, left)
    assert _rows(dst) == after


def test_the_newer_row_is_kept_through_the_pair(tmp_path):
    """`newer_wins` on the path every pair repairs by: row 3 is later on
    the target and kept, row 2 older and overwritten (backlog 10.more)."""
    eng, dst = _pair(tmp_path, newer_wins="at")
    _repair(eng)
    got = dict((i, v) for i, v, _ in _rows(dst))
    assert got[2] == "two" and got[3] == "THREE", got
    kept = (tmp_path / "data-t.kept-newer").read_text().splitlines()
    assert [json.loads(k) for k in kept] == [["3"]], kept
    # the kept row is not in the record: nothing was written to it
    [path] = repairlog.records(tmp_path / "undo")
    assert ("3",) not in {tuple(s["key"]) for s in repairlog.read(path)[1]}


def test_a_newer_wins_column_not_on_both_sides_is_refused(tmp_path):
    eng, dst = _pair(tmp_path, newer_wins="updated_at")
    before = _rows(dst)
    with pytest.raises(SystemExit) as e:
        _repair(eng)
    assert "newer_wins names updated_at, and it is not on both sides" \
        in str(e.value), e.value
    assert _rows(dst) == before


def test_a_value_comes_back_from_its_record_as_it_was():
    """The record keeps canonical text; the value it puts back is the value,
    not the text: bytes from their hex, numbers as numbers."""
    from decimal import Decimal
    assert repairlog.value("bytes", "00FF") == b"\x00\xff"
    assert repairlog.value("integer", "12") == 12
    assert repairlog.value("decimal", "1.50") == Decimal("1.50")
    assert repairlog.value("boolean", "0") is False
    assert repairlog.value("text", "x") == "x"
    assert repairlog.value("text", None) is None
