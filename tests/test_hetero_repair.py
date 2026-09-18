"""Repairing a target that is on a different engine from the source.

The drilldown named the rows; this carries them. The keys come out of those
files as canonical text and are turned back into values per side, which is what
lets a key written by PostgreSQL address a row in SQLite - the text is the one
form the two engines agree on.

Two things are checked here beyond "the rows arrived". The undo file has to
hold what the target held first, and the writes have to happen before the
deletions: a repair cut off in the middle should leave rows that should not be
there, which the next check names, rather than a hole nothing looks for.

The seed carries a `numeric` column on purpose. Measured, a PostgreSQL numeric
arrives as a `Decimal` and binding one raises `sqlite3.ProgrammingError: Error
binding parameter 3: type 'decimal.Decimal' is not supported` - which is where
a move or a repair to SQLite meets it first.
"""
import json
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

AWKWARD = 'a:b["x"]'
TARGET_START = [(1, "CHANGED", "1.50"), (2, "two", "2.00"),
                (4, "four", "4.00"), (9, "only here", "9.00")]


def _engine(pg_port, lite_path, tmp_path):
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


def _seed(pg_port, lite_path, target_rows=None):
    psql(pg_port,
         "create table t (id bigint primary key, label text,"
         " n numeric(10,2));"
         f" insert into t values (1, $${AWKWARD}$$, 1.50), (2, 'two', 2.00),"
         " (3, 'three', 3.00), (4, 'four', 4.00);")
    conn = sqlite3.connect(lite_path)
    conn.execute("create table t (id integer primary key, label text,"
                 " n text)")
    conn.executemany("insert into t values (?,?,?)",
                     TARGET_START if target_rows is None else target_rows)
    conn.commit()
    conn.close()


def _rows(lite_path):
    conn = sqlite3.connect(lite_path)
    try:
        return conn.execute("select * from t order by id").fetchall()
    finally:
        conn.close()


def _repair(eng, db="postgres"):
    actions = eng.repair_plan(db, "rows")
    for action in actions:
        eng.apply(db, action)
    return actions


def test_the_repair_makes_the_two_sides_agree(pg_pair, tmp_path):
    lite = tmp_path / "t.db"
    _seed(pg_pair["src"], lite)
    eng = _engine(pg_pair["src"], lite, tmp_path)
    before = eng.check_data("postgres")
    assert [r.status for r in before] == ["diff"], [r.detail for r in before]

    actions = _repair(eng)
    assert len(actions) == 1, actions
    assert "1 missing, 1 changed, 1 extra" in actions[0].note
    assert any(s.startswith("copy 2 rows") for s in actions[0].statements)
    assert any(s.startswith("delete 1 rows") for s in actions[0].statements)

    after = eng.check_data("postgres")
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]
    assert _rows(lite) == [(1, AWKWARD, "1.50"), (2, "two", "2.00"),
                           (3, "three", "3.00"), (4, "four", "4.00")]
    # the decimal crossed as the text both sides render it as
    assert eng.repair_plan("postgres", "rows") == []


def test_what_it_overwrote_or_removed_is_written_down_first(pg_pair,
                                                            tmp_path):
    lite = tmp_path / "t.db"
    _seed(pg_pair["src"], lite)
    eng = _engine(pg_pair["src"], lite, tmp_path)
    eng.check_data("postgres")
    _repair(eng)

    undo = tmp_path / "undo" / "t.rows.jsonl"
    saved = {tuple(json.loads(l)["key"]): json.loads(l)["row"]
             for l in undo.read_text().splitlines()}
    assert sorted(saved) == [("1",), ("9",)], sorted(saved)
    assert saved[("1",)] == {"id": "1", "label": "CHANGED", "n": "1.50"}
    assert saved[("9",)] == {"id": "9", "label": "only here", "n": "9.00"}
    # the row that was only missing on the target had nothing to save
    assert ("3",) not in saved


def test_rows_are_written_before_any_are_deleted(pg_pair, tmp_path):
    """Interrupted between the two, the target should hold a row too many -
    visible to the next check - rather than a row too few."""
    lite = tmp_path / "t.db"
    _seed(pg_pair["src"], lite)
    eng = _engine(pg_pair["src"], lite, tmp_path)
    eng.check_data("postgres")

    def refuse(*args, **kwargs):
        raise RuntimeError("interrupted")
    eng.dst_engine._apply_delete = refuse

    with pytest.raises(RuntimeError):
        _repair(eng)

    ids = [r[0] for r in _rows(lite)]
    assert 3 in ids, "the copy did not happen before the delete was tried"
    assert 9 in ids, "the extra row went before the copies were safe"
    got = eng.check_data("postgres")
    assert got[0].status == "diff"
    assert "1 only on the target (9)" in got[0].detail, got[0].detail


def test_a_pair_that_cannot_carry_rows_refuses_and_says_so(tmp_path):
    """Answered from the classes, so no server is involved: redis has no
    row-shaped read at all."""
    from migkit.engines.base import RepairAction
    from migkit.engines.hetero import HeteroEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    hop = Hop(name="h", engine="hetero", source=ep, target=ep,
              options={"source_engine": "postgres", "target_engine": "redis"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    with pytest.raises(SystemExit) as caught:
        eng.apply("db", RepairAction("db.t", "rows", ["copy 1 rows"], []))
    said = str(caught.value)
    assert "cannot repair rows" in said, said
    assert "postgres -> redis" in said, said
    assert "migkit assess" in said, said


def test_nothing_to_repair_when_the_check_found_nothing(pg_pair, tmp_path):
    lite = tmp_path / "t.db"
    _seed(pg_pair["src"], lite,
          target_rows=[(1, AWKWARD, "1.50"), (2, "two", "2.00"),
                       (3, "three", "3.00"), (4, "four", "4.00")])
    eng = _engine(pg_pair["src"], lite, tmp_path)
    got = eng.check_data("postgres")
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]
    assert eng.repair_plan("postgres", "rows") == []
    assert eng.repair_plan("postgres", "all") == []


def test_a_kind_this_engine_does_not_repair_is_left_alone(pg_pair, tmp_path):
    lite = tmp_path / "t.db"
    _seed(pg_pair["src"], lite)
    eng = _engine(pg_pair["src"], lite, tmp_path)
    eng.check_data("postgres")
    assert eng.repair_plan("postgres", "sequences") == []
    assert eng.repair_plan("postgres", "rows")
    assert eng.repair_plan("postgres", "all")


def test_a_composite_key_is_carried_across_whole(pg_pair, tmp_path):
    lite = tmp_path / "c.db"
    psql(pg_pair["src"],
         "create table c (a text, b int, v text, primary key (a, b));"
         f" insert into c values ($${AWKWARD}$$, 1, 'first'),"
         f" ($${AWKWARD}$$, 2, 'second'), ('other', 1, 'third');")
    conn = sqlite3.connect(lite)
    conn.execute("create table c (a text, b integer, v text,"
                 " primary key (a, b))")
    conn.executemany("insert into c values (?,?,?)",
                     [(AWKWARD, 1, "first"), (AWKWARD, 2, "CHANGED")])
    conn.commit()
    conn.close()

    eng = _engine(pg_pair["src"], lite, tmp_path)
    eng.check_data("postgres")
    _repair(eng)
    after = eng.check_data("postgres")
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]
    conn = sqlite3.connect(lite)
    try:
        got = conn.execute("select * from c order by a, b").fetchall()
    finally:
        conn.close()
    assert got == [(AWKWARD, 1, "first"), (AWKWARD, 2, "second"),
                   ("other", 1, "third")], got
