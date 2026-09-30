"""A check of a source written while it is read: what differs is read
again, generation by generation, until it holds still - and what never
settles is said (backlog F1.d; migration-verifier's generations, over the
digest tree).

A leaf that differs because a row is still on its way settles: equal on
both sides a generation later. One that differs and holds still on both
sides is a difference as of now, and its rows are named. One still moving
after the last generation never settled, and no verdict is given for its
rows - never "ok".
"""
import sqlite3

from migkit import leaves
from migkit.config import Endpoint, Hop

ROWS = 400


def _sides(tmp_path):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    for path in (src, dst):
        con = sqlite3.connect(path)
        con.execute("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?, ?)",
                        [(i, f"v{i}") for i in range(ROWS)])
        con.commit()
        con.close()
    hop = Hop(name="g", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"],
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    cols = [("id", "integer"), ("v", "text")]
    a = leaves.Side(eng.src_engine, "src", "main", "t", cols, ["id"])
    b = leaves.Side(eng.dst_engine, "dst", "main", "t", cols, ["id"])
    return a, b, src, dst


def _set(path, sql):
    con = sqlite3.connect(path)
    con.execute(sql)
    con.commit()
    con.close()


def _trees(a, b, spec):
    return leaves.scan(a, spec), leaves.scan(b, spec)


def test_a_leaf_still_arriving_settles(tmp_path):
    """The target is behind by one row and catches up before the next
    generation: settled, and nothing is named."""
    a, b, src, dst = _sides(tmp_path)
    spec = leaves.plan(ROWS, "hash", salt="s")
    _set(dst, "update t set v = 'old' where id = 7")
    ta, tb = _trees(a, b, spec)
    assert ta.differing(tb), "the control: the leaf differs first"

    def catch_up(n):
        _set(dst, "update t set v = 'v7' where id = 7")
    got = leaves.generations(a, b, spec, ta, tb, catch_up)
    assert got.all_settled and got.rounds == 1, got
    assert "every one read equal on both sides" in got.said(), got.said()


def test_a_difference_that_holds_still_is_one(tmp_path):
    a, b, src, dst = _sides(tmp_path)
    spec = leaves.plan(ROWS, "hash", salt="s")
    _set(dst, "update t set v = 'wrong' where id = 9")
    ta, tb = _trees(a, b, spec)
    got = leaves.generations(a, b, spec, ta, tb, lambda n: None)
    assert got.rounds == 1 and len(got.still) == 1 and not got.moving, got
    assert not got.all_settled
    # localized from the leaves that held still, and held to their sums
    still = leaves.Tree({x: v for x, v in got.ta.leaves.items()
                         if x in got.still})
    there = leaves.Tree({x: v for x, v in got.tb.leaves.items()
                         if x in got.still})
    found = leaves.localize(a, b, spec, still, there)
    assert found.complete and found.changed == [("9",)], found
    assert "1 held still and differ" in got.said(), got.said()


def test_a_leaf_written_every_generation_never_settles(tmp_path):
    """The source keeps writing the same leaf: after the last generation
    it is said to have never settled, and no row of it is called equal."""
    a, b, src, dst = _sides(tmp_path)
    spec = leaves.plan(ROWS, "hash", salt="s")
    _set(src, "update t set v = 'w0' where id = 11")
    ta, tb = _trees(a, b, spec)

    def keeps_writing(n):
        _set(src, f"update t set v = 'w{n}' where id = 11")
    got = leaves.generations(a, b, spec, ta, tb, keeps_writing, most=3)
    assert got.rounds == 3 and len(got.moving) == 1, got
    assert not got.still and not got.all_settled, got
    assert "never settled - still being written after 3 generations" \
        in got.said(), got.said()


def test_settle_tells_the_three_apart():
    before_a, before_b = {1: (1, 5, 5), 2: (1, 6, 6), 3: (1, 7, 7)}, \
        {1: (1, 4, 4), 2: (1, 9, 9), 3: (1, 8, 8)}
    now_a = {1: (1, 4, 4), 2: (1, 6, 6), 3: (1, 70, 70)}
    now_b = {1: (1, 4, 4), 2: (1, 9, 9), 3: (1, 8, 8)}
    assert leaves.settle(before_a, before_b, now_a, now_b, [1, 2, 3]) == (
        {1}, {2}, {3})


def test_no_generation_is_read_where_none_is_asked(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_GENERATIONS", "0")
    a, b, src, dst = _sides(tmp_path)
    spec = leaves.plan(ROWS, "hash", salt="s")
    _set(dst, "update t set v = 'x' where id = 3")
    ta, tb = _trees(a, b, spec)
    got = leaves.generations(a, b, spec, ta, tb, lambda n: None)
    assert got.rounds == 0 and got.still == set(ta.differing(tb)), got
    assert got.said() == ""


def test_generations_are_a_rung_a_live_source_climbs():
    """The confirm part climbs to them only where the source's log moved
    while the table was read."""
    live = leaves.strategy("t", {"key": "integer", "sql": False,
                                 "written": True})
    assert live.parts["confirm"].name == "generations", live.parts
    quiet = leaves.strategy("t", {"key": "integer", "sql": False,
                                  "written": False})
    assert quiet.parts["confirm"].name != "generations", quiet.parts
    assert "log did not move" in quiet.reasons["confirm"], quiet.reasons
