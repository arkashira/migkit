"""`migkit sync --kind rows` on sqlite, which could only say "recopy it".

Measured across every engine with the same three differences seeded into
each - one row changed, one missing on the target, one extra there - sqlite
was the only full engine whose `check` said `diff` and whose `sync` then
found nothing to repair:

    postgres   diff   1 action    missing=1, extra=1, changed=1
    mysql      diff   1 action    delete extra/changed, reinsert from source
    mongodb    diff   1 action    replace/insert from source per _id
    redis      diff   1 action    1 missing, 1 changed, 1 extra
    sqlite     diff   0 actions   <- nothing to repair, about a table it
                                     had just called different

A checksum over the whole table says it is wrong and nothing more, and the
answer here used to be to recopy the file - which is often right for a file,
and is still offered. It is not the same command behaving the same way,
though, and an empty answer standing in for a no is the shape of bug this
project treats as the worst kind.

The walk that finds the differing rows is the one the cross-engine hop uses;
it moved to the base rather than being written a second time, with both
sides of it pointed at this one engine.
"""
import json
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import RepairAction


def _build(tmp_path, ddl, rows, changes=()):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    for path, apply_changes in ((src, False), (dst, True)):
        con = sqlite3.connect(path)
        con.execute(ddl)
        marks = ",".join("?" * len(rows[0]))
        con.executemany(f"insert into t values ({marks})", rows)
        if apply_changes:
            for statement in changes:
                con.execute(statement)
        con.commit()
        con.close()
    report = tmp_path / "report"
    report.mkdir()
    from migkit.engines.sqlite import SQLiteEngine
    hop = Hop(name="s", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              db_map={"main": "main"})
    hop.report_dir = lambda db=None: report
    return SQLiteEngine(hop), dst, report


PLAIN = "create table t (id integer primary key, v text, r real)"
ROWS = [(i, f"v{i}", i * 1.5) for i in range(1, 21)]
THREE = ("update t set v='CHANGED' where id=3",
         "delete from t where id=11",
         "insert into t values (99,'extra',0.25)")


def _repair(eng, db="main"):
    actions = eng.repair_plan(db, "rows")
    for action in actions:
        eng.apply(db, action)
    return actions


def test_the_three_kinds_of_difference_are_named_and_put_right(tmp_path):
    eng, dst, report = _build(tmp_path, PLAIN, ROWS, THREE)
    before = eng.check_data("main")
    assert [r.status for r in before] == ["diff"], before
    assert "1 missing on the target (11)" in before[0].detail, before[0].detail
    assert "1 with different values (3)" in before[0].detail
    assert "1 only on the target (99)" in before[0].detail

    read = {kind: [json.loads(l) for l in
                   (report / f"data-t.{kind}").read_text().splitlines()]
            for kind in ("missing", "changed", "extra")}
    assert read == {"missing": [["11"]], "changed": [["3"]],
                    "extra": [["99"]]}, read

    actions = _repair(eng)
    assert len(actions) == 1, actions
    assert actions[0].kind == "rows"
    assert "1 missing, 1 changed, 1 extra" in actions[0].note

    after = eng.check_data("main")
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]
    con = sqlite3.connect(dst)
    assert con.execute("select count(*) from t").fetchone()[0] == 20
    assert con.execute("select v, r from t where id=3").fetchone() == ("v3",
                                                                       4.5)
    assert con.execute("select v, r from t where id=11").fetchone() == ("v11",
                                                                        16.5)
    assert con.execute("select count(*) from t where id=99").fetchone()[0] == 0
    con.close()


def test_everything_it_overwrites_or_removes_goes_to_undo_first(tmp_path):
    eng, dst, report = _build(tmp_path, PLAIN, ROWS, THREE)
    eng.check_data("main")
    _repair(eng)
    saved = [json.loads(l) for l in
             (report / "undo" / "t.rows.jsonl").read_text().splitlines()]
    assert {tuple(r["key"]) for r in saved} == {("3",), ("99",)}, saved
    by_key = {tuple(r["key"]): r["row"] for r in saved}
    assert by_key[("3",)]["v"] == "CHANGED"
    assert by_key[("99",)]["v"] == "extra"
    # the row that was only missing had no old value to keep
    assert all(r["key"] != ["11"] for r in saved), saved


def test_the_same_type_on_both_sides_is_compared_and_repaired(tmp_path):
    """`numeric` has no rendering migkit shares across engines. Declared
    the same on both sides of one engine it is compared by the engine's
    own text, and repaired: the target comes out as the source."""
    eng, dst, report = _build(
        tmp_path, "create table t (id integer primary key, v text, n numeric)",
        [(i, f"v{i}", i * 1.5) for i in range(1, 21)], THREE)
    assert eng.check_data("main")[0].status == "diff"
    for action in eng.repair_plan("main", "rows"):
        eng.apply("main", action)
    got = eng.check_data("main")[0]
    assert got.status == "ok", got.detail
    con = sqlite3.connect(dst)
    assert con.execute("select sum(n) from t").fetchone()[0] == 315.0
    assert con.execute("select n, typeof(n) from t where id = 11"
                       ).fetchone() == (16.5, "real")
    con.close()


def test_a_column_it_cannot_render_is_refused_rather_than_written_as_null(
        tmp_path):
    """Measured: `numeric` has no canonical rendering in sqlite, so where
    the two sides declare it differently it is left out of the comparison -
    and a repair that wrote the row anyway put the column in as NULL and
    left the table still differing."""
    eng, dst, report = _build(
        tmp_path, "create table t (id integer primary key, v text, n numeric)",
        [(i, f"v{i}", i * 1.5) for i in range(1, 21)], THREE)
    con = sqlite3.connect(dst)
    con.executescript("create table k as select * from t; drop table t;"
                      " create table t (id integer primary key, v text,"
                      " n decimal(10,2)); insert into t select * from k;"
                      " drop table k;")
    con.close()
    got = eng.check_data("main")
    assert got[0].status == "diff"
    actions = eng.repair_plan("main", "rows")
    assert actions, "nothing to repair, so the refusal cannot be shown"

    with pytest.raises(SystemExit) as caught:
        eng.apply("main", actions[0])
    said = str(caught.value)
    assert "n could not be rendered" in said, said
    assert "Nothing has been written" in said, said
    assert "migkit move" in said, said

    con = sqlite3.connect(dst)
    assert con.execute("select sum(n) from t").fetchone()[0] == 298.75
    assert con.execute("select v from t where id=3").fetchone() == ("CHANGED",)
    assert con.execute("select count(*) from t where id=99").fetchone()[0] == 1
    con.close()


def test_a_table_with_no_key_says_so_instead_of_listing_nothing(tmp_path):
    eng, dst, report = _build(tmp_path, "create table t (v text, r real)",
                              [(f"v{i}", i * 1.5) for i in range(1, 6)],
                              ["update t set v='CHANGED' where r=3.0"])
    got = eng.check_data("main")
    assert got[0].status == "diff", got[0].detail
    assert "rows not localised" in got[0].detail, got[0].detail
    assert "no key" in got[0].detail, got[0].detail
    assert not list(report.glob("data-t.*")), list(report.iterdir())
    assert eng.repair_plan("main", "rows") == []


def test_a_clean_run_clears_what_the_last_one_listed(tmp_path):
    eng, dst, report = _build(tmp_path, PLAIN, ROWS,
                              ["insert into t values (99,'extra',0.25)"])
    eng.check_data("main")
    assert (report / "data-t.extra").exists()

    con = sqlite3.connect(dst)          # put right by hand, then re-check
    con.execute("delete from t where id=99")
    con.commit()
    con.close()
    assert [r.status for r in eng.check_data("main")] == ["ok"]
    assert not list(report.glob("data-t.*")), list(report.iterdir())
    assert eng.repair_plan("main", "rows") == []


def test_the_counters_are_still_repaired_and_an_unknown_kind_raises(tmp_path):
    """Rows are new here; sequences were the only kind this engine had, and
    a dispatch that quietly did nothing with a kind it did not know would be
    the same empty answer in a different place."""
    eng, dst, report = _build(tmp_path, PLAIN, ROWS, THREE)
    eng.check_data("main")
    assert [a.kind for a in eng.repair_plan("main", "rows")] == ["rows"]
    assert [a.kind for a in eng.repair_plan("main", "all")] == ["rows"]

    with pytest.raises(SystemExit) as caught:
        eng.apply("main", RepairAction("main.t", "schema", [], [], ""))
    assert "schema" in str(caught.value)
