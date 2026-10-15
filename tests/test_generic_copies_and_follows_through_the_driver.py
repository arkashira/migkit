"""The generic engine copies, follows, fences, confirms and verifies only
what changed through a standard driver (backlog R13-GENERIC): the copy in
the key's order resumed after the last committed key, a cut-off copy's
rows past its checkpoint removed before it goes on, a follow by the
column of when a row changed, a fence as a marker row in migkit's own
table, rows compared by key. The driver here is a real DB-API one (an
in-memory database in this process, placeholder style `qmark`); no
server is reached."""
import json
import sqlite3
from types import SimpleNamespace

import pytest

from migkit import capabilities, tailctl
from migkit.engines.generic import GenericEngine

PROVES = [("move.table-copy", "generic"), ("tail.follows", "generic"),
          ("verify.fence", "generic"), ("verify.confirm", "generic"),
          ("verify.delta", "generic")]

COLUMNS = ("id", "name", "updated_at")
TYPES = {"id": "INTEGER", "name": "TEXT", "updated_at": "TIMESTAMP"}


class Checkpoint(dict):
    def __init__(self):
        super().__init__()
        self.saves = 0

    def save(self):
        self.saves += 1


def _db(rows=()):
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT,"
              " updated_at TIMESTAMP)")
    c.executemany("INSERT INTO items VALUES (?, ?, ?)", rows)
    c.commit()
    return c


def engine(tmp_path, src, dst, key="id", unique=True, types=None,
           named=None, columns=COLUMNS):
    eng = GenericEngine.__new__(GenericEngine)
    eng.hop = SimpleNamespace(name="g", report_dir=lambda db: tmp_path,
                              options={})
    raw = {"src": src, "dst": dst}
    eng._dbapi = lambda side: ((lambda fn: fn(raw[side])), "qmark")
    eng._quote = lambda side, name: f'"{name}"'
    eng._from = lambda conn, t: t
    eng._connect = lambda side: raw[side]
    eng._close = lambda: None
    eng._tables = lambda: ["items"]
    eng._key = lambda: [key] if isinstance(key, str) else list(key)
    eng._unique = lambda table: unique
    eng._side_columns = lambda which, table: tuple(columns)
    eng._declared_types = lambda side, table: dict(types or TYPES)
    eng.newer_wins_column = lambda: named
    eng.drills = {}
    eng._write_drill = lambda db, name, **kinds: eng.drills.update(
        {name: kinds})
    return eng


def rows(c, table="items"):
    return sorted(c.execute(f"SELECT * FROM {table}").fetchall())


SOURCE = [(i, f"n{i}", f"2026-10-0{1 + i % 9} 00:00:00")
          for i in range(1, 26)]


def test_a_table_is_copied_in_the_key_order_in_batches(tmp_path):
    src, dst = _db(SOURCE), _db()
    eng, ck, said = engine(tmp_path, src, dst), Checkpoint(), []
    eng.move_table("db", "", "items", 7, ck, said.append)
    assert rows(dst) == rows(src)
    st = ck["db.items"]
    assert st["way"] == "key-order" and st["done"] and st["moved"] == 25
    assert st["after"] == [25]
    # a checkpoint per committed batch, never one at the end only
    assert ck.saves >= 25 // 7 + 2
    assert any("rows copied in the key's order" in s for s in said)


def test_a_cut_off_copy_removes_its_uncheckpointed_rows_and_goes_on(
        tmp_path):
    src = _db(SOURCE)
    # 10 rows were committed but the checkpoint says only 5
    dst = _db(SOURCE[:10])
    eng, ck = engine(tmp_path, src, dst), Checkpoint()
    ck["db.items"] = {"way": "key-order", "began": True, "after": [5],
                      "moved": 5}
    eng.move_table("db", "", "items", 4, ck, lambda s: None)
    assert rows(dst) == rows(src)
    assert ck["db.items"]["moved"] == 25


def test_a_target_holding_rows_this_copy_did_not_write_is_refused(tmp_path):
    src, dst = _db(SOURCE), _db([(99, "stray", None)])
    eng = engine(tmp_path, src, dst)
    with pytest.raises(SystemExit, match="did not write"):
        eng.move_table("db", "", "items", 5, Checkpoint(), lambda s: None)
    assert rows(dst) == [(99, "stray", None)]


def test_a_key_the_source_does_not_hold_unique_copies_the_table_whole(
        tmp_path):
    src, dst = _db(SOURCE), _db()
    eng, ck = engine(tmp_path, src, dst, unique=False), Checkpoint()
    eng.move_table("db", "", "items", 6, ck, lambda s: None)
    assert ck["db.items"]["way"] == "whole-table"
    assert "after" not in ck["db.items"]
    assert rows(dst) == rows(src)


def test_a_column_missing_on_the_target_stops_the_copy(tmp_path):
    src, dst = _db(SOURCE), _db()
    eng = engine(tmp_path, src, dst)
    eng._side_columns = lambda which, table: (
        COLUMNS if which == "src" else COLUMNS[:2])
    with pytest.raises(SystemExit, match="updated_at not on the target"):
        eng.move_table("db", "", "items", 5, Checkpoint(), lambda s: None)
    assert rows(dst) == []


def test_the_placeholder_style_is_the_drivers():
    eng = GenericEngine.__new__(GenericEngine)
    assert eng._marks("qmark", [1, 2]) == (["?", "?"], (1, 2))
    assert eng._marks("format", [1]) == (["%s"], (1,))
    assert eng._marks("numeric", [1, 2], start=3) == ([":3", ":4"], (1, 2))
    assert eng._marks("named", ["a"]) == ([":p1"], {"p1": "a"})


def test_a_many_column_key_resumes_column_by_column():
    eng = GenericEngine.__new__(GenericEngine)
    eng._quote = lambda side, name: name
    sql, params = eng._keyed_where("src", "qmark", ["a", "b"], [1, 2],
                                   op=">")
    assert sql == "(a > ?) OR (a = ? AND b > ?)"
    assert params == (1, 1, 2)


def test_the_change_column_is_the_catalogues_or_the_hops(tmp_path):
    eng = engine(tmp_path, _db(), _db())
    column, choice = eng.follow_way("items")
    assert column == "updated_at"
    assert choice.rung.name == "catalogue-column"
    eng.newer_wins_column = lambda: "name"
    assert eng.follow_way("items")[0] == "name"


@pytest.mark.parametrize("types, why", [
    ({"id": "INTEGER", "name": "TEXT", "updated_at": "TEXT"},
     "no column on both sides is named and typed"),
    ({"id": "INTEGER", "modified_at": "TIMESTAMP",
      "updated_at": "TIMESTAMP"}, "more than one column"),
])
def test_a_table_no_change_column_finds_is_not_followed(tmp_path, types,
                                                        why):
    eng = engine(tmp_path, _db(), _db(), types=types)
    column, choice = eng.follow_way("items")
    assert column is None and why in choice.reason
    with pytest.raises(SystemExit, match="Nothing is followed"):
        eng._follow_point("db")


def test_a_table_without_its_key_is_not_followed(tmp_path):
    eng = engine(tmp_path, _db(), _db(), key="code")
    column, choice = eng.follow_way("items")
    assert column is None and "the key is not on both sides" in choice.reason


def test_a_follow_round_carries_changes_and_the_fence_marker(tmp_path,
                                                             monkeypatch):
    src, dst = _db(SOURCE[:3]), _db(SOURCE[:3])
    eng = engine(tmp_path, src, dst)
    token = eng._follow_point("db")
    assert token["tables"]["items"]["column"] == "updated_at"
    src.execute("UPDATE items SET name = 'x', updated_at ="
                " '2026-10-09 00:00:00' WHERE id = 2")
    src.execute("INSERT INTO items VALUES (4, 'n4', '2026-10-09 00:00:00')")
    src.commit()
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: True)
    marker = eng.src_lsn("db")
    assert eng.fence_wait("db", marker, timeout=0.2) is False
    after, went, settled = eng._follow_round(token, lambda s: None)
    assert went == 2 and settled
    assert rows(dst) == rows(src)
    assert eng.fence_wait("db", marker, timeout=1) is True
    # a second round finds nothing new; the rows at the saved value are put
    # again without being counted
    again, went, _ = eng._follow_round(after, lambda s: None)
    assert went == 0 and rows(dst) == rows(src)


def test_no_follow_running_is_no_fence(tmp_path, monkeypatch):
    eng = engine(tmp_path, _db(), _db())
    monkeypatch.setattr(tailctl, "alive", lambda where, hop=None: False)
    assert eng.src_lsn("db") is None
    assert eng.fence_wait("db", None) is None


def test_the_confirm_compares_the_named_keys_on_both_sides(tmp_path):
    src = _db(SOURCE[:4])
    dst = _db([SOURCE[0], (2, "other", SOURCE[1][2]), SOURCE[2],
               (9, "stray", None)])
    eng = engine(tmp_path, src, dst)
    keys = {json.dumps([str(k)]) for k in (1, 2, 4, 9)}
    missing, extra, changed = eng._compare_pks("db", "items", keys)
    assert missing == ['["4"]'] and extra == ['["9"]']
    assert changed == ['["2"]']


def test_delta_verifies_only_rows_changed_since_the_last_clean_pass(
        tmp_path):
    src, dst = _db(SOURCE[:5]), _db(SOURCE[:5])
    eng = engine(tmp_path, src, dst)
    eng._delta_held = lambda result: (result, True)
    first = eng.delta_verify("db")
    assert first[0].status == "ok" and "baseline" in first[0].detail
    src.execute("UPDATE items SET name = 'x', updated_at ="
                " '2026-10-09 00:00:00' WHERE id = 3")
    src.commit()
    got = eng.delta_verify("db")
    assert got[0].status == "diff" and "changed=1" in got[0].detail
    assert eng.drills["items"]["changed"] == ['["3"]']
    held = json.loads((tmp_path / "generic-delta.json").read_text())
    # a table that differs keeps its position
    assert held["tables"]["items"] != "2026-10-09 00:00:00"
    dst.execute("UPDATE items SET name = 'x', updated_at ="
                " '2026-10-09 00:00:00' WHERE id = 3")
    dst.commit()
    clean = eng.delta_verify("db")
    assert clean[0].status == "ok" and "deleted" in clean[0].detail


def test_delta_says_a_table_no_column_finds(tmp_path):
    eng = engine(tmp_path, _db(), _db(),
                 types={"id": "INTEGER", "name": "TEXT",
                        "updated_at": "TEXT"})
    got = eng.delta_verify("db")
    assert got[0].status == "skip"
    assert "not verified by change" in got[0].detail


def test_the_five_cells_are_read_off_the_code():
    row = capabilities.matrix()["generic"]
    for cap in ("table-copy", "stream", "fence", "confirm", "delta"):
        assert row[cap] == "yes"
        assert cap not in capabilities.GAPS["generic"]
