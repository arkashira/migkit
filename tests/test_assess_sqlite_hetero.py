"""`assess` on the two that used to answer with the bare template.

SQLite has no server to ask, so everything it can say is about the file and
about settings that live on the connection. Two of those are worth the check
and both were measured rather than assumed:

    pragma foreign_keys      0        on a fresh connection, always
    pragma integrity_check   raises   on a damaged file, rather than listing

A hetero hop has two servers and neither engine's own assess knows the other
is there, so it runs both and labels every row with which side it came from -
and then answers the question only the pair can answer, which is which of
migkit's operations work for this particular combination.
"""
import os
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine
from migkit.engines.hetero import HeteroEngine
from migkit.engines.sqlite import SQLiteEngine


def _sqlite_engine(src, dst):
    def ep(path):
        return Endpoint(host=str(path), port=0, user="", password="")
    return SQLiteEngine(Hop(name="s", engine="sqlite", source=ep(src),
                            target=ep(dst)))


def _seed(path, rows=200):
    conn = sqlite3.connect(path)
    conn.execute("create table t (id integer primary key,"
                 " parent integer references t(id), v text)")
    conn.executemany("insert into t values (?,?,?)",
                     [(i, None, "x" * 100) for i in range(rows)])
    conn.commit()
    assert conn.execute("select count(*) from t").fetchone()[0] == rows
    conn.close()


def _rows(items, needle):
    return [i for i in items if needle in i["item"]]


def test_a_healthy_pair_of_files_passes_and_names_them(tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    items = _sqlite_engine(src, dst).assess()
    for side in ("src", "dst"):
        got = _rows(items, f"{side} database file")
        assert got and got[0]["level"] == "pass", got
        assert str(src if side == "src" else dst) in got[0]["detail"]
        assert _rows(items, f"{side} integrity_check")[0]["level"] == "pass"


def test_foreign_keys_being_off_is_reported_rather_than_assumed(tmp_path):
    """SQLite parses the constraint and does not enforce it unless the
    connection turns it on - measured, `pragma foreign_keys` is 0 on a fresh
    connection. A source written that way can hold rows a target with
    enforcement will refuse."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    conn = sqlite3.connect(src)
    assert conn.execute("pragma foreign_keys").fetchone()[0] == 0
    conn.close()

    got = _rows(_sqlite_engine(src, dst).assess(), "src foreign_keys")
    assert got and got[0]["level"] == "warn", got
    assert "does not enforce them" in got[0]["detail"]


def test_a_damaged_file_is_a_failure_and_does_not_take_assess_down(tmp_path):
    """`pragma integrity_check` raises `database disk image is malformed`
    instead of listing problems, so a check that only read the returned rows
    would let the exception escape and lose every other row with it."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    data = bytearray(src.read_bytes())
    for i in range(4096, 4096 + 600):
        data[i] ^= 0xFF
    src.write_bytes(bytes(data))
    with pytest.raises(sqlite3.DatabaseError):
        sqlite3.connect(src).execute("pragma integrity_check").fetchall()

    items = _sqlite_engine(src, dst).assess()
    bad = _rows(items, "src integrity_check")
    assert bad and bad[0]["level"] == "fail", bad
    assert "malformed" in bad[0]["detail"]
    # and the rest of the assess still ran
    assert _rows(items, "dst integrity_check")[0]["level"] == "pass"
    assert _rows(items, "room for the target"), items


def test_a_missing_source_file_fails_and_a_missing_target_only_warns(
        tmp_path):
    """A target file is created by the first write. A source that is not
    there is the migration having nothing to read."""
    src, dst = tmp_path / "a.db", tmp_path / "gone.db"
    _seed(src)
    items = _sqlite_engine(src, dst).assess()
    missing = _rows(items, "dst database file")
    assert missing and missing[0]["level"] == "warn", missing
    assert "created by the first write" in missing[0]["detail"]

    items = _sqlite_engine(tmp_path / "nope.db", dst).assess()
    gone = _rows(items, "src database file")
    assert gone and gone[0]["level"] == "fail", gone


def test_the_version_row_is_answered_rather_than_skipped(tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _seed(src)
    _seed(dst)
    got = _rows(_sqlite_engine(src, dst).assess(), "server version match")
    assert got and got[0]["level"] == "pass", got
    assert "src 3." in got[0]["detail"], got[0]["detail"]


# ---------------------------------------------------------------- hetero

def _hetero(src, dst):
    ep = Endpoint(host="127.0.0.1", port=1, user="u", password="p")
    return HeteroEngine(Hop(name="h", engine="hetero", source=ep, target=ep,
                            options={"source_engine": src,
                                     "target_engine": dst}))


def test_the_pair_capabilities_are_answered_without_connecting():
    """A capability question is about the two engines, not about whether the
    servers happen to be up - so it is answered from the classes, and this
    test points at a port with nothing behind it to prove it."""
    cases = {
        ("postgres", "mysql"): (True, True, True, True),
        ("mongodb", "postgres"): (True, True, True, True),
        # sqlite has no change log, so nothing to tail
        ("sqlite", "postgres"): (True, True, True, False),
        # mongo makes the collection on the first write
        ("sqlite", "mongodb"): (True, True, True, False),
        ("mysql", "redis"): (False, False, False, False),
    }
    for pair, expected in cases.items():
        eng = _hetero(*pair)
        rows = {r["item"]: r for r in eng._pair_capabilities()}
        got = tuple(rows[f"this pair can {k}"]["level"] == "pass"
                    for k in ("compare", "move rows",
                              "create the target table", "tail changes"))
        assert got == expected, (pair, got, expected)


def test_an_unreachable_side_is_reported_and_does_not_hide_the_rest():
    """Both engines point at a dead port. The pair rows still come through,
    which is the half that does not need a server."""
    eng = _hetero("sqlite", "postgres")
    items = eng.assess()
    assert any(r["scope"] == "pair" and r["item"] == "engines"
               for r in items), items
    assert any("sqlite -> postgres" in r["detail"] for r in items), items


def test_every_row_says_which_side_it_came_from():
    """"server version match" means nothing in a cross-engine report unless
    the row says whose version it is."""
    eng = _hetero("sqlite", "postgres")
    items = [r for r in eng.assess() if r["scope"] != "pair"]
    assert items, "the two sides contributed nothing at all"
    for row in items:
        assert (row["item"].startswith(("source:", "target:"))
                or row["scope"].startswith(("source (", "target ("))), row


def test_a_pair_with_no_tail_says_so_instead_of_staying_quiet():
    eng = _hetero("sqlite", "postgres")
    rows = {r["item"]: r for r in eng._pair_capabilities()}
    tail = rows["this pair can tail changes"]
    assert tail["level"] == "warn", tail
    assert "sqlite has a change log" in tail["detail"], tail["detail"]


def test_the_capability_rows_match_what_the_classes_actually_override():
    """The rows are a reading of the code, so they are checked against the
    code rather than against a list written beside them."""
    eng = _hetero("postgres", "mysql")
    rows = {r["item"]: r for r in eng._pair_capabilities()}
    assert (rows["this pair can move rows"]["level"] == "pass") is (
        type(eng.src_engine).neutral_read is not Engine.neutral_read
        and type(eng.dst_engine).neutral_write is not Engine.neutral_write)
    assert (rows["this pair can tail changes"]["level"] == "pass") is (
        type(eng.src_engine).neutral_changes is not Engine.neutral_changes
        and type(eng.dst_engine)._apply_upsert is not Engine._apply_upsert)
