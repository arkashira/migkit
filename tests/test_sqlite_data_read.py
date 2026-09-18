"""The SQLite data check, on the tables it used to quietly excuse.

`check_data` ordered every table by `rowid`. Not every table has one: a
WITHOUT ROWID table raises `no such column: rowid`, and the old read caught
that, returned the error text as the table's digest and `-1` as its row count,
and then compared the two sides. Both sides had failed the same way, so both
strings matched and the table was reported **ok** - with entirely different
contents in the two files:

    ok    main.wr   rows -1==-1, md5 error: no such column: rowid both sides

A check that cannot read a table has to say so. Two failures are not agreement.

The second thing here is how much of the file one read holds. Measured on a WAL
database with 157 frames waiting to be checkpointed:

    no reader at all              157 of 157 frames checkpointed
    a select part-way through       0 of 157   - blocked
    the moment that select ended  157 of 157

So a whole-table read pins the write-ahead log for as long as it runs, and
anything else writing to that file keeps growing a log it cannot reclaim. The
read is chunked now, and what releases the snapshot is the statement ending -
not the connection closing, which was measured too.
"""
import os
import sqlite3

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.sqlite import SQLiteEngine


def _engine(src, dst):
    def ep(path):
        return Endpoint(host=str(path), port=0, user="", password="")
    return SQLiteEngine(Hop(name="s", engine="sqlite", source=ep(src),
                            target=ep(dst)))


def _build(path, value, rows=6):
    conn = sqlite3.connect(path)
    conn.execute("create table wr (k text, part integer, v text,"
                 " primary key (k, part)) without rowid")
    conn.executemany("insert into wr values (?,?,?)",
                     [(f"k{i}", i, value) for i in range(rows)])
    conn.execute("create table nokey (a text, b blob)")
    conn.executemany("insert into nokey values (?,?)",
                     [(value, b"\x00\x01\xff")] * rows)
    conn.execute("create table normal (id integer primary key, v text)")
    conn.executemany("insert into normal (v) values (?)", [(value,)] * rows)
    conn.commit()
    conn.close()


def _by_table(results):
    return {r.scope.split(".", 1)[1]: r for r in results}


def test_a_table_without_rowid_is_compared_instead_of_excused(tmp_path):
    """The bug itself: two files whose WITHOUT ROWID table holds completely
    different values, which used to report ok."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "alice")
    _build(dst, "COMPLETELY DIFFERENT")
    with pytest.raises(sqlite3.OperationalError, match="no such column: rowid"):
        sqlite3.connect(src).execute("select * from wr order by rowid")

    got = _by_table(_engine(src, dst).check_data("main"))
    assert got["wr"].status == "diff", got["wr"].detail
    assert "rows 6 vs 6" in got["wr"].detail, got["wr"].detail
    # and nothing anywhere reports the old `-1` row count
    assert all("-1" not in r.detail for r in got.values()), got


def test_identical_files_still_agree_on_every_shape_of_table(tmp_path):
    """A brake that reports differences everywhere is not an improvement.
    Three tables: composite key without rowid, no key at all, integer key."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "same")
    _build(dst, "same")
    got = _by_table(_engine(src, dst).check_data("main"))
    assert set(got) == {"wr", "nokey", "normal"}, sorted(got)
    for name, res in got.items():
        assert res.status == "ok", (name, res.detail)
        assert "rows 6==6" in res.detail, (name, res.detail)


def test_two_reads_that_both_fail_are_an_error_not_an_agreement(tmp_path):
    """The general form of the same mistake. Both files are damaged in the
    same way, so both reads raise the same sentence - which is exactly when
    the old comparison was most confident."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "alice", rows=400)
    _build(dst, "bob", rows=400)
    for path in (src, dst):
        data = bytearray(path.read_bytes())
        for i in range(4096, min(len(data), 4096 + 900)):
            data[i] ^= 0xFF
        path.write_bytes(bytes(data))

    results = _engine(src, dst).check_data("main")
    assert results, "a damaged pair produced no rows at all"
    assert not any(r.status == "ok" for r in results), [
        (r.scope, r.status, r.detail) for r in results]
    broken = [r for r in results if r.status == "error"]
    assert broken, [(r.scope, r.status, r.detail) for r in results]
    assert any("source:" in r.detail or "target:" in r.detail for r in broken)
    assert any("not the same as" in r.fix_hint or "not the same as" in r.detail
               for r in broken), [r.fix_hint for r in broken]


def test_a_table_missing_on_the_target_is_a_difference_not_an_error(tmp_path):
    """Absent is a finding, not a failure to measure - the two are worth
    telling apart in the report."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "same")
    _build(dst, "same")
    conn = sqlite3.connect(dst)
    conn.execute("drop table wr")
    conn.commit()
    conn.close()

    got = _by_table(_engine(src, dst).check_data("main"))
    assert got["wr"].status == "diff", got["wr"].detail
    assert "missing on target" in got["wr"].detail
    assert got["normal"].status == "ok"


def test_a_target_that_cannot_be_opened_is_not_a_clean_check(tmp_path):
    src = tmp_path / "a.db"
    _build(src, "same")
    missing = tmp_path / "not-there.db"
    results = _engine(src, missing).check_data("main")
    assert [r.status for r in results] == ["error"], results
    # the path and which side, which the bare driver error carried neither of
    assert str(missing) in results[0].detail, results[0].detail
    assert "target database file does not exist" in results[0].detail


def test_the_chunk_boundary_does_not_change_what_is_compared(tmp_path):
    """Paging a composite key uses a row-value comparison, and a mistake
    there drops or repeats rows at every boundary. The digests are checked
    against the same tables read in one go."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "same", rows=25)
    _build(dst, "same", rows=25)
    whole = _engine(src, dst)
    small = _engine(src, dst)
    small.CHUNK = 2
    for name in ("wr", "nokey", "normal"):
        a = whole._hash("src", name)
        b = small._hash("src", name)
        assert a == b, (name, a, b)
        assert a[1] == 25 and not a[2], (name, a)

    # and a change in a later chunk is still caught
    conn = sqlite3.connect(dst)
    conn.execute("update wr set v = 'x' where part = 24")
    conn.commit()
    conn.close()
    assert _by_table(small.check_data("main"))["wr"].status == "diff"


def test_the_read_is_issued_as_bounded_statements(tmp_path):
    """What the chunking is for, read off the statements that reach SQLite
    rather than off the row count."""
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    _build(src, "same", rows=25)
    _build(dst, "same", rows=25)
    eng = _engine(src, dst)
    eng.CHUNK = 10
    seen = []
    plain = eng._reader

    def traced(side):
        conn = plain(side)
        conn.set_trace_callback(seen.append)
        return conn
    eng._reader = traced
    assert eng._hash("src", "normal")[1] == 25

    selects = [s for s in seen if s.lower().startswith("select")]
    assert len(selects) == 3, selects
    assert all("limit 10" in s for s in selects), selects
    assert not any(s for s in selects if "limit" not in s), selects


def test_a_finished_statement_is_what_lets_the_log_be_reclaimed(tmp_path):
    """The measurement the chunking rests on, re-run here so it stays true on
    whatever SQLite the machine has: a read in progress blocks a checkpoint,
    and finishing it - not closing the connection - unblocks one."""
    path = tmp_path / "wal.db"
    w = sqlite3.connect(path)
    w.execute("pragma journal_mode=wal")
    w.execute("pragma wal_autocheckpoint=0")
    w.execute("create table t (id integer primary key, v text)")
    w.executemany("insert into t (v) values (?)", [("x" * 300,)] * 5000)
    w.commit()
    w.execute("pragma wal_checkpoint(truncate)")

    def churn():
        for i in range(1500):
            w.execute("update t set v = ? where id = ?", ("y" * 300, i + 1))
            if i % 500 == 0:
                w.commit()
        w.commit()

    def checkpoint():
        return w.execute("pragma wal_checkpoint(passive)").fetchone()

    r = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    cur = r.execute("select * from t order by rowid")
    cur.fetchone()
    churn()
    _, frames, moved = checkpoint()
    assert frames > 0, "the writer produced no log to reclaim"
    assert moved == 0, (frames, moved)

    cur.fetchall()                      # the statement ends; r stays open
    churn()
    _, frames2, moved2 = checkpoint()
    assert frames2 > 0 and moved2 == frames2, (frames2, moved2)
    r.close()
    w.close()
    assert os.path.exists(path)
