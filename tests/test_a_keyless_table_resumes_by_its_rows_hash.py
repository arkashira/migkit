"""A table with no key, copied, checked and gone on from by buckets of its
rows' hash (backlog P0-KEYLESS).

Nothing identifies a row of a table with no key, so on a source that keeps
no position of its rows (MySQL, SQL Server, a file) the copy went in one
pass, and a stop started it over. It goes in buckets now: the top bits of
each row's canonical hash, the same buckets the check reads the table by
(`leaves.Spec("row")`), found by each server - so a bucket a stop left
begun is emptied on the target by the same hash and copied again, and a
row held twice is carried twice.

SQLite to SQLite needs no server: the hash is a function migkit registers
on the file's connection (`migkit_hash60`). MySQL to MySQL is the same in
the server's own SQL.
"""
import sqlite3

import pytest

from migkit.config import Endpoint, Hop

ROWS = 600


class _Checkpoint(dict):
    def save(self):
        pass


def _pair(tmp_path, dupes=True):
    from migkit.engines.hetero import HeteroEngine
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    con = sqlite3.connect(src)
    con.execute("create table log (at integer, msg text, raw blob)")
    con.executemany("insert into log values (?, ?, ?)",
                    [(i, f"entry {i}", bytes([i % 256]) * 3)
                     for i in range(ROWS)])
    if dupes:
        # rows held more than once: told apart by nothing, carried by count
        con.executemany("insert into log values (?, ?, ?)",
                        [(7, "entry 7", bytes([7]) * 3)] * 3)
    con.commit()
    con.close()
    con = sqlite3.connect(dst)
    con.execute("create table log (at integer, msg text, raw blob)")
    con.execute("insert into log values (-1, 'a stray the copy removes',"
                " null)")
    con.commit()
    con.close()
    hop = Hop(name="k", engine="hetero",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"],
              options={"source_engine": "sqlite", "target_engine": "sqlite"})
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop), src, dst


def _held(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("select at, msg, raw, count(*) from log group by"
                           " at, msg, raw order by at").fetchall()
    finally:
        con.close()


@pytest.fixture
def small_buckets(monkeypatch):
    from migkit import ranges
    # buckets of about 50 rows, so 600 rows are 16 of them
    monkeypatch.setattr(ranges, "BUCKET_ROWS", 50)


def test_the_bucket_is_a_node_of_the_checks_tree():
    """One logic: the copy's bucket is where the check's `row` leaves put
    the row, a level up the same tree."""
    from migkit import leaves
    spec = leaves.Spec("row", bits=4, salt="s")
    fine = leaves.Spec("row", bits=10, salt="s")
    h = leaves.Hasher(spec, ["integer", "text"], [])
    for row in ([1, "a"], [2, "b"], [99, "zz"]):
        b, rh, _ = h.of(row)
        assert b == rh >> (leaves.BITS - 4)
        assert fine.bucket(rh) >> 6 == b


def test_the_way_is_chosen_by_what_the_table_is(tmp_path, small_buckets):
    """Buckets where the table is large enough and both sides hash a row;
    one pass for a small one, with the reason."""
    from migkit import ranges
    eng, _, _ = _pair(tmp_path)
    src, dst = eng.src_engine, eng.dst_engine
    cols = [("at", "integer"), ("msg", "text"), ("raw", "bytes")]
    st = {}
    way = ranges.keyless("main.log", st, src, dst, "main", "log", "log", cols,
                         cols, ROWS, lambda: None, "salt")
    assert way.chunk == "buckets" and way.by_bucket, way.strategy
    assert len(way.spans) == 16, len(way.spans)
    assert all(w and there for _, w, there in way.spans), way.spans
    assert st["keyless"] == {"chunk": "buckets", "resume": "by bucket"}
    small = ranges.keyless("main.log", {}, src, dst, "main", "log", "log",
                           cols, cols, 60, lambda: None, "salt")
    assert small.chunk == "one pass", small.strategy
    assert "starting it over costs less" in small.strategy.reasons["chunk"], \
        small.strategy.reasons


def test_the_buckets_partition_the_rows_on_both_sides(tmp_path,
                                                      small_buckets):
    """Every row falls in exactly one bucket, and the same one found on the
    source and on the target: the buckets' counts add up to the table."""
    from migkit import ranges
    eng, src_path, _ = _pair(tmp_path)
    cols = [("at", "integer"), ("msg", "text"), ("raw", "bytes")]
    way = ranges.keyless("main.log", {}, eng.src_engine, eng.dst_engine,
                         "main", "log", "log", cols, cols, ROWS,
                         lambda: None, "salt")
    total = 0
    con = sqlite3.connect(src_path)
    eng.src_engine._register(con)
    try:
        for _, where, there in way.spans:
            assert where == there
            total += con.execute(
                f'select count(*) from "log" where {where}').fetchone()[0]
    finally:
        con.close()
    assert total == ROWS + 3, total


def test_a_keyless_table_is_copied_by_bucket_with_its_duplicates(
        tmp_path, small_buckets):
    eng, src, dst = _pair(tmp_path)
    said = []
    ck = _Checkpoint()
    eng.move_table("main", "", "log", 1000, ck, said.append)
    assert _held(dst) == _held(src)
    st = ck["main.log"]
    assert st["done"] and len(st["spans_done"]) == len(st["spans"]) == 16
    assert any("buckets of each row's hash" in m for m in said), said
    # the check reads it by the same hash, and finds it equal
    got = [r for r in eng.check_data("main") if r.scope.endswith(".log")]
    assert [r.status for r in got] == ["ok"], [r.detail for r in got]


def test_a_stop_costs_the_bucket_it_was_in(tmp_path, small_buckets,
                                          monkeypatch):
    """Stopped after a bucket's rows landed and before it was marked done:
    the next run empties that bucket on the target by the same hash, copies
    it again, and copies no bucket that was done."""
    from migkit import ranges
    eng, src, dst = _pair(tmp_path)
    real, calls = ranges.span_copied, []

    def dies_on_the_third(*a, **k):
        calls.append(1)
        if len(calls) == 3:
            raise RuntimeError("the machine went away")
        return real(*a, **k)
    monkeypatch.setattr(ranges, "span_copied", dies_on_the_third)
    ck = _Checkpoint()
    with pytest.raises(RuntimeError):
        eng.move_table("main", "", "log", 1000, ck, [].append)
    st = ck["main.log"]
    assert len(st["spans_done"]) == 2 and len(st["spans_begun"]) == 3, st
    # the third bucket's rows are on the target, and nothing says so but
    # the mark that it was begun
    assert sum(n for *_, n in _held(dst)) > sum(
        int(v[0]) for v in st["span_tally"].values()), _held(dst)

    again = []
    monkeypatch.setattr(ranges, "span_copied",
                        lambda *a, **k: again.append(1) or real(*a, **k))
    said = []
    eng.move_table("main", "", "log", 1000, ck, said.append)
    assert len(again) == 16 - 2, len(again)
    assert any("was begun and not finished; emptied" in m for m in said), said
    assert _held(dst) == _held(src)


def test_a_bucket_the_target_changed_is_found_and_copied_again(
        tmp_path, small_buckets, monkeypatch):
    """The whole table held to what was copied; where it does not add up,
    each bucket is held to the source's and only those that differ are
    copied again."""
    from migkit.engines.sqlite import SQLiteEngine
    eng, src, dst = _pair(tmp_path)
    real, spoiled = SQLiteEngine.neutral_write, []

    def spoils_one_batch(self, side, db, table, columns, rows):
        # a target that changes one value of the first batch it is given
        if not spoiled and rows:
            spoiled.append(1)
            rows = [list(rows[0])] + [list(r) for r in rows[1:]]
            rows[0][1] = "changed on the way"
        return real(self, side, db, table, columns, rows)
    monkeypatch.setattr(SQLiteEngine, "neutral_write", spoils_one_batch)
    said = []
    eng.move_table("main", "", "log", 1000, _Checkpoint(), said.append)
    assert any("1 of 16 buckets differ, and only those are copied again"
               in m for m in said), said
    assert _held(dst) == _held(src)


def test_a_checkpoint_from_a_copy_by_pages_goes_on_by_them(tmp_path,
                                                          small_buckets):
    """Spans a checkpoint held before the way was kept beside them were a
    copy by pages; a source that cannot read pages drops them and starts
    over, said."""
    from migkit import ranges
    eng, _, _ = _pair(tmp_path)
    cols = [("at", "integer"), ("msg", "text"), ("raw", "bytes")]
    st = {"spans": [[0, "rowid < 10"], [10, "rowid >= 10"]],
          "spans_done": [0], "span_tally": {"0": [9, None]}}
    way = ranges.keyless("main.log", st, eng.src_engine, eng.dst_engine,
                         "main", "log", "log", cols, cols, ROWS,
                         lambda: None, "salt")
    assert way.restart and way.chunk == "buckets", way.strategy
    assert "spans_done" not in st


# --- MySQL to MySQL ---------------------------------------------------------

MY_SRC, MY_DST = "migkit-test-vd-my-src", "migkit-test-vd-my-dst"
MY_PORTS = {MY_SRC: 16272, MY_DST: 16273}


def _my(name, sql):
    import subprocess
    return subprocess.run(
        ["docker", "exec", "-i", name, "mysql", "-uroot", "-ptest", "-N",
         "-e", sql], capture_output=True, text=True)


@pytest.fixture(scope="module")
def my_pair():
    import socket
    import subprocess
    import time

    from tests.conftest import _have_docker
    if not _have_docker():
        pytest.skip("docker not available")
    for n in MY_PORTS:
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
    try:
        for n, p in MY_PORTS.items():
            subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                            "MYSQL_ROOT_PASSWORD=test", "-p",
                            f"127.0.0.1:{p}:3306", "mysql:8.4"],
                           check=True, capture_output=True)
        for n, p in MY_PORTS.items():
            end = time.time() + 300
            while time.time() < end:
                with socket.socket() as s:
                    s.settimeout(2)
                    up = s.connect_ex(("127.0.0.1", p)) == 0
                if up and _my(n, "select 1").returncode == 0:
                    break
                time.sleep(2)
            else:
                pytest.fail(f"{n} never answered")
        yield MY_PORTS
    finally:
        for n in MY_PORTS:
            subprocess.run(["docker", "rm", "-f", "-v", n],
                           capture_output=True)


def _my_engine(tmp_path):
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="myk", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=MY_PORTS[MY_SRC],
                              user="root", password="test"),
              target=Endpoint(host="127.0.0.1", port=MY_PORTS[MY_DST],
                              user="root", password="test"),
              databases=["k"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


def _my_seed():
    for n in MY_PORTS:
        assert _my(n, "drop database if exists k; create database k;"
                      " create table k.log (at int, msg varchar(40),"
                      " raw varbinary(8))").returncode == 0
    got = _my(MY_SRC, "insert into k.log with recursive g(i) as (select 0"
                      " union all select i + 1 from g where i < 599)"
                      " select i, concat('entry ', i), unhex(lpad(hex(i % 256),"
                      " 2, '0')) from g; insert into k.log values"
                      " (7, 'entry 7', unhex('07')), (7, 'entry 7',"
                      " unhex('07')); analyze table k.log")
    assert got.returncode == 0, got.stderr


def _my_held(name):
    return _my(name, "select at, msg, hex(raw), count(*) from k.log group by"
                     " at, msg, raw order by at, msg").stdout


def test_mysql_copies_a_keyless_table_by_bucket_and_goes_on(
        my_pair, tmp_path, small_buckets, monkeypatch):
    """MySQL keeps no position of a row: a stop used to start the table
    over. Each bucket now lands in one transaction, is read back by both
    servers, and a stop costs one."""
    from migkit import ranges
    from migkit.cli import _Checkpoint as Saved
    _my_seed()
    eng = _my_engine(tmp_path)
    real, calls = ranges.span_copied, []

    def dies_on_the_fourth(*a, **k):
        calls.append(1)
        if len(calls) == 4:
            raise RuntimeError("the machine went away")
        return real(*a, **k)
    monkeypatch.setattr(ranges, "span_copied", dies_on_the_fourth)
    with pytest.raises(RuntimeError):
        eng.move_table("k", "", "log", 1000,
                       Saved(tmp_path / "move.json"), [].append)
    monkeypatch.setattr(ranges, "span_copied", real)
    saved = Saved(tmp_path / "move.json")
    st = saved["k.log"]
    assert st.get("keyless") == {"chunk": "buckets",
                                 "resume": "by bucket"}, st
    assert len(st["spans_done"]) == 3, st
    said = []
    eng.move_table("k", "", "log", 1000, Saved(tmp_path / "move.json"),
                   said.append)
    assert any("was begun and not finished; emptied" in m for m in said), said
    assert _my_held(MY_SRC) == _my_held(MY_DST)
    assert "entry 7\t07\t3" in _my_held(MY_DST)
