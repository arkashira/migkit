"""What the table copier holds is a read, not the table (backlog 45).

Measured with `/usr/bin/time -l` on real moves, MySQL to PostgreSQL:

    table              rows       before            after
    keyed by text      200,000    220 MB            99 MB
                     1,600,000    1,334 MB          99 MB
    no key at all      200,000    (the whole table)  89 MB
                     1,600,000                       88 MB

Three things held it. A table without a single integer key went down a
path that read it into one list. A table with no key was read by
`neutral_read` in one piece. A keyed read took `--chunk` rows at a time -
500,000 - whatever the rows weighed. Now a keyless table is read through a
cursor the server keeps, and each read is capped by a count and by the
table's own bytes a row. The same copy took 22.8 s against 23.0 s.
"""
import sqlite3
import tracemalloc

from migkit.config import Endpoint, Hop


def _lite(tmp_path, ddl, rows):
    from migkit.engines.sqlite import SQLiteEngine
    for name in ("a.db", "b.db"):
        con = sqlite3.connect(tmp_path / name)
        con.execute(ddl)
        if name == "a.db":
            con.executemany("insert into t values (?, ?)", rows)
        con.commit()
        con.close()
    ep = lambda p: Endpoint(host=str(tmp_path / p), port=0, user="",  # noqa
                            password="")
    hop = Hop(name="c", engine="sqlite", source=ep("a.db"),
              target=ep("b.db"), databases=["main"])
    hop.report_dir = lambda db=None: tmp_path
    return SQLiteEngine(hop)


ROWS = [(f"{i:08d}" + "x" * 200, i) for i in range(100_000)]


def test_a_keyless_table_comes_a_batch_at_a_time(tmp_path):
    eng = _lite(tmp_path, "create table t (a text, n int)", ROWS[:25])
    cols = [("a", "text"), ("n", "integer")]
    sizes = [len(b) for b in eng.neutral_batches("src", "main", "t", cols,
                                                 10)]
    assert sizes == [10, 10, 5], sizes


def test_reading_a_keyless_table_holds_one_batch(tmp_path):
    eng = _lite(tmp_path, "create table t (a text, n int)", ROWS)
    cols = [("a", "text"), ("n", "integer")]
    tracemalloc.start()
    try:
        seen = sum(len(b) for b in eng.neutral_batches("src", "main", "t",
                                                       cols, 1000))
        _, batched = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        whole, _ = eng.neutral_read("src", "main", "t", cols)
        _, one_piece = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert seen == len(whole) == 100_000
    # the control: the one-piece read of the same table, as it was
    assert one_piece > 20 * batched, (batched, one_piece)


def test_the_copier_moves_a_keyless_table_a_batch_at_a_time(tmp_path,
                                                          monkeypatch):
    eng = _lite(tmp_path, "create table t (a text, n int)", ROWS[:25])
    from migkit.engines.sqlite import SQLiteEngine
    written = []
    real = SQLiteEngine.neutral_write

    def spy(self, side, db, table, columns, rows):
        written.append(len(rows))
        return real(self, side, db, table, columns, rows)
    monkeypatch.setattr(SQLiteEngine, "neutral_write", spy)

    class Checkpoint(dict):
        def save(self):
            pass
    said = []
    eng.move_table("main", "", "t", 10, Checkpoint(), said.append)
    assert written == [10, 10, 5], (written, said)
    con = sqlite3.connect(tmp_path / "b.db")
    assert con.execute("select count(*) from t").fetchone()[0] == 25
    con.close()
    assert any("25 rows in one pass" in s for s in said), said


def test_a_read_is_capped_by_the_rows_weight(tmp_path, monkeypatch):
    """`--chunk` asked for 500,000 rows at a time whatever they weighed."""
    from migkit.engines.hetero import HeteroEngine
    eng = _lite(tmp_path, "create table t (a text primary key, n int)",
                ROWS[:10])
    pair = eng._as_pair()
    facts = {"t": {"rows": 1000, "bytes": 1000 * 2 ** 20}}  # a MB a row
    monkeypatch.setattr(type(pair.src_engine), "table_facts",
                        lambda self, side, db: facts)
    assert pair._read_rows("main", "t", 500_000) == \
        HeteroEngine.READ_BYTES // 2 ** 20
    facts["t"] = {"rows": 10_000_000, "bytes": 2_500_000_000}  # 250 B
    assert pair._read_rows("main", "t", 500_000) == HeteroEngine.READ_ROWS
    # a smaller chunk is kept, and no estimate leaves the count alone
    assert pair._read_rows("main", "t", 1000) == 1000
    facts.clear()
    assert pair._read_rows("main", "t", 500_000) == HeteroEngine.READ_ROWS
