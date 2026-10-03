"""The tail pays once what it can, not once a change (audit 2026-10-01,
SP3, SP7, SP8, SP12).

* Holding a change to what the copy read: each entry's plan indexed once
  and a mark asked once a batch about a transaction; an entry the tail is
  past every mark of retired - and never where counters add.
* The kept proof: a table copied or parked beside the tail flagged, not
  every leaf of it rewritten each batch.
* The source's shape: a digest of its catalogue asked of the server
  first, and the whole shape read only where it moved; held to the last
  by its fingerprint, compared and written only where it moved.
* The row filter's translation and a key's classes: once a table, again
  after the shape changes.
"""
import json

import pytest

from migkit import kept, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine, _CopiedRanges
from tests.conftest import needs_docker, psql


class _Src:
    def __init__(self):
        self.asked = 0

    def mark_covers(self, side, db, mark, change):
        self.asked += 1
        return change["txn"] < 100

    def snapshot_mark(self, side, db):
        return None


class _Eng:
    def __init__(self, options=None):
        self.hop = Hop(name="sp", engine="postgres", source=Endpoint(),
                       target=Endpoint(), options=dict(options or {}))
        self.src_engine = _Src()

    @staticmethod
    def _leaf(t):
        return str(t).split(".")[-1]


def _copied(where, n=50, done=True):
    rs = [[i * 10, (i + 1) * 10] for i in range(n)]
    (where / "move.json").write_text(json.dumps({"db..t": {
        "table": "public.t", "key": "id", "ranges": rs,
        "ranges_done": [r[0] for r in rs],
        "ranges_seen": {str(r[0]): f"m{r[0]}" for r in rs}, "done": done}}))


def _change(k, txn):
    return {"op": "update", "table": "public.t", "key": {"id": k},
            "values": {"id": k}, "txn": txn}


def test_a_mark_is_asked_once_a_batch_about_a_transaction(tmp_path):
    _copied(tmp_path)
    copied = _CopiedRanges(_Eng(), "db", tmp_path)
    eng = copied.eng
    # forty changes of one transaction in one range
    batch = [_change(5, 7) for _ in range(40)]
    out, left = copied.leave_out(batch)
    assert out == [] and left == {"public.t": 40}
    # its range's mark and the newest, once each
    assert eng.src_engine.asked == 2, eng.src_engine.asked


def test_an_entry_the_tail_is_past_retires(tmp_path):
    _copied(tmp_path)
    copied = _CopiedRanges(_Eng(), "db", tmp_path)
    copied.RETIRE_AFTER = 5
    # covered by the copy's marks: left out, the entry stays
    assert copied.leave_out([_change(1, 3)])[1] == {"public.t": 1}
    # past every mark: applied - and after five in a row, retired
    for i in range(5):
        out, left = copied.leave_out([_change(1, 200 + i)])
        assert len(out) == 1 and not left
    assert len(copied.retired) == 1
    copied.eng.src_engine.asked = 0
    out, _ = copied.leave_out([_change(1, 300), _change(2, 3)])
    # retired: nothing asked, everything applied - a change the copy read
    # applied again is superseded by the changes after it
    assert copied.eng.src_engine.asked == 0 and len(out) == 2
    # a copy not finished never retires: new ranges and marks may come
    _copied(tmp_path, done=False)
    still = _CopiedRanges(_Eng(), "db", tmp_path)
    still.RETIRE_AFTER = 5
    for i in range(10):
        still.leave_out([_change(1, 200 + i)])
    assert not still.retired


def test_where_counters_add_an_entry_never_retires(tmp_path):
    _copied(tmp_path)
    eng = _Eng({"two_way": {"delta": ["n"]}})
    copied = _CopiedRanges(eng, "db", tmp_path)
    assert copied.keep_all
    copied.RETIRE_AFTER = 2
    for i in range(10):
        copied.leave_out([_change(1, 200 + i)])
    assert not copied.retired
    assert copied.leave_out([_change(1, 3)])[1] == {"public.t": 1}


def test_a_table_beside_the_tail_is_flagged_not_rewritten(tmp_path,
                                                          monkeypatch):
    st = kept.Store(tmp_path / kept.FILE)
    assert st.conn.execute("pragma user_version").fetchone()[0] == \
        kept.Store.VERSION
    st.begin()
    st.write("t", {b: kept._new() for b in range(64)})
    st.commit()
    writes = []
    real = kept.Store.write
    monkeypatch.setattr(kept.Store, "write",
                        lambda self, name, got: writes.append(
                            (name, len(got))) or real(self, name, got))
    keeper = kept.Keeper.__new__(kept.Keeper)
    keeper.store, keeper.unplaced = st, {"t"}
    keeper._tables = {}
    for _ in range(3):
        keeper._commit({}, set(), 0, "0/2", "0/1")
    # no leaf written: the table flagged once
    assert writes == [] and st.stale() == {"t": True}

    class T:
        name = "t"
    why = kept._due(st, T(), 0, 10 ** 9)
    assert why["marked"] == set(range(64))


def test_the_shape_is_compared_and_written_only_where_it_moved(
        tmp_path, monkeypatch):
    from migkit import drift
    shapes = iter([{"public.t": [["id", "int"]]}] * 3
                  + [{"public.t": [["id", "int"], ["w", "int"]]}])
    monkeypatch.setattr(drift, "reader", lambda eng: object())
    monkeypatch.setattr(drift, "shape", lambda eng, side, db: next(shapes))
    compared = []
    real = drift.changes
    monkeypatch.setattr(drift, "changes",
                        lambda a, b: compared.append(1) or real(a, b))
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="sh", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    pair.src_engine = object()
    monkeypatch.setattr(HeteroEngine, "_lacking",
                        lambda self, db, a, b: {})
    path = tmp_path / "tail-token.json"
    for _ in range(3):
        got, behind = pair._shape_now("d", path, "0/1")
        assert got == {"public.t": [["id", "int"]]} and behind == {}
    store = tailtables.Store.of(tmp_path)
    assert store.get("shape") == {"public.t": [["id", "int"]]}
    # read three times, compared and written once: the first
    assert compared == []
    pair._shape_now("d", path, "0/2", stop=False)
    assert compared == [1]
    assert store.get("shape")["public.t"] == [["id", "int"], ["w", "int"]]


def _shaping(tmp_path, monkeypatch, src):
    from migkit import drift
    now = {"shape": {"public.t": [["id", "int"]]}}
    reads = []
    monkeypatch.setattr(drift, "reader", lambda eng: src)
    monkeypatch.setattr(drift, "shape", lambda eng, side, db: reads.append(
        1) or json.loads(json.dumps(now["shape"])))
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="sm", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    pair.src_engine = src
    monkeypatch.setattr(HeteroEngine, "_lacking",
                        lambda self, db, a, b: {})
    return pair, now, reads, tmp_path / "tail-token.json"


def test_a_source_that_digests_its_catalogue_is_read_whole_once_it_moved(
        tmp_path, monkeypatch):
    class Src:
        mark = "a"

        def shape_mark(self, side, db):
            return self.mark
    src = Src()
    pair, now, reads, path = _shaping(tmp_path, monkeypatch, src)
    for _ in range(4):
        got, _ = pair._shape_now("d", path, "0/1")
        assert got == {"public.t": [["id", "int"]]}
    # one whole read: every batch after it asked the digest alone
    assert reads == [1]
    src.mark = "b"
    now["shape"] = {"public.t": [["id", "int"], ["w", "int"]]}
    got, _ = pair._shape_now("d", path, "0/2", stop=False)
    assert len(reads) == 2 and got["public.t"][1] == ["w", "int"]
    assert tailtables.Store.of(tmp_path).get("shape") == now["shape"]
    # a digest that cannot be asked: the whole shape, every batch
    src.shape_mark = lambda side, db: (_ for _ in ()).throw(
        RuntimeError("gone"))
    pair._shape_now("d", path, "0/3", stop=False)
    pair._shape_now("d", path, "0/3", stop=False)
    assert len(reads) == 4


@needs_docker
def test_postgresql_digests_its_catalogue_and_a_change_moves_it(pg_pair):
    from migkit.engines.postgres import PostgresEngine
    port = pg_pair["src"]
    eng = PostgresEngine(Hop(
        name="sm", engine="postgres",
        source=Endpoint(host="127.0.0.1", port=port, user="postgres",
                        password="test"),
        target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                        user="postgres", password="test"),
        databases=["postgres"]))
    got = psql(port, "drop table if exists public.sm; create table"
                     " public.sm (id int primary key, v text)")
    assert got.returncode == 0, got.stderr
    was = eng.shape_mark("src", "postgres")
    assert len(was) == 32 and eng.shape_mark("src", "postgres") == was
    # what drift compares beyond a column's name and type moves it too
    for ddl in ("alter table public.sm alter column v type varchar(9)",
                "alter table public.sm alter column v set default 'x'",
                "create index sm_v on public.sm (v)",
                "alter table public.sm add constraint sm_c check (id > 0)"):
        got = psql(port, ddl)
        assert got.returncode == 0, got.stderr
        now = eng.shape_mark("src", "postgres")
        assert now != was, ddl
        was = now


def test_the_row_filters_translation_is_made_once_a_table(monkeypatch):
    pair = HeteroEngine.__new__(HeteroEngine)
    pair.hop = Hop(name="rf", engine="postgres", source=Endpoint(),
                   target=Endpoint())
    monkeypatch.setattr(type(pair.hop), "row_filter",
                        lambda self, db, *parts: "id > 5", raising=False)
    made = []
    monkeypatch.setattr(HeteroEngine, "_row_scope_of",
                        lambda self, db, t, pred: made.append(t)
                        or (pred, f"{pred} /* {t} */"))
    for _ in range(4):
        assert pair._row_scope("d", "public.a") == \
            ("id > 5", "id > 5 /* public.a */")
    pair._row_scope("d", "public.b")
    assert made == ["public.a", "public.b"]
    # a shape that moved: made again
    pair.forget_shapes()
    pair._row_scope("d", "public.a")
    assert made == ["public.a", "public.b", "public.a"]
    # one that cannot be translated is said every time, never kept
    monkeypatch.setattr(HeteroEngine, "_row_scope_of",
                        lambda self, db, t, pred: (_ for _ in ()).throw(
                            SystemExit("no translation")))
    for _ in range(2):
        with pytest.raises(SystemExit):
            pair._row_scope("d", "public.c")
