"""The tail's state survives a crash and a power loss (audit 2026-10-01,
ST1, ST9, ST18-ST21, ST25, SEC21).

The tail's position was one file rewritten in place every batch and its
own state four more, none synced but the spools, with no barrier between
them. Now the position is one envelope for every engine, written whole
and on the disk after the target committed (`tailctl`), and the rest -
the tables' states, what they set aside, the accounts, the shape - is one
SQLite store, a batch's part written in one transaction before the target
commits it and taken back out where the position never passed it
(`tailtables.Store`). A power loss is played here by putting the files
back as they were on the disk at a moment of the batch, and a crash by
the process stopping there.
"""
import json
import pickle
import shutil
from collections import Counter

import pytest

from migkit import canon, durable, tailctl, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine


# --- the position, one envelope ------------------------------------------------

def test_every_earlier_shape_reads_as_the_one_envelope(tmp_path):
    path = tmp_path / tailctl.TOKEN
    path.write_text(json.dumps({"token": "0/1A", "batch": 7,
                                "rung": "table"}))
    (tmp_path / tailctl.SOURCE).write_text(json.dumps({"system": "s1"}))
    got = tailctl.position(path)
    assert got["token"] == "0/1A" and got["batch"] == 7
    assert got["rung"] == "table" and got["v"] == tailctl.V
    # the source kept beside an earlier position is the envelope's now
    assert got["source"] == {"system": "s1"}
    (tmp_path / tailctl.SOURCE).unlink()
    # MongoDB's resume token, saved bare
    path.write_text(json.dumps({"_data": "8263ABC"}))
    assert tailctl.position(path)["token"] == {"_data": "8263ABC"}
    assert tailctl.saved(path) == {"_data": "8263ABC"}
    # Kafka's positions
    path.write_text(json.dumps({"token": {"t": {"parts": {"0": 5}}}}))
    assert tailctl.saved(path) == {"t": {"parts": {"0": 5}}}


def test_a_torn_or_newer_position_stops_naming_the_file(tmp_path):
    path = tmp_path / tailctl.TOKEN
    for torn in ('{"token": "0/1', "", '{"neither": 1}'):
        path.write_text(torn)
        with pytest.raises(SystemExit) as e:
            tailctl.position(path)
        assert str(path) in str(e.value), (torn, e.value)
    path.write_text(json.dumps({"v": tailctl.V + 1, "token": "x"}))
    with pytest.raises(SystemExit) as e:
        tailctl.position(path)
    assert "newer migkit" in str(e.value)


def test_a_position_moves_by_one_write_its_generation_with_it(tmp_path):
    path = tmp_path / tailctl.TOKEN

    class Eng:
        CANON_ENGINE = "postgres"

        def stream_identity(self, side, db):
            return {"system": "s2"}
    (tmp_path / tailctl.SOURCE).write_text(json.dumps({"system": "old"}))
    env = tailctl.begin(Eng(), "d", path, "0/1")
    # one write: the source in the envelope, the old file of it gone
    assert not (tmp_path / tailctl.SOURCE).exists()
    on_disk = json.loads(path.read_text())
    assert on_disk["source"] == {"system": "s2"} and on_disk["gen"] == 0
    assert on_disk["id"] == env["id"] and len(env["id"]) == 16
    env = tailctl.keep(path, {**env, "next": {"token": "0/2", "batch": 1}})
    moved = tailctl.moved(path, env, "0/2", batch=1)
    assert moved["gen"] == 1 and "next" not in moved
    assert json.loads(path.read_text()) == moved
    # whole and private
    assert oct(path.stat().st_mode & 0o777) == oct(0o600)


# --- the tail's own state ----------------------------------------------------

def _row(table, i):
    return canon.change("insert", table, {"id": i}, {"id": i, "v": "x"})


class _Src:
    CANON_ENGINE = "postgres"

    def snapshot_mark(self, side, db):
        return None

    def mark_covers(self, side, db, mark, change):
        return None


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop):
        self.hop, self.src_engine = hop, _Src()

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _hop(where, ddl="park"):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="ts", engine="postgres", source=ep, target=ep,
              databases=["d"], options={"ddl": ddl})
    hop.report_dir = lambda db=None: where
    return hop


def _tables(where, ddl="park"):
    got = tailtables.TableSet(_Pair(_hop(where, ddl)), "d",
                              where / tailctl.TOKEN, lambda m: None)
    got.file.said = None
    got.fresh = False
    got.tables = {"a": {"state": "live", "name": "a"},
                  "b": {"state": "live", "name": "b"}}
    got.save()
    return got


def _snapshot(where, into):
    """The files of `where` as they are on the disk now - the store's
    database and its write-ahead log as SQLite left them, every commit
    synced - what a machine that lost its power at this moment comes
    back to."""
    shutil.copytree(where, into)
    for extra in ("-shm",):
        (into / (tailtables.STORE + extra)).unlink(missing_ok=True)
    return into


def test_a_power_loss_anywhere_in_a_batch_leaves_state_and_position_in_step(
        tmp_path):
    """A batch parks `a`, sets two of its changes aside and counts them:
    its part of the state set down, then the target commits, then the
    position. Each moment of it played back as a power loss: before the
    position is written the batch is taken back out - read again, it is
    set aside again, once - and after, it stands."""
    where = tmp_path / "w"
    where.mkdir()
    env = tailctl.keep(where / tailctl.TOKEN,
                       {"id": "s1", "token": "0/1", "gen": 0})
    st = tailtables.Store.of(where)
    st.reset("s1")
    t = _tables(where)
    states = t.begin()
    t.park(states, "a", "0/1", "the target does not have w")
    out, left = t.route(states, [_row("a", 1), _row("b", 2), _row("a", 3)],
                        "0/1")
    assert out == [_row("b", 2)] and left[tailtables.SPOOLED] == \
        Counter({"a": 2})
    t.prepare(states, 1, {"a": {"read": 2, "set aside while its table is"
                                " parked": 2}, "b": {"read": 1,
                                                     "applied": 1}},
              {"batch": None, "token": "0/2"})
    lost_before = _snapshot(where, tmp_path / "set-down")
    tailctl.moved(where / tailctl.TOKEN, env, "0/2")
    t.committed()
    lost_after = _snapshot(where, tmp_path / "saved")

    # power lost after the batch was set down, before its position
    back = tailtables.TableSet(_Pair(_hop(lost_before)), "d",
                               lost_before / tailctl.TOKEN, print)
    assert back.store.gen() == 0
    assert back.tables["a"]["state"] == "live"
    assert back.spool("a").changes() == []
    assert tailtables.accounts(lost_before) == {}
    # power lost after the position: the batch stands, set aside once
    on = tailtables.TableSet(_Pair(_hop(lost_after)), "d",
                             lost_after / tailctl.TOKEN, print)
    assert on.store.gen() == 1 and on.tables["a"]["state"] == "parked"
    assert [c["key"]["id"] for c in on.spool("a").changes()] == [1, 3]
    assert tailtables.accounts(lost_after)["b"] == {"read": 1,
                                                     "applied": 1}


def test_a_state_of_another_stream_starts_afresh(tmp_path):
    st = tailtables.Store.of(tmp_path)
    st.reset("old")
    with st.batch(1) as s:
        s.put("tables", "a", {"state": "parked", "name": "a"})
        s.add_frame("a", "0/1", [_row("a", 1)], [])
    st.settled()
    said = []
    assert st.align({"id": "new", "gen": 0}, said.append) == "afresh"
    assert st.tables() == {} and st.spooled() == {}
    assert any("earlier position" in s for s in said), said


def test_a_state_behind_its_position_stands_only_where_nothing_was_held(
        tmp_path):
    where = tmp_path
    tailctl.keep(where / tailctl.TOKEN, {"id": "s1", "token": "0/9",
                                         "gen": 4})
    st = tailtables.Store.of(where)
    st.reset("s1", gen=1)
    with st.batch() as s:
        s.put("tables", "a", {"state": "live", "name": "a"})
    said = []
    # every table live, nothing set aside, ddl stops: nothing it would
    # have held can be missing
    got = tailtables.TableSet(_Pair(_hop(where, "stop")), "d",
                              where / tailctl.TOKEN, said.append)
    assert got.store.gen() == 4
    assert any("behind its position" in s for s in said), said
    # a table parked: which changes it held cannot be known
    with st.batch() as s:
        s.put("meta", "gen", 1)
        s.put("tables", "a", {"state": "parked", "name": "a"})
    with pytest.raises(SystemExit) as e:
        tailtables.TableSet(_Pair(_hop(where, "park")), "d",
                            where / tailctl.TOKEN, print)
    assert "not saved together" in str(e.value)
    assert str(where / tailtables.STORE) in str(e.value)


def test_a_store_that_cannot_be_read_stops_naming_it(tmp_path):
    (tmp_path / tailtables.STORE).write_bytes(b"not a database at all" * 50)
    tailtables.Store.forget(tmp_path)
    with pytest.raises(SystemExit) as e:
        tailtables.Store.of(tmp_path)
    assert str(tmp_path / tailtables.STORE) in str(e.value)


def test_its_layout_is_numbered_and_a_newer_one_refused(tmp_path):
    import sqlite3
    st = tailtables.Store.of(tmp_path)
    assert st.conn.execute("pragma user_version").fetchone()[0] == \
        tailtables.STORE_VERSION
    tailtables.Store.forget(tmp_path)
    con = sqlite3.connect(tmp_path / tailtables.STORE)
    con.execute(f"pragma user_version = {tailtables.STORE_VERSION + 1}")
    con.close()
    with pytest.raises(SystemExit) as e:
        tailtables.Store.of(tmp_path)
    assert "newer migkit" in str(e.value)


def test_a_frame_half_released_is_written_again_alone(tmp_path):
    """Released rows go from their frame; a frame of another batch is not
    written (audit ST18: every frame read and the spool rewritten whole
    each batch)."""
    st = tailtables.Store.of(tmp_path)
    st.reset("s")
    with st.batch(1) as s:
        one = s.add_frame("a", "0/1", [_row("a", 1), _row("a", 2)], [])
        two = s.add_frame("a", "0/2", [_row("a", 3)], [])
    st.settled()
    body_two = dict((f, b) for f, _, _, b in st.frames("a"))[two]
    with st.batch(2) as s:
        s.set_frame(one, [_row("a", 2)], [])
    got = {f: (n, b) for f, _, n, b in st.frames("a")}
    assert got[one][0] == 1 and got[two][1] == body_two
    with st.batch(3) as s:
        s.set_frame(one, [], [])
    assert [f for f, _, _, _ in st.frames("a")] == [two]
    # taken back out: the frame comes back as it was before the batch
    assert st.undo()
    assert [f for f, _, _, _ in st.frames("a")] == [one, two]


def test_a_held_change_is_not_read_again_while_nothing_it_waits_on_moved(
        tmp_path, monkeypatch):
    t = _tables(tmp_path)
    t.tables["a"] = {"state": "copying", "name": "a", "way": "copy"}
    ck = t.copies()
    ck["d.a"] = {"table": "a", "key": "id", "ranges": [[0, 10]],
                 "ranges_done": [], "ranges_seen": {}}
    states = t.begin()
    t.route(states, [_row("a", 1)], "0/1")
    t.prepare(states, 1)
    t.committed()
    reads = []
    real = tailtables.Spool.frames
    monkeypatch.setattr(tailtables.Spool, "frames",
                        lambda self: reads.append(self.leaf) or real(self))
    for _ in range(3):
        states = t.begin()
        back, _, _ = t.release(states)
        assert back == []
        t.commit(states)
    # read once, then not again until its range moves
    assert reads == ["a"], reads
    ck["d.a"]["ranges_seen"] = {"0": None}
    ck["d.a"]["ranges_done"] = [0]
    ck["d.a"]["done"] = True
    states = t.begin()
    back, _, _ = t.release(states)
    assert [c["key"]["id"] for c in back] == [1]
    assert reads == ["a", "a"], reads


def test_a_spool_that_names_a_value_outside_a_change_log_is_refused(
        tmp_path):
    import datetime
    import decimal
    import uuid
    import zstandard
    values = [canon.change("insert", "a", {"id": 1}, {
        "id": 1, "at": datetime.datetime(2026, 1, 2, 3, 4, 5),
        "n": decimal.Decimal("1.50"), "u": uuid.UUID(int=7), "b": b"\x00",
        "c": canon.Added(2, 9)})]
    assert tailtables._unpack(tailtables._pack(values, [])) == values

    class Boom:
        def __reduce__(self):
            import os
            return (os.system, ("echo pwned",))
    bad = zstandard.ZstdCompressor().compress(pickle.dumps([Boom()]))
    with pytest.raises(pickle.UnpicklingError) as e:
        tailtables._unpack(bad)
    assert "system" in str(e.value)

    # a function of a module whose classes are allowed: refused by name
    class Command:
        def __reduce__(self):
            return (uuid._get_command_stdout, ("echo", "pwned"))
    bad = zstandard.ZstdCompressor().compress(pickle.dumps([Command()]))
    with pytest.raises(pickle.UnpicklingError) as e:
        tailtables._unpack(bad)
    assert "_get_command_stdout" in str(e.value)

    # getattr reaches nothing but the one attribute a zone is made by
    class Reach:
        def __reduce__(self):
            return (getattr, (uuid.UUID, "__init__"))
    bad = zstandard.ZstdCompressor().compress(pickle.dumps([Reach()]))
    with pytest.raises(pickle.UnpicklingError):
        tailtables._unpack(bad)
    from zoneinfo import ZoneInfo
    zoned = [{"at": datetime.datetime(2026, 1, 2,
                                      tzinfo=ZoneInfo("Asia/Bangkok"))}]
    assert tailtables._unpack(tailtables._pack(zoned, [])) == zoned


class _Odd:
    """A value of a kind no change log hands over."""


def test_a_value_that_cannot_be_read_back_is_refused_before_it_is_kept(
        tmp_path):
    """It was set aside, the position moved past it, and the frame was
    refused when it was released - the change lost (SEC21)."""
    st = tailtables.Store.of(tmp_path)
    st.reset("s1", 3)
    odd = canon.change("insert", "a", {"id": 1}, {"id": 1, "v": _Odd()})
    with pytest.raises(SystemExit) as e:
        with st.batch(4) as s:
            s.put("tables", "a", {"state": "parked", "name": "a"})
            s.add_frame("a", '"0/9"', [odd], [])
    said = str(e.value)
    assert "_Odd" in said and "position stays" in said, said
    # nothing of the batch kept, its generation not moved
    assert st.gen() == 3 and st.tables() == {}
    assert tailtables.Spool(st, "a").changes() == []


def test_an_earlier_versions_files_are_taken_in_once(tmp_path):
    (tmp_path / "tail-tables.json").write_text(json.dumps(
        {"tables": {"a b": {"state": "parked", "name": "a b"},
                    "a_b": {"state": "live", "name": "a_b"}}}))
    (tmp_path / "tail-accounts.json").write_text(json.dumps(
        {"a b": {"read": 4}}))
    (tmp_path / "tail-shape.json").write_text(json.dumps(
        {"a b": [["id", "integer"]]}))
    st = tailtables.Store.of(tmp_path)
    assert st.import_legacy(tmp_path)
    assert st.tables()["a b"]["state"] == "parked"
    assert st.accounts() == {"a b": {"read": 4}}
    assert st.get("shape") == {"a b": [["id", "integer"]]}
    assert not (tmp_path / "tail-tables.json").exists()
    # "a b" and "a_b" kept apart: a spool a table's name mapped every
    # other character to "_" and shared one file (audit ST25)
    with st.batch(1) as s:
        s.add_frame("a b", "0/1", [_row("a b", 1)], [])
        s.add_frame("a_b", "0/1", [_row("a_b", 2)], [])
    assert [c["key"]["id"] for c in tailtables.Spool(st, "a b").changes()] \
        == [1]
    # a torn state file stops, naming it
    (tmp_path / "tail-tables.json").write_text('{"tables": {"a"')
    tailtables.Store.forget(tmp_path)
    (tmp_path / tailtables.STORE).unlink()
    with pytest.raises(durable.Torn) as e:
        tailtables.Store.of(tmp_path).import_legacy(tmp_path)
    assert "tail-tables.json" in str(e.value)
