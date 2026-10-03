"""A running tail's tables, each in a state of its own (backlog L1, L4,
L6, L7; `tailtables`), held without a server: the hop file read again as
it changes, what a DDL does under each `ddl`, a parked table's changes
set aside and given back once its target is level, the way a table comes
back chosen by the ladder, the spool's frames, and the accounts' identity
with what was set aside and given back.
"""
import json
from collections import Counter

import pytest

from migkit import canon, decide, tailtables
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine, _Accounts


class _Src:
    CANON_ENGINE = "postgres"
    REREADS_FROM_A_POSITION = False

    def __init__(self):
        self.lost = None

    def snapshot_mark(self, side, db):
        return None

    def mark_covers(self, side, db, mark, change):
        return None

    def position_lost(self, side, db, at):
        return self.lost

    def neutral_columns(self, side, db, table):
        return [("id", "integer"), ("v", "text")]

    def log_position(self, side, db):
        return "0/10"


class _Pair:
    _leaf = staticmethod(HeteroEngine._leaf)

    def __init__(self, hop):
        self.hop, self.src_engine = hop, _Src()

    @staticmethod
    def move_key(db, sch, tbl):
        return f"{db}.{tbl or sch}"


def _tables(tmp_path, ddl=None, said=None):
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="tt", engine="postgres", source=ep, target=ep,
              databases=["d"], options={"ddl": ddl} if ddl else {})
    hop.report_dir = lambda db=None: tmp_path
    got = tailtables.TableSet(_Pair(hop), "d", tmp_path / "tail-token.json",
                              said.append if said is not None
                              else (lambda m: None))
    got.file.said = None
    got.fresh = False
    got.tables = {"a": {"state": "live", "name": "a"},
                  "b": {"state": "live", "name": "b"}}
    return got


def _row(table, i):
    return canon.change("insert", table, {"id": i}, {"id": i, "v": "x"})


def test_the_hop_file_is_read_again_when_it_changes(tmp_path, monkeypatch):
    import migkit.config
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  tt:\n    exclude: [b]\n")
    monkeypatch.setattr(migkit.config, "CONF", conf)
    ep = Endpoint(host="x", port=1, user="x", password="x")
    hop = Hop(name="tt", engine="postgres", source=ep, target=ep,
              exclude=["b", "from_the_environment"])
    got = tailtables._HopFile(hop)
    assert got.refresh() is False
    conf.write_text("hops:\n  tt:\n    exclude: [c]\n")
    import os
    os.utime(conf, ns=(1, 10 ** 18))
    assert got.refresh() is True
    # the file's word, and what the environment added kept
    assert hop.exclude == ["c", "from_the_environment"]
    # a hop the file does not name is left alone
    other = Hop(name="not_there", engine="postgres", source=ep, target=ep,
                exclude=["x"])
    none = tailtables._HopFile(other)
    os.utime(conf, ns=(2, 2 * 10 ** 18))
    assert none.refresh() is False and other.exclude == ["x"]


def test_under_stop_a_truncate_stops_the_tail_before_the_batch(tmp_path):
    t = _tables(tmp_path)
    states = t.begin()
    with pytest.raises(SystemExit) as e:
        t.ddl(states, canon.ddl("a", "truncate"), "0/1")
    assert "emptied" in str(e.value) and "ddl: apply" in str(e.value)
    # a drop or an alter goes on as it always did
    assert t.ddl(states, canon.ddl("a", "drop"), "0/1") is None
    assert states["a"]["state"] == "live"


def test_under_apply_a_truncate_is_carried_at_its_place(tmp_path):
    t = _tables(tmp_path, "apply")
    states = t.begin()
    assert t.ddl(states, canon.ddl("a", "truncate"), "0/1") \
        == ("truncate", "a")
    # two ways, a truncate is a copy again, never carried as a DDL
    assert t.ddl(states, canon.ddl("b", "truncate"), "0/1",
                 two_way=True) is None
    assert states["b"]["state"] == "copying" and states["b"]["way"] == "copy"


def test_under_park_a_ddl_parks_only_its_table(tmp_path):
    said = []
    t = _tables(tmp_path, "park", said)
    states = t.begin()
    t.park(states, "a", "0/1", "the target does not have w")
    out, left = t.route(states, [_row("a", 1), _row("b", 2), _row("a", 3)],
                        "0/1")
    # b goes on; a is set aside and counted - written with the batch, in
    # the transaction set down before the target commits it
    assert out == [_row("b", 2)]
    assert left[tailtables.SPOOLED] == Counter({"a": 2})
    assert t.spool("a").changes() == []
    t.prepare(states, 1)
    assert [c["key"]["id"] for c in t.spool("a").changes()] == [1, 3]
    assert tailtables.states(tmp_path)["a"]["state"] == "parked"
    t.committed()
    assert any("a: parked" in s for s in said), said
    # not level yet: nothing given back
    t.check_level(lambda name: False)
    states = t.begin()
    back, counts, _ = t.release(states)
    assert back == [] and states["a"]["state"] == "parked"
    # level: what was set aside first, in its order, then the batch's own
    t.level_at = 0
    t.check_level(lambda name: name == "a")
    states = t.begin()
    back, counts, _ = t.release(states)
    assert [c["key"]["id"] for c in back] == [1, 3]
    assert counts == Counter({"a": 2})
    out, _ = t.route(states, [_row("a", 5)], "0/2")
    assert out == [_row("a", 5)]
    t.prepare(states, 2)
    t.committed()
    assert t.tables["a"]["state"] == "live"
    assert t.spool("a").changes() == []


def test_a_drop_and_a_rename_move_tables_out_and_in(tmp_path):
    t = _tables(tmp_path, "park")
    states = t.begin()
    t.ddl(states, canon.ddl("a", "rename", to="c"), "0/1")
    t.ddl(states, canon.ddl("b", "drop"), "0/1")
    assert states["a"]["state"] == "removed"
    assert states["b"]["state"] == "removed"
    assert states["c"]["state"] == "copying"
    out, left = t.route(states, [_row("b", 1)], "0/1")
    assert out == [] and left[tailtables.OUT_OF_SCOPE] == Counter({"b": 1})


def test_a_table_that_left_comes_back_the_cheapest_exact_way(tmp_path):
    t = _tables(tmp_path, "park")
    t.src.REREADS_FROM_A_POSITION = True
    left = {"state": "removed", "name": "a", "at": "0/5",
            "digest": [3, "9"], "shape": [["id", "integer"], ["v", "text"]],
            "columns": [["id", "integer"]]}
    states = {"a": dict(left)}
    t._add(states, "a", "a", "0/9", "put back")
    assert states["a"]["way"] == "from-removal"
    assert states["a"]["at"] == "0/5" and states["a"]["upto"] == "0/10"
    # its shape changed since: copied
    states = {"a": dict(left, shape=[["id", "integer"]])}
    t._add(states, "a", "a", "0/9", "put back")
    assert states["a"]["way"] == "copy"
    # the log no longer holds where it left: copied, and said why
    t.src.lost = "the binlog was purged"
    got = decide.climb("a", tailtables._add_ways(), decide.Facts(
        {"at": "0/5", "digest": True, "rereads": True, "same_shape": True,
         "lost": "the binlog was purged"}))
    assert got.path == "copy" and "purged" in got.reason
    # a source read once (a slot): copied
    t.src.lost = None
    t.src.REREADS_FROM_A_POSITION = False
    states = {"a": dict(left)}
    t._add(states, "a", "a", "0/9", "put back")
    assert states["a"]["way"] == "copy"


def test_a_copy_that_stops_is_tried_again_after_a_wait(tmp_path):
    t = _tables(tmp_path, "park")
    t._park(t.tables, "a", "0/1", "its copy stopped: refused", "copy")
    t.check_level(lambda name: True)
    # not at once, nor every five seconds
    assert t.tables["a"]["state"] == "parked"
    t.tables["a"]["parked_at"] = 0
    t.level_at = 0
    started = []
    t._start = lambda leaf, st, fresh: started.append(leaf)
    t.check_level(lambda name: True)
    assert t.tables["a"]["state"] == "copying" and started == ["a"]


def _legacy_frame(at, changes):
    import pickle
    import struct

    import zstandard
    blob = zstandard.ZstdCompressor(level=3).compress(
        pickle.dumps((at, list(changes))))
    return struct.pack(">Q", len(blob)) + blob


def test_an_earlier_versions_spool_is_taken_in_its_torn_frame_left_out(
        tmp_path):
    spools = tmp_path / tailtables.SPOOLS
    spools.mkdir()
    raw = (_legacy_frame('"0/1"', [_row("a", 1)])
           + _legacy_frame('"0/2"', [_row("a", 2)]))
    # a stop cut the last frame
    (spools / "a.spool").write_bytes(raw[:-3])
    (tmp_path / tailtables.STATE).write_text(json.dumps(
        {"tables": {"a": {"state": "parked", "name": "a"}}}))
    st = tailtables.Store.of(tmp_path)
    assert st.import_legacy(tmp_path)
    assert [c["key"]["id"] for c in tailtables.Spool(st, "a").changes()] \
        == [1]
    assert st.tables()["a"]["state"] == "parked"
    # taken in once: the files are gone
    assert not (spools / "a.spool").exists()
    assert not (tmp_path / tailtables.STATE).exists()


def test_a_batch_set_down_and_not_saved_is_taken_back_out(tmp_path):
    t = _tables(tmp_path, "park")
    t.save()
    states = t.begin()
    t.park(states, "a", "0/1", "the target does not have w")
    t.route(states, [_row("a", 1)], "0/1")
    t.prepare(states, 1, {"a": {"read": 1}})
    assert t.store.gen() == 1 and t.spool("a").changes()
    # the batch's connection lost before its position was saved
    t.forget()
    assert t.store.gen() == 0 and t.spool("a").changes() == []
    assert tailtables.states(tmp_path)["a"]["state"] == "live"
    assert tailtables.accounts(tmp_path) == {}


def test_the_accounts_hold_what_was_set_aside_and_given_back(tmp_path):
    acc = _Accounts(tailtables.Store.of(tmp_path))
    acc.read([_row("a", 1), _row("a", 2), _row("b", 3)])
    acc.left(tailtables.SPOOLED, Counter({"a": 1}))
    acc.released(Counter({"b": 2}))
    applied = [_row("a", 1), _row("b", 3), _row("b", 7), _row("b", 8)]
    acc.check("d", applied, "0/1")
    assert acc.rows["b"] == {"read": 1, "applied": 3, "taken back": 2}
    # a change set aside and given back that goes nowhere is caught
    acc.read([_row("a", 1)])
    acc.released(Counter({"a": 1}))
    with pytest.raises(SystemExit) as e:
        acc.check("d", [_row("a", 1)], "0/1")
    assert "took back 1 set aside" in str(e.value)


def test_a_constant_default_is_told_from_one_worked_out_when_taken():
    for text in (None, "7", "-1.5", "'new'", "'it''s'", "NULL", "true",
                 "'x'::character varying", "(0)"):
        assert tailtables.constant_default(text), text
    for text in ("now()", "CURRENT_TIMESTAMP", "nextval('s'::regclass)",
                 "gen_random_uuid()", "uuid()"):
        assert not tailtables.constant_default(text), text
