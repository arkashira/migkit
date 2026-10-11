"""A load into a MongoDB target holds its TTL indexes from deleting what
the load writes (backlog P0-PARITY.all, `move.load-window`), held
without a server.

A TTL index has the server delete the documents whose date is past its
expiry. A load writes the source's documents with the dates they have, and
the target's expiry, running beside it, deleted them as they landed. So
the window sets each TTL index of a collection the load writes to the
longest expiry the server takes, its own value saved to disk first and put
back on the way out - on an error as on success; what a load that died
held goes back at the end of the next one, and `check` names it until then.

The calls the engine makes are answered by `tests/mongo_fake.py`.
"""
import json
import os
import socket

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines.mongodb import MongoEngine, _TtlWindow
from tests.mongo_fake import Server

#: nothing is declared proved here: the window sets an index's expiry with
#: `collMod`, a command no server has answered for it yet. It waits in
#: `capabilities.UNPROVED` for a real replica set

LONGEST = 2_147_483_647


def _eng(tmp_path, dst, **hop):
    h = Hop(name="m", engine="mongodb", source=Endpoint(host="s", port=1),
            target=Endpoint(host="t", port=2), databases=["app"], **hop)
    h.report_dir = lambda db=None: tmp_path
    eng = MongoEngine(h)
    eng._client = lambda side: dst.client()
    return eng


@pytest.fixture
def dst():
    s = Server()
    d = s.db("app")
    d.create_collection("sessions").create_index(
        [("at", 1)], name="ttl", expireAfterSeconds=3600)
    d.create_collection("events").create_index(
        [("at", 1)], name="ttl2", expireAfterSeconds=60)
    d.create_collection("orders").create_index([("sku", 1)], name="sku")
    d.create_collection("recent", viewOn="orders", pipeline=[])
    return s


def _expiry(dst, coll, index):
    return dst.db("app").colls[coll].indexes[index]["expireAfterSeconds"]


def test_a_ttl_index_is_held_for_the_load_and_put_back(tmp_path, dst):
    eng = _eng(tmp_path, dst)
    said = []
    with eng.load_window("app", said.append, {"sessions", "orders"}):
        # the collections the load writes are held, and no other
        assert _expiry(dst, "sessions", "ttl") == LONGEST
        assert _expiry(dst, "events", "ttl2") == 60
        # the value is on disk before the first index was touched
        saved, = tmp_path.glob("dropped-indexes.*.json")
        assert json.loads(json.loads(saved.read_text())["sessions.ttl"]) == {
            "collection": "sessions", "index": "ttl", "seconds": 3600}
    assert _expiry(dst, "sessions", "ttl") == 3600
    assert not list(tmp_path.glob("dropped-indexes.*.json"))
    assert any("put back" in m for m in said)


def test_a_load_that_fails_still_puts_them_back(tmp_path, dst):
    eng = _eng(tmp_path, dst)
    with pytest.raises(RuntimeError):
        with eng.load_window("app", None, None):
            assert _expiry(dst, "events", "ttl2") == LONGEST
            raise RuntimeError("the load died")
    assert _expiry(dst, "sessions", "ttl") == 3600
    assert _expiry(dst, "events", "ttl2") == 60


def test_the_target_names_a_collection_the_hop_renames_to(tmp_path, dst):
    eng = _eng(tmp_path, dst, mapping={"tables": {"old_sessions":
                                                  "sessions"}})
    with eng.load_window("app", None, {"old_sessions"}):
        assert _expiry(dst, "sessions", "ttl") == LONGEST
        assert _expiry(dst, "events", "ttl2") == 60


def test_a_collection_the_hop_leaves_out_is_not_touched(tmp_path, dst):
    eng = _eng(tmp_path, dst, exclude=["app.events"])
    with eng.load_window("app", None, None):
        assert _expiry(dst, "sessions", "ttl") == LONGEST
        assert _expiry(dst, "events", "ttl2") == 60


def test_nothing_to_hold_touches_nothing(tmp_path):
    s = Server()
    s.db("app").create_collection("orders").create_index([("a", 1)])
    with _eng(tmp_path, s).load_window("app", None, None):
        pass
    assert s.collmods == [] and not list(tmp_path.glob("*.json"))


def test_a_value_that_could_not_be_saved_stops_the_load_before_any_hold(
        tmp_path, dst, monkeypatch):
    from migkit.setaside import SetAside
    monkeypatch.setattr(SetAside, "save", lambda self, defs, stale=(): False)
    with pytest.raises(SystemExit) as e:
        _eng(tmp_path, dst).load_window("app", None, None).__enter__()
    assert "so nothing was loaded" in str(e.value)
    assert dst.collmods == []


def test_an_index_that_will_not_go_back_is_kept_on_record_and_said(
        tmp_path, dst):
    eng = _eng(tmp_path, dst)
    said = []
    window = eng.load_window("app", said.append, {"sessions"})
    window.__enter__()
    dst.refuse_collmod = "not authorized"
    with pytest.raises(SystemExit) as e:
        window.__exit__(None)
    assert "sessions.ttl" in str(e.value) and "not put back" in str(e.value)
    assert any(m.startswith("TTL INDEX NOT PUT BACK: sessions.ttl")
               for m in said)
    assert list(tmp_path.glob("dropped-indexes.*.json"))


def _a_dead_loads_record(tmp_path, entries):
    pid = 2 ** 22 + 12345
    assert not _alive(pid)
    path = tmp_path / f"dropped-indexes.{socket.gethostname()}.{pid}.1.json"
    path.write_text(json.dumps({
        k: json.dumps({"collection": k.split(".")[0],
                       "index": k.split(".")[1], "seconds": s},
                      sort_keys=True) for k, s in entries.items()}))
    return path


def _alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def test_what_a_load_that_died_held_goes_back_at_the_end_of_the_next(
        tmp_path, dst):
    dead = _a_dead_loads_record(tmp_path, {"events.ttl2": 60})
    # as the dead load left it: held
    dst.db("app").colls["events"].indexes["ttl2"][
        "expireAfterSeconds"] = LONGEST
    eng = _eng(tmp_path, dst)
    check = eng.set_aside("app")
    assert check.status == "diff" and "events.ttl2" in check.detail
    said = []
    with eng.load_window("app", said.append, {"sessions"}):
        assert any("an earlier load held" in m for m in said)
        assert _expiry(dst, "sessions", "ttl") == LONGEST
    assert _expiry(dst, "events", "ttl2") == 60
    assert _expiry(dst, "sessions", "ttl") == 3600
    assert not dead.exists()
    assert eng.set_aside("app") is None


def test_check_names_what_is_held_while_a_load_runs_and_what_was_lost(
        tmp_path, dst):
    eng = _eng(tmp_path, dst)
    assert eng.set_aside("app") is None
    with eng.load_window("app", None, {"sessions"}):
        # this process is running: held on purpose
        got = eng.set_aside("app")
        assert got.status == "ok" and "sessions.ttl" in got.detail
    _a_dead_loads_record(tmp_path, {"sessions.ttl": 3600})
    got = eng.set_aside("app")
    assert got.status == "diff" and "never put back" in got.detail


def test_the_window_is_the_one_the_copy_enters():
    """`cli._move_full` wraps the copy in `Engine.load_window`."""
    assert MongoEngine.load_window is not None
    assert _TtlWindow.MOST == LONGEST
