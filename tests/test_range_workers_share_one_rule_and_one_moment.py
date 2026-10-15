"""A table copied by many ranges side by side writes under one rule for
its target's triggers and keys - taken once, given back once after the
last range, read back as it was - and its readers read one moment where
the source exports one. Fake engines: no server is reached."""
import contextlib
import inspect

import pytest

from migkit import decide
from migkit.engines import base, hetero, postgres
from migkit.engines.base import Engine, TableHold


class FakeTarget(Engine):
    SESSION_HOLDS = ("_as_replica",)
    WRITE_STATES_SQL = "states {schema} {table}"

    def __init__(self, states):
        self.states = states
        self.opened = self.closed = 0
        self.asked = []

    def write_states(self, side, db, table):
        assert side == "dst"
        self.asked.append(table)
        return dict(self.states)

    def load_window(self, db, log=None, tables=None):
        @contextlib.contextmanager
        def window():
            self.opened += 1
            self.__dict__.setdefault("_as_replica", set()).add(db)
            try:
                yield
            finally:
                self.__dict__["_as_replica"].discard(db)
                self.closed += 1
        return window()


def test_a_table_of_many_ranges_is_held_once_and_given_back_once():
    eng = FakeTarget({"trigger audit": True, "key fk_parent": True})
    holds = [eng.table_hold("app", "public.orders") for _ in range(4)]
    sessions = [h.__enter__() for h in holds]
    assert eng.opened == 1 and eng.closed == 0
    assert all(s == {"_as_replica": ["app"]} for s in sessions)
    for h in holds[:-1]:
        h.__exit__(None, None, None)
    assert eng.closed == 0
    holds[-1].__exit__(None, None, None)
    assert eng.closed == 1
    assert holds[-1].restored is True
    assert eng.asked == ["public.orders", "public.orders"]


def test_a_trigger_left_off_after_the_last_range_stops_the_copy():
    eng = FakeTarget({"trigger audit": True})
    with pytest.raises(decide.Blocked, match="trigger audit"):
        with eng.table_hold("app", "orders"):
            eng.states["trigger audit"] = False
    assert not TableHold._held


def test_a_worker_process_adopts_the_target_sessions_hold_not_the_source():
    held = {"_as_replica": ["app"]}
    worker = FakeTarget({})
    worker.adopt_session_hold(held)
    assert worker.__dict__["_as_replica"] == {"app"}
    worker.adopt_session_hold({"_unknown": ["x"]})
    assert "_unknown" not in worker.__dict__
    # the source's export connection runs with nothing set for writing
    from migkit.engines import pg_pass
    assert "session_replication_role" not in pg_pass.LOAD_OPTIONS
    assert postgres.PostgresEngine.SESSION_HOLDS == ("_as_replica",)


class Exporting(FakeTarget):
    def __init__(self, fail=False):
        super().__init__({})
        self.fail, self.ended = fail, 0

    def export_moment(self, db):
        if self.fail:
            raise RuntimeError("snapshot export refused")
        return "00000003-1", lambda: setattr(self, "ended", self.ended + 1)


def test_ranges_of_one_table_read_the_one_exported_moment():
    src = Exporting()
    with src.read_moment("app") as token:
        assert token == "00000003-1"
        reader = Exporting()
        reader.adopt_moment("app", token)
        assert reader.__dict__["_moment"] == {"app": "00000003-1"}
    assert src.ended == 1
    assert src.shared_snapshot("app")


def test_a_source_with_no_shared_moment_says_so_and_reads_each_range_alone():
    src = FakeTarget({})
    with src.read_moment("app") as token:
        assert token is None
    assert src.shared_snapshot("app") == ""
    said = []
    with Exporting(fail=True).read_moment("app", said.append) as token:
        assert token is None
    assert said and "its own mark" in said[0]


def test_the_range_copier_holds_reads_and_names_through_the_one_owner():
    moving = inspect.getsource(hetero.HeteroEngine._move_in_ranges)
    assert "table_hold(" in moving and "read_moment(" in moving
    worker = inspect.getsource(hetero._copy_range)
    assert "adopt_session_hold" in worker and "adopt_moment" in worker
    ranged = inspect.getsource(hetero.HeteroEngine.copy_range)
    assert "copy_sizes(" in ranged and "_write_sized(" in ranged
    assert "named_write(" in ranged
    assert "named_write(" in inspect.getsource(Engine.neutral_apply)
    assert "tuning.sized(" in inspect.getsource(
        hetero.HeteroEngine._write_sized)


def test_a_wide_table_is_carried_as_payload_way_decides_at_a_measured_size(
        monkeypatch):
    from migkit import pieces, sizing, tuning

    class Way:
        rung = decide.Rung("pieces", "a piece at a time")
    asked = []
    monkeypatch.setattr(tuning, "engine_payload",
                        lambda eng, side, db, t, c, **kw: asked.append(c)
                        or Way())
    monkeypatch.setattr(pieces, "_limits_of", lambda eng, side, db: ())
    monkeypatch.setattr(sizing, "host", lambda: {"memory": 64 * 2 ** 20})
    eng = FakeTarget({})
    eng.hop = None
    wide = pieces.PIECE
    retune, ways = eng.copy_sizes("app", "docs", ["id", "body"], wide, 4)
    assert asked == ["id", "body"] and ways == {"id": "pieces",
                                                "body": "pieces"}
    assert retune.settings()["way"] == "pieces"
    assert retune.settings()["bytes"] <= max(16 * 2 ** 20, wide)
    asked.clear()
    retune, ways = eng.copy_sizes("app", "small", ["id"], 100, 4)
    assert asked == [] and ways == {}
    assert retune.settings()["rows"] <= retune.settings()["bytes"] // 100


def test_a_write_is_named_through_the_one_door(monkeypatch):
    from migkit import freeze
    got = []
    monkeypatch.setattr(freeze, "name_write",
                        lambda hop, db, t, kind, unit, rows=1:
                        got.append((db, t, kind, unit, rows)) or True)
    eng = FakeTarget({})
    eng.hop = object()
    assert eng.named_write("app", "orders", "copy", "orders 0..10", 10)
    assert not eng.named_write("app", "orders", "copy", "u", 0)
    assert got == [("app", "orders", "copy", "orders 0..10", 10)]
    assert base.TableHold is TableHold
