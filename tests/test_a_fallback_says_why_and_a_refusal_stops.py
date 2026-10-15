"""The fallbacks that were ad hoc (backlog DEC30.sweep) go through
`decide.attempt`: the switch is said with its reason, and a refusal - the
sign-in, a read-only target - stops the work rather than fall through to
a way that would meet it too. Stubs only; no server."""
import threading
import types

import pytest

from migkit import decide, movers, runner
from migkit.engines import pg_pass
from migkit.engines.base import Engine


# --- what a refusal is -------------------------------------------------------

def test_a_refusal_is_read_from_the_database_words():
    assert isinstance(movers.refusal(RuntimeError(
        "FATAL: password authentication failed for user x")), runner.Refused)
    assert isinstance(movers.refusal(RuntimeError(
        "ERROR: cannot execute INSERT in a read-only transaction")),
        runner.Refused)
    assert movers.refusal(RuntimeError("connection reset by peer")) is None
    assert movers.switchable(RuntimeError("connection reset by peer"))
    assert not movers.switchable(RuntimeError(
        "password authentication failed"))


# --- the change stream: one reading of its facts ------------------------------

def test_the_stream_facts_are_read_once_in_movers(monkeypatch):
    from migkit import cli
    assert not hasattr(cli, "_stream_facts")
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    follow = next(r for r in movers.stream_ways() if r.name == "follow")
    dial = follow.needs[1]
    assert not dial.holds({})
    # the servers' own replication failed here: the leg run from here holds
    assert dial.holds({"own replication failed": True})


# --- the pass-through's ranges to text ---------------------------------------

class _Trials:
    def __init__(self, rungs):
        self.rungs, self.facts = rungs, decide.Facts()
        self.given = {}

    def pick(self):
        r = next(r for r in self.rungs if r.name not in self.given)
        return decide.climb("t", (r,), decide.Facts())

    def give_up(self, name, why):
        self.given[name] = why


def _ways(lease):
    rungs = (decide.Rung("pass-through", "the rows passed server to server"),
             decide.Rung("pipe", "the rows copied as text"))
    said = []
    w = pg_pass.Ways.__new__(pg_pass.Ways)
    w.eng = types.SimpleNamespace(snapshot_mark=lambda s, d: "m",
                                  hop=types.SimpleNamespace())
    w.db, w.table, w.log = "d", "public.t", said.append
    w.ctx = types.SimpleNamespace(snapshot=types.SimpleNamespace(lease=lease))
    w.trials = _Trials(rungs)
    w.passing = {"pass-through"}
    w._said, w._lock = {"pass-through", "pipe"}, threading.Lock()
    w._say = lambda got: None
    return w, said


def test_a_snapshot_not_exported_goes_as_text_and_says_why():
    def lease():
        raise RuntimeError("ERROR: cannot export a snapshot from a standby")
    w, said = _ways(lease)
    way = w.pick(lambda mark: None)
    assert way.name == "pipe" and not way.passes
    assert "pass-through" in w.trials.given
    assert any("could not run" in s and "standby" in s and "switching" in s
               for s in said), said


def test_a_refused_sign_in_at_the_snapshot_stops_the_table():
    def lease():
        raise RuntimeError("FATAL: password authentication failed for user")
    w, said = _ways(lease)
    with pytest.raises(runner.Refused):
        w.pick(lambda mark: None)
    assert "pass-through" not in w.trials.given


def test_a_refusal_after_a_range_passed_never_falls_to_text():
    w, _ = _ways(lambda: None)
    way = pg_pass.Way(w, w.trials.pick(), lambda m: None)
    with pytest.raises(runner.Refused):
        way.instead("ERROR: cannot execute COPY in a read-only transaction")
    assert w.trials.given == {}


def test_the_source_export_sets_nothing_only_a_superuser_may():
    assert "session_replication_role" not in pg_pass.LOAD_OPTIONS


# --- a table added to a running tail -----------------------------------------

def _tail(position):
    from migkit import tailtables
    cls = next(c for c in vars(tailtables).values()
               if isinstance(c, type) and "_add" in vars(c))
    said = []
    t = cls.__new__(cls)
    t.db, t.log = "d", said.append
    t.src = types.SimpleNamespace(REREADS_FROM_A_POSITION=True,
                                  log_position=position,
                                  position_lost=lambda s, d, a: "")
    t._same_shape = lambda prev, name: True
    states = {"t": {"state": "removed", "at": "p1", "digest": "h"}}
    return t, states, said


def test_a_source_that_cannot_say_its_position_copies_and_says_why():
    def position(side, db):
        raise RuntimeError("connection reset by peer")
    t, states, said = _tail(position)
    t._add(states, "t", "t", "p2", "taken in")
    assert states["t"]["way"] == "copy"
    assert any("switching" in s and "connection reset" in s for s in said)


def test_a_position_read_refused_stops_the_add():
    def position(side, db):
        raise RuntimeError("permission denied for function pg_current_wal_lsn")
    t, states, said = _tail(position)
    with pytest.raises(RuntimeError):
        t._add(states, "t", "t", "p2", "taken in")
    assert states["t"]["state"] == "removed"


def test_a_position_read_goes_on_from_where_it_left():
    t, states, said = _tail(lambda side, db: "p9")
    t._add(states, "t", "t", "p2", "taken in")
    assert states["t"]["way"] == "from-removal"
    assert states["t"]["upto"] == "p9"


# --- a schema change on another engine's target --------------------------------

def _hetero(dst):
    from migkit.engines.hetero import HeteroEngine
    said = []
    h = HeteroEngine.__new__(HeteroEngine)
    h.dst_engine = dst
    h.__dict__["_lent_log"] = said.append
    return h, said


def _dst(ddl=None, ran=None):
    ran = ran if ran is not None else []
    body = {"__init__": lambda self: None,
            "neutral_create_code": lambda self, s, d, sql: ran.append(sql),
            "forget_shapes": lambda self: ran.append("forgot")}
    if ddl is not None:
        body["execute_ddl"] = lambda self, s, d, sql: ddl(sql)
    return type("Dst", (Engine,), body)(), ran


def test_a_target_without_its_own_schema_change_runs_a_converted_create():
    dst, ran = _dst()
    h, _ = _hetero(dst)
    h._run_ddl("d", "alter table t add c int")
    assert ran == ["alter table t add c int", "forgot"]


def test_a_statement_the_engine_does_not_take_switches_said():
    def ddl(sql):
        raise NotImplementedError("this statement is not one it runs")
    dst, ran = _dst(ddl)
    h, said = _hetero(dst)
    h._run_ddl("d", "alter table t add c int")
    assert ran == ["alter table t add c int", "forgot"]
    assert any("not one it runs" in s and "switching" in s for s in said)


def test_a_schema_change_refused_never_runs_as_a_create():
    def ddl(sql):
        raise RuntimeError("ERROR: permission denied for table t")
    dst, ran = _dst(ddl)
    h, _ = _hetero(dst)
    with pytest.raises(RuntimeError, match="permission denied"):
        h._run_ddl("d", "alter table t add c int")
    assert ran == []
