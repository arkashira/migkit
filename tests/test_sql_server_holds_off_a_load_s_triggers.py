"""A load into a SQL Server target switches off the triggers on the tables it
writes, and on again after (P0-PARITY.all, `_LoadWindow`): the record of
what goes off is on disk before anything does, what a load that died left
off is put back by the next one, and `check` names it until then. The
secondary indexes stay on: this window is the change tail's too, held for
days, and a repair's.

Without a server: a scripted one (`tests/mssql_stub.py`) answers the
catalogue reads and keeps the statements sent. Every statement here is new
and has not run on a server: `move.load-window` waits in
`capabilities.UNPROVED`.
"""
import json
import os
import re
import socket

import pytest

from tests.mssql_stub import Server, engine

TRIGGERS_ON = "tr\\.is_disabled = 0"
TRIGGERS_OFF = "tr\\.is_disabled = 1"


def _record(tmp_path):
    return sorted(tmp_path.glob("dropped-triggers.*.json"))


def _target(off=()):
    """A target with a trigger on `dbo.a` and one on `dbo.b`, and `off`
    ("trigger dbo.b.trg_b") already switched off."""
    s = Server()
    s.on(TRIGGERS_ON, [("dbo.a", "trg_a"), ("dbo.b", "trg_b")])
    s.on(TRIGGERS_OFF, [(x.rsplit(".", 1)[0].split(" ", 1)[1],
                         x.rsplit(".", 1)[1]) for x in off])
    return s


def test_what_goes_off_is_recorded_first_and_comes_back_on(monkeypatch,
                                                            tmp_path):
    s = _target()
    seen = []

    def off(side, sql):
        # recorded before it is done
        seen.append((sql, [json.loads(p.read_text())
                           for p in _record(tmp_path)]))
        return []
    s.on("^disable trigger", off)
    eng = engine(monkeypatch, tmp_path, s)
    said = []
    with eng.load_window("appdb", said.append, {"a"}):
        assert s.sent("^disable trigger") == [
            "disable trigger [dbo].[trg_a] on [dbo].[a]"]
        (sql, records), = seen
        assert records == [{"trigger dbo.a.trg_a":
                            "enable trigger [dbo].[trg_a] on [dbo].[a]"}]
        # the load's own table only: dbo.b is not written
        assert not s.sent("trg_b")
    assert s.sent("^enable trigger") == [
        "enable trigger [dbo].[trg_a] on [dbo].[a]"]
    # everything back: nothing left on disk to put back
    assert not _record(tmp_path)
    assert any("1 triggers switched off for the load" in m for m in said)
    assert any("1 of 1 triggers switched on again" in m for m in said)


def test_the_indexes_are_never_switched_off(monkeypatch, tmp_path):
    s = _target()
    eng = engine(monkeypatch, tmp_path, s)
    with eng.load_window("appdb", None, {"a"}):
        pass
    assert not s.sent("alter index")
    assert not s.sent("sys\\.indexes")


def test_the_whole_databases_load_writes_every_table_the_hop_takes(
        monkeypatch, tmp_path):
    s = _target()
    eng = engine(monkeypatch, tmp_path, s, exclude=["appdb.dbo.b"])
    with eng.load_window("appdb"):
        assert s.sent("disable trigger \\[dbo\\]\\.\\[trg_a\\]")
        # a table the hop leaves out is the target's own
        assert not s.sent("trg_b")


def test_it_is_put_back_when_the_load_fails(monkeypatch, tmp_path):
    s = _target()
    eng = engine(monkeypatch, tmp_path, s)
    with pytest.raises(ValueError):
        with eng.load_window("appdb", None, {"a"}):
            raise ValueError("the load stopped")
    assert s.sent("enable trigger \\[dbo\\]\\.\\[trg_a\\]")
    assert not _record(tmp_path)


def test_one_that_could_not_be_put_back_is_named_and_its_record_kept(
        monkeypatch, tmp_path):
    s = _target()

    def refuse(side, sql):
        raise RuntimeError("the target refused a statement: denied")
    s.rules.insert(0, (None, re.compile("^enable trigger"), refuse))
    eng = engine(monkeypatch, tmp_path, s)
    said = []
    with pytest.raises(SystemExit) as e:
        with eng.load_window("appdb", said.append, {"a"}):
            pass
    assert "1 triggers were not switched on again" in str(e.value)
    assert "trigger dbo.a.trg_a" in str(e.value)
    assert any("NOT SWITCHED ON: trigger dbo.a.trg_a" in m for m in said)
    # the record stays, so the next load - and `check` - know
    assert _record(tmp_path)


def test_nothing_is_switched_off_that_cannot_be_recorded_first(monkeypatch,
                                                                tmp_path):
    s = _target()
    eng = engine(monkeypatch, tmp_path, s)
    import migkit.indexes as ix
    monkeypatch.setattr(ix, "saved", lambda path, defs: False)
    with pytest.raises(SystemExit) as e:
        with eng.load_window("appdb", None, {"a"}):
            pytest.fail("the load ran")
    assert "could not be recorded first" in str(e.value)
    assert not s.sent("^disable trigger")


def test_one_that_cannot_be_switched_off_is_left_and_the_load_runs(
        monkeypatch, tmp_path):
    s = _target()

    def refuse(side, sql):
        raise RuntimeError("the target refused a statement: denied")
    s.rules.insert(0, (None, re.compile("^disable trigger"), refuse))
    eng = engine(monkeypatch, tmp_path, s)
    said = []
    with eng.load_window("appdb", said.append, {"a"}):
        pass
    assert any("could not switch off trigger dbo.a.trg_a, leaving it" in m
               for m in said)
    # it was never off, so it is not switched on
    assert not s.sent("^enable trigger")


def _dead_load(tmp_path, defs):
    """The record of a load whose process is gone."""
    path = tmp_path / f"dropped-triggers.{socket.gethostname()}.999999.1.json"
    path.write_text(json.dumps(defs))
    return path


def test_what_a_load_that_died_left_off_is_put_back_by_the_next(monkeypatch,
                                                                 tmp_path):
    died = _dead_load(tmp_path, {"trigger dbo.b.trg_b":
                                 "enable trigger [dbo].[trg_b] on [dbo].[b]"})
    s = _target(off=["trigger dbo.b.trg_b"])
    # the trigger the dead load left off is off now, and is not enabled
    s.rules[0] = (None, s.rules[0][1], [("dbo.a", "trg_a")])
    eng = engine(monkeypatch, tmp_path, s)
    said = []
    with eng.load_window("appdb", said.append, {"a"}):
        pass
    assert s.sent("enable trigger \\[dbo\\]\\.\\[trg_b\\] on \\[dbo\\]\\.\\[b\\]")
    assert any("1 triggers an earlier load switched off" in m for m in said)
    assert not died.exists()
    assert not _record(tmp_path)


def test_a_trigger_the_dead_load_left_that_is_on_again_is_not_touched(
        monkeypatch, tmp_path):
    _dead_load(tmp_path, {"trigger dbo.b.trg_b":
                          "enable trigger [dbo].[trg_b] on [dbo].[b]"})
    s = _target()       # nothing is off now
    eng = engine(monkeypatch, tmp_path, s)
    with eng.load_window("appdb", None, {"a"}):
        pass
    assert not s.sent("trg_b")


def test_check_names_what_a_load_that_died_left_off(monkeypatch, tmp_path):
    path = _dead_load(tmp_path, {"trigger dbo.a.trg_a":
                                 "enable trigger [dbo].[trg_a] on [dbo].[a]"})
    s = _target(off=["trigger dbo.a.trg_a"])
    eng = engine(monkeypatch, tmp_path, s)
    got = eng.set_aside("appdb")
    assert got.status == "diff" and "trigger dbo.a.trg_a" in got.detail
    assert str(path) in got.detail
    # on again by hand: nothing to name
    assert engine(monkeypatch, tmp_path, _target()).set_aside("appdb") is None


def test_check_says_what_a_running_load_holds_off_on_purpose(monkeypatch,
                                                              tmp_path):
    # the record of this very process: a load that is running
    path = tmp_path / (f"dropped-triggers.{socket.gethostname()}"
                       f".{os.getpid()}.1.json")
    path.write_text(json.dumps({"trigger dbo.a.trg_a": "enable ..."}))
    s = _target(off=["trigger dbo.a.trg_a"])
    eng = engine(monkeypatch, tmp_path, s)
    got = eng.set_aside("appdb")
    assert got.status == "ok" and "while a migkit load runs" in got.detail


def test_the_deep_check_reports_it(monkeypatch, tmp_path):
    import migkit.workload as workload
    from migkit.engines.base import Result
    _dead_load(tmp_path, {"trigger dbo.a.trg_a":
                          "enable trigger [dbo].[trg_a] on [dbo].[a]"})
    s = _target(off=["trigger dbo.a.trg_a"])
    eng = engine(monkeypatch, tmp_path, s)
    eng._pk_cols = lambda db, t: ["id"]
    monkeypatch.setattr(workload, "compare",
                        lambda e, db: Result("deep", f"{db} performance",
                                             "skip"))
    monkeypatch.setattr(type(eng), "_mojibake",
                        lambda self, db: Result("deep", f"{db} mojibake",
                                                "ok"))
    got = {r.scope: r for r in eng.check_deep("appdb")}
    assert got["appdb triggers set aside"].status == "diff"
