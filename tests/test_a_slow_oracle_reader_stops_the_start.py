"""An Oracle source whose change reader is measured to fall behind the
redo it writes is not followed (backlog S1.e): the gate of a move or a
tail (`ready.gate`) stops the start with the reason and the figures, unless
the operator said to go on (`MIGKIT_PREFLIGHT=warn`); a reader that could
not be measured is said as unknown, never a pass.

The verdict's use is held on stand-ins; on the Oracle lane (Oracle Free
in the VM, archiving its redo: `tests/test_oracle_is_a_whole_side.py`'s
sandbox) both figures are measured for real - a quiet source goes on, a
burst of redo the test writes raises the measured busiest hour, and an
account that may not read the archived logs is unknown and warns (lead
decision: an unmeasured reader warns). A reader slower than the lane can
write redo is not something one machine can make: there the reader's
figure is a stand-in under the real measured redo, which is what stops.
"""
import pytest

from tests.conftest import needs_docker
from tests.test_oracle_follows_and_resumes_on_its_lane import lane  # noqa: F401
from tests.test_oracle_is_a_whole_side import (  # noqa: F401 - fixtures
    DST, PW, SRC, _ep, alone, engine, ora, sandbox)

MB = 2 ** 20


def _hop(goal="keep-in-sync"):
    from migkit.config import Endpoint, Hop
    return Hop(name="ora", engine="oracle", source=Endpoint(),
               target=Endpoint(), databases=["app"], options={}, goal=goal)


class _Oracle:
    """The source engine as the gate meets it."""

    def __init__(self, verdict):
        self.verdict = verdict
        self.asked = []

    def databases(self):
        return ["app"]

    def _mapping_items(self):
        return []

    def change_reader_check(self, db):
        self.asked.append(db)
        if isinstance(self.verdict, Exception):
            raise self.verdict
        return self.verdict


class _Pair:
    """A pair: the Oracle source is its `src_engine`."""

    def __init__(self, source):
        self.src_engine = source

    def databases(self):
        return ["app"]

    def _mapping_items(self):
        return []


@pytest.fixture(autouse=True)
def _only_the_reader(monkeypatch):
    """Every other part of the gate answers nothing."""
    from migkit import interlock, ready
    for name in ("_servers", "_managed", "_target", "_resources", "_case",
                 "_goal_checks", "_rehearsed_quick", "_mapping"):
        monkeypatch.setattr(ready, name, lambda *a, **k: [])
    monkeypatch.setattr(interlock, "reached", lambda hop: True)
    monkeypatch.delenv("MIGKIT_PREFLIGHT", raising=False)


FAIL = ("fail", "the source archived 40.00 MB of redo a second at its"
        " busiest hour (12.00 on average); the reader mined 25.00 MB a"
        " second - it would fall behind at the busiest hour", True)


def test_a_reader_that_falls_behind_stops_a_move_with_the_figures():
    from migkit import ready
    eng = _Oracle(FAIL)
    with pytest.raises(SystemExit) as e:
        ready.gate(_hop(), eng, None, "full+cdc", False, lambda m: None)
    text = str(e.value)
    assert "nothing was written" in text, text
    assert "the change reader keeps up with the redo" in text, text
    assert "40.00 MB of redo a second" in text and "25.00 MB" in text, text
    assert "MIGKIT_PREFLIGHT=warn" in text, text
    assert eng.asked == ["app"]


def test_a_tail_alone_is_stopped_the_same_way():
    from migkit import ready
    got = ready.stops(_hop(), _Oracle(FAIL), None, "cdc", False)
    assert [i.what for i in got] == [
        "source: app: the change reader keeps up with the redo"]
    assert got[0].level == "fail" and got[0].gates


def test_the_operator_goes_on_through_the_existing_override(monkeypatch):
    from migkit import ready
    monkeypatch.setenv("MIGKIT_PREFLIGHT", "warn")
    said = []
    got = ready.gate(_hop(), _Oracle(FAIL), None, "cdc", False, said.append)
    assert len(got) == 1 and len(said) == 1, said
    assert "25.00 MB a second" in said[0], said


def test_an_unmeasured_reader_is_unknown_and_not_a_pass():
    from migkit import ready
    unknown = ("warn", "not measured: the source has archived no redo in"
               " the last day, so neither its rate nor the reader's could"
               " be read", False)
    eng = _Oracle(unknown)
    its = ready._change_reader(_hop(), eng, ["app"], "cdc")
    assert [i.level for i in its] == ["warn"], its
    assert "unknown, not clean" in its[0].detail, its[0].detail
    # the rule every unknown follows: said, and it does not stop the step
    assert not its[0].gates
    assert ready.stops(_hop(), eng, None, "cdc", False) == []
    # and one that cannot be asked at all is the same, never a pass
    boom = _Oracle(RuntimeError("ORA-12541: no listener"))
    its = ready._change_reader(_hop(), boom, ["app"], "cdc")
    assert its[0].level == "warn" and "unknown" in its[0].detail, its


def test_a_reader_that_keeps_up_is_said_and_does_not_stop():
    from migkit import ready
    for level in ("pass", "warn"):
        eng = _Oracle((level, "the source archived 10.00 MB of redo a"
                       " second at its busiest hour; the reader mined"
                       " 30.00 MB a second", True))
        its = ready._change_reader(_hop(), eng, ["app"], "cdc")
        assert [i.level for i in its] == [level] and not its[0].gates
        assert ready.stops(_hop(), eng, None, "cdc", False) == []


def test_a_step_that_follows_nothing_is_not_asked():
    from migkit import ready
    eng = _Oracle(FAIL)
    assert ready.stops(_hop("copy"), eng, None, "full", False) == []
    # tearing a stream down is the cutover's own preflight
    assert ready.stops(_hop(), eng, None, "cdc", True) == []
    assert ready._change_reader(_hop("verify"), eng, ["app"], "cdc") == []
    assert eng.asked == []


def test_assess_does_not_measure_twice_for_the_same_listing():
    # the engine's own assess already carries the verdict; only the gate's
    # quick listing adds it
    from migkit import ready
    eng = _Oracle(FAIL)
    its = ready.items(_hop(), eng, ["app"], "cdc", quick=False,
                      assessed=[])
    assert not [i for i in its if "change reader" in i.what], its
    assert eng.asked == []


def test_the_source_of_a_pair_is_the_engine_asked():
    from migkit import ready
    pair = _Pair(_Oracle(FAIL))
    got = ready.stops(_hop(), pair, None, "full+cdc", False)
    assert len(got) == 1 and "40.00 MB" in got[0].detail, got
    # an engine with no such measure is not asked, and not stopped
    assert ready.stops(_hop(), _Pair(object()), None, "cdc", False) == []


def test_each_database_is_held_to_its_own_reader():
    from migkit import ready
    eng = _Oracle(FAIL)
    its = ready._change_reader(_hop(), eng, ["app", "crm"], "cdc")
    assert [i.what for i in its] == [
        "source: app: the change reader keeps up with the redo",
        "source: crm: the change reader keeps up with the redo"]


# ---- the one measure ----------------------------------------------------

def _measure(monkeypatch, rate, mined):
    from migkit.engines import oracle_logminer as lm
    monkeypatch.setattr(lm, "redo_rate", lambda *a: rate)
    monkeypatch.setattr(lm, "reader_rate", lambda *a: mined)
    return lm


def test_the_verdict_is_judged_in_one_place(monkeypatch):
    lm = _measure(monkeypatch, (40 * MB, 12 * MB), (25 * MB, "log1"))
    level, detail, measured = lm.reader_check(None, "src", "app")
    assert (level, measured) == ("fail", True)
    assert "40.00 MB" in detail and "25.00 MB" in detail
    lm = _measure(monkeypatch, (40 * MB, 12 * MB), (60 * MB, "log1"))
    assert lm.reader_check(None, "src", "app")[0] == "warn"
    lm = _measure(monkeypatch, (40 * MB, 12 * MB), (90 * MB, "log1"))
    assert lm.reader_check(None, "src", "app")[0] == "pass"
    lm = _measure(monkeypatch, None, (90 * MB, "log1"))
    level, detail, measured = lm.reader_check(None, "src", "app")
    assert (level, measured) == ("warn", False) and "not measured" in detail
    monkeypatch.setattr(lm, "redo_rate", lambda *a: 1 / 0)
    assert lm.reader_check(None, "src", "app")[::2] == ("warn", False)

    def refused(*a):
        raise SystemExit("no")
    monkeypatch.setattr(lm, "redo_rate", refused)
    assert lm.reader_check(None, "src", "app")[::2] == ("warn", False)


def test_assess_and_the_gate_read_the_same_verdict(monkeypatch):
    from migkit.engines import oracle_logminer as lm
    monkeypatch.setattr(lm, "readiness", lambda *a, **k: [])
    lm = _measure(monkeypatch, (40 * MB, 12 * MB), (25 * MB, "log1"))

    class _E:
        hop = _hop()

        def _rows(self, *a):
            return []

        def _endpoint(self, side):
            return self.hop.source
    rows = lm.assess(_E(), "src", "app")
    rate = [r for r in rows if r["item"].endswith("reader rate")]
    level, detail, _ = lm.reader_check(_E(), "src", "app")
    assert rate == [{"level": level, "scope": "app", "detail": detail,
                     "item": "change feed from the source: reader rate"}]


def test_the_engine_hands_over_the_measure_unless_the_redo_files_are_read(
        monkeypatch):
    from migkit.engines import oracle_logminer as lm
    from migkit.engines import oracle_redo
    from migkit.engines.oracle import OracleEngine
    monkeypatch.setattr(lm, "reader_check", lambda *a: ("fail", "x", True))
    monkeypatch.setattr(oracle_redo, "chosen", lambda *a:
                        oracle_redo.LOGMINER)
    assert OracleEngine.change_reader_check(object(), "app") == (
        "fail", "x", True)
    monkeypatch.setattr(oracle_redo, "chosen", lambda *a: oracle_redo.RUNG)
    assert OracleEngine.change_reader_check(object(), "app") is None


# ---- on the Oracle lane ---------------------------------------------------

def _burst(rows):
    """Redo the test writes: `rows` rows of 400 bytes, committed, the log
    switched so the archive holds them."""
    alone("redoburst")
    ora("begin execute immediate 'drop table app.redoburst purge';"
        " exception when others then null; end;")
    ora("create table app.redoburst (id number(10) primary key,"
        " s varchar2(400))")
    ora("insert into app.redoburst select level, rpad('r', 400, 'x') from"
        " dual connect by level <= :1", rows)
    ora("alter system archive log current", user="sys", sysdba=True,
        root=True)


@needs_docker
def test_on_the_lane_a_quiet_source_measured_against_its_reader_goes_on(
        lane):
    from migkit import ready
    alone()
    ora("alter system archive log current", user="sys", sysdba=True,
        root=True)
    eng = engine()
    level, detail, measured = eng.change_reader_check("app")
    assert measured is True and level in ("pass", "warn"), detail
    assert "MB of redo a second" in detail and "the reader mined" in detail
    its = ready._change_reader(eng.hop, eng, ["app"], "cdc")
    assert its and not any(i.gates for i in its), its
    ready.gate(eng.hop, eng, None, "full+cdc", False, lambda m: None)


@needs_docker
def test_on_the_lane_a_burst_of_redo_stops_a_reader_slower_than_it(
        lane, monkeypatch):
    from migkit import ready
    from migkit.engines import oracle_logminer as miner
    eng = engine()
    before = miner.redo_rate(eng, "src", "app")
    _burst(200000)
    after = miner.redo_rate(eng, "src", "app")
    # the burst is in the measured busiest hour
    assert after and after[0] > (before[0] if before else 0), (before, after)
    real = miner.reader_rate(eng, "src", "app")
    assert real and real[0] > 0, real
    # a reader slower than the measured redo: the redo's figure is real
    peak = after[0]
    monkeypatch.setattr(miner, "reader_rate",
                        lambda *a: (peak / 2, "the stand-in reader"))
    with pytest.raises(SystemExit) as e:
        ready.gate(eng.hop, eng, None, "full+cdc", False, lambda m: None)
    text = str(e.value)
    assert "nothing was written" in text, text
    assert f"{peak / MB:.2f} MB of redo a second" in text, text
    assert f"mined {peak / 2 / MB:.2f} MB a second" in text, text


@needs_docker
def test_on_the_lane_a_reader_that_cannot_be_measured_warns(lane):
    from migkit import ready
    from migkit.config import Endpoint, Hop
    from migkit.engines.oracle import OracleEngine
    ep = _ep(SRC)
    # an account that may not read the archived logs: unknown, a warning
    eng = OracleEngine(Hop(name="ora", engine="oracle", source=Endpoint(
        host=ep.host, port=ep.port, user="app", password=PW,
        options=ep.options), target=_ep(DST), databases=["app"],
        options={}))
    level, detail, measured = eng.change_reader_check("app")
    assert (level, measured) == ("warn", False), (level, detail)
    assert detail.startswith("not measured"), detail
    its = ready._change_reader(eng.hop, eng, ["app"], "cdc")
    assert [i.level for i in its] == ["warn"] and not its[0].gates, its
    assert ready.stops(eng.hop, eng, None, "cdc", False) == []
