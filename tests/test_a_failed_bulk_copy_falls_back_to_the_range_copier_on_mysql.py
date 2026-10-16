"""A MySQL bulk copy that fails is told by the fault catalogue, and the move
falls back to migkit's own table copier instead of stopping (W15J-MYBULK,
P0-RESILIENCE-ALL).

What stopped the rowmeta pair's copy was the bulk way itself: a name its
move still called after the masking record it named was gone, raised as a
fault of migkit's own code, which no class describes. Fixed, the copy runs
whole; here the bulk load is failed on purpose after it has loaded every
row, so the copier after it must converge on a target already holding them
- each range replaced by key, never a row twice - and a second run copies
nothing again."""
import types

import pytest

from migkit import decide, faults
from migkit.movers import _bulk_classed
from tests import servers
from tests.test_autonomous_scenarios import GO, Side, _conf, _run

rowmeta = servers.fixture("mysql-rowmeta")
plain = servers.fixture("mysql")

HOP = types.SimpleNamespace(name="t")


@pytest.fixture(params=["rowmeta", "plain"])
def pair(request):
    got = request.getfixturevalue(request.param)
    return Side("mysql", got.src, got.dst)


def test_a_bulk_load_failing_after_it_loaded_ends_whole_by_the_copier(
        pair, tmp_path):
    conf = _conf(tmp_path, pair)
    pair.seed()
    want = pair.digest("src")
    r = _run(conf, pair, *GO,
             MIGKIT_FAILPOINT="bulk.loaded:1:fault=connectivity")["r"]
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    # the switch names the class and what reconciles the partial load
    assert "(connectivity:" in out, out
    assert "replaces each range by key" in out, out
    got = pair.digest("dst")
    assert got == want, (got, want)
    assert got[0] == got[1], "a key arrived twice"
    again = _run(conf, pair, *GO)["r"]
    assert again.returncode == 0, again.stdout + again.stderr
    assert "dumped and loaded" not in again.stdout, again.stdout
    assert pair.digest("dst") == want


def test_a_failure_of_a_class_another_way_gets_past_switches_the_way():
    def fail():
        raise faults.Injected("connectivity")
    assert faults.classify(faults.Injected("connectivity")).fault.name \
        == "connectivity"
    with pytest.raises(decide.Blocked) as e:
        _bulk_classed(HOP, "w9c", fail)
    assert decide.operational(e.value)
    assert "connectivity" in str(e.value) and "by key" in str(e.value)


def test_a_program_that_exits_still_falls_to_the_copier():
    def fail():
        raise RuntimeError("the load program exited with 1")
    with pytest.raises(decide.Blocked):
        _bulk_classed(HOP, "w9c", fail)


def test_the_recorded_failure_stops_as_a_blocker_naming_the_next_action():
    # the text the rowmeta pair's move stopped on (W15J's commands.log)
    def fail():
        raise NameError("name '_masked_record' is not defined")
    assert faults.classify(
        NameError("name '_masked_record' is not defined")).fault.name \
        == "unknown"
    with pytest.raises(SystemExit) as e:
        _bulk_classed(HOP, "w9c", fail)
    said = str(e.value)
    assert "unknown" in said and "NameError" in said
    assert "Next:" in said and "nothing it loaded is taken as done" in said
    assert not decide.operational(e.value)


def test_a_refused_account_is_not_tried_over_by_another_way():
    def fail():
        raise faults.Injected("auth")
    with pytest.raises(SystemExit) as e:
        _bulk_classed(HOP, "w9c", fail)
    assert "(auth:" in str(e.value) and "Next:" in str(e.value)


def test_a_verdict_is_never_turned_into_a_switch():
    def fail():
        raise decide.Verdict("o differs")
    with pytest.raises(decide.Verdict):
        _bulk_classed(HOP, "w9c", fail)
