"""The choices stay inside; the outcomes are said (backlog P0-SMART, the
owner's note on D of 2026-09-30).

"The logic and the choices inside - the user doesn't need to know them":
the console says what will happen, the time with its spread, the
guarantees, what the operator must do. The decision in full - the way
taken and why, the runner-up and why not with the times, the facts it
stood on - goes to the run's decision record (the hop's
`decisions.jsonl`, in the shape the run keeps every choice in) and to
`Choice.as_evidence` for the evidence export, and to the console only
where `MIGKIT_EXPLAIN=1`. Never a rung's label or a program's name in
what the operator reads. No server.
"""
import json

import pytest

from migkit import decide as d
from migkit.config import Endpoint, Hop
from tests.test_the_report_does_not_name_its_tools import TOOLS

R = d.Rung


def _hop(tmp_path):
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=1, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=2, user="u",
                              password="CHANGE_ME"), databases=["appdb"])
    hop.report_dir = lambda db="", _p=tmp_path: _p
    return hop


def _choice():
    ways = [R("mydumper", "dumped in parallel chunks and loaded"),
            R("builtin", "copied table by table")]
    return d.climb("appdb", ways, costs=d.Costs({"mydumper": 2.0,
                                                 "builtin": 1.0}))


def test_a_choice_carries_its_runner_up_and_why_not():
    got = _choice()
    assert got.path == "builtin"
    assert got.runner_up.name == "mydumper"
    assert got.why_not == ("measured 2.0 times the time a unit of copied"
                           " table by table took")
    ev = got.as_evidence()
    assert ev["chosen"]["way"] == "copied table by table"
    assert ev["runner_up"]["way"] == "dumped in parallel chunks and loaded"
    assert ev["runner_up"]["why_not"] == got.why_not
    assert ev["chosen"]["predicted"] == {"seconds_a_unit": 1.0,
                                         "truth": "measured"}


def test_the_evidence_never_names_a_label_or_a_program():
    text = json.dumps(_choice().as_evidence()).lower()
    assert "mydumper" not in text and '"builtin"' not in text
    assert not [t for t in TOOLS if t in text]


def test_the_record_keeps_the_choice_in_the_runs_shape(tmp_path,
                                                       monkeypatch):
    monkeypatch.delenv("MIGKIT_EXPLAIN", raising=False)
    hop = _hop(tmp_path)
    said = []
    d.record(hop, _choice(), said.append)
    d.record(hop, {"work": "the move of appdb", "plan": {"makespan": 60},
                   "facts": {"known": ["bytes"], "measured": [],
                             "predicted": [], "unknown": ["deadline"]}},
             said.append, way="the tables planned as one",
             reason="the copy ~1m00s")
    # nothing said on the console without MIGKIT_EXPLAIN
    assert said == []
    got = d.recorded(hop)
    assert [e["work"] for e in got] == ["appdb", "the move of appdb"]
    first = got[0]
    assert first["way"] == "copied table by table"
    assert first["detail"]["runner_up"]["why_not"].startswith("measured")
    assert {"session", "at", "work", "way", "reason", "passed",
            "detail"} <= set(first)
    assert got[1]["way"] == "the tables planned as one"
    # one file with the run's own record of each choice
    from migkit.session import DECISIONS
    assert (tmp_path / DECISIONS).exists()


def test_explain_says_the_whole_decision_on_the_console(tmp_path,
                                                        monkeypatch):
    monkeypatch.setenv("MIGKIT_EXPLAIN", "1")
    said = []
    d.record(_hop(tmp_path), _choice(), said.append)
    assert said[0] == "decision:"
    text = "\n".join(said)
    assert "runner_up:" in text and "why_not:" in text
    assert "mydumper" not in text


def test_the_explained_lines_are_bounded():
    got = d.explained({"tables": {f"t{i}": {"way": "w"} for i in range(40)},
                       "order": [f"t{i}" for i in range(40)]})
    assert any("... and 28 more" in line for line in got)
    assert len(got) < 12 * 4 + 10


def test_doctor_says_what_the_last_plan_stood_on(tmp_path):
    from migkit import cli
    hop = _hop(tmp_path)
    d.record(hop, {"work": "the move of appdb", "plan": {},
                   "facts": {"known": ["bytes", "rows"],
                             "measured": ["rtt"], "predicted": ["costs"],
                             "unknown": ["deadline", "money"]}},
             way="the tables planned as one")
    got = cli._decisions_said(hop)
    assert len(got) == 1
    line = got[0]
    assert line.startswith("the move of appdb: the last plan (")
    assert "known 2" in line and "measured 1 (rtt)" in line
    assert "unknown 2 (deadline, money)" in line


def test_nothing_is_recorded_without_a_hop():
    got = d.record(None, _choice())
    assert got["way"] == "copied table by table"


@pytest.mark.parametrize("value", ["0", "", "no"])
def test_explain_is_off_unless_asked(value, monkeypatch):
    monkeypatch.setenv("MIGKIT_EXPLAIN", value)
    assert not d.explaining()
