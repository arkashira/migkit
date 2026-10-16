"""The drain, the cutover's approval readiness, the one-way guard and the
change sheet reach the commands that run them: `migkit drain`, the
cutover's drain and point of no return, the copy's end and the view. All
on fakes: no server."""
import json
from pathlib import Path

import pytest

from migkit import cutover, drainmark, report, tailctl
from migkit.web import runs, views



class Hop:
    name = "h"
    options = {}

    def __init__(self, root):
        self.root = Path(root)

    def report_dir(self, db=None):
        d = self.root / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d


class Fence:
    CANON_ENGINE = "postgres"


def test_the_seam_says_a_batch_in_flight_went_past_the_position(tmp_path,
                                                                monkeypatch):
    hop = Hop(tmp_path)
    monkeypatch.setattr(tailctl, "saved", lambda path: "0/30")
    said = []
    got = drainmark.seam(hop, Fence(), "d", "0/20", 1.5, held=True,
                         log=said.append)
    assert got["past"] == 1 and got["applied"] == "0/30"
    assert "went past" in got["said"] and said
    line = json.loads((tmp_path / "changelog.jsonl").read_text().splitlines()[-1])
    assert line["op"] == "drained" and line["held"] is True


def test_a_cutover_drain_leaves_the_same_record_without_holding(tmp_path,
                                                                monkeypatch):
    hop = Hop(tmp_path)
    monkeypatch.setattr(tailctl, "saved", lambda path: "0/20")
    leg = type("L", (), {"fence": Fence()})()
    got = cutover._seam(hop, leg, "d", "0/20", 0.2, lambda m: None)
    assert got["past"] == 0 and got["held"] is None
    line = json.loads((tmp_path / "changelog.jsonl").read_text().splitlines()[-1])
    assert line["held"] is None and line["position"] == "0/20"


def test_a_phase_that_ends_writes_back_only_its_own_steps(tmp_path):
    hop = Hop(tmp_path)
    phases = [{"name": "copy", "table": "t1"}, {"name": "copy"},
              {"name": "catch-up"}]
    report.write_change_sheet(hop, report.change_sheet(hop, "d", phases))
    ids = report.step_ended(hop, "d", "copy", "done", 12.0, "the copy ended")
    assert len(ids) == 2
    steps = {s["id"]: s for s in report.read_change_sheet(hop)}
    by_table = {s["table"]: s for s in steps.values() if s["phase"] == "copy"}
    assert by_table[""]["seconds"] == 12.0
    assert by_table["t1"]["seconds"] is None
    assert "not split by table" in by_table["t1"]["proof"]
    waiting = [s for s in steps.values() if s["phase"] == "catch-up"]
    assert waiting[0]["status"] == "planned"
    assert report.step_ended(hop, "d", "copy", "done", 1.0, "again") == []
    assert report.step_ended(hop, "other", "copy", "done", 1.0, "x") == []


def test_no_sheet_means_nothing_is_written(tmp_path):
    assert report.step_ended(Hop(tmp_path), "d", "copy", "done", 1.0, "x") \
        == []


def test_a_cutover_phase_maps_onto_the_sheet_phase_of_the_plan():
    assert cutover.SHEET_PHASE == {"drain": "catch-up", "flip": "cutover"}


def test_the_drain_action_takes_a_position_and_refuses_an_option(tmp_path):
    act = runs.ACTIONS["drain"]
    argv, _ = act.argv("h", {"db": "d", "to": "0/16B3748"})
    assert argv == ["drain", "h", "--db", "d", "--to", "0/16B3748"]
    assert act.writes({"go": True}) and not act.writes({})
    argv, _ = act.argv("h", {"db": "d", "to": "uuid:1-500", "go": True})
    assert argv[-1] == "--go"
    for bad in ("--go", "; rm", "-1"):
        with pytest.raises(runs.Refused):
            act.argv("h", {"db": "d", "to": bad})
    with pytest.raises(runs.Refused):
        act.argv("h", {"to": "now"})


def test_the_view_gives_readiness_to_a_cutover_request_only(tmp_path,
                                                            monkeypatch):
    from migkit import approvals
    monkeypatch.setattr(approvals, "readiness", lambda hop, db: {
        "ready": False, "blocks": ["the tail is held"], "next": "resume",
        "lag": 3.0, "bound": 1.0, "verdict": None})
    got = views._readiness(Hop(tmp_path), {"step": "cutover", "db": "d"})
    assert got["ready"] is False and got["blocks"] == ["the tail is held"]
    assert views._readiness(Hop(tmp_path), {"step": "repair"}) is None
