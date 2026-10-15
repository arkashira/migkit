"""A target is kept from the application or every write migkit makes is
named (BAR-3); doctor says the rung each side stands on and its footprint
(P0-DE.2); the plan's phases are exported as a change sheet whose steps
take the run's evidence back (R25.3). Fake hops and holds, no server."""
import csv
import json
import types

import pytest

from migkit import cli, decide, freeze, report


def _hop(tmp_path, goal=True, protect=True):
    hop = types.SimpleNamespace(
        name="h1", engine="postgres", databases=["app"],
        options={"protect_target": True} if protect else {},
        target=types.SimpleNamespace(user="mover"),
        source=types.SimpleNamespace(user="reader"))

    def report_dir(db=""):
        d = tmp_path / (db or "_")
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    if goal:
        hop._from_goal = ("protect_target",)
    return hop


class _Hold(freeze.Hold):
    TARGET = ""
    count = 10
    released = False

    def writes(self, db):
        return {"t1": self.count, "t2": 0}

    def release(self, db, record, say):
        _Hold.released = True


def _unkept(monkeypatch, hold):
    monkeypatch.setattr(freeze, "hold_for", lambda *a, **k: hold)
    monkeypatch.setattr(freeze, "keeping", lambda *a, **k: decide.Choice(
        work="w", rung=None, reason="no account of migkit's can be kept"))


def test_a_target_nothing_keeps_has_every_write_named(tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    h = _Hold(hop, None, "dst")
    _unkept(monkeypatch, h)
    said = []
    record = freeze.protect(hop, None, "app", said.append)
    assert record["way"] == freeze.UNNAMED and record["writes0"] == 10
    assert "named" in said[0]
    assert freeze.name_write(hop, "app", "t1", "copy", "u1", rows=3)
    assert freeze.name_write(hop, "app", "t1", "copy", "u2", rows=2)
    got = freeze.attributed(hop, object(), "app")
    assert got["kept"] is False and got["findings"] == []
    assert got["named"] == [{"table": "t1", "kind": "copy", "rows": 5,
                             "units": ["u1", "u2"]}]
    h.count = 18
    got = freeze.attributed(hop, object(), "app")
    assert got["findings"] == [f"3 writes on the target since"
                               f" {record['since']} that no unit of"
                               " migkit's named"]
    rows = freeze.kept_rows(hop, object(), "app", "fail")
    assert ("fail", "app: the target: " + got["findings"][0]) in rows


def test_listed_writes_are_each_attributed(tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    _unkept(monkeypatch, _Hold(hop, None, "dst"))
    freeze.protect(hop, None, "app", lambda m: None)
    freeze.name_write(hop, "app", "t1", "change", "lane-0")
    seen = [{"table": "t1", "kind": "change", "user": "mover"},
            {"table": "t1", "kind": "change", "user": "mover"},
            {"table": "t2", "kind": "insert", "user": "app_rw"}]
    got = freeze.attributed(hop, None, "app", seen=seen)
    assert got["findings"] == [
        "t1: a change by migkit's account that no unit named",
        "t2: a insert that is not migkit's"]


def test_a_target_kept_by_its_hold_names_nothing(tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    freeze._save(hop, "app", {"db": "app", "way": "pg_database"}, "dst")
    assert not freeze.names_writes(hop, "app")
    assert not freeze.name_write(hop, "app", "t1", "copy", "u1")
    got = freeze.attributed(hop, None, "app")
    assert got["kept"] and "pg_database" in got["said"]
    assert not freeze.name_write(_hop(tmp_path, protect=False), "x", "t",
                                 "copy", "u")


def test_thaw_of_named_target_releases_nothing(tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    _Hold.released = False
    _unkept(monkeypatch, _Hold(hop, None, "dst"))
    freeze.protect(hop, None, "app", lambda m: None)
    assert freeze.thaw(hop, None, "app", lambda m: None)["way"] == \
        freeze.UNNAMED
    assert _Hold.released is False and freeze.state(hop, "app") is None


def test_the_hop_itself_asking_still_stops(tmp_path, monkeypatch):
    hop = _hop(tmp_path, goal=False)
    _unkept(monkeypatch, _Hold(hop, None, "dst"))
    with pytest.raises(SystemExit):
        freeze.protect(hop, None, "app", lambda m: None)


def test_doctor_says_the_rung_why_higher_refused_and_footprint(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    top = decide.Rung("server_ro", "the whole server read-only")
    got = decide.Rung("db_ro", "the database read-only",
                      loads=(("network", 1), ("disk", 2)),
                      leaves="a role taken and given back")
    monkeypatch.setattr(freeze, "choose", lambda *a, **k: decide.Choice(
        work="w", rung=got, reason="cheapest that holds",
        passed=((top, "the account is not a superuser"),)))
    monkeypatch.setattr(freeze, "keeping", lambda *a, **k: decide.Choice(
        work="w", rung=None, reason="nothing keeps it"))
    lines = cli._rungs_said(hop, None, ["src", "dst"])
    assert lines[0] == ("src: its writes are held on db_ro - cheapest that"
                        " holds; above it, server_ro refused: the account is"
                        " not a superuser; passes network x1, disk x2;"
                        " leaves a role taken and given back")
    assert lines[1].startswith("dst: no rung holds its writes")
    assert lines[1].endswith("every write migkit makes is named instead")
    assert cli._rungs_said(hop, None, ["dst"]) == [lines[1]]


def test_change_sheet_ids_are_stable_and_evidence_comes_back(tmp_path):
    hop = _hop(tmp_path)
    phases = [{"name": "copy orders", "p50": 12.0, "p90": 15.0,
               "binds": "network", "truth": "measured"},
              {"name": "index", "p50": None},
              {"name": "copy orders", "p50": 99.0}]
    steps = report.change_sheet(hop, "app", phases)
    assert [s["phase"] for s in steps] == ["copy", "index"]
    assert steps[0]["id"] == report.step_id(hop, "app/copy", "orders")
    assert steps[0]["id"] != report.step_id(hop, "app/copy", "lines")
    assert steps[1]["p50"] is None
    js, cs = report.write_change_sheet(hop, steps)
    report.write_back(hop, steps[0]["id"], "done", 11.25, "rows hashed equal",
                      rows=40)
    again = report.change_sheet(hop, "app", phases)
    report.write_change_sheet(hop, again)
    kept = {s["id"]: s for s in json.loads(js.read_text())}
    assert kept[steps[0]["id"]]["status"] == "done"
    assert kept[steps[0]["id"]]["seconds"] == 11.25
    assert kept[steps[1]["id"]]["status"] == "planned"
    with open(cs) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["proof"] == "rows hashed equal" and rows[0]["p50"] == "12.0"
    assert freeze.named(hop, "app")[0]["unit"] == steps[0]["id"]
    assert freeze.named(hop, "app")[0]["rows"] == 40
    with pytest.raises(SystemExit):
        report.write_back(hop, "mk-000000000000", "done", 1, "")


def test_the_report_shows_unaccounted_writes(tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    _unkept(monkeypatch, _Hold(hop, None, "dst"))
    freeze.protect(hop, None, "app", lambda m: None)
    freeze.name_write(hop, "app", "t1", "copy", "u1")
    monkeypatch.setattr(freeze, "attributed", lambda h, e, d: {
        "kept": False, "said": "1 writes of migkit's named",
        "named": [{"table": "t1", "kind": "copy", "rows": 1,
                   "units": ["u1"]}],
        "findings": ["2 writes no unit named"]})
    html = report._target_writes_section(hop, ["app"])
    assert "target writes" in html and "2 writes no unit named" in html
    assert report._target_writes_section(_hop(tmp_path, protect=False),
                                         ["app"]) == ""
