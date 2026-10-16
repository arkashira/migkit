"""A run writes each step back to the change sheet as it ends (R25.3), the
audit record is anchored every N entries with N a rung on the entry rate
(R5.anchor), doctor lists the side connections a run holds with their
state (R21.9), and the web view drains a running follow through the
command line's own action (R5.actions)."""
import json
import os
import subprocess
import textwrap
import types
from pathlib import Path

import pytest

from migkit import decide, report
from tests import servers
from tests.conftest import needs_docker, migkit_cmd

PROVES = [("verify.identity.sequence-parity", "postgres")]

ROOT = Path(__file__).resolve().parent.parent
pg = servers.fixture("pg")


class _Hop:
    def __init__(self, tmp_path, name="h"):
        self.name = name
        self._dir = tmp_path

    def report_dir(self, d=""):
        out = self._dir / d if d else self._dir
        out.mkdir(parents=True, exist_ok=True)
        return out


# --- the sheet is opened by the run, and a phase the plan lacks is added ---

def test_a_run_opens_a_step_for_every_phase_it_will_end(tmp_path):
    hop = _Hop(tmp_path)
    ids = report.open_sheet(hop, ["d"], follows=False)
    got = {s["phase"] for s in report.read_change_sheet(hop)}
    assert got == set(report.RUN_PHASES) and len(ids) == len(got)
    # a second open adds nothing, and a following run adds its two
    assert report.open_sheet(hop, ["d"]) == []
    assert len(report.open_sheet(hop, ["d"], follows=True)) == 2
    assert {s["phase"] for s in report.read_change_sheet(hop)} == set(
        report.RUN_PHASES + report.FOLLOW_PHASES)


def test_a_step_the_plan_did_not_hold_is_created_where_asked(tmp_path):
    hop = _Hop(tmp_path)
    assert report.step_ended(hop, "d", "repair", "done", 1.0, "x",
                             create=True) == []      # no sheet kept
    report.open_sheet(hop, ["d"])
    ids = report.step_ended(hop, "d", "repair", "done", 2.0, "applied",
                            create=True)
    assert len(ids) == 1
    step = next(s for s in report.read_change_sheet(hop)
                if s["phase"] == "repair")
    assert (step["status"], step["seconds"], step["proof"]) == \
        ("done", 2.0, "applied")
    # without create, a phase the sheet lacks writes nothing
    assert report.step_ended(hop, "d", "cutover", "done", 1.0, "x") == []


# --- the anchor interval is a rung on the entry rate ------------------------

def test_the_anchor_interval_follows_the_measured_entry_rate():
    slow, _ = decide.anchor_interval(0.5, 0.2, 600)
    fast, why = decide.anchor_interval(50, 0.2, 600)
    assert fast > slow >= 1
    assert fast == round(50 * (2 * 0.2 * 600) ** 0.5)
    assert "50" in why and "0.2" in why
    # a dearer anchor is taken less often
    assert decide.anchor_interval(50, 2.0, 600)[0] > fast
    # nothing measured: every entry, and it says why
    n, said = decide.anchor_interval(None, None, None)
    assert n == 1 and "not been measured" in said


def test_the_record_is_anchored_when_the_interval_is_reached(
        tmp_path, monkeypatch):
    from migkit import anchor
    hop = _Hop(tmp_path)
    (tmp_path / "changelog.jsonl").write_text("".join(
        json.dumps({"at": f"2026-10-16 10:00:{i:02d}"}) + "\n"
        for i in range(40)))
    monkeypatch.setattr(anchor, "settings", lambda h: {"bucket": "b"})
    seen = []
    monkeypatch.setattr(anchor, "checkpoint",
                        lambda h, say=None, s3=None: seen.append(1) or {})
    every, _ = anchor.interval(hop)
    assert every == 1          # no anchor put measured yet
    assert anchor.keep_up(hop) == {} and seen
    # with a measured put the interval is a rung on the rate, not 1
    (tmp_path / anchor.STATE).write_text(json.dumps(
        {"put_seconds": 4.0, "anchored": {"seq": 40}}))
    every, _ = anchor.interval(hop)
    assert every > 1
    seen.clear()
    assert anchor.keep_up(hop) is None and not seen    # none due yet


# --- doctor lists the side connections with their state ---------------------

def test_doctor_lists_each_side_connection_with_its_state(
        tmp_path, monkeypatch):
    from migkit import cli, second_reader, tailctl
    hop = _Hop(tmp_path)
    monkeypatch.setattr(second_reader, "interpreter", lambda: None)
    lines = cli._connections_said(hop, ["d"])
    assert any("change stream: not running" in x for x in lines)
    assert any("fence read back from the source: no hold" in x
               for x in lines)
    assert any("second reader: not installed" in x for x in lines)
    monkeypatch.setattr(tailctl, "state", lambda where: {
        "running": True, "paused": False, "stopped": None, "behind": 2.5,
        "changes": 120, "beat_age": 0.4})
    lines = cli._connections_said(hop, ["d"])
    assert any("change stream: running, 2.5s behind" in x
               and "120 changes" in x for x in lines)
    monkeypatch.setattr(tailctl, "state", lambda where: {
        "running": False, "paused": False, "stopped": "lost the source",
        "behind": None, "changes": None, "beat_age": None})
    assert any("STOPPED" in x and "lost the source" in x
               for x in cli._connections_said(hop, ["d"]))


def test_a_server_verdict_carries_the_numbers_it_stands_on():
    from migkit import cli
    assert cli._capacity_said({"free_connections": 90, "running": 3,
                               "cpus": 8}) == \
        "; 90 connections free, 3 running, 8 CPUs"
    assert cli._capacity_said({}) == ""


# --- the web drains a running follow, and only one ---------------------------

def test_the_drain_route_runs_the_drain_action_on_a_running_follow(
        tmp_path, monkeypatch):
    from migkit import tailctl
    from migkit.web import api, views
    monkeypatch.setattr(views, "databases", lambda hop: ["d"])
    started = []
    monkeypatch.setattr(api, "_start", lambda ctx, a, p: started.append(
        (a, p)) or (202, {"id": "r1"}))
    ctx = types.SimpleNamespace(hop=_Hop(tmp_path),
                                body={"to": "now", "go": True})
    monkeypatch.setattr(tailctl, "alive", lambda where: False)
    with pytest.raises(api.Answer) as e:
        api.drain(ctx, "d")
    assert e.value.status == 409 and not started
    monkeypatch.setattr(tailctl, "alive", lambda where: True)
    assert api.drain(ctx, "d")[0] == 202
    assert started == [("drain", {"db": "d", "to": "now", "go": True})]
    assert any(r.handler == "drain" for r in api.ROUTES)


def test_the_view_offers_drain_only_beside_a_running_tail():
    from pathlib import Path
    html = (Path(__file__).resolve().parent.parent / "migkit" / "web"
            / "templates" / "hop.html").read_text()
    at = html.index("/drain")
    assert html.rfind("d.tail.running", 0, at) > html.rfind("<td>", 0, at)


# --- a move with a check: every phase written as it ended (pg lane) ---------

@needs_docker
def test_a_move_and_check_write_every_phase_back_as_it_ends(pg, tmp_path):
    db = pg.name
    for s in (pg.src, pg.dst):
        s.sql("create table public.t (id int primary key, v text)", db=db)
    pg.src.sql("insert into public.t select g, 'v' || g"
               " from generate_series(1, 2000) g", db=db)
    conf = tmp_path / "hops.yaml"
    conf.write_text(textwrap.dedent(f"""
        hops:
          t:
            engine: postgres
            source: {{host: 127.0.0.1, port: {pg.src.port}, user: postgres, password: test}}
            target: {{host: 127.0.0.1, port: {pg.dst.port}, user: postgres, password: test}}
            databases: [{db}]
    """))
    env = dict(os.environ, MIGKIT_CONF=str(conf),
               MIGKIT_REPORTS=str(tmp_path / "reports"),
               PYTHONPATH=str(ROOT))

    def run(*args):
        return subprocess.run(migkit_cmd() + list(args), env=env,
                              capture_output=True, text=True, timeout=500)
    moved = run("move", "t", "--mode", "full", "--go")
    assert moved.returncode == 0, moved.stdout + moved.stderr
    found = list((tmp_path / "reports").rglob("change-sheet.json"))
    assert found, "\n".join(l for l in moved.stdout.splitlines() if "sheet" in l) + str(sorted(p.name for p in (tmp_path / "reports").rglob("*"))[:30])
    sheet = found[0]
    mid = {s["phase"]: s for s in json.loads(sheet.read_text())}
    # the move's own phases are written when the move ends, the check's not
    # the bulk way ran here: its schema and index builds are inside the
    # program, with no boundary of their own (left: only the table copier
    # writes those two back)
    for phase in ("copy",):
        assert mid[phase]["status"] == "done", mid[phase]
        assert mid[phase]["seconds"] is not None and mid[phase]["proof"]
    assert mid["check"]["status"] == "planned"
    checked = run("check", "t")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    end = {s["phase"]: s for s in json.loads(sheet.read_text())}
    # written by the check as it ended, whatever it found (on this lane the
    # sheet says "failed": see the report's left list)
    assert end["check"]["status"] != "planned" and end["check"]["proof"]
    assert end["check"]["seconds"] is not None
    stamps = [end[p]["at"] for p in ("copy", "check")]
    assert stamps == sorted(stamps)
