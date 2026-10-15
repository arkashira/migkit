"""`migkit change-sheet` writes the plan's steps as a sheet and takes a
run's evidence back per step (R25.3); doctor says the connections, memory,
disk spool and server objects of the move per side and leg from the
owners' figures, and never names a wrapped program (P0-DE.2). Fake hops
and ways, no server."""
import csv
import json
import types

from click.testing import CliRunner

from migkit import cli, decide, movers, planner, report, sizing
from migkit import model as resource_model


def _hop(tmp_path, dbs=("app",)):
    hop = types.SimpleNamespace(name="h1", engine="postgres",
                                databases=list(dbs), options={})

    def report_dir(db=""):
        d = tmp_path / (db or "_")
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


class _Eng:
    def databases(self):
        return ["app"]

    def table_facts(self, side, d):
        return {"t1": {"rows": 10, "bytes": 1000}}


def _fake_plan(monkeypatch, hop):
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: _Eng())
    monkeypatch.setattr(cli, "_require_configured", lambda h: None)
    monkeypatch.setattr(cli, "_units", lambda eng, d, tables=None: 3)
    monkeypatch.setattr(resource_model, "for_move",
                        lambda *a, **k: types.SimpleNamespace(
                            worker_memory=1 << 20, carried=1000))
    monkeypatch.setattr(sizing, "estimate", lambda *a, **k: sizing.Plan(
        2, 4, {}))
    monkeypatch.setattr(movers, "for_hop", lambda h, e, *a, **k: ("builtin",
                                                                 ""))
    monkeypatch.setattr(movers, "fitted", lambda h, e, v: (v, ""))
    monkeypatch.setattr(planner, "phases", lambda *a, **k: [
        planner.Phase("copy orders", p50=60.0, p90=90.0, binds="the link",
                      truth="measured"),
        planner.Phase("check orders", p50=None)])


def test_the_command_exports_the_plan_and_takes_evidence_back(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    _fake_plan(monkeypatch, hop)
    run = CliRunner().invoke(cli.main, ["change-sheet", "h1"])
    assert run.exit_code == 0, run.output
    js, cs = report._sheet_paths(hop)
    steps = json.loads(js.read_text())
    assert [s["phase"] for s in steps] == ["copy", "check"]
    assert steps[0]["p50"] == 60.0 and steps[1]["p50"] is None
    assert all(s["status"] == "planned" for s in steps)
    rows = list(csv.DictReader(open(cs)))
    assert [r["id"] for r in rows] == [s["id"] for s in steps]
    sid = steps[0]["id"]
    back = CliRunner().invoke(cli.main, [
        "change-sheet", "h1", "--back", sid, "--status", "failed",
        "--seconds", "12.5", "--proof", "rows hashed equal"])
    assert back.exit_code == 0, back.output
    got = {s["id"]: s for s in report.read_change_sheet(hop)}
    assert got[sid]["status"] == "failed" and got[sid]["seconds"] == 12.5
    assert got[sid]["proof"] == "rows hashed equal"
    assert got[steps[1]["id"]]["status"] == "planned"
    # exporting again keeps what was written back
    CliRunner().invoke(cli.main, ["change-sheet", "h1"])
    again = {s["id"]: s for s in report.read_change_sheet(hop)}
    assert again[sid]["status"] == "failed"


def test_evidence_for_a_step_not_in_the_sheet_is_refused(tmp_path,
                                                         monkeypatch):
    hop = _hop(tmp_path)
    _fake_plan(monkeypatch, hop)
    CliRunner().invoke(cli.main, ["change-sheet", "h1"])
    bad = CliRunner().invoke(cli.main, ["change-sheet", "h1", "--back",
                                        "mk-000000000000"])
    assert bad.exit_code != 0
    assert "no step mk-000000000000" in bad.output


def test_a_rung_names_the_objects_it_leaves_and_where():
    got = report.server_objects(
        "a replication slot and a publication on the source, tables on"
        " the target")
    assert ("replication slot", "src", False) in got
    assert ("publication", "src", False) in got
    assert ("table", "", True) in got
    assert report.server_objects("a directory of dump files on this machine;"
                                 " nothing on the servers") == []
    assert report.server_objects("") is None


def _ways():
    dump = decide.Rung("dumpway", "d", leaves="a directory of dump files on"
                       " this machine; nothing on the servers")
    stream = decide.Rung("streamway", "s", leaves="a replication slot and a"
                         " publication on the source")
    return [("the whole-database move", dump),
            ("following its changes", stream)]


def test_the_numbers_come_per_side_and_leg(tmp_path):
    hop = _hop(tmp_path, ("a", "b", "c"))
    lines = report.footprint_lines(
        hop, hop.databases, sizing.Plan(2, 4, {}),
        {"src": {"free_connections": 100}, "dst": {}}, 1 << 20, 5 << 30,
        _ways(), {"memory": 8 << 30}, 2 << 30)
    text = "\n".join(lines)
    per = sizing.CONNECTIONS_PER_WORKER
    assert any(l.startswith("src: opens %d connections at the start, up to"
                            " %d" % (2 * per, 4 * per))
               and "100 free" in l for l in lines)
    assert any(l.startswith("dst: opens") and "could not be read" in l
               for l in lines)
    assert any(l.startswith("this machine: memory") and "free here" in l
               for l in lines)
    spool = [l for l in lines if "disk spool" in l]
    assert len(spool) == 1 and "NOT ENOUGH" in spool[0]
    assert any(l.startswith("following its changes: creates")
               and "on src:" in l and "replication slot x3" in l
               and "publication x3" in l for l in lines)
    assert any(l == "the whole-database move: creates nothing on the"
               " servers" for l in lines)
    assert "None" not in text


def test_a_way_not_chosen_or_not_said_is_said_so(tmp_path):
    hop = _hop(tmp_path)
    lines = report.footprint_lines(
        hop, hop.databases, sizing.Plan(1, 1, {}), {}, None, None,
        [("the whole-database move", None),
         ("following its changes", decide.Rung("x", "x"))])
    text = "\n".join(lines)
    assert "said when its way is chosen" in text
    assert "is not said" in text
    assert "no disk spool" in text
    assert "1 workers" in text or "1 worker" in text


def test_a_file_way_without_a_known_size_does_not_invent_one(tmp_path):
    hop = _hop(tmp_path)
    lines = report.footprint_lines(
        hop, hop.databases, sizing.Plan(1, 1, {}), {}, None, None, _ways(),
        None, None)
    spool = [l for l in lines if "keeps files here" in l]
    assert spool and "not known yet" in spool[0]


def test_doctor_numbers_need_both_sides_and_never_fail_doctor(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path)
    one = cli._numbers_said(hop, _Eng(), ["src"])
    assert len(one) == 1 and "both servers must answer" in one[0]
    monkeypatch.setattr(sizing, "capacity", lambda e, s, d: (_ for _ in ()
                                                             ).throw(
        RuntimeError("down\nthe server did not answer")))
    got = cli._numbers_said(hop, _Eng(), ["src", "dst"])
    assert got == ["not worked out now - the server did not answer"]


def test_doctor_numbers_say_the_widest_database_and_name_no_program(
        tmp_path, monkeypatch):
    hop = _hop(tmp_path, ("small", "wide"))
    _fake_plan(monkeypatch, hop)
    monkeypatch.setattr(sizing, "capacity", lambda e, s, d: {
        "free_connections": 50})
    monkeypatch.setattr(sizing, "estimate", lambda h, e, d, *a, **k:
                        sizing.Plan(2, 8 if d == "wide" else 3, {}))
    monkeypatch.setattr(report, "ways_of", lambda *a: _ways())
    got = cli._numbers_said(hop, _Eng(), ["src", "dst"])
    per = sizing.CONNECTIONS_PER_WORKER
    assert got[0].startswith(f"src: opens {2 * per} connections at the"
                             f" start, up to {8 * per}")
    assert "50 free" in got[0]
    for line in got:
        for program in movers.DRIVEN:
            assert program not in line.lower()
