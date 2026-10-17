"""A hold of writes a run left behind (`leftovers.holds`) is listed by
`migkit doctor` and given back by `migkit leftovers` where the one
decision engine says lift (`decide.lift_hold`): its holder gone. One whose
holder still runs is left, with why, and the lift is exactly the record's
own way (`freeze.thaw_source`, `freeze.thaw`)."""
import os
import subprocess
import time
from types import SimpleNamespace

from click.testing import CliRunner

from migkit import cli, decide, freeze, leftovers


def _dead_pid():
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


def _held(monkeypatch, tmp_path):
    since = time.strftime("%Y-%m-%d %H:%M:%S",
                          time.localtime(time.time() - 3600))
    dead = _dead_pid()
    recs = {("a", "dst"): {"way": "revoke", "since": since, "pid": dead},
            ("b", "dst"): {"way": "revoke", "since": since,
                           "pid": os.getpid()}}
    monkeypatch.setattr(freeze, "server_state", lambda hop, side: {
        "way": "read-only", "since": since, "pid": dead}
        if side == "src" else None)
    monkeypatch.setattr(freeze, "state",
                        lambda hop, d, side: recs.get((d, side)))
    thawed, sources = [], []
    monkeypatch.setattr(freeze, "thaw", lambda hop, eng, db, say,
                        side="dst": thawed.append((hop, eng, db, side)))
    monkeypatch.setattr(freeze, "thaw_source", lambda hop, eng, dbs, say:
                        sources.append((hop, eng, list(dbs))))
    hop = SimpleNamespace(name="t", databases=["a", "b"],
                          report_dir=lambda db=None: tmp_path)
    return hop, thawed, sources


def test_a_hold_whose_holder_is_gone_is_lifted_its_own_way(
        monkeypatch, tmp_path):
    hop, thawed, sources = _held(monkeypatch, tmp_path)
    eng = object()
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)
    r = CliRunner().invoke(cli.main, ["leftovers", "t"])
    assert r.exit_code == 0, r.output
    # exactly what each record took: the source's freeze, and `a` alone
    assert sources == [(hop, eng, [])]
    assert thawed == [(hop, eng, "a", "dst")]
    out = " ".join(r.output.split())
    assert "b on the target" in out and "still runs and may be writing" \
        in out
    assert "2 hold(s) given back, 1 left as they are" in out


def test_the_choice_is_the_decision_engine_s_naming_its_facts(
        monkeypatch, tmp_path):
    hop, _, _ = _held(monkeypatch, tmp_path)
    got = {(h["side"], h["db"]): c for h, c in leftovers.decided(hop)}
    assert got[("dst", "a")].path == "lift"
    assert got[("dst", "b")].path == "wait"
    assert {"holder gone", "side"} <= set(got[("dst", "b")].asked)
    # a record naming no process: lifted only past the run's longest step
    no_pid = {"holder gone": None, "age": 30.0, "side": "dst"}
    assert decide.lift_hold({**no_pid, "longest step": None}).path == "wait"
    assert decide.lift_hold({**no_pid, "longest step": 60.0}).path == "wait"
    assert decide.lift_hold({**no_pid, "longest step": 10.0}).path == "lift"
    assert decide.lift_hold({"holder gone": False, "age": 1e9, "side": "src",
                             "longest step": 1.0}).path == "wait"


def test_list_only_gives_nothing_back_and_doctor_says_each_hold(
        monkeypatch, tmp_path):
    hop, thawed, sources = _held(monkeypatch, tmp_path)
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    r = CliRunner().invoke(cli.main, ["leftovers", "t", "--list"])
    assert r.exit_code == 0, r.output
    assert thawed == [] and sources == []
    lines = cli._holds_said("t", hop)
    assert len(lines) == 3
    assert sum("migkit leftovers t gives it back" in x for x in lines) == 2


def test_a_setting_a_crashed_run_left_is_put_back_by_leftovers(
        monkeypatch, tmp_path):
    import json
    hop = SimpleNamespace(name="t", databases=["a"],
                          report_dir=lambda db=None: tmp_path)
    monkeypatch.setattr(freeze, "server_state", lambda h, s: None)
    monkeypatch.setattr(freeze, "state", lambda h, d, s: None)
    settings_file = tmp_path / "target-settings.json"
    entry = {"name": "max_allowed_packet", "old": 4194304, "new": 18874368,
             "db": "a", "pid": 1234, "at": "2026-10-17 00:00:00"}
    settings_file.write_text(json.dumps([entry]))

    calls = []
    eng = SimpleNamespace(set_target_setting=lambda n, v: calls.append((n, v)))
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)

    r = CliRunner().invoke(cli.main, ["leftovers", "t"])
    assert r.exit_code == 0, r.output
    assert calls == [("max_allowed_packet", 4194304)]
    assert leftovers.settings_left(hop) == []
    assert "put the target's max_allowed_packet back to 4194304" in r.output


def test_a_setting_a_crashed_run_left_is_only_listed_with_list_flag(
        monkeypatch, tmp_path):
    import json
    hop = SimpleNamespace(name="t", databases=["a"],
                          report_dir=lambda db=None: tmp_path)
    monkeypatch.setattr(freeze, "server_state", lambda h, s: None)
    monkeypatch.setattr(freeze, "state", lambda h, d, s: None)
    settings_file = tmp_path / "target-settings.json"
    entry = {"name": "max_allowed_packet", "old": 4194304, "new": 18874368,
             "db": "a", "pid": 1234, "at": "2026-10-17 00:00:00"}
    settings_file.write_text(json.dumps([entry]))

    calls = []
    eng = SimpleNamespace(set_target_setting=lambda n, v: calls.append((n, v)))
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)

    r = CliRunner().invoke(cli.main, ["leftovers", "t", "--list"])
    assert r.exit_code == 0, r.output
    assert calls == []
    assert leftovers.settings_left(hop) == [entry]
    assert "the target's max_allowed_packet is 18874368 (was 4194304)" in r.output
    assert "migkit leftovers t puts it back" in r.output


def test_leftovers_with_db_flag_only_puts_back_matching_db_settings(
        monkeypatch, tmp_path):
    import json
    hop = SimpleNamespace(name="t", databases=["a", "b"],
                          report_dir=lambda db=None: tmp_path)
    monkeypatch.setattr(freeze, "server_state", lambda h, s: None)
    monkeypatch.setattr(freeze, "state", lambda h, d, s: None)
    settings_file = tmp_path / "target-settings.json"
    e_a = {"name": "max_allowed_packet", "old": 4194304, "new": 18874368,
           "db": "a", "pid": 1234, "at": "2026-10-17 00:00:00"}
    e_b = {"name": "innodb_buffer_pool_size", "old": 1048576, "new": 2097152,
           "db": "b", "pid": 1234, "at": "2026-10-17 00:00:00"}
    settings_file.write_text(json.dumps([e_a, e_b]))

    calls = []
    eng = SimpleNamespace(set_target_setting=lambda n, v: calls.append((n, v)))
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)

    r = CliRunner().invoke(cli.main, ["leftovers", "t", "--db", "a"])
    assert r.exit_code == 0, r.output
    assert calls == [("max_allowed_packet", 4194304)]
    remaining = leftovers.settings_left(hop)
    assert len(remaining) == 1 and remaining[0]["name"] == "innodb_buffer_pool_size"


def test_leftovers_when_server_refuses_putting_back_setting(
        monkeypatch, tmp_path):
    import json
    hop = SimpleNamespace(name="t", databases=["a"],
                          report_dir=lambda db=None: tmp_path)
    monkeypatch.setattr(freeze, "server_state", lambda h, s: None)
    monkeypatch.setattr(freeze, "state", lambda h, d, s: None)
    settings_file = tmp_path / "target-settings.json"
    entry = {"name": "max_allowed_packet", "old": 4194304, "new": 18874368,
             "db": "a", "pid": 1234, "at": "2026-10-17 00:00:00"}
    settings_file.write_text(json.dumps([entry]))

    def _fail(n, v):
        raise RuntimeError("variable is read only")

    eng = SimpleNamespace(set_target_setting=_fail)
    monkeypatch.setattr(cli, "get_hop", lambda name: hop)
    monkeypatch.setattr(cli, "get_engine", lambda h: eng)

    r = CliRunner().invoke(cli.main, ["leftovers", "t"])
    assert r.exit_code == 0, r.output
    assert "it could not be put back to 4194304" in r.output
    assert "migkit leftovers tries again" in r.output
    assert leftovers.settings_left(hop) == [entry]
