"""A run started from the web view or the API is the command line's own
command, run whole in a process of its own - no second logic (backlog
W2).

Held: what a person may start is a fixed list, each value checked before
it becomes an argument (an option smuggled in as a value, a shell's
words, an option the command does not take - refused); one run that
writes on a hop at a time; the run carries who started it and its id, so
the hop's record names them and its evidence is found; it is stopped as
ctrl-c stops one; and - end to end, a real process - the evidence export
started through the API is `migkit report` itself: its bundle written,
anchored in the hop's record under the run's id and the person's name.
"""
import json
import sys
import time

import pytest

from migkit.web import runs
from migkit.web.identity import Principal
from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import browser, call, get, signed_in

ANN = Principal(id="ann@example.com", issuer="oidc:stand-in", via="oidc")


@pytest.mark.parametrize("action,params,argv", [
    ("plan", {}, ["move", "h", "--mode", "full"]),
    ("plan", {"mode": "full+cdc", "db": "app"},
     ["move", "h", "--mode", "full+cdc", "--db", "app"]),
    ("check", {"db": "app", "table": "public.orders", "only": "counts,data",
               "deep": True},
     ["check", "h", "--db", "app", "--table", "public.orders", "--only",
      "counts,data", "--deep"]),
    ("move", {"mode": "cdc"}, ["move", "h", "--mode", "cdc", "--go"]),
    ("sync", {"apply": True}, ["sync", "h", "--apply"]),
    ("sync", {"go": True, "tag": "pre-cutover"},
     ["sync", "h", "--tag", "pre-cutover", "--go"]),
    ("sync", {}, ["sync", "h"]),
    ("cutover", {}, ["move", "h", "--mode", "cdc", "--drop"]),
    ("cutover", {"go": True, "db": "app"},
     ["move", "h", "--mode", "cdc", "--drop", "--db", "app", "--go"]),
    ("rollback", {"db": "app", "apply": True},
     ["rollback", "h", "--db", "app", "--apply"]),
    ("report", {}, ["report", "h"]),
    ("assess", {}, ["assess", "h"]),
])
def test_each_run_is_a_command_line(action, params, argv):
    got, _ = runs.ACTIONS[action].argv("h", params)
    assert got == argv


@pytest.mark.parametrize("action,params,writes", [
    ("plan", {}, False), ("check", {}, False), ("report", {}, False),
    ("move", {}, True), ("sync", {}, False), ("sync", {"apply": True}, True),
    ("sync", {"go": True}, True), ("cutover", {}, False),
    ("cutover", {"go": True}, True), ("rollback", {"db": "a"}, False),
    ("rollback", {"db": "a", "apply": True}, True),
])
def test_what_writes_is_known(action, params, writes):
    spec = runs.ACTIONS[action]
    _, got = spec.argv("h", params)
    assert spec.writes(got) is writes


@pytest.mark.parametrize("action,params,words", [
    ("check", {"db": "--go"}, "not a name"),
    ("check", {"db": "app; rm -rf /"}, "not a name"),
    ("check", {"db": "a b"}, "not a name"),
    ("check", {"table": "$(id)"}, "not a name"),
    ("check", {"only": "counts --deep"}, "list of checks"),
    ("check", {"mode": "full"}, "takes no mode"),
    ("plan", {"mode": "everything"}, "one of full"),
    ("move", {"go": "yes"}, "takes no go"),
    ("sync", {"apply": "true"}, "true or false"),
    ("rollback", {}, "needs db"),
    ("check", {"serve": True}, "takes no serve"),
])
def test_a_value_that_is_not_what_the_option_takes_is_refused(action,
                                                             params, words):
    with pytest.raises(runs.Refused, match=words):
        runs.ACTIONS[action].argv("h", params)


class _Started:
    """Popen, standing in: what would have been started, and how."""

    def __init__(self):
        self.calls = []

    def __call__(self, args, **kw):
        self.calls.append((args, kw))

        class P:
            pid = 999_999

            def wait(self):
                return 0
        return P()


def test_a_run_starts_the_runner_with_who_and_which_run(web, monkeypatch):
    from migkit.web import views
    started = _Started()
    monkeypatch.setattr(runs.subprocess, "Popen", started)
    monkeypatch.setenv("MIGKIT_EXPLAIN", "1")
    hop, _ = views.one(ANN, "h")
    rec = runs.start(hop, "check", {"db": "app"}, ANN, "api")
    (args, kw), = started.calls
    path = hop.report_dir() / runs.DIR / f"{rec['id']}.json"
    assert args == [sys.executable, "-m", "migkit.web.runner", str(path)]
    assert kw["env"]["MIGKIT_ACTOR"] == "ann@example.com"
    assert kw["env"]["MIGKIT_RUN_ID"] == rec["id"] == rec["run"]
    # the decision record stays in the record: a run's output shows it
    # only where its reader asked, which the page does
    assert "MIGKIT_EXPLAIN" not in kw["env"]
    assert kw["start_new_session"] is True
    kept = json.loads(path.read_text())
    assert kept["argv"] == ["check", "h", "--db", "app"]
    assert kept["by"] == "ann@example.com" and kept["writes"] is False
    record = [json.loads(line) for line in (
        hop.report_dir() / "changelog.jsonl").read_text().splitlines()]
    assert record[-1]["op"] == "view-run"
    assert record[-1]["argv"] == ["migkit", "check", "h", "--db", "app"]
    assert record[-1]["by"] == "ann@example.com"
    assert record[-1]["through"] == "api"


def test_one_run_that_writes_at_a_time(web, monkeypatch):
    from migkit.web import views
    started = _Started()
    monkeypatch.setattr(runs.subprocess, "Popen", started)
    hop, _ = views.one(ANN, "h")
    first = runs.start(hop, "move", {"mode": "full"}, ANN, "web")
    # the runner says it started, as it does: its own pid, this machine's
    path = hop.report_dir() / runs.DIR / f"{first['id']}.json"
    rec = json.loads(path.read_text())
    import os
    import socket
    rec.update(state="running", pid=os.getpid(), host=socket.gethostname())
    path.write_text(json.dumps(rec))
    with pytest.raises(runs.Refused, match="a run that writes") as got:
        runs.start(hop, "sync", {"apply": True}, ANN, "web")
    assert got.value.status == 409
    # reading is not writing: a check goes on beside it
    runs.start(hop, "check", {}, ANN, "web")
    # once it has ended, the next write may start
    rec.update(state="ended", exit=0)
    path.write_text(json.dumps(rec))
    runs.start(hop, "sync", {"apply": True}, ANN, "web")
    assert len(started.calls) == 3


def test_a_run_whose_process_is_gone_says_so(web, monkeypatch):
    from migkit.web import views
    monkeypatch.setattr(runs.subprocess, "Popen", _Started())
    hop, _ = views.one(ANN, "h")
    rid = runs.start(hop, "check", {}, ANN, "web")["id"]
    path = hop.report_dir() / runs.DIR / f"{rid}.json"
    rec = json.loads(path.read_text())
    import socket
    rec.update(state="running", pid=2 ** 22 + 12345,
               host=socket.gethostname())
    path.write_text(json.dumps(rec))
    assert runs.read(hop, rid)["state"] == "lost"
    rec.update(pid=None, state="queued", asked=time.time() - 3600)
    path.write_text(json.dumps(rec))
    assert runs.read(hop, rid)["state"] == "lost"
    rec.update(asked=time.time())
    path.write_text(json.dumps(rec))
    assert runs.read(hop, rid)["state"] == "queued"


def test_the_runner_runs_the_command_line_and_keeps_its_end(tmp_path,
                                                            monkeypatch):
    import signal
    import types

    from migkit import cli
    from migkit.verdict import EXIT, Refused, Usage
    from migkit.web import runner
    # the runner takes SIGTERM as ctrl-c; this process's own is left alone
    seen_sig = []
    monkeypatch.setattr(runner, "signal", types.SimpleNamespace(
        SIGTERM=signal.SIGTERM,
        signal=lambda sig, handler: seen_sig.append((sig, handler))))
    seen = []

    class Main:
        def main(self, args, prog_name, standalone_mode):
            seen.append((args, prog_name, standalone_mode))
            if isinstance(end, BaseException):
                raise end
            raise SystemExit(end)
    monkeypatch.setattr(cli, "main", Main())
    path = tmp_path / "run.json"
    for end, want in ((0, 0), (1, 1), (3, 3), (None, 0),
                      ("the step stopped part way", EXIT["failed"]),
                      (Refused("held by a guard"), EXIT["refused"]),
                      (Usage("flags that do not go together"),
                       EXIT["usage"]),
                      (KeyboardInterrupt(), EXIT["interrupted"])):
        path.write_text(json.dumps({"argv": ["check", "h"], "state":
                                    "queued"}))
        assert runner.main(["runner", str(path)]) == want, end
        rec = json.loads(path.read_text())
        assert rec["state"] == "ended" and rec["exit"] == want
        assert rec["pid"] and rec["started"] <= rec["ended"]
    assert seen[0] == (["check", "h"], "migkit", True)
    # stopped from the view, the command sees what ctrl-c raises
    sig, handler = seen_sig[0]
    assert sig == signal.SIGTERM
    with pytest.raises(KeyboardInterrupt):
        handler(sig, None)


def test_a_run_stopped_from_the_view_is_stopped_as_ctrl_c(web, monkeypatch):
    from migkit.web import views
    killed = []
    monkeypatch.setattr(runs.subprocess, "Popen", _Started())
    monkeypatch.setattr(runs.os, "killpg", lambda pid, sig: killed.append(
        (pid, sig)))
    hop, _ = views.one(ANN, "h")
    rid = runs.start(hop, "check", {}, ANN, "web")["id"]
    path = hop.report_dir() / runs.DIR / f"{rid}.json"
    rec = json.loads(path.read_text())
    import os
    import signal
    import socket
    rec.update(state="running", pid=os.getpid(), host=socket.gethostname())
    path.write_text(json.dumps(rec))
    got = runs.cancel(hop, rid, ANN, "web")
    assert killed == [(os.getpid(), signal.SIGTERM)]
    assert got["cancel"]["by"] == "ann@example.com"
    rec.update(state="ended", exit=130)
    path.write_text(json.dumps(rec))
    with pytest.raises(runs.Refused, match="ended already"):
        runs.cancel(hop, rid, ANN, "web")


def test_a_runs_output_hides_the_hops_secrets_and_names_no_program(
        web, monkeypatch):
    from migkit.web import views
    monkeypatch.setattr(runs.subprocess, "Popen", _Started())
    hop, _ = views.one(ANN, "h")
    rid = runs.start(hop, "check", {}, ANN, "web")["id"]
    log = hop.report_dir() / runs.DIR / f"{rid}.log"
    log.write_text("connecting as app:s3cret-pass-word@10.0.0.1\n"
                   "pgcopydb: copied 10 tables\nsame\n")
    text, end = runs.output(hop, rid)
    assert "s3cret-pass-word" not in text and "pgcopydb" not in text
    assert "copied 10 tables" in text and "same" in text
    assert end == log.stat().st_size
    # read on from where it left off
    with open(log, "a") as f:
        f.write("more\n")
    text, _ = runs.output(hop, rid, end)
    assert text == "more\n"


def _until_ended(c, rid, most=180):
    deadline = time.time() + most
    while time.time() < deadline:
        got = get(c, f"/api/v1/hops/h/runs/{rid}").get_json()
        if got["state"] not in ("queued", "running"):
            return got
        time.sleep(0.5)
    raise AssertionError(f"run {rid} did not end in {most} s")


def test_the_evidence_export_started_through_the_api_is_migkit_report(web):
    """End to end, a real process: `report` started through the API is
    the command line's own, run as the person who started it."""
    c = browser(web)
    csrf = signed_in(web, c, {"id": "ann@example.com", "via": "oidc",
                              "issuer": "oidc:stand-in"})
    r = call(c, "POST", "/api/v1/hops/h/runs", csrf, {"action": "report"})
    assert r.status_code == 202, r.data
    rid = r.get_json()["id"]
    got = _until_ended(c, rid)
    out = get(c, f"/api/v1/hops/h/runs/{rid}/output").get_json()
    assert got["state"] == "ended" and got["exit"] == 0, out["text"]
    assert "evidence:" in out["text"]
    from migkit.config import REPORTS
    root = REPORTS / "h"
    assert (root / "evidence" / "evidence.json").exists()
    record = [json.loads(line) for line in
              (root / "changelog.jsonl").read_text().splitlines()]
    anchors = [e for e in record if e.get("op") == "evidence"]
    assert anchors and anchors[-1]["by"] == "ann@example.com"
    assert anchors[-1]["session"] == rid
    # the run's page shows it as it ended
    page = get(c, f"/hops/h/runs/{rid}").data.decode()
    assert "ended" in page and "migkit report h" in page
    # and the evidence it wrote is listed for the hop
    listed = get(c, "/api/v1/hops/h/evidence").get_json()
    assert any(b["run"] == "" and "evidence.json" in b["files"]
               for b in listed["bundles"])
