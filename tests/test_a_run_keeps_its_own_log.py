"""Every run keeps its own log, and the hop's long files stay short
(audit 2026-10-01, LG4, LG5, LG12, LG13, K23).

`reports/<hop>/runs/<run>.jsonl` is a JSON line an event, in UTC, in
migkit's words, with no secret in it, under the run id its evidence is
filed by; the libraries' warnings go there, not to the terminal. The
record, the digests, the choices and `commands.log` are rotated as a run
begins, the record's chain carried across; what a run left is read from
where each file stood as it began, not read whole at the end of every
command; a digest that could not be kept is counted where the evidence
is.
"""
import json
import logging
import sqlite3
import time

import pytest
from click.testing import CliRunner

from migkit.config import Endpoint, Hop

PW = "CHANGE_ME-runlog-pw"


def _hop(tmp_path):
    ep = Endpoint(host="10.0.0.5", port=5432, user="app", password=PW)
    hop = Hop(name="rl", engine="postgres", source=ep, target=ep)
    hop.report_dir = lambda db=None: tmp_path / db if db else tmp_path
    return hop


def _lines(path):
    return [json.loads(x) for x in path.read_text().splitlines()]


def _lite(tmp_path, monkeypatch):
    import migkit.config as cfg
    for name, rows in (("a.db", [(1, "x"), (2, "y")]), ("b.db", [(1, "x")])):
        with sqlite3.connect(tmp_path / name) as con:
            con.execute("create table t (id integer primary key, v text)")
            con.executemany("insert into t values (?, ?)", rows)
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}}}\n"
        "    databases: [main]\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path / "reports" / "lite"


def test_a_command_writes_its_run_log(tmp_path, monkeypatch):
    from migkit import cli
    root = _lite(tmp_path, monkeypatch)
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    assert got.exit_code == 1, got.output
    (log,) = (root / "runs").glob("*.jsonl")
    lines = _lines(log)
    first, last = lines[0], lines[-1]
    assert first["event"] == "begin" and first["command"] == "check"
    assert last["event"] == "end" and last["outcome"] == "done" \
        and last["exit"] == 1, last
    run = log.stem
    assert {x["run"] for x in lines} == {run}
    assert all(x["ts"].endswith("+00:00") for x in lines), lines
    assert set(first) >= {"ts", "run", "hop", "db", "table", "phase",
                          "level", "event", "msg"}
    # the run the evidence is filed under is this one
    assert (root / "evidence" / run / "evidence.json").is_file()
    # a command that stops says so
    got = CliRunner().invoke(cli.main, ["sync", "lite", "--apply"])
    logs = sorted((root / "runs").glob("*.jsonl"),
                  key=lambda p: p.stat().st_mtime)
    end = _lines(logs[-1])[-1]
    assert end["outcome"] == "stopped" and end["level"] == "error", end


def test_what_it_says_holds_no_secret_and_no_program(tmp_path):
    from migkit import runlog, session
    hop = _hop(tmp_path)
    log = runlog.begin(hop, session.begin(), "move")
    try:
        runlog.event("step", f"mydumper said password={PW} for app",
                     db="app")
    finally:
        runlog.end(log, "done")
    text = log.path.read_text()
    assert PW not in text and "mydumper" not in text, text
    step = [x for x in _lines(log.path) if x["event"] == "step"][0]
    assert step["db"] == "app" and "for app" in step["msg"], step


def test_a_librarys_warning_goes_to_the_run_log(tmp_path, capsys):
    """LG12: the libraries' warnings reached the terminal raw, their own
    names first; only three libraries were silenced."""
    from migkit import runlog, session
    hop = _hop(tmp_path)
    log = runlog.begin(hop, session.begin(), "check")
    try:
        logging.getLogger("cassandra.cluster").warning(
            "host 10.0.0.9 is down, retrying")
    finally:
        runlog.end(log, "done")
    said = capsys.readouterr()
    assert "retrying" not in said.err and "cassandra" not in said.err
    got = [x for x in _lines(log.path) if x["event"] == "library"]
    assert any("retrying" in x["msg"] and x["level"] == "warn"
               for x in got), got
    # once the run is over, migkit's handler is gone again
    assert runlog._Library._ONE is None
    assert not any(isinstance(h, runlog._Library)
                   for h in logging.getLogger().handlers)


def test_the_record_is_rotated_with_its_chain_carried(tmp_path,
                                                      monkeypatch):
    from migkit import audit, runlog
    record = tmp_path / "changelog.jsonl"
    for i in range(40):
        audit.append(record, {"op": "repair", "db": "app", "n": i})
    monkeypatch.setattr(runlog, "ROTATE_BYTES", 1024)
    rotated = runlog.housekeep(tmp_path)
    assert len(rotated) == 1 and rotated[0].name.startswith("changelog.")
    first = runlog.last_first(record)
    assert first["op"] == "rotated" and first["entries"] == 40
    audit.append(record, {"op": "repair", "db": "app", "n": 40})
    # each part verifies alone, and the parts verify as one
    assert audit.verify(record)[2] == ""
    n, chained, broken, parts = runlog.verify_parts(record)
    assert (n, chained, broken, parts) == (42, 42, "", 2), \
        (n, chained, broken, parts)
    # a part edited since is caught - the chain can say broken
    part = rotated[0]
    text = part.read_text().replace('"n": 3,', '"n": 33,')
    part.write_text(text)
    assert runlog.verify_parts(record)[2], "an edited part read whole"


def test_old_parts_by_age_and_every_part_read(tmp_path, monkeypatch):
    from migkit import digests, runlog
    old = {"session": "r0", "at": "2025-01-01 00:00:00", "db": "app",
           "table": "t", "side": "src", "rows": 1, "digest": "1",
           "method": "own"}
    (tmp_path / "digests.jsonl").write_text(json.dumps(old) + "\n")
    (rotated,) = runlog.housekeep(tmp_path)
    assert not (tmp_path / "digests.jsonl").exists()
    with open(tmp_path / "digests.jsonl", "a") as f:
        f.write(json.dumps({**old, "side": "dst",
                            "at": time.strftime("%F %T")}) + "\n")
    assert runlog.parts(tmp_path / "digests.jsonl") == [
        rotated, tmp_path / "digests.jsonl"]
    got = digests.read_in(tmp_path)[("app", "t", "")]
    assert set(got) == {"src", "dst"}, got
    # a fresh file is left alone
    assert runlog.housekeep(tmp_path) == []


def test_the_run_logs_are_kept_to_the_newest(tmp_path, monkeypatch):
    import os
    from migkit import runlog
    runs = tmp_path / runlog.DIR
    runs.mkdir()
    for i in range(7):
        p = runs / f"r{i}.jsonl"
        p.write_text("{}\n")
        os.utime(p, (1000 + i, 1000 + i))
    monkeypatch.setattr(runlog, "KEEP_RUNS", 3)
    runlog.housekeep(tmp_path)
    assert sorted(p.name for p in runs.iterdir()) == [
        "r4.jsonl", "r5.jsonl", "r6.jsonl"]


def test_what_a_run_left_is_read_from_where_it_began(tmp_path,
                                                     monkeypatch):
    """K23: the export at the end of every command read the whole record
    and the whole choices file - each per-minute "not due" tick of a
    schedule included. Now the run reads what it wrote."""
    from migkit import audit, export, runlog, session
    hop = _hop(tmp_path)
    for i in range(200):
        audit.append(tmp_path / "changelog.jsonl",
                     {"op": "repair", "session": "older", "n": i})
    run = session.begin()
    log = runlog.begin(hop, run, "check")
    try:
        monkeypatch.setattr(export, "_lines", lambda p: pytest.fail(
            f"{p} read whole"))
        assert export.left_anything(hop, run) is False
        audit.append(tmp_path / "changelog.jsonl", {"op": "evidence"})
        assert export.left_anything(hop, run) is False
        audit.append(tmp_path / "changelog.jsonl", {"op": "repair"})
        assert export.left_anything(hop, run) is True
        got = runlog.appended(hop, run, "changelog.jsonl")
        assert [e["op"] for e in got] == ["evidence", "repair"], got
    finally:
        runlog.end(log, "done")
    # a run this process did not begin is read the whole way
    assert runlog.appended(hop, "someone-elses", "changelog.jsonl") is None


def test_a_digest_that_could_not_be_kept_is_counted(tmp_path):
    """LG4: a digest that could not be written was swallowed, and the
    evidence listed fewer tables with nothing said."""
    from migkit import digests, export, runlog, session
    hop = _hop(tmp_path)
    run = session.begin()
    log = runlog.begin(hop, run, "check")
    blocked = tmp_path / "file"
    blocked.write_text("")
    real = hop.report_dir
    try:
        hop.report_dir = lambda db=None: blocked
        digests.record(hop, "app", "t", "src", 3, "42", method="own")
        hop.report_dir = real
        assert digests.unwritten(run) == 1
        from migkit import verdict
        assert verdict.summarize(hop, [{"check": "data", "scope": "app.t",
                                        "status": "ok"}])[
            "digests_unwritten"] == 1
        doc = export.gather(hop, run)
        assert doc["digests_unwritten"] == 1
        assert "could not be kept" in export.markdown(doc)
        assert export.left_anything(hop, run) is True
    finally:
        hop.report_dir = real
        runlog.end(log, "done")
    said = [x for x in _lines(log.path) if x["event"] == "digest-unwritten"]
    assert said and said[0]["level"] == "warn" and said[0]["table"] == "t"


def test_the_bundle_carries_the_run_logs_and_what_was_run(tmp_path,
                                                          monkeypatch):
    """LG5: the diagnostics bundle left out `commands.log`, "what someone
    attaches to a bug report", and the ways chosen."""
    import zipfile

    from migkit import cli
    root = _lite(tmp_path, monkeypatch)
    assert CliRunner().invoke(cli.main, ["check", "lite"]).exit_code == 1
    # a password written out in the configuration, found by value
    conf = tmp_path / "hops.yaml"
    conf.write_text(conf.read_text().replace(
        f"source: {{host: {tmp_path / 'a.db'}}}",
        f"source: {{host: {tmp_path / 'a.db'}, password: {PW}}}"))
    assert PW in conf.read_text()
    (root / "main").mkdir(exist_ok=True)
    (root / "main" / "commands.log").write_text(
        f"2026-10-01T00:00:00 $ program --token {PW} --dsn"
        f" postgres://app:{PW}@10.0.0.5/app\n")
    (root / "decisions.jsonl").write_text(json.dumps(
        {"session": "s", "work": "t", "way": "w",
         "reason": f"refused with {PW}"}) + "\n")
    bundle = tmp_path / "diag.zip"
    monkeypatch.setenv("MIGKIT_DIAGNOSE", str(bundle))
    got = CliRunner().invoke(cli.main, ["doctor"])
    assert got.exit_code == 0, got.output
    with zipfile.ZipFile(bundle) as z:
        names = z.namelist()
        text = "".join(z.read(n).decode() for n in names)
    assert "lite/main/commands.log" in names, names
    assert "lite/decisions.jsonl" in names, names
    assert any(n.startswith("lite/runs/") for n in names), names
    assert PW not in text, "a secret reached the bundle"
    assert "10.0.0.5/app" in text and "refused with" in text
