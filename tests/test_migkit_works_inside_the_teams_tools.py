"""migkit inside the team's tools (backlog R23.8).

What the tools inside migkit measure comes out through migkit's own
surface, under its names: the copy's progress and rates, each check's
time, the verify's progress, every table on both sides, the cutover's
outage, the change stream reader's own readings - Prometheus metrics
with the alert rules and a Grafana dashboard shipped over exactly those,
and a run as OpenTelemetry spans. And migkit runs where the team's work
runs: an Airflow task, a Dagster op, a GitHub Actions step, an Argo
workflow step - each a thin wrapper over the command a person types.
"""
import json
import logging
import re
import sqlite3
import sys
import threading
import time
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from migkit.config import Endpoint, Hop
from tests.test_the_report_does_not_name_its_tools import TOOLS

ROOT = Path(__file__).resolve().parents[1]
LBL = 'hop="h",engine="postgres"'


def _hop():
    return Hop(name="h", engine="postgres",
               source=Endpoint(host="x", port=0, user="", password=""),
               target=Endpoint(host="x", port=0, user="", password=""))


def every_reading(tmp_path, monkeypatch):
    """The metrics of a hop with every reading there is: a tail stopped
    and running with every margin, a copy under way, the rates of a copy
    by a way the code labels by a program's name, a check's times, a
    verify half done, two sides' digests of a table, a cutover run and
    one under way, and a change stream with its parts and its reader's
    readings."""
    import migkit.config as cfg
    from migkit import movers, tailctl, ui
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    hop = _hop()
    root = tmp_path / "h"
    where = root / "appdb"
    where.mkdir(parents=True)
    (root / "summary.json").write_text(json.dumps([
        {"check": "data", "scope": "appdb", "status": "ok"}]))
    # the change leg run from here, as its own sentinel reads it
    (where / "follow").mkdir()
    monkeypatch.setattr(movers, "follow_lag", lambda hop, db: {
        "apply": True, "held": 10, "behind": 20})
    (where / "move.json").write_text(json.dumps({
        "appdb.t": {"moved": 1200, "done": True,
                    "last": ["ann@example.com"]},
        "appdb.u": {"moved": 5}}))
    (root / "throughput.json").write_text(json.dumps({"pgcopydb": [
        {"rows": 1000, "seconds": 4.0, "at": "2026-09-30T10:00:00"}]}))
    (root / "check-timings.json").write_text(json.dumps({
        "session": "s", "at": "2026-09-30 10:00:00",
        "databases": {"appdb": {"data": 1.5, "schema": 0.2}}}))
    (where / "checkpoint.json").write_text(json.dumps({
        "format_version": 1, "tables": {"public.big": {
            "fingerprint": "f",
            "done": {"1..100": [100, "9"], "101..200": [100, "7"]}}}}))
    (root / "digests.jsonl").write_text("".join(json.dumps({
        "session": "s", "db": "appdb", "table": "public.t", "side": side,
        "rows": rows, "digest": digest, "method": "own"}) + "\n"
        for side, rows, digest in (("src", 3, "42"), ("dst", 2, "41"))))
    (root / "cutover-runs.jsonl").write_text(json.dumps({
        "id": "c1", "state": "flipped", "outage": 1.5,
        "took": {"freeze": 0.4, "flip": 0.2}}) + "\n")
    (root / "cutover.json").write_text(json.dumps({"state": "running"}))
    (root / "stream").mkdir()
    (root / "stream" / "docker-compose.yml").write_text("services: {}\n")
    monkeypatch.setattr(movers, "stream_parts",
                        lambda name, port=8083, timeout=2:
                        [("source", True), ("sink", False)])
    monkeypatch.setattr(movers, "stream_readings",
                        lambda attrs, port, timeout=2, server=None: {
                            "MilliSecondsBehindSource": 2500,
                            "TotalNumberOfEventsSeen": 7,
                            "QueueRemainingCapacity": 8000,
                            "Connected": 1, "RemainingTableCount": 0})
    with pytest.raises(RuntimeError):
        with tailctl.Running(where):
            raise RuntimeError("x")
    stopped = ui.prometheus({"h": hop})
    with tailctl.Running(where):
        # every reading an engine gives, though no one engine gives all
        tailctl.beat(where, time.time(), 1,
                     {"seconds": 1, "bytes": 1, "held_bytes": 1})
        running = ui.prometheus({"h": hop})
    return stopped + running


def test_what_the_tools_inside_measure_comes_out_under_migkits_names(
        tmp_path, monkeypatch):
    text = every_reading(tmp_path, monkeypatch)
    db = f'{LBL},db="appdb"'
    for line in (
            f'migkit_move_rows{{{db},table="appdb.t"}} 1200',
            f'migkit_move_rows{{{db},table="appdb.u"}} 5',
            f"migkit_move_tables{{{db}}} 2",
            f"migkit_move_tables_done{{{db}}} 1",
            f'migkit_copy_rows_per_second{{{LBL},way="the streaming copy"}}'
            " 250.0",
            f'migkit_copy_seconds{{{LBL},way="the streaming copy"}} 4.0',
            f'migkit_check_seconds{{{db},check="data"}} 1.5',
            f'migkit_verify_ranges_done{{{db},table="public.big"}} 2',
            f'migkit_verify_rows_done{{{db},table="public.big"}} 200',
            f'migkit_table_rows{{{db},table="public.t",side="src"}} 3',
            f'migkit_table_rows{{{db},table="public.t",side="dst"}} 2',
            f'migkit_table_same{{{db},table="public.t"}} 0',
            f"migkit_cutover_outage_seconds{{{LBL}}} 1.5",
            f'migkit_cutover_phase_seconds{{{LBL},phase="freeze"}} 0.4',
            f"migkit_cutover_running{{{LBL}}} 1",
            f'migkit_stream_running{{{LBL},part="source"}} 1',
            f'migkit_stream_running{{{LBL},part="sink"}} 0',
            f"migkit_stream_behind_source_seconds{{{LBL}}} 2.5",
            f"migkit_stream_events_seen{{{LBL}}} 7",
            f"migkit_stream_events_seen_total{{{LBL}}} 7",
            f'migkit_verdict{{{LBL},status="none"}} 1'):
        assert line in text, (line, text)
    # where a copy stands in a table is a key - data - and never goes out
    assert "ann@example.com" not in text
    assert not [t for t in TOOLS if t in text.lower()], text
    # every metric carried out is described once, a count as a counter
    for name in set(re.findall(r"^(migkit_[a-z_]+)\{", text, re.M)):
        kind = "counter" if name.endswith("_total") else "gauge"
        assert f"# HELP {name} " in text and \
            f"# TYPE {name} {kind}\n" in text, name


def test_the_stream_readers_own_readings_are_read(tmp_path, monkeypatch):
    """The pipeline's API for its parts, and its reader's metrics over
    HTTP, stood in for by one local server."""
    import migkit.config as cfg
    from migkit import ui
    asked = []
    beans = {
        "debezium.postgres:type=connector-metrics,context=streaming,"
        "server=migkit-h": {"MilliSecondsBehindSource": 1500,
                            "TotalNumberOfEventsSeen": 12,
                            "QueueRemainingCapacity": 8192,
                            "Connected": True},
        "debezium.postgres:type=connector-metrics,context=snapshot,"
        "server=migkit-h": {"RemainingTableCount": 2,
                            "TotalNumberOfEventsSeen": 3,
                            "MilliSecondsBehindSource": -1}}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            asked.append(self.path)
            if self.path.startswith("/connectors/migkit-h-source/"):
                body = {"connector": {"state": "RUNNING"},
                        "tasks": [{"state": "RUNNING"}]}
            elif self.path.startswith("/connectors/migkit-h-sink/"):
                body = {"connector": {"state": "RUNNING"},
                        "tasks": [{"state": "FAILED"}]}
            elif self.path.startswith("/jolokia/read/"):
                body = {"value": beans, "status": 200}
            else:
                self.send_response(404)
                self.end_headers()
                return
            out = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        monkeypatch.setattr(cfg, "REPORTS", tmp_path)
        monkeypatch.setattr(ui, "REPORTS", tmp_path)
        monkeypatch.setattr(ui, "STREAM_API", srv.server_port)
        monkeypatch.setattr(ui, "STREAM_READINGS", srv.server_port)
        (tmp_path / "h" / "stream").mkdir(parents=True)
        (tmp_path / "h" / "stream" / "docker-compose.yml").write_text("")
        text = ui.prometheus({"h": _hop()})
    finally:
        srv.shutdown()
        srv.server_close()
    for line in (f'migkit_stream_running{{{LBL},part="source"}} 1',
                 f'migkit_stream_running{{{LBL},part="source task 0"}} 1',
                 f'migkit_stream_running{{{LBL},part="sink"}} 1',
                 f'migkit_stream_running{{{LBL},part="sink task 0"}} 0',
                 f"migkit_stream_behind_source_seconds{{{LBL}}} 1.5",
                 f"migkit_stream_events_seen{{{LBL}}} 15",
                 f"migkit_stream_queue_remaining{{{LBL}}} 8192",
                 f"migkit_stream_connected{{{LBL}}} 1",
                 f"migkit_stream_snapshot_tables_remaining{{{LBL}}} 2"):
        assert line in text, (line, text)
    assert any(p.startswith("/jolokia/read/debezium") for p in asked), asked
    # and the pipeline it reads is started with its readings on, on this
    # machine only
    from migkit import movers
    import inspect
    made = inspect.getsource(movers.stream_codegen)
    assert 'ENABLE_JOLOKIA: "true"' in made
    assert '"127.0.0.1:8778:8778"' in made


def test_the_shipped_dashboard_shows_only_metrics_that_exist(tmp_path,
                                                             monkeypatch):
    board = json.loads((ROOT / "deploy" / "grafana" / "migkit.json")
                       .read_text())
    named = set()
    for panel in board["panels"]:
        for target in panel["targets"]:
            named |= set(re.findall(r"migkit_[a-z_]+", target["expr"]))
    exported = set(re.findall(r"^(migkit_[a-z_]+)\{",
                              every_reading(tmp_path, monkeypatch), re.M))
    assert len(named) > 20 and named <= exported, named - exported
    text = json.dumps(board).lower()
    assert not [t for t in TOOLS if t in text], text
    assert board["templating"]["list"][0]["query"] == \
        "label_values(migkit_verdict, hop)"
    # nothing on the board reads a family kept only until 1.0
    from migkit import ui
    assert not named & set(ui.DEPRECATED), named & set(ui.DEPRECATED)


def _lite(tmp_path, monkeypatch=None):
    for name, rows in (("a.db", [(1, "x"), (2, "y")]), ("b.db", [(1, "x")])):
        with sqlite3.connect(tmp_path / name) as con:
            con.execute("create table t (id integer primary key, v text)")
            con.executemany("insert into t values (?, ?)", rows)
    conf = tmp_path / "hops.yaml"
    conf.write_text("hops:\n  lite:\n    engine: sqlite\n"
                    f"    source: {{host: {tmp_path / 'a.db'}}}\n"
                    f"    target: {{host: {tmp_path / 'b.db'}}}\n"
                    "    databases: [main]\n")
    if monkeypatch is not None:
        import migkit.config as cfg
        monkeypatch.setattr(cfg, "CONF", str(conf))
        monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return conf


def test_a_run_is_a_trace(tmp_path, monkeypatch):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import \
        InMemorySpanExporter
    from opentelemetry.trace import StatusCode

    from migkit import cli, traces
    _lite(tmp_path, monkeypatch)
    spans = InMemorySpanExporter()
    traces.configure(spans)
    try:
        # a command that stops, then a check that finds a difference
        stopped = CliRunner().invoke(cli.main, ["sync", "lite", "--apply"])
        found = CliRunner().invoke(cli.main, ["check", "lite"])
    finally:
        traces.configure()
    assert stopped.exit_code != 0 and found.exit_code == 1, found.output
    assert not traces.enabled()
    got = spans.get_finished_spans()
    sync = [s for s in got if s.name == "migkit sync"]
    check = [s for s in got if s.name == "migkit check"]
    assert len(sync) == 1 and len(check) == 1, [s.name for s in got]
    assert sync[0].status.status_code == StatusCode.ERROR
    # a difference is the check's answer, not an error of the run
    root = check[0]
    assert root.status.status_code != StatusCode.ERROR, root.status
    assert root.attributes["migkit.hop"] == "lite"
    assert root.attributes["migkit.exit"] == "1"
    assert root.attributes["migkit.session"], root.attributes
    data = [s for s in got if s.name == "check data"]
    assert data and data[0].attributes["migkit.db"] == "main", data
    assert data[0].attributes["migkit.status"] == "diff", data[0].attributes
    # checked on a worker thread, and still under the command's span
    assert data[0].parent is not None
    assert data[0].parent.span_id == root.context.span_id
    assert data[0].context.trace_id == root.context.trace_id
    said = json.dumps([[s.name, dict(s.attributes or {}),
                        [e.name for e in s.events]] for s in got]).lower()
    assert not [t for t in TOOLS if t in said], said


def test_a_task_runs_migkit_as_a_person_would(tmp_path):
    """The one runner every wrapper stands on: the command in a process
    of its own, and what it answered read back from what it wrote."""
    from migkit import integrations
    conf = _lite(tmp_path)
    reports = tmp_path / "reports"
    got = integrations.run("check", "lite", conf=conf, reports=reports,
                           timeout=600)
    assert got["exit"] == 1 and got["status"] == "different", got
    assert Path(got["verdict"]) == reports / "lite" / "verdict.json"
    doc = json.loads(Path(got["evidence"]).read_text())
    assert doc["scope"] == "run" and doc["verdict"]["status"] == \
        "different", doc["verdict"]
    assert not integrations.passed(got)
    assert integrations.passed(got, fail_on_difference=False)
    assert "check lite: different" in integrations.said(got)
    # an error is not a difference, whatever the task is told
    broken = {**got, "exit": 1, "status": None}
    assert not integrations.passed(broken, fail_on_difference=False)


def test_a_ci_step_writes_its_outputs_and_summary(tmp_path):
    from migkit import integrations
    conf = _lite(tmp_path)
    out, summary = tmp_path / "out.txt", tmp_path / "summary.md"
    env = {"MIGKIT_COMMAND": "check", "MIGKIT_HOP": "lite",
           "MIGKIT_ARGS": "--only data", "MIGKIT_CONF": str(conf),
           "MIGKIT_REPORTS": str(tmp_path / "reports"),
           "MIGKIT_FAIL_ON_DIFFERENCE": "false",
           "GITHUB_OUTPUT": str(out), "GITHUB_STEP_SUMMARY": str(summary),
           "MIGKIT_OUTPUTS": str(tmp_path / "outputs")}
    assert integrations.step(env) == 0
    said = dict(line.split("=", 1) for line in
                out.read_text().splitlines())
    assert said["status"] == "different" and said["exit"] == "1", said
    assert Path(said["evidence"], "evidence.json").is_file(), said
    assert summary.read_text().startswith("# migkit evidence: lite"), \
        summary.read_text()[:200]
    outputs = tmp_path / "outputs"
    assert (outputs / "status").read_text() == "different"
    assert (outputs / "evidence" / "evidence.json").is_file()
    env["MIGKIT_FAIL_ON_DIFFERENCE"] = "true"
    assert integrations.step(env) == 1


def _stub(monkeypatch, name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    monkeypatch.setitem(sys.modules, name, mod)
    return mod


def test_the_airflow_task_and_the_dagster_op_are_thin(monkeypatch):
    """Each only runs `run` and fails where the command does - checked
    against stand-ins for the two schedulers' own classes, which migkit
    does not install."""
    from migkit import integrations
    asked = []
    answer = {"command": "check", "hop": "orders", "exit": 1,
              "status": "different", "verdict": "/r/verdict.json",
              "evidence": "/r/evidence/x/evidence.json", "output": "..."}

    def run(*args, **kw):
        asked.append(args)
        return dict(answer)
    monkeypatch.setattr(integrations, "run", run)

    class BaseOperator:
        def __init__(self, task_id=None, **kw):
            self.task_id = task_id
            self.log = logging.getLogger("stand-in")

    class AirflowException(Exception):
        pass
    _stub(monkeypatch, "airflow")
    _stub(monkeypatch, "airflow.sdk", BaseOperator=BaseOperator)
    _stub(monkeypatch, "airflow.exceptions",
          AirflowException=AirflowException)
    operator = integrations.MigkitOperator
    assert issubclass(operator, BaseOperator)
    assert operator.template_fields == ("command", "hop", "args")
    task = operator(task_id="verify", command="check", hop="orders",
                    args=["--only", "data"])
    with pytest.raises(AirflowException, match="check orders: different"):
        task.execute({})
    assert asked[-1][:3] == ("check", "orders", ["--only", "data"]), asked
    lenient = operator(task_id="watch", command="check", hop="orders",
                       fail_on_difference=False)
    assert lenient.execute({})["status"] == "different"

    class Failure(Exception):
        def __init__(self, description=None, metadata=None):
            super().__init__(description)
            self.metadata = metadata

    class MetadataValue:
        text = staticmethod(lambda v: ("text", v))
        path = staticmethod(lambda v: ("path", v))

    def op(name=None, **kw):
        def wrap(fn):
            fn.op_name = name
            return fn
        return wrap
    _stub(monkeypatch, "dagster", Failure=Failure,
          MetadataValue=MetadataValue, op=op)
    verify = integrations.migkit_op("verify_orders", "check", "orders")
    assert verify.op_name == "verify_orders"
    ctx = types.SimpleNamespace(log=logging.getLogger("stand-in"))
    with pytest.raises(Failure) as failed:
        verify(ctx)
    assert failed.value.metadata["evidence"] == ("path", answer["evidence"])
    answer["exit"], answer["status"] = 0, "same"
    assert verify(ctx)["status"] == "same"


def _step_reads():
    """The MIGKIT_* names `integrations.step` reads from its environment,
    read off its code."""
    import inspect

    from migkit import integrations
    return set(re.findall(r'"(MIGKIT_[A-Z_]+)"',
                          inspect.getsource(integrations.step)))


def test_the_github_action_runs_the_step(tmp_path):
    action = yaml.safe_load((ROOT / "action.yml").read_text())
    assert action["runs"]["using"] == "composite"
    steps = action["runs"]["steps"]
    run = next(s for s in steps if s.get("id") == "migkit")
    assert "from migkit.integrations import step" in run["run"]
    # the step is given what it reads, each from the action's inputs
    reads = _step_reads() - {"MIGKIT_OUTPUTS"}
    assert set(run["env"]) == reads, (set(run["env"]), reads)
    text = (ROOT / "action.yml").read_text()
    for name in action["inputs"]:
        assert f"inputs.{name}" in text, name
    # its outputs are the ones the step writes
    for name, out in action["outputs"].items():
        assert out["value"] == f"${{{{ steps.migkit.outputs.{name} }}}}"
    assert set(action["outputs"]) == {"status", "exit", "evidence"}
    assert not [t for t in TOOLS if t in text.lower()], text


def test_the_argo_template_runs_the_step():
    path = ROOT / "deploy" / "argo" / "migkit-workflowtemplate.yaml"
    flow = yaml.safe_load(path.read_text())
    assert flow["kind"] == "WorkflowTemplate"
    tpl = flow["spec"]["templates"][0]
    box = tpl["container"]
    assert "from migkit.integrations import step" in " ".join(box["args"])
    env = {e["name"]: e.get("value") for e in box["env"]}
    assert set(env) == _step_reads(), (set(env), _step_reads())
    where = env["MIGKIT_OUTPUTS"]
    for p in tpl["outputs"]["parameters"]:
        assert p["valueFrom"]["path"] == f"{where}/{p['name']}", p
    assert {p["name"] for p in tpl["outputs"]["parameters"]} <= \
        {"status", "exit", "evidence"}
    assert tpl["outputs"]["artifacts"][0]["path"] == f"{where}/evidence"
    assert not [t for t in TOOLS if t in path.read_text().lower()]


def test_the_libraries_behind_are_accounted_for():
    from migkit import tools, tooluse
    for tool in ("apprise", "opentelemetry"):
        assert tooluse.unexplained(tool) == [], tool
        assert tooluse.stale(tool) == [], tool
        assert tooluse.REGISTRY[tool].licence in tools.AGPL_COMPATIBLE
    assert {"instantiate", "notify", "AppriseAsset", "NotifyType",
            "title"} <= set(tooluse.used("apprise"))
    assert {"TracerProvider", "OTLPSpanExporter", "start_as_current_span",
            "add_event", "start_span"} <= set(tooluse.used("opentelemetry"))
    # and each is a dependency migkit installs with itself
    import tomllib
    deps = tomllib.loads((ROOT / "pyproject.toml").read_text())[
        "project"]["dependencies"]
    for name in ("apprise", "opentelemetry-sdk",
                 "opentelemetry-exporter-otlp-proto-http"):
        assert any(d.startswith(name) for d in deps), name
