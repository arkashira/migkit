"""The dashboard and `/metrics` say the verdict the check wrote, and only
to whom they may (audit 2026-10-01, LG1, LG6-LG10, SEC6, SEC7, SEC16).

Measured before: both counted every `skip` as a difference, so a clean
two-database MySQL check - its settings compared on the first database
only - paged `MigkitHopDifferent` while its verdict said `same`; a run
narrowed to one table showed `ok` where the verdict said `incomplete`;
`migkit_table_same` paired a side from one run with the other side from
another, each salted apart; every hop was given the readings of whichever
change stream the machine ran; each scrape loaded the hops and read
everything live; labels went out unescaped; `/metrics` needed no
sign-in and ignored `access`; a database named `../other/app` held
another hop's tail; a name holding a quote ran as script.
"""
import hashlib
import base64
import http.client
import json
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

from migkit.config import Endpoint, Hop

ROOT = Path(__file__).resolve().parents[1]
LBL = 'hop="h",engine="mysql"'


def _hop(name="h", **options):
    return Hop(name=name, engine="mysql",
               source=Endpoint(host="10.0.0.1", port=3306),
               target=Endpoint(host="10.0.0.2", port=3306),
               databases=["app", "shop"], options=options)


@pytest.fixture
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import ui
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    (tmp_path / "h").mkdir()
    return tmp_path


def _checked(where, rows, status, **more):
    where.mkdir(parents=True, exist_ok=True)
    (where / "summary.json").write_text(json.dumps(rows))
    (where / "verdict.json").write_text(json.dumps({"status": status,
                                                    **more}))


#: a clean check of two MySQL databases: settings compared on the first
#: only, the second's a skip
CLEAN = [{"check": "schema", "scope": "app", "status": "ok"},
         {"check": "data", "scope": "app", "status": "ok"},
         {"check": "params", "scope": "app", "status": "ok"},
         {"check": "schema", "scope": "shop", "status": "ok"},
         {"check": "data", "scope": "shop", "status": "ok"},
         {"check": "params", "scope": "shop", "status": "skip"}]


def test_a_clean_check_is_same_everywhere(reports):
    from migkit import ui
    _checked(reports / "h", CLEAN, "same")
    shown = ui._hop_data("h", _hop())
    assert shown["status"] == "same" and shown["word"] == "same", shown
    assert {d["name"]: d["status"] for d in shown["dbs"]} == {
        "app": "same", "shop": "same"}, shown["dbs"]
    assert shown["checks"]["params"] == {"same": 1, "answers": 2,
                                         "status": "same"}
    text = ui.prometheus({"h": _hop()})
    assert f'migkit_verdict{{{LBL},status="same"}} 1' in text
    assert f'migkit_verdict{{{LBL},status="different"}} 0' in text
    assert f"migkit_hop_status{{{LBL}}} 0" in text
    assert f'migkit_check_answers{{{LBL},check="params",status="skip"}} 1' \
        in text


def test_a_difference_is_said_as_one(reports):
    """The same rules can say different: one row that differs."""
    from migkit import ui
    rows = CLEAN[:-2] + [{"check": "data", "scope": "shop.t",
                          "status": "diff", "detail": "3 rows missing"},
                         CLEAN[-1]]
    _checked(reports / "h", rows, "different")
    shown = ui._hop_data("h", _hop())
    assert shown["status"] == "different"
    dbs = {d["name"]: d for d in shown["dbs"]}
    assert dbs["shop"]["status"] == "different" and \
        dbs["shop"]["note"] == "3 rows missing", dbs
    assert dbs["app"]["status"] == "same", dbs
    assert shown["checks"]["data"]["status"] == "different"
    text = ui.prometheus({"h": _hop()})
    assert f'migkit_verdict{{{LBL},status="different"}} 1' in text
    assert f"migkit_hop_status{{{LBL}}} 1" in text


def test_a_narrowed_run_is_incomplete_not_same(reports):
    """Every answer read `ok`, the run was narrowed to one table: the
    verdict says incomplete, and so does everything reading it."""
    from migkit import ui
    _checked(reports / "h", [{"check": "data", "scope": "app.t",
                              "status": "ok"}], "incomplete",
             why="the run was narrowed to table app.t")
    shown = ui._hop_data("h", _hop())
    assert shown["status"] == "incomplete", shown
    assert "narrowed" in shown["why"]
    text = ui.prometheus({"h": _hop()})
    assert f'migkit_verdict{{{LBL},status="incomplete"}} 1' in text
    assert f'migkit_verdict{{{LBL},status="same"}} 0' in text


def test_a_verdict_nobody_wrote_is_no_pass(reports):
    from migkit import ui
    _checked(reports / "h", CLEAN, "fine")
    assert ui._hop_data("h", _hop())["status"] == ui.NONE
    (reports / "h" / "verdict.json").unlink()
    text = ui.prometheus({"h": _hop()})
    assert f'migkit_verdict{{{LBL},status="none"}} 1' in text
    assert f'migkit_verdict{{{LBL},status="same"}} 0' in text
    assert f"migkit_hop_status{{{LBL}}} 3" in text


def _digest(where, session, side, rows, digest, table="t"):
    with open(where / "digests.jsonl", "a") as f:
        f.write(json.dumps({"session": session, "db": "app",
                            "table": table, "side": side, "rows": rows,
                            "digest": digest, "method": "own"}) + "\n")


def test_the_two_sides_of_a_table_are_paired_within_one_run(reports):
    """A run killed after reading one side: before, its side was paired
    with the other run's other side, salted apart, and read different."""
    from migkit import digests, export, ui
    where = reports / "h"
    _digest(where, "r1", "src", 3, "111")
    _digest(where, "r1", "dst", 3, "111")
    _digest(where, "r2", "src", 3, "222")
    _digest(where, "r1", "src", 5, "501", table="u")
    _digest(where, "r1", "dst", 4, "400", table="u")
    got = digests.read_in(where)
    assert set(got[("app", "t", "")]) == {"src"}, got
    assert got[("app", "t", "")]["src"]["session"] == "r2"
    text = ui.prometheus({"h": _hop()})
    assert 'table="t"' in text
    assert f'migkit_table_same{{{LBL},db="app",table="t"}}' not in text
    # and a pair that differs within its run is said different
    assert f'migkit_table_same{{{LBL},db="app",table="u"}} 0' in text
    hop = _hop()
    hop.report_dir = lambda db=None: where
    rows = {r["table"]: r["status"] for r in export._tables(hop, None)}
    assert rows == {"t": "one side only", "u": "different"}, rows


class _Stream:
    """The change stream's pipeline API and its reader's metrics, with
    two hops' connectors on one machine."""

    def __init__(self, running):
        beans = {
            "debezium.mysql:type=connector-metrics,context=streaming,"
            "server=migkit-h": {"MilliSecondsBehindSource": 1000,
                                "TotalNumberOfEventsSeen": 5},
            "debezium.mysql:type=connector-metrics,context=streaming,"
            "server=migkit-other": {"MilliSecondsBehindSource": 900000,
                                    "TotalNumberOfEventsSeen": 70}}

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                if self.path.startswith("/connectors/"):
                    state = "RUNNING" if running else "UNASSIGNED"
                    body = {"connector": {"state": state}, "tasks": []}
                elif self.path.startswith("/jolokia/read/"):
                    body = {"value": beans, "status": 200}
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                out = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.port = self.srv.server_port

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


@pytest.mark.parametrize("running", [True, False])
def test_a_hop_is_given_its_own_streams_readings(reports, monkeypatch,
                                                 running):
    from migkit import ui
    stream = _Stream(running)
    try:
        monkeypatch.setattr(ui, "STREAM_API", stream.port)
        monkeypatch.setattr(ui, "STREAM_READINGS", stream.port)
        (reports / "h" / "stream").mkdir()
        (reports / "h" / "stream" / "docker-compose.yml").write_text("")
        text = ui.prometheus({"h": _hop()})
    finally:
        stream.close()
    if running:
        assert f"migkit_stream_behind_source_seconds{{{LBL}}} 1" in text
        assert f"migkit_stream_events_seen_total{{{LBL}}} 5" in text
        assert "} 900" not in text and "} 70" not in text, text
    else:
        # a stopped stream is said stopped, and given no readings at all
        assert f'migkit_stream_running{{{LBL},part="source"}} 0' in text
        assert "migkit_stream_behind_source_seconds{" not in text


def test_a_scrape_reads_a_snapshot_and_says_what_failed(reports,
                                                        monkeypatch):
    from migkit import ui
    _checked(reports / "h", CLEAN, "same")
    loads = []
    hops = {"h": _hop()}

    def loader():
        loads.append(1)
        if len(loads) == 2:
            raise SystemExit("hops.yaml: a line that does not read")
        return hops
    c = ui.Collector(loader)
    c.once()
    first = c.text()
    assert f'migkit_verdict{{{LBL},status="same"}} 1' in first
    assert "migkit_collect_age_seconds " in first
    # a scrape reads nothing: three scrapes, one load
    c.text(), c.text()
    assert len(loads) == 1
    # a hops file broken since: the last snapshot served, the failure
    # counted
    c.once()
    broken = c.text()
    assert f'migkit_verdict{{{LBL},status="same"}} 1' in broken
    assert 'migkit_collect_errors_total{family="hops"} 1' in broken
    # one reading that fails is counted, and the rest are read
    monkeypatch.setattr(ui, "_table_metrics", lambda w, lbl: 1 / 0)
    c.once()
    got = c.text()
    assert 'migkit_collect_errors_total{family="table"} 1' in got
    assert f'migkit_verdict{{{LBL},status="same"}} 1' in got


def test_the_metrics_parse_and_keep_to_their_names(tmp_path, monkeypatch):
    """LG9: every family described once, with its type; its samples
    together; a count a counter ending in `_total`; no `quantile` label
    but in a family kept until 1.0; keys, not sentences, in the labels
    the alerts match on; every label value escaped."""
    from prometheus_client.parser import text_string_to_metric_families

    from migkit import ui
    from tests import test_migkit_works_inside_the_teams_tools as teams
    teams.every_reading(tmp_path, monkeypatch)
    # one scrape of a hop with every reading there is
    text = ui.prometheus({"h": teams._hop()})
    odd = 'a"b\\c\nd'
    extra = ui._sample("migkit_move_rows", ui._labels(hop=odd, engine="x"),
                       3, table=odd)
    families = list(text_string_to_metric_families(text))
    names = [f.name for f in families]
    for name, (kind, _, _) in ui.FAMILIES.items():
        assert text.count(f"# TYPE {name} {kind}\n") == 1, name
        if kind == "counter":
            assert name.endswith("_total"), name
    seen = set()
    for f in families:
        for s in f.samples:
            key = (s.name, tuple(sorted(s.labels.items())))
            assert key not in seen, key
            seen.add(key)
            if "quantile" in s.labels:
                assert s.name in ui.DEPRECATED, s.name
            for label in ("status", "check", "resource", "truth",
                          "way_id", "phase", "estimate", "state", "side"):
                if label in s.labels and s.name not in ui.DEPRECATED:
                    assert re.fullmatch(r"[a-z0-9_.:-]*", s.labels[label]), \
                        (s.name, label, s.labels[label])
    # a family's samples come together: a second run of one is an error
    runs = [m for m in re.findall(r"^(migkit_[a-z_]+)[{ ]", text, re.M)]
    for name in set(runs):
        first = runs.index(name)
        assert runs[first:first + runs.count(name)] == \
            [name] * runs.count(name), name
    (parsed,) = [f for f in text_string_to_metric_families(
        ui.render({"x": [extra]})) if f.name == "migkit_move_rows"]
    (sample,) = parsed.samples
    assert sample.labels == {"hop": odd, "engine": "x", "table": odd}
    with pytest.raises(KeyError):
        ui._sample("migkit_made_up", "", 1)
    assert names


def test_a_way_is_keyed_by_what_names_no_program(tmp_path, monkeypatch):
    from migkit import ui
    from tests.test_migkit_works_inside_the_teams_tools import every_reading
    from tests.test_the_report_does_not_name_its_tools import TOOLS
    text = every_reading(tmp_path, monkeypatch)
    key = ui.way_key("pgcopydb")
    assert key == ui.way_key("pgcopydb") and key != ui.way_key("builtin")
    assert (f'migkit_copy_way_rows_per_second{{hop="h",engine="postgres",'
            f'way_id="{key}"}} 250.0') in text, text
    assert (f'migkit_way_info{{hop="h",engine="postgres",way_id="{key}",'
            f'way="the streaming copy"}} 1') in text
    assert not [t for t in TOOLS if t in text.lower()]


# ---- who may see and do what ------------------------------------------------

@pytest.fixture
def served(reports, monkeypatch):
    from migkit import tailctl, ui
    hops = {"h": _hop(access={"operator": ["ann@example.com"]}),
            "other": _hop("other")}
    for name in hops:
        where = reports / name / "app"
        where.mkdir(parents=True, exist_ok=True)
        (where / tailctl.PID).write_text(f"{socket.gethostname()}"
                                         f" {os.getpid()}")
        _checked(reports / name, CLEAN, "same")
    monkeypatch.setattr(ui, "load_hops", lambda: hops)
    monkeypatch.setenv("MIGKIT_UI_USER_HEADER", "X-Auth-Request-Email")
    monkeypatch.setenv("MIGKIT_UI_PROXY_SECRET", "proxy-" + "s" * 20)
    monkeypatch.setenv("MIGKIT_METRICS_TOKEN", "metrics-" + "m" * 20)
    srv = ThreadingHTTPServer(("127.0.0.1", 0), ui.Handler)
    port = srv.server_address[1]
    monkeypatch.setattr(ui.Handler, "token", "tok-" + "z" * 20)
    monkeypatch.setattr(ui.Handler, "port", port)
    monkeypatch.setattr(ui.Handler, "collector", None)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield port, reports
    srv.shutdown()


def _get(port, path, headers=()):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request("GET", path, headers={"Host": f"127.0.0.1:{port}",
                                    **dict(headers)})
    r = c.getresponse()
    return r.status, dict(r.getheaders()), r.read().decode()


def _as(user):
    return (("X-Auth-Request-Email", user),
            ("X-Migkit-Proxy", "proxy-" + "s" * 20))


def test_metrics_are_read_with_a_token_or_a_sign_in_and_by_role(served):
    """SEC16: `/metrics` listed every hop and table to anyone who asked."""
    port, _ = served
    assert _get(port, "/metrics")[0] == 401
    assert _get(port, "/metrics", [("Authorization",
                                    "Bearer wrong")])[0] == 401
    status, _, text = _get(port, "/metrics", [(
        "Authorization", "Bearer metrics-" + "m" * 20)])
    assert status == 200
    assert 'hop="h"' in text and 'hop="other"' in text
    # a person sees the hops their access gives them
    status, _, text = _get(port, "/metrics", _as("eve@elsewhere.test"))
    assert status == 200 and 'hop="h"' not in text and \
        'hop="other"' in text, text
    status, _, text = _get(port, "/metrics", _as("ann@example.com"))
    assert 'hop="h"' in text and 'hop="other"' in text


def test_a_tail_is_held_only_in_its_own_hop(served):
    """SEC6: the path was checked only for being inside the reports, so
    `../other/app` held another hop's tail with the role of the first."""
    from migkit import tailctl
    port, reports = served
    headers = dict(_as("ann@example.com"))
    csrf = json.loads(_get(port, "/api/data", headers.items())[2])["csrf"]
    for db in ("../other/app", "../../h/app", "app/../../other/app"):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        c.request("POST", "/api/hold", body=json.dumps({"hop": "h",
                                                        "db": db}),
                  headers={"Host": f"127.0.0.1:{port}",
                           "Content-Type": "application/json",
                           "X-Migkit-Csrf": csrf, **headers})
        assert c.getresponse().status == 404, db
    assert not (reports / "other" / "app" / tailctl.PAUSE).exists()
    assert not (reports / "h" / "app" / tailctl.PAUSE).exists()


def test_the_page_runs_only_its_own_script(served):
    """SEC7: a value ran as script from a button's `onclick` - `esc` left
    `'` as it was. Now nothing on the page is built as markup from data,
    and the policy names the page's own script and style by digest."""
    from migkit import ui
    port, _ = served
    status, headers, body = _get(port, "/")
    policy = headers["Content-Security-Policy"]
    script = re.search(r"<script>(.*)</script>", body, re.S).group(1)
    style = re.search(r"<style>(.*)</style>", body, re.S).group(1)
    for text in (script, style):
        digest = base64.b64encode(hashlib.sha256(text.encode()).digest())
        assert f"'sha256-{digest.decode()}'" in policy
    assert "default-src 'none'" in policy and "unsafe-inline" not in policy
    for markup in ("onclick", "innerHTML", "outerHTML", "insertAdjacent",
                   "document.write", "eval("):
        assert markup not in body, markup
    assert "textContent" in ui.SCRIPT and "addEventListener" in ui.SCRIPT
    # a report's page may run nothing at all
    assert "script-src" not in ui.REPORT_POLICY
    assert "default-src 'none'" in ui.REPORT_POLICY


def test_the_view_binds_where_it_is_told(monkeypatch):
    """LG10: the shipped container published a port the view, bound to
    the container's own 127.0.0.1, never answered on."""
    from migkit import schedule, ui
    bound = []

    class Srv:
        def __init__(self, address, handler):
            bound.append(address)

        def serve_forever(self):
            raise KeyboardInterrupt
    monkeypatch.setattr(ui, "ThreadingHTTPServer", Srv)
    monkeypatch.setattr(schedule, "loop", lambda *a: None)
    monkeypatch.setattr(ui, "load_hops", lambda: {})
    monkeypatch.delenv("MIGKIT_UI_USER_HEADER", raising=False)
    monkeypatch.setenv("MIGKIT_UI_BIND", "0.0.0.0")
    ui.serve(18997)
    monkeypatch.delenv("MIGKIT_UI_BIND")
    ui.serve(18996)
    assert bound == [("0.0.0.0", 18997), ("127.0.0.1", 18996)], bound
    assert ui.Handler.collector is None


def test_the_shipped_deployment_can_be_scraped_and_only_from_here():
    compose = yaml.safe_load((ROOT / "deploy" / "docker-compose.yml")
                             .read_text())
    view = compose["services"]["migkit-ui"]
    assert view["ports"] == ["127.0.0.1:8899:8899"], view
    env = view["environment"]
    assert env["MIGKIT_UI_BIND"] == "0.0.0.0"
    assert "MIGKIT_METRICS_TOKEN" in env and ":?" in \
        env["MIGKIT_METRICS_TOKEN"]
    assert "./conf:/conf:ro" in view["volumes"]
    rules = yaml.safe_load((ROOT / "deploy" / "prometheus-alerts.yml")
                           .read_text())
    exprs = {r["alert"]: r["expr"] for g in rules["groups"]
             for r in g["rules"]}
    assert exprs["MigkitHopDifferent"] == \
        'migkit_verdict{status="different"} == 1'
    from migkit import ui
    named = {m for e in exprs.values()
             for m in re.findall(r"migkit_[a-z_]+", e)}
    assert not named & set(ui.DEPRECATED), named & set(ui.DEPRECATED)
