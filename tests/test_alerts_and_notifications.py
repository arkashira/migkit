"""Alerts and notifications (backlog item 32).

The Prometheus metrics said how the last check went and how long ago it
was. They said nothing about a change tail: whether it was behind, whether
it had stopped going round its loop, or whether it had stopped on an
error. And nothing told anyone unless something scraped them.

Now:
* a tail writes a heartbeat every time round its loop, and `/metrics`
  reads it
* `deploy/prometheus-alerts.yml` ships rules over those metrics
* the hop option `notify`, or `MIGKIT_NOTIFY`, sends a message when a
  verdict changes or a tail stops
"""
import json
import re
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from click.testing import CliRunner

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql


class _Receiver:
    def __init__(self):
        got = self.got = []
        self.code = 200

        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                got.append((self.path, json.loads(self.rfile.read(n))))
                self.send_response(outer.code)
                self.end_headers()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/hook"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def receiver():
    r = _Receiver()
    yield r
    r.close()


def _hop(tmp_path, notify=None, mask=None):
    ep = Endpoint(host="x", port=0, user="", password="")
    options = {}
    if notify is not None:
        options["notify"] = notify
    if mask is not None:
        options["mask"] = mask
    hop = Hop(name="n", engine="sqlite", source=ep, target=ep,
              options=options)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _env(status, findings=()):
    return {"status": status, "totals": {"diff": len(findings)},
            "findings": [dict(f) for f in findings]}


FINDING = {"status": "diff", "check": "data", "scope": "main.people",
           "category": "rows-missing",
           "detail": "missing key ann@example.com"}


def test_each_receiver_gets_the_shape_it_takes():
    from migkit import notify
    facts = {"event": "verdict", "hop": "h"}
    url, body = notify.body("https://hooks.slack.com/services/T/B/C",
                            "msg", facts, True, "d")
    assert body == {"text": "msg"}, body
    url, body = notify.body("https://discord.com/api/webhooks/1/x", "msg",
                            facts, True, "d")
    assert body == {"content": "msg"}, body
    url, body = notify.body(
        "https://prod-01.westus.logic.azure.com:443/workflows/x", "msg",
        facts, True, "d")
    card = body["attachments"][0]
    assert card["contentType"] == \
        "application/vnd.microsoft.card.adaptive", body
    assert card["content"]["body"][0]["text"] == "msg", body
    url, body = notify.body("https://example.com/hook", "msg", facts, True,
                            "d")
    assert body == {"event": "verdict", "hop": "h", "text": "msg"}, body
    url, body = notify.body("pagerduty:R0UTING", "msg", facts, True, "d1")
    assert url == notify.PAGERDUTY
    assert body["routing_key"] == "R0UTING"
    assert body["event_action"] == "trigger" and body["dedup_key"] == "d1"
    assert body["payload"]["summary"] == "msg", body
    url, body = notify.body("pagerduty:R0UTING", "msg", facts, False, "d1")
    assert body == {"routing_key": "R0UTING", "event_action": "resolve",
                    "dedup_key": "d1"}, body
    assert notify.body("ftp://example.com/x", "m", facts, True, "d") is None


def test_a_verdict_is_sent_when_it_changes_and_not_again(tmp_path,
                                                         receiver):
    from migkit import notify
    hop = _hop(tmp_path, notify=receiver.url)
    said = []
    # the first check that finds nothing wrong tells nobody anything
    assert notify.verdict(hop, _env("same"), said.append) == 0
    assert notify.verdict(hop, _env("different", [FINDING]),
                          said.append) == 1
    assert notify.verdict(hop, _env("different", [FINDING]),
                          said.append) == 0
    # a narrowed run is no answer either way
    assert notify.verdict(hop, _env("incomplete"), said.append) == 0
    assert notify.verdict(hop, _env("error"), said.append) == 1
    assert notify.verdict(hop, _env("same"), said.append) == 1
    got = [b for _, b in receiver.got]
    assert [(b["status"], b["was"]) for b in got] == [
        ("different", None), ("error", "different"), ("same", "error")]
    assert "n is different: diff data main.people" in got[0]["text"], \
        got[0]
    assert "n is error, was different" in got[1]["text"], got[1]
    # the finding's detail, where values live, is not sent
    assert "ann@example.com" not in json.dumps(receiver.got)
    assert said == [], said


def test_an_unreachable_receiver_is_said_and_asked_again(tmp_path,
                                                         receiver):
    from migkit import notify
    down = _Receiver()
    url = down.url
    down.close()
    hop = _hop(tmp_path, notify=[url])
    said = []
    assert notify.verdict(hop, _env("different", [FINDING]),
                          said.append) == 0
    assert said and "not reached" in said[0], said
    # the address is a secret: never in what is said
    assert all(url not in s and "127.0.0.1" not in s for s in said), said
    # nobody was told, so the next check tells them
    hop.options["notify"] = [receiver.url]
    assert notify.verdict(hop, _env("different", [FINDING]),
                          said.append) == 1
    receiver.code = 500
    assert notify.verdict(hop, _env("same"), said.append) == 0
    assert "answered 500" in said[-1], said


def test_receivers_from_the_hop_and_the_environment(tmp_path, monkeypatch):
    from migkit import notify
    monkeypatch.setenv("MIGKIT_NOTIFY", "https://a.example.com/x, "
                                        "pagerduty:K")
    assert notify.receivers(_hop(tmp_path, notify="https://b.example.com")
                            ) == ["https://b.example.com",
                                  "https://a.example.com/x", "pagerduty:K"]
    monkeypatch.delenv("MIGKIT_NOTIFY")
    assert notify.receivers(_hop(tmp_path)) == []


def test_pagerduty_is_triggered_and_resolved_as_one_incident(tmp_path,
                                                             receiver,
                                                             monkeypatch):
    from migkit import notify
    monkeypatch.setattr(notify, "PAGERDUTY", receiver.url)
    hop = _hop(tmp_path, notify=["pagerduty:K"])
    notify.verdict(hop, _env("different", [FINDING]), print)
    notify.verdict(hop, _env("same"), print)
    (a, first), (b, second) = receiver.got
    assert first["event_action"] == "trigger", first
    assert second["event_action"] == "resolve", second
    assert first["dedup_key"] == second["dedup_key"] == "migkit-n"


def test_check_sends_its_verdict(tmp_path, monkeypatch, receiver):
    """End to end on the command a cron job runs."""
    import migkit.config as cfg
    from migkit import cli
    for name, rows in (("a.db", [(1, "x"), (2, "y")]), ("b.db", [(1, "x")])):
        con = sqlite3.connect(tmp_path / name)
        con.executescript("create table t (id integer primary key, v text)")
        con.executemany("insert into t values (?, ?)", rows)
        con.commit()
        con.close()
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}, user: x, password: x}}\n"
        "    databases: [main]\n"
        f"    options: {{notify: '{receiver.url}'}}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    got = CliRunner().invoke(cli.main, ["check", "lite", "--only", "data"])
    assert got.exit_code != 0, got.output
    # the same difference found again by the full run: told once
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    assert got.exit_code != 0, got.output
    assert [b["status"] for _, b in receiver.got] == ["different"], \
        receiver.got
    con = sqlite3.connect(tmp_path / "b.db")
    con.execute("insert into t values (2, 'y')")
    con.commit()
    con.close()
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    assert got.exit_code == 0, got.output
    assert [b["status"] for _, b in receiver.got] == ["different", "same"]


def test_the_tail_files_say_how_it_is(tmp_path):
    from migkit import tailctl
    with tailctl.Running(tmp_path):
        tailctl.beat(tmp_path, time.time() - 30, 12)
        now = tailctl.state(tmp_path)
        assert now["running"] is True and now["paused"] is False
        assert now["changes"] == 12
        assert 29 <= now["behind"] <= 40, now
        assert now["beat_age"] < 5, now
    assert tailctl.state(tmp_path) is None
    # an error on the way out is kept, and said; ctrl-c is not an error
    with pytest.raises(RuntimeError):
        with tailctl.Running(tmp_path):
            raise RuntimeError("could not apply: relation does not exist")
    now = tailctl.state(tmp_path)
    assert now["running"] is False and now["stopped"], now
    assert "RuntimeError: could not apply" in now["stopped"]["why"], now
    with pytest.raises(KeyboardInterrupt):
        with tailctl.Running(tmp_path) as r:
            assert r.was_stopped["why"].startswith("RuntimeError")
            raise KeyboardInterrupt
    assert tailctl.state(tmp_path) is None


def test_the_metrics_carry_the_tail(tmp_path, monkeypatch):
    import migkit.config as cfg
    from migkit import tailctl, ui
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="x", port=0, user="", password=""),
              target=Endpoint(host="x", port=0, user="", password=""))
    where = tmp_path / "h" / "appdb"
    where.mkdir(parents=True)
    with tailctl.Running(where):
        tailctl.beat(where, time.time() - 100, 7)
        text = ui.prometheus({"h": hop})
    lbl = 'hop="h",engine="postgres",db="appdb"'
    assert f"migkit_tail_running{{{lbl}}} 1" in text, text
    assert f"migkit_tail_paused{{{lbl}}} 0" in text, text
    assert f"migkit_tail_changes_applied{{{lbl}}} 7" in text, text
    behind = re.search(rf"migkit_tail_behind_seconds{{{lbl}}} (\d+)", text)
    assert behind and 99 <= int(behind.group(1)) <= 110, text
    assert re.search(rf"migkit_tail_heartbeat_age_seconds{{{lbl}}} \d+",
                     text), text
    # a process that died without going through its exit
    import socket
    (where / tailctl.PID).write_text(f"{socket.gethostname()} 999999")
    text = ui.prometheus({"h": hop})
    assert f"migkit_tail_running{{{lbl}}} 0" in text, text


def test_the_shipped_rules_name_only_metrics_that_exist(tmp_path,
                                                        monkeypatch):
    """A rule over a metric nobody exports never fires, and says nothing
    about that."""
    import yaml

    import migkit.config as cfg
    from migkit import tailctl, ui
    rules = yaml.safe_load(
        (Path(__file__).parent.parent / "deploy" / "prometheus-alerts.yml")
        .read_text())
    named = set()
    for group in rules["groups"]:
        for rule in group["rules"]:
            named |= set(re.findall(r"migkit_[a-z_]+", rule["expr"]))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host="x", port=0, user="", password=""),
              target=Endpoint(host="x", port=0, user="", password=""))
    (tmp_path / "h").mkdir()
    (tmp_path / "h" / "summary.json").write_text("[]")
    where = tmp_path / "h" / "appdb"
    where.mkdir()
    with pytest.raises(RuntimeError):
        with tailctl.Running(where):
            raise RuntimeError("x")
    stopped = ui.prometheus({"h": hop})
    with tailctl.Running(where):
        # every reading an engine gives, though no one engine gives all
        tailctl.beat(where, time.time(), 1,
                     {"seconds": 1, "bytes": 1, "held_bytes": 1})
        running = ui.prometheus({"h": hop})
    exported = set(re.findall(r"^(migkit_[a-z_]+)\{", stopped + running,
                              re.M))
    assert named and named <= exported, named - exported


@needs_docker
def test_a_tail_that_stops_on_an_error_is_said(pg_pair, tmp_path,
                                               monkeypatch, receiver):
    """End to end: a table dropped on the target under a running tail. The
    tail stops, the metrics say it stopped, a receiver is told; started
    again once the table is back, the receiver is told that too."""
    import migkit.config as cfg
    from migkit import tailctl, ui
    from migkit.engines.hetero import HeteroEngine
    monkeypatch.setattr(cfg, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    for port in pg_pair.values():
        assert psql(port, "create table public.nt (id int primary key,"
                          " v text)").returncode == 0
    hop = Hop(name="nt", engine="hetero",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"],
              options={"source_engine": "postgres",
                       "target_engine": "postgres",
                       "notify": [receiver.url]})
    eng = HeteroEngine(hop)
    token = hop.report_dir("postgres") / "tail-token.json"
    ended = {}

    def run():
        try:
            eng.tail_apply("postgres", True, token, lambda m: None)
        except BaseException as e:
            ended["why"] = e

    try:
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        where = token.parent
        for _ in range(60):
            if tailctl.state(where) and tailctl.state(where).get("beat_age") \
                    is not None:
                break
            time.sleep(0.5)
        psql(pg_pair["src"], "insert into public.nt values (1, 'a')")
        for _ in range(60):
            if psql(pg_pair["dst"], "select count(*) from public.nt"
                    ).stdout.strip() == "1":
                break
            time.sleep(0.5)
        text = ui.prometheus({"nt": hop})
        assert 'migkit_tail_running{hop="nt",engine="hetero",' \
               'db="postgres"} 1' in text, text
        # the slot holds the log for it, and nothing caps how much
        assert re.search(r'migkit_tail_source_held_bytes\{hop="nt",'
                         r'engine="hetero",db="postgres"\} \d+', text), text
        psql(pg_pair["dst"], "drop table public.nt")
        psql(pg_pair["src"], "insert into public.nt values (2, 'b')")
        thread.join(timeout=60)
        assert not thread.is_alive() and "why" in ended, ended
        text = ui.prometheus({"nt": hop})
        assert 'migkit_tail_stopped{hop="nt",engine="hetero",' \
               'db="postgres"} 1' in text, text
        assert "migkit_tail_running{" not in text, text
        stops = [b for _, b in receiver.got
                 if b.get("event") == "tail stopped"]
        assert len(stops) == 1 and stops[0]["db"] == "postgres", \
            receiver.got
        assert "nt" in stops[0]["why"], stops
        # back again
        psql(pg_pair["dst"], "create table public.nt (id int primary key,"
                             " v text)")
        ended.clear()
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        for _ in range(60):
            if psql(pg_pair["dst"], "select count(*) from public.nt"
                    ).stdout.strip() == "1":
                break
            time.sleep(0.5)
        assert [b.get("event") for _, b in receiver.got] == [
            "tail stopped", "tail started"], receiver.got
        assert "migkit_tail_stopped{" not in ui.prometheus({"nt": hop})
    finally:
        import ctypes
        if thread.is_alive():
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(thread.ident),
                ctypes.py_object(KeyboardInterrupt))
            thread.join(timeout=30)
        psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name)"
                             " from pg_replication_slots where not active")
        for port in pg_pair.values():
            psql(port, "drop table if exists public.nt")
