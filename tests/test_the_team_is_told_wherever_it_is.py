"""The team is told wherever it is (backlog R23.10).

A receiver is any service the notification library behind migkit reaches
- Teams, Google Chat, Lark/Feishu, LINE's Messaging API, Telegram,
Discord, email, SNS, Opsgenie, Mattermost, Rocket.Chat, DingTalk, WeCom,
ntfy, Pushover, Matrix, Signal, phones through Firebase, webhooks - each
tested here against a local stub standing in for the service, with every
request the library makes caught: the message arrives at the service's
own endpoint, in migkit's words, under migkit's name, with no row value
and nothing of the library's in it. migkit's own receivers (PagerDuty,
a plain JSON webhook) go on as they did.

What is told: a verdict moving, the schema drifting under a check or a
move, a change tail falling behind and coming back, each cutover step,
an approval waiting and given, a command stopping, a way given up for
another, a writing command done - each once, each a hop may leave out.
"""
import json
import socketserver
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote_plus, urlsplit

import pytest
from click.testing import CliRunner

from migkit.config import Endpoint, Hop

TEXT = "n - different"
#: what a stubbed service answers, enough for every channel's own check
ANSWER = {"ok": True, "code": 0, "errcode": 0, "errmsg": "ok",
          "StatusCode": 0, "status": 1, "success": 1,
          "result": {"message_id": 1}, "requestId": "r1",
          "event_id": "$e", "room_id": "!r:example.com",
          "access_token": "t", "user_id": "@user:example.com",
          "home_server": "example.com"}


class _Services:
    """One local server standing in for every service: a request's host
    is kept as the first part of its path."""

    def __init__(self):
        got = self.got = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _any(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(n) if n else b""
                got.append((self.command, self.path, dict(self.headers),
                            body))
                if "/hooks.slack.com/services/" in self.path:
                    code, out = 200, b"ok"
                else:
                    code = 202 if "api.opsgenie.com" in self.path else 200
                    out = json.dumps(ANSWER).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)
            do_POST = do_PUT = do_GET = do_DELETE = _any
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.port = self.server.server_port

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def services(monkeypatch):
    """Every request the library makes goes to the stub instead."""
    import requests
    stub = _Services()
    real = requests.Session.request

    def to_stub(self, method, url, *a, **kw):
        u = urlsplit(url)
        kw.pop("verify", None)
        there = (f"http://127.0.0.1:{stub.port}/{u.hostname}{u.path or '/'}"
                 + (f"?{u.query}" if u.query else ""))
        return real(self, method, there, *a, **kw)
    monkeypatch.setattr(requests.Session, "request", to_stub)
    yield stub
    stub.close()


def _hop(tmp_path, notify=None, **options):
    ep = Endpoint(host="x", port=0, user="", password="CHANGE_ME-pw")
    if notify is not None:
        options["notify"] = notify
    hop = Hop(name="n", engine="sqlite", source=ep, target=ep,
              options=options)
    hop.report_dir = lambda db=None: tmp_path / db if db else tmp_path
    return hop


def _env(status, findings=()):
    return {"status": status, "totals": {"diff": len(findings)},
            "findings": [dict(f) for f in findings]}


FINDING = {"status": "diff", "check": "data", "scope": "main.people",
           "category": "rows-missing",
           "detail": "missing key ann@example.com"}

CHANNELS = [
    ("slack", "https://hooks.slack.com/services/T0/B0/C0",
     "/hooks.slack.com/services/T0/B0/C0"),
    ("slack by scheme", "slack://T1/B1/C1",
     "/hooks.slack.com/services/T1/B1/C1"),
    ("discord", "https://discord.com/api/webhooks/1/tok",
     "/discord.com/api/webhooks/1/tok"),
    ("teams workflow",
     "https://prod-01.westus.logic.azure.com:443/workflows/w/triggers/manual"
     "/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0"
     "&sig=s",
     "/prod-01.westus.logic.azure.com/workflows/w/triggers/manual/paths/"
     "invoke"),
    ("google chat", "gchat://workspace/key/token",
     "/chat.googleapis.com/v1/spaces/workspace/messages"),
    ("lark", "lark://token", "/open.larksuite.com/open-apis/bot/v2/hook/"
                             "token"),
    ("feishu", "feishu://token", "/open.feishu.cn/open-apis/bot/v2/hook/"
                                 "token"),
    ("line messaging api", "line://token/U0123",
     "/api.line.me/v2/bot/message/push"),
    ("telegram", "tgram://123456789:AABBCC/12345",
     "/api.telegram.org/bot123456789:AABBCC/sendMessage"),
    ("dingtalk", "dingtalk://token", "/oapi.dingtalk.com/robot/send"),
    ("wecom", "wecombot://key", "/qyapi.weixin.qq.com/cgi-bin/webhook/send"),
    ("ntfy", "ntfy://topic", "/ntfy.sh/"),
    ("pushover", "pover://user@token", "/api.pushover.net/1/messages.json"),
    ("mattermost", "mmost://example.com/token", "/example.com/hooks/token"),
    ("rocket.chat", "rocket://abcdefg/1234567890@example.com/#general",
     "/example.com/hooks/abcdefg/1234567890"),
    ("opsgenie", "opsgenie://apikey", "/api.opsgenie.com/v2/alerts"),
    ("firebase cloud messaging", "fcm://apikey/device",
     "/fcm.googleapis.com/fcm/send"),
    ("json webhook", "json://example.com/hook", "/example.com/hook"),
    ("signal", "signal://example.com:8080/+12345678901/+12345678902",
     "/example.com/v2/send"),
    ("sns", "sns://AKIAIOSFODNN7EXAMPLE/CHANGE_ME/us-east-1/+12223334444",
     "/sns.us-east-1.amazonaws.com/"),
    ("matrix", "matrix://user:pass@example.com/#room",
     "/example.com/_matrix/client/"),
]


def _readable(body):
    text = body.decode("utf-8", "replace")
    return text + " " + unquote_plus(text)


@pytest.mark.parametrize("name,url,path", CHANNELS,
                         ids=[c[0] for c in CHANNELS])
def test_each_channel_hears_migkits_words_at_its_own_endpoint(
        tmp_path, services, name, url, path):
    from migkit import notify
    said = []
    hop = _hop(tmp_path, url)
    sent = notify.verdict(hop, _env("different", [FINDING]), said.append)
    reached = [(m, p, h, b) for m, p, h, b in services.got
               if p.startswith(path)]
    assert reached, (name, [p for _, p, _, _ in services.got])
    assert any(TEXT in _readable(b) for _, _, _, b in reached), \
        (name, [b[:300] for _, _, _, b in reached])
    everything = " ".join(f"{p} {json.dumps(h)} {_readable(b)}"
                          for _, p, h, b in services.got).lower()
    # nothing of the library's goes out with migkit's message, and no
    # finding's detail (where a row's values are)
    assert "apprise" not in everything, name
    assert "ann@example.com" not in everything, name
    if name != "opsgenie":
        # Opsgenie's answer is an alert id kept for its later close
        assert sent == 1, (name, said)
    # the address is a password, and is never said
    assert not [s for s in said if url in s or "token" in s], said


class _Mail(socketserver.StreamRequestHandler):
    got = []

    def handle(self):
        self.wfile.write(b"220 stub\r\n")
        data, lines = False, []
        while True:
            line = self.rfile.readline()
            if not line:
                return
            if data:
                if line in (b".\r\n", b".\n"):
                    data = False
                    _Mail.got.append(b"".join(lines).decode("utf-8",
                                                            "replace"))
                    self.wfile.write(b"250 ok\r\n")
                else:
                    lines.append(line)
                continue
            word = line.strip().upper()
            if word.startswith((b"EHLO", b"HELO")):
                self.wfile.write(b"250 stub\r\n")
            elif word.startswith(b"DATA"):
                data = True
                self.wfile.write(b"354 go on\r\n")
            elif word.startswith(b"QUIT"):
                self.wfile.write(b"221 bye\r\n")
                return
            else:
                self.wfile.write(b"250 ok\r\n")


def test_an_email_is_sent_under_migkits_name(tmp_path):
    from migkit import notify
    _Mail.got = []
    srv = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _Mail)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        hop = _hop(tmp_path, f"mailto://127.0.0.1:{srv.server_address[1]}"
                             "?from=migkit@example.com&to=ops@example.com"
                             "&mode=insecure")
        said = []
        assert notify.verdict(hop, _env("different", [FINDING]),
                              said.append) == 1, said
    finally:
        srv.shutdown()
        srv.server_close()
    mail = _Mail.got[0]
    assert TEXT in mail and "Subject: migkit verdict: n" in mail, mail
    assert "X-Application: migkit" in mail, mail
    assert "apprise" not in mail.lower() and "ann@example.com" not in mail


class _Hook:
    """A plain JSON webhook: migkit's own receiver, with the facts."""

    def __init__(self):
        got = self.got = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                got.append(json.loads(self.rfile.read(n)))
                self.send_response(200)
                self.end_headers()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/hook"

    def events(self):
        return [b.get("event") for b in self.got]

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def hook():
    h = _Hook()
    yield h
    h.close()


def test_every_event_is_told_once(tmp_path, hook):
    from migkit import notify, tailctl
    said = []
    hop = _hop(tmp_path, {"to": [hook.url], "lag": 60})
    # the schema drifting under a move
    assert notify.drift(hop, "main", ["people: column note added"],
                        said.append, stop="main: the source's schema"
                                          " changed") == 1
    # a tail past the hop's 60s, told once, and once back
    where = tmp_path / "main"
    where.mkdir()
    with tailctl.Running(where):
        tailctl.beat(where, time.time() - 400, 3)
        assert notify.lags(hop, ["main"], said.append) == 1
        assert notify.lags(hop, ["main"], said.append) == 0
        tailctl.beat(where, time.time(), 4)
        assert notify.lags(hop, ["main"], said.append) == 1
    # a cutover's steps
    rec = {"id": "c1", "phases": {"freeze": {"began": 1.0, "ended": 1.5}}}
    for entry in ({"event": "began", "budget": 30, "dbs": ["main"]},
                  {"phase": "freeze", "event": "done", "seconds": 0.5},
                  {"event": "flipped", "outage": 1.25}):
        assert notify.cutover(hop, rec, entry, said.append) == 1
    # an approval waiting, told once for its request, again once signed
    for have in (0, 0, 1):
        notify.approval_asked(hop, "cutover", "main", 2, have,
                              time.time() + 60, "nonce1", said.append)
    notify.approved(hop, "cutover", "main", ["ann", "bo"], said.append)
    # a way given up, a command stopped, a writing command done
    notify.fallback(hop, "public.t", "the streaming copy", "not here",
                    "the table copier", said.append)
    notify.failed(hop, "move", "RuntimeError: pgcopydb exited 1 for"
                                " CHANGE_ME-pw", said.append)
    notify.done(hop, "sync", 12.0, said.append)
    assert hook.events() == [
        "drift", "lag", "lag", "cutover", "cutover", "cutover", "approval",
        "approval", "approval", "fallback", "failure", "done"], hook.got
    lag, back = hook.got[1], hook.got[2]
    assert lag["behind_seconds"] >= 399 and "past the 60s" in lag["text"]
    assert "back within 60s" in back["text"], back
    assert "1.25s of write outage" in hook.got[5]["text"], hook.got[5]
    assert [b["have"] for b in hook.got[6:8]] == [0, 1], hook.got[6:8]
    assert hook.got[8]["approvers"] == ["ann", "bo"], hook.got[8]
    stopped = hook.got[10]["text"]
    # never a password, never a program migkit drives
    assert "CHANGE_ME-pw" not in stopped and "pgcopydb" not in stopped, \
        stopped
    # a move that stopped on the drift already told is not told twice
    assert notify.failed(hop, "move", "main: the source's schema changed",
                         said.append) == 0
    assert said == [], said


def test_a_hop_keeps_only_the_events_it_names(tmp_path, hook):
    from migkit import notify
    hop = _hop(tmp_path, {"to": hook.url, "events": ["verdict", "done"]})
    assert notify.drift(hop, "main", ["t changed"], print) == 0
    assert notify.failed(hop, "move", "RuntimeError: x", print) == 0
    assert notify.verdict(hop, _env("different", [FINDING]), print) == 1
    assert notify.done(hop, "move", 3.0, print) == 1
    assert hook.events() == ["verdict", "done"], hook.got


def test_a_masked_hop_tells_only_what_stopped(tmp_path, hook):
    from migkit import notify
    hop = _hop(tmp_path, hook.url, mask="all")
    notify.failed(hop, "move", "IntegrityError: duplicate key (id)=(ann)",
                  print)
    text = hook.got[0]["text"]
    assert "IntegrityError" in text and "ann" not in text, text


def test_a_receiver_is_read_from_where_the_hop_keeps_it(tmp_path, hook,
                                                        monkeypatch):
    from migkit import notify
    monkeypatch.setenv("MIGKIT_TEST_HOOK", hook.url)
    monkeypatch.delenv("MIGKIT_TEST_GONE", raising=False)
    said = []
    hop = _hop(tmp_path, ["env:MIGKIT_TEST_HOOK", "env:MIGKIT_TEST_GONE"])
    assert notify.verdict(hop, _env("different", [FINDING]),
                          said.append) == 1
    assert hook.events() == ["verdict"], hook.got
    assert len(said) == 1 and "receiver 2 could not be read" in said[0], \
        said
    assert hook.url not in " ".join(said)
    # PagerDuty's routing key the same way
    monkeypatch.setenv("MIGKIT_TEST_PD", "R0UTING")
    assert notify._address("pagerduty:env:MIGKIT_TEST_PD") == \
        "pagerduty:R0UTING"


def test_the_hop_is_checked_for_its_receivers():
    from migkit import hopkeys

    def problems(notify):
        return [p for p in hopkeys.check_hop("h", {
            "source": "sqlite:///tmp/a.db", "target": "sqlite:///tmp/b.db",
            "options": {"notify": notify}})
            if "notify" in p.where]
    plain = problems("https://hooks.slack.com/services/T/B/C")
    assert [p.level for p in plain] == ["warn"], plain
    assert "keychain" in plain[0].text and "hooks.slack" not in \
        plain[0].text, plain
    assert problems(["env:HOOK", "pagerduty:keyring:pd"]) == []
    bad = problems({"to": "env:HOOK", "events": ["verdict", "gossip"],
                    "lag": "soon"})
    assert sorted(p.level for p in bad) == ["error", "error"], bad
    assert any("gossip" in p.text for p in bad), bad


def _lite(tmp_path, monkeypatch, url):
    import migkit.config as cfg
    for name, rows in (("a.db", [(1, "x"), (2, "y")]), ("b.db", [(1, "x")])):
        with sqlite3.connect(tmp_path / name) as con:
            con.execute("create table t (id integer primary key, v text)")
            con.executemany("insert into t values (?, ?)", rows)
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}}}\n"
        f"    target: {{host: {tmp_path / 'b.db'}}}\n"
        "    databases: [main]\n"
        f"    options: {{notify: '{url}'}}\n")
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def test_a_command_that_stops_is_told_and_one_that_wrote_is_done(
        tmp_path, monkeypatch, hook):
    from migkit import cli
    _lite(tmp_path, monkeypatch, hook.url)
    got = CliRunner().invoke(cli.main, ["sync", "lite", "--apply"])
    assert got.exit_code != 0 and "check first" in got.output + str(
        got.exception), got.output
    assert hook.events() == ["failure"], hook.got
    # the refusal's words stay on this machine unless the hop's notify
    # asks for them (`detail`): an error's words can quote a row
    said = hook.got[0]["text"]
    assert said.startswith("migkit: sync on lite stopped: a refusal;") \
        and "check first" not in said, hook.got
    # a difference found is a verdict, not a failure
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    assert got.exit_code == 1, got.output
    assert hook.events() == ["failure", "verdict"], hook.got
    got = CliRunner().invoke(cli.main, ["sync", "lite", "--apply"])
    assert got.exit_code == 0, got.output
    assert hook.events() == ["failure", "verdict", "done"], hook.got
    assert hook.got[-1]["command"] == "sync", hook.got[-1]
    # a dry run wrote nothing, and is not told done
    got = CliRunner().invoke(cli.main, ["sync", "lite"])
    assert hook.events()[-1] == "done" and len(hook.got) == 3, hook.got
