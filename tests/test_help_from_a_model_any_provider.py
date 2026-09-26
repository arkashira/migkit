"""Help from a language model, from any provider, off unless configured
(backlog 43). The providers here are local stand-ins that answer in each
API's own shape; nothing is sent anywhere else."""
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from click.testing import CliRunner

ANSWERS = {
    "openai": {"choices": [{"message": {"content": "one row is missing"}}]},
    "anthropic": {"content": [{"type": "text", "text": "one row is missing"}]},
    "google": {"candidates": [{"content": {"parts": [
        {"text": "one row is missing"}]}}]},
}


@pytest.fixture
def stand_in():
    got = []

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n))
            got.append((self.path, {k.lower(): v for k, v in
                                    self.headers.items()}, body))
            kind = ("anthropic" if self.path.endswith("/messages") else
                    "google" if ":generateContent" in self.path else "openai")
            out = json.dumps(ANSWERS[kind]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)
    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", got
    server.shutdown()
    server.server_close()


def _lite(tmp_path, monkeypatch, mask=None):
    import migkit.config as cfg
    for name, rows in (("a.db", [("ann@example.com",), ("bo@example.com",)]),
                       ("b.db", [("ann@example.com",)])):
        con = sqlite3.connect(tmp_path / name)
        con.execute("create table people (email text primary key)")
        con.executemany("insert into people values (?)", rows)
        con.commit()
        con.close()
    text = ("hops:\n  lite:\n    engine: sqlite\n"
            f"    source: {{host: {tmp_path / 'a.db'}, user: x,"
            " password: x}\n"
            f"    target: {{host: {tmp_path / 'b.db'}, user: x,"
            " password: x}\n    databases: [main]\n")
    if mask:
        text += f"    options: {{mask: {mask}}}\n"
    (tmp_path / "hops.yaml").write_text(text)
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")


def _check():
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["check", "lite"])
    return got, " ".join(got.output.split())


def test_off_unless_configured(tmp_path, monkeypatch, stand_in):
    monkeypatch.delenv("MIGKIT_AI", raising=False)
    _lite(tmp_path, monkeypatch)
    got, said = _check()
    assert got.exit_code != 0 and "explained by" not in said, said
    assert stand_in[1] == []


@pytest.mark.parametrize("kind", ["openai", "anthropic", "google"])
def test_each_provider_is_asked_in_its_own_shape(tmp_path, monkeypatch,
                                                 stand_in, kind):
    url, got = stand_in
    monkeypatch.setenv("MIGKIT_AI", kind)
    monkeypatch.setenv("MIGKIT_AI_URL", url)
    monkeypatch.setenv("MIGKIT_AI_KEY", "CHANGE_ME-key")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "stand-in-model")
    _lite(tmp_path, monkeypatch)
    result, said = _check()
    assert result.exit_code != 0, said   # the verdict is still the verdict
    assert f"explained by {kind} - a proposal, not part of the verdict:" \
        " one row is missing" in said, said
    path, headers, body = got[0]
    sent = json.dumps(body)
    assert "people" in sent and "different" in sent, sent
    # the finding's detail - the key that is missing - is not sent
    assert "bo@example.com" not in sent, sent
    if kind == "anthropic":
        assert path == "/messages" and headers["x-api-key"] == "CHANGE_ME-key"
    elif kind == "google":
        assert path == "/models/stand-in-model:generateContent"
    else:
        assert path == "/chat/completions"
        assert headers["authorization"] == "Bearer CHANGE_ME-key"


def test_details_only_when_asked_and_never_when_masked(tmp_path, monkeypatch,
                                                       stand_in):
    url, got = stand_in
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_URL", url)
    monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
    monkeypatch.setenv("MIGKIT_AI_SHARE", "detail")
    _lite(tmp_path, monkeypatch)
    _check()
    assert "bo@example.com" in json.dumps(got[-1][2])
    (tmp_path / "masked").mkdir()
    _lite(tmp_path / "masked", monkeypatch, mask="[people.email]")
    _check()
    assert "bo@example.com" not in json.dumps(got[-1][2])


def test_a_provider_that_is_not_there_does_not_stop_the_check(
        tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.setenv("MIGKIT_AI_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("MIGKIT_AI_MODEL", "m")
    _lite(tmp_path, monkeypatch)
    got, said = _check()
    assert "verdict: different" in said, said
    assert "could not be reached" in said, said


def test_no_model_is_chosen_for_an_openai_style_endpoint(monkeypatch):
    from migkit import assist
    monkeypatch.setenv("MIGKIT_AI", "openai")
    monkeypatch.delenv("MIGKIT_AI_MODEL", raising=False)
    with pytest.raises(SystemExit, match="needs MIGKIT_AI_MODEL"):
        assist._request("openai", "x")
