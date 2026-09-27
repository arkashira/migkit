"""The dashboard can hold a running tail between batches and let it go on
- only for the page migkit printed the address of: with its token, from
this machine's own name, with the header only that page sends. Each
action is written to the hop's record.

Before this the dashboard was read-only, and what it served - the hops,
their verdicts, their reports - was served to anything that asked on
127.0.0.1, a page on another site reaching it through a name it controls
included.
"""
import http.client
import json
import os
import socket
import threading
from http.server import ThreadingHTTPServer

import pytest

from migkit.config import Endpoint, Hop


@pytest.fixture
def board(tmp_path, monkeypatch):
    from migkit import tailctl, ui
    hop = Hop(name="h", engine="postgres", source=Endpoint(host="10.0.0.1"),
              target=Endpoint(host="10.0.0.2"), databases=["app"])
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "load_hops", lambda: {"h": hop})
    where = tmp_path / "h" / "app"
    where.mkdir(parents=True)
    (where / tailctl.PID).write_text(f"{socket.gethostname()} {os.getpid()}")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), ui.Handler)
    port = srv.server_address[1]
    monkeypatch.setattr(ui.Handler, "token", "tok-" + "x" * 20)
    monkeypatch.setattr(ui.Handler, "port", port)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield port, where
    srv.shutdown()


def _ask(port, method, path, body=None, host=None, cookie=None, csrf=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Host": host or f"127.0.0.1:{port}"}
    if cookie:
        headers["Cookie"] = cookie
    if csrf:
        headers["X-Migkit-Csrf"] = csrf
    if body is not None:
        headers["Content-Type"] = "application/json"
    c.request(method, path, body=json.dumps(body) if body else None,
              headers=headers)
    r = c.getresponse()
    return r.status, r.getheader("Set-Cookie"), r.read()


def test_only_the_printed_address_opens_the_data(board):
    port, _ = board
    assert _ask(port, "GET", "/api/data")[0] == 403
    status, cookie, _ = _ask(port, "GET", "/?t=wrong")
    assert status == 200 and cookie is None
    status, cookie, _ = _ask(port, "GET", "/?t=tok-" + "x" * 20)
    assert status == 303 and "HttpOnly" in cookie and "SameSite=Strict" \
        in cookie
    jar = cookie.split(";")[0]
    status, _, body = _ask(port, "GET", "/api/data", cookie=jar)
    data = json.loads(body)
    assert status == 200 and data["hops"][0]["tails"] == [
        {"db": "app", "running": True, "paused": False, "asked": False}]
    # a name that is not this machine's: a page elsewhere, rebinding a
    # name it controls to 127.0.0.1
    assert _ask(port, "GET", "/api/data", host="evil.example.com",
                cookie=jar)[0] == 403


def test_a_tail_is_held_and_let_go_only_from_the_page(board):
    from migkit import audit, tailctl
    port, where = board
    jar = _ask(port, "GET", "/?t=tok-" + "x" * 20)[1].split(";")[0]
    csrf = json.loads(_ask(port, "GET", "/api/data", cookie=jar)[2])["csrf"]
    body = {"hop": "h", "db": "app"}
    # no token, no header, the wrong header, another host: refused
    assert _ask(port, "POST", "/api/hold", body, csrf=csrf)[0] == 403
    assert _ask(port, "POST", "/api/hold", body, cookie=jar)[0] == 403
    assert _ask(port, "POST", "/api/hold", body, cookie=jar,
                csrf="0" * 64)[0] == 403
    assert _ask(port, "POST", "/api/hold", body, cookie=jar, csrf=csrf,
                host="evil.example.com")[0] == 403
    assert not (where / tailctl.PAUSE).exists()
    assert _ask(port, "POST", "/api/hold", body, cookie=jar,
                csrf=csrf)[0] == 200
    assert (where / tailctl.PAUSE).exists()
    assert _ask(port, "POST", "/api/resume", body, cookie=jar,
                csrf=csrf)[0] == 200
    assert not (where / tailctl.PAUSE).exists()
    # outside the reports: not a database of the hop
    assert _ask(port, "POST", "/api/hold", {"hop": "h", "db": "../../x"},
                cookie=jar, csrf=csrf)[0] == 404
    record = where.parent / "changelog.jsonl"
    ops = [json.loads(line)["op"] for line in record.read_text().splitlines()]
    assert ops == ["tail-hold", "tail-resume"]
    assert audit.verify(record)[2] == ""


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """The view shared behind a proxy that signs people in and names them
    in a header of its own."""
    from migkit import tailctl, ui
    hops = {}
    for name, access in (("pay", {"operator": ["ann@example.com"],
                                  "viewer": ["*@example.com"]}),
                         ("open", None)):
        hops[name] = Hop(name=name, engine="postgres",
                         source=Endpoint(host="10.0.0.1"),
                         target=Endpoint(host="10.0.0.2"),
                         databases=["app"],
                         options={"access": access} if access else {})
        where = tmp_path / name / "app"
        where.mkdir(parents=True)
        (where / tailctl.PID).write_text(
            f"{socket.gethostname()} {os.getpid()}")
    monkeypatch.setattr(ui, "REPORTS", tmp_path)
    monkeypatch.setattr(ui, "load_hops", lambda: hops)
    monkeypatch.setenv("MIGKIT_UI_USER_HEADER", "X-Auth-Request-Email")
    monkeypatch.setenv("MIGKIT_UI_HOSTS", "migkit.example.com")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), ui.Handler)
    port = srv.server_address[1]
    monkeypatch.setattr(ui.Handler, "token", "tok-" + "y" * 20)
    monkeypatch.setattr(ui.Handler, "port", port)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield port, tmp_path
    srv.shutdown()


def _as(port, user, method, path, body=None, csrf=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Host": "migkit.example.com"}
    if user:
        headers["X-Auth-Request-Email"] = user
    if csrf:
        headers["X-Migkit-Csrf"] = csrf
    if body is not None:
        headers["Content-Type"] = "application/json"
    c.request(method, path, body=json.dumps(body) if body else None,
              headers=headers)
    r = c.getresponse()
    return r.status, r.read()


def test_each_person_sees_and_does_what_the_hop_gives_them(shared):
    """Before, the view had one key - the printed address - and whoever
    held it did everything to every hop. Shared behind a proxy, each
    person is named, and a hop's `access` says who may see it and who may
    hold its tail."""
    from migkit import audit
    port, reports = shared
    # nobody named and no token: nothing
    assert _as(port, None, "GET", "/api/data")[0] == 403
    status, body = _as(port, "ann@example.com", "GET", "/api/data")
    data = json.loads(body)
    roles = {h["name"]: h["role"] for h in data["hops"]}
    assert status == 200 and roles == {"pay": "operator", "open": "operator"}
    status, body = _as(port, "bob@example.com", "GET", "/api/data")
    roles = {h["name"]: h["role"] for h in json.loads(body)["hops"]}
    assert roles == {"pay": "viewer", "open": "operator"}, roles
    csrf = json.loads(body)["csrf"]
    # a viewer may not hold the tail
    assert _as(port, "bob@example.com", "POST", "/api/hold",
               {"hop": "pay", "db": "app"}, csrf)[0] == 403
    assert _as(port, "ann@example.com", "POST", "/api/hold",
               {"hop": "pay", "db": "app"}, csrf)[0] == 200
    record = reports / "pay" / "changelog.jsonl"
    _, _, broken = audit.verify(record)
    last = json.loads(record.read_text().splitlines()[-1])
    assert not broken and last["by"] == "ann@example.com", last
    # someone the hop does not name does not see it at all
    status, body = _as(port, "eve@elsewhere.test", "GET", "/api/data")
    assert [h["name"] for h in json.loads(body)["hops"]] == ["open"]
    assert _as(port, "eve@elsewhere.test", "GET", "/report/pay")[0] == 404
