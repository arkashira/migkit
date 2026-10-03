"""The web view is safe by default, and refuses to start where it would
not be (backlog W2).

Held, each able to say "refused": a view listening beyond this machine
without TLS; a provider reached or returned to in the clear; a request
for a name the view is not reached by; behind a proxy, a connection that
is not the proxy's; a change that rides on the session's cookie without
the page's token, or from another site; a session idle too long, signed
out, or kept across a sign-in; a password guessed again and again; the
set-up address used twice; `/metrics` for anyone without its token or
a session, and a person's beyond their own hops. And what every answer carries: a content security policy with no
inline script, no framing, no sniffing, no referrer, nothing cached.
"""
import re

import pytest

from migkit.web import settings
from tests.web_stand_ins import web  # noqa: F401 - the fixture
from tests.web_stand_ins import (URL, browser, call, get, place, post,
                                 signed_in, view)


def _load(tmp_path, **env):
    return settings.load(8899, environ={"MIGKIT_WEB_STATE":
                                        str(tmp_path / "s"), **env})


@pytest.mark.parametrize("env,words", [
    ({"MIGKIT_UI_BIND": "0.0.0.0"}, "without TLS"),
    ({"MIGKIT_UI_BIND": "10.0.0.5"}, "without TLS"),
    ({"MIGKIT_UI_BIND": "0.0.0.0",
      "MIGKIT_WEB_URL": "https://migkit.example.com"}, "without TLS"),
    ({"MIGKIT_WEB_URL": "http://migkit.example.com",
      "MIGKIT_WEB_OIDC_ISSUER": "https://login.example.com",
      "MIGKIT_WEB_OIDC_CLIENT_ID": "m",
      "MIGKIT_WEB_OIDC_CLIENT_SECRET": "x" * 24}, "https:// address"),
    ({"MIGKIT_WEB_OIDC_ISSUER": "http://login.example.com",
      "MIGKIT_WEB_OIDC_CLIENT_ID": "m",
      "MIGKIT_WEB_OIDC_CLIENT_SECRET": "x" * 24}, "not an https://"),
    ({"MIGKIT_WEB_OIDC_ISSUER": "https://login.example.com"},
     "MIGKIT_WEB_OIDC_CLIENT_ID"),
    ({"MIGKIT_WEB_TLS_CERT": "/etc/hosts"}, "go together"),
    ({"MIGKIT_WEB_SCIM_TOKEN": "short"}, "shorter than 24"),
    ({"MIGKIT_WEB_URL": "https://migkit.example.com/some/path"},
     "no path"),
])
def test_a_view_that_would_not_be_safe_does_not_start(tmp_path, env, words):
    with pytest.raises(SystemExit, match=re.escape(words)):
        _load(tmp_path, **env)


def test_the_safe_ways_to_listen_beyond_this_machine_start(tmp_path):
    s = _load(tmp_path, MIGKIT_UI_BIND="0.0.0.0",
              MIGKIT_WEB_URL="https://migkit.example.com",
              MIGKIT_WEB_PROXIES="10.0.0.9")
    assert s.https and s.proxies == ("10.0.0.9",)
    assert _load(tmp_path).url == "http://127.0.0.1:8899"
    # a container published on the host's loopback, said so by the operator
    s = _load(tmp_path, MIGKIT_UI_BIND="0.0.0.0", MIGKIT_WEB_PLAIN="on")
    assert s.plain and not s.bound_locally and not s.https


def test_a_request_for_another_name_gets_nothing(web):
    c = browser(web)
    signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    r = c.get("/api/v1/me", base_url="http://evil.example.com:8899")
    assert r.status_code == 403 and r.data == b"not this host"
    assert get(c, "/api/v1/me").status_code == 200


def test_every_answer_carries_the_headers(web):
    c = browser(web)
    signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    for path in ("/", "/hops/h", "/signin", "/api/v1/me", "/nowhere"):
        r = get(c, path)
        csp = r.headers["Content-Security-Policy"]
        assert "script-src 'self'" in csp and "unsafe-inline" not in csp
        assert "frame-ancestors 'none'" in csp
        assert r.headers["X-Frame-Options"] == "DENY"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["Referrer-Policy"] == "no-referrer"
        assert r.headers["Cache-Control"] == "no-store", path
    page = get(c, "/hops/h").data.decode()
    # no script written into a page: the only script is the view's file
    assert "<script>" not in page and "onclick" not in page
    assert '<script src="/static/web.js" defer></script>' in page


def test_a_change_needs_the_pages_token_and_its_origin(web):
    c = browser(web)
    csrf = signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    body = {"name": "ci"}
    assert call(c, "POST", "/api/v1/tokens", None, body).status_code == 403
    assert call(c, "POST", "/api/v1/tokens", "a-guess", body
                ).status_code == 403
    assert call(c, "POST", "/api/v1/tokens", csrf, body,
                **{"Sec-Fetch-Site": "cross-site"}).status_code == 403
    assert call(c, "POST", "/api/v1/tokens", csrf, body,
                Origin="https://evil.example.com").status_code == 403
    # none of those made a token; the page's own call does
    assert get(c, "/api/v1/tokens").get_json() == []
    made = call(c, "POST", "/api/v1/tokens", csrf, body)
    assert made.status_code == 201
    token, tid = made.get_json()["token"], made.get_json()["id"]
    # a token is not a cookie: it needs no page token, and makes none
    api = browser(web)
    auth = {"Authorization": f"Bearer {token}"}
    r = api.post("/api/v1/tokens", base_url=URL, headers=auth, json=body)
    assert r.status_code == 403 and "makes no tokens" in r.get_json()["error"]
    r = api.delete(f"/api/v1/tokens/{tid}", base_url=URL, headers=auth)
    assert r.status_code == 200
    assert get(api, "/api/v1/me", headers=auth).status_code == 401
    # kept only as its digest
    rows = web.store.rows("select id_hash from tokens")
    assert all(token not in str(r) for r in rows)


def test_a_session_ends_idle_signed_out_and_is_new_at_each_sign_in(web):
    c = browser(web)
    csrf = signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    assert get(c, "/api/v1/me").status_code == 200
    with web.store.tx() as db:
        db.execute("update sessions set seen = seen - ?",
                   (web.settings.idle_minutes * 60 + 5,))
    assert get(c, "/api/v1/me").status_code == 401
    csrf = signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    first = c.get_cookie(web.cookie, domain="127.0.0.1").value
    r = post(c, "/signout", data={"csrf": csrf},
             headers={"Origin": URL})
    assert r.status_code == 303 and r.headers["Location"] == "/signin"
    assert get(c, "/api/v1/me").status_code == 401
    # the cookie a signed-out session had opens nothing either
    c.set_cookie(web.cookie, first, domain="127.0.0.1")
    assert get(c, "/api/v1/me").status_code == 401
    signed_in(web, c, {"id": "ann@example.com", "via": "oidc"})
    assert c.get_cookie(web.cookie, domain="127.0.0.1").value != first
    # the session's cookie is kept only as its digest
    rows = web.store.rows("select id_hash from sessions")
    assert first not in str(rows)


def _form(c, path="/signin"):
    page = get(c, path).data.decode()
    return re.search(r'name="form" value="([^"]+)"', page).group(1)


def test_the_operators_account_is_set_once_through_the_printed_code(
        tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    app = view(tmp_path)
    try:
        assert app.local is not None and not app.local.exists()
        code = app.local.setup_code()
        c = browser(app)
        form = _form(c, "/setup")
        fields = {"form": form, "code": code, "name": "admin",
                  "password": "short", "again": "short"}
        r = post(c, "/setup", data=fields, headers={"Origin": URL})
        assert r.status_code == 403 and b"12 characters" in r.data
        # the form of another site, with the right code, is refused
        other = browser(app)
        r = post(other, "/setup", data={**fields, "password": "p" * 16,
                                        "again": "p" * 16})
        assert r.status_code == 403
        assert not app.local.exists()
        good = {**fields, "password": "correct-horse-battery",
                "again": "correct-horse-battery"}
        r = post(c, "/setup", data=good, headers={"Origin": URL})
        assert r.status_code == 303 and app.local.exists()
        assert get(c, "/api/v1/me").get_json()["admin"] is True
        # the code opens nothing twice
        again = browser(app)
        form = _form(again, "/setup")
        r = post(again, "/setup", data={**good, "form": form},
                 headers={"Origin": URL})
        assert r.status_code == 403 and b"has been used" in r.data
        # the password is kept hashed
        name, hashed = app.store.local_account()
        assert name == "admin" and hashed.startswith("$argon2id$")
        assert "correct-horse-battery" not in hashed
    finally:
        app.store.close()


def test_a_password_guessed_again_and_again_waits(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    app = view(tmp_path)
    try:
        from argon2 import PasswordHasher
        app.store.set_local("admin", PasswordHasher().hash("the-right-one!"))
        c = browser(app)
        form = _form(c)

        def attempt(password):
            return post(c, "/auth/local", headers={"Origin": URL},
                        data={"form": form, "name": "admin",
                              "password": password, "next": "/"})
        for _ in range(6):
            r = attempt("wrong-password")
            assert r.status_code == 403 and b"do not sign in here" in r.data
        r = attempt("the-right-one!")
        assert r.status_code == 403 and b"too many wrong passwords" in r.data
        assert get(c, "/api/v1/me").status_code == 401
        # once the wait is over, the right password signs in
        with app.store.tx() as db:
            db.execute("update throttle set until = 0")
        r = attempt("the-right-one!")
        assert r.status_code == 303
        assert get(c, "/api/v1/me").get_json()["id"] == "admin"
    finally:
        app.store.close()


def test_a_sign_in_form_from_elsewhere_signs_nobody_in(tmp_path,
                                                      monkeypatch):
    place(tmp_path, monkeypatch)
    app = view(tmp_path)
    try:
        from argon2 import PasswordHasher
        app.store.set_local("admin", PasswordHasher().hash("the-right-one!"))
        c = browser(app)
        r = post(c, "/auth/local", headers={"Origin": URL},
                 data={"form": "made-up", "name": "admin",
                       "password": "the-right-one!"})
        assert r.status_code == 403 and b"not this view" in r.data
        assert get(c, "/api/v1/me").status_code == 401
    finally:
        app.store.close()


def test_behind_a_proxy_only_the_proxy_is_answered(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    app = view(tmp_path, MIGKIT_UI_BIND="0.0.0.0",
               MIGKIT_WEB_URL="https://migkit.example.com",
               MIGKIT_WEB_PROXIES="10.0.0.9")
    try:
        c = browser(app)
        site = "https://migkit.example.com"
        r = c.get("/signin", base_url=site,
                  environ_base={"REMOTE_ADDR": "10.0.0.66"})
        assert r.status_code == 403
        r = c.get("/signin", base_url="http://10.0.0.5:8899",
                  environ_base={"REMOTE_ADDR": "10.0.0.9"},
                  headers={"X-Forwarded-Proto": "https",
                           "X-Forwarded-Host": "migkit.example.com",
                           "X-Forwarded-For": "10.0.0.77"})
        assert r.status_code == 200
        assert r.headers["Strict-Transport-Security"].startswith("max-age=")
        # over https the cookies are the __Host- kind: Secure, this host
        # only, every path
        code = app.local.setup_code()
        form = re.search(r'name="form" value="([^"]+)"',
                         c.get("/setup", base_url=site, environ_base={
                             "REMOTE_ADDR": "10.0.0.9"}).data.decode()
                         ).group(1)
        r = c.post("/setup", base_url=site,
                   environ_base={"REMOTE_ADDR": "10.0.0.9"},
                   headers={"Origin": site},
                   data={"form": form, "code": code, "name": "admin",
                         "password": "correct-horse-battery",
                         "again": "correct-horse-battery"})
        assert r.status_code == 303
        cookie = [x for x in r.headers.getlist("Set-Cookie")
                  if x.startswith("__Host-migkit_session=")]
        assert cookie, r.headers.getlist("Set-Cookie")
        assert "Secure" in cookie[0] and "Path=/" in cookie[0]
        assert "Domain" not in cookie[0] and "HttpOnly" in cookie[0]
    finally:
        app.store.close()


def test_metrics_are_a_scrapers_with_its_token_or_a_persons_own(
        tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    token = "metrics-token-" + "m" * 20
    app = view(tmp_path, MIGKIT_METRICS_TOKEN=token)
    try:
        c = browser(app)
        r = get(c, "/metrics")
        assert r.status_code == 401 and r.headers["WWW-Authenticate"] == \
            "Bearer"
        assert get(c, "/metrics", headers={
            "Authorization": "Bearer not-it"}).status_code == 401
        r = get(c, "/metrics", headers={"Authorization": f"Bearer {token}"})
        text = r.data.decode()
        assert r.status_code == 200 and "migkit_hop_status" in text
        # the scraper reads every hop
        assert 'hop="h"' in text and 'hop="other"' in text
        # a person signed in reads the hops theirs to see, no other
        signed_in(app, c, {"id": "carol@example.com", "via": "oidc"})
        text = get(c, "/metrics").data.decode()
        assert 'hop="h"' in text and 'hop="other"' not in text
    finally:
        app.store.close()
