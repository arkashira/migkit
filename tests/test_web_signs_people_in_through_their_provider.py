"""People sign in to the web view through the organisation's OpenID
Connect provider - the code flow with PKCE, the ID token checked against
the provider's keys - and nothing else signs them in (backlog W2).

The provider is a stand-in on this machine (`web_stand_ins.Issuer`): real
RSA keys, a real token endpoint that checks the client's secret, the code
once, the redirect and the PKCE verifier. Every way a sign-in can be
forged is tried, and each is proved able to say "refused": a token signed
by another key, `alg: none`, a shared-secret algorithm, another audience,
another nonce, an expired token, a code replayed, a code finished in
another browser, a provider mixed up for another, an unverified e-mail.
"""
import time
from urllib.parse import parse_qs, urlsplit

import pytest

from tests.web_stand_ins import (Issuer, browser, get, place, signed_in,
                                 view)


@pytest.fixture
def issuer():
    got = Issuer()
    yield got
    got.close()


@pytest.fixture
def oidc(tmp_path, monkeypatch, issuer):
    place(tmp_path, monkeypatch)
    app = view(tmp_path, MIGKIT_WEB_OIDC_ISSUER=issuer.url,
               MIGKIT_WEB_OIDC_CLIENT_ID=issuer.client_id,
               MIGKIT_WEB_OIDC_CLIENT_SECRET=issuer.secret)
    yield app, issuer
    app.store.close()


def _start(c, nxt="/hops/h"):
    r = get(c, f"/auth/oidc/start?next={nxt}")
    assert r.status_code == 303, r.status_code
    return r.headers["Location"]


def _finish(c, code, state):
    return get(c, f"/auth/oidc/callback?code={code}&state={state}")


def _me(c):
    r = get(c, "/api/v1/me")
    return r.status_code, (r.get_json() if r.status_code == 200 else None)


def test_a_person_signs_in_and_lands_where_they_were_going(oidc):
    app, issuer = oidc
    c = browser(app)
    where = _start(c)
    q = parse_qs(urlsplit(where).query)
    # PKCE with S256, and a state and nonce of this sign-in's own
    assert q["code_challenge_method"] == ["S256"]
    assert len(q["code_challenge"][0]) >= 43
    assert q["redirect_uri"] == ["http://127.0.0.1:8899/auth/oidc/callback"]
    code, state = issuer.approve(where)
    r = _finish(c, code, state)
    assert r.status_code == 303 and r.headers["Location"] == "/hops/h"
    cookie = r.headers.getlist("Set-Cookie")
    session = [x for x in cookie if x.startswith(app.cookie + "=")]
    assert session and "HttpOnly" in session[0] and \
        "SameSite=Lax" in session[0]
    status, me = _me(c)
    assert status == 200
    assert me["id"] == "ann@example.com" and me["via"] == "oidc"
    assert "dba" in me["groups"]
    # the file gives ann the operator's role on h through the hop group
    assert me["roles"]["h"] == "approver"
    assert "other" not in me["roles"]
    # the sign-in is in the view's record, with who and how
    from migkit.durable import read_lines
    entries = read_lines(app.record_path, "cache")
    assert any(e["op"] == "signed-in" and e["by"] == "ann@example.com"
               and e["via"] == "oidc" for e in entries)


def test_the_code_counts_once(oidc):
    app, issuer = oidc
    c = browser(app)
    code, state = issuer.approve(_start(c))
    assert _finish(c, code, state).status_code == 303
    other = browser(app)
    # the same answer again, even with the browser's own value, finds no
    # sign-in under way
    r = _finish(c, code, state)
    assert r.status_code == 403
    assert b"not to a sign-in this view started" in r.data
    assert _me(other)[0] == 401


def test_a_code_finished_in_another_browser_signs_nobody_in(oidc):
    app, issuer = oidc
    victim, attacker = browser(app), browser(app)
    code, state = issuer.approve(_start(attacker), email="eve@example.com")
    r = _finish(victim, code, state)
    assert r.status_code == 403 and b"another browser" in r.data
    assert _me(victim)[0] == 401
    # and the provider was never asked for a token for it
    assert issuer.token_calls == 0


@pytest.mark.parametrize("forge,words", [
    ("another key", b"does not hold"),
    ("none", b"does not hold"),
    ("HS256", b"does not hold"),
    ("audience", b"does not hold"),
    ("nonce", b"does not hold"),
    ("expired", b"does not hold"),
    ("issuer", b"does not hold"),
    ("unverified", b"not verified"),
])
def test_a_forged_or_wrong_id_token_is_refused(oidc, forge, words):
    from joserfc.jwk import RSAKey
    app, issuer = oidc
    c = browser(app)
    where = _start(c)
    code, state = issuer.approve(where)
    if forge == "another key":
        issuer.sign_with = RSAKey.generate_key(2048, parameters={"kid": "k1"})
    elif forge in ("none", "HS256"):
        issuer.alg = forge
    elif forge == "audience":
        issuer.claims = {"aud": "someone-else"}
    elif forge == "nonce":
        issuer.claims = {"nonce": "a-nonce-this-sign-in-never-sent"}
    elif forge == "expired":
        issuer.claims = {"exp": int(time.time()) - 3600,
                         "iat": int(time.time()) - 7200}
    elif forge == "issuer":
        issuer.claims = {"iss": "https://elsewhere.example.com"}
    elif forge == "unverified":
        issuer.claims = {"email_verified": False}
    r = _finish(c, code, state)
    assert r.status_code == 403, (forge, r.status_code)
    assert words in r.data, (forge, r.data[:400])
    assert _me(c)[0] == 401
    from migkit.durable import read_lines
    assert any(e["op"] == "sign-in-refused" for e in read_lines(
        app.record_path, "cache"))


def test_the_same_flow_holds_when_nothing_is_forged(oidc):
    """The forgeries above fail for their own reason: the same steps with
    nothing changed sign the person in (a check that refused everything
    would pass them all)."""
    app, issuer = oidc
    c = browser(app)
    code, state = issuer.approve(_start(c))
    assert _finish(c, code, state).status_code == 303
    assert _me(c)[0] == 200


def test_a_provider_mixed_up_for_another_is_refused(oidc):
    app, issuer = oidc
    issuer.says_issuer = "https://another-provider.example.com"
    r = get(browser(app), "/auth/oidc/start")
    assert r.status_code == 403
    assert b"names another issuer" in r.data


def test_an_error_from_the_provider_is_said_and_signs_nobody_in(oidc):
    app, issuer = oidc
    c = browser(app)
    where = _start(c)
    state = parse_qs(urlsplit(where).query)["state"][0]
    r = get(c, f"/auth/oidc/callback?error=access_denied&state={state}")
    assert r.status_code == 403 and b"access_denied" in r.data
    assert _me(c)[0] == 401


def test_where_it_goes_on_to_is_this_views_own(oidc):
    """`next` is a path of this view: another site's address, or one
    that a browser reads as another site's, goes to the front page."""
    app, issuer = oidc
    for bad in ("https://evil.example.com/", "//evil.example.com/x",
                "/\\evil.example.com"):
        c = browser(app)
        from urllib.parse import quote
        where = _start(c, quote(bad, safe=""))
        code, state = issuer.approve(where)
        r = _finish(c, code, state)
        assert r.status_code == 303 and r.headers["Location"] == "/", bad


def test_with_a_provider_there_is_no_password_sign_in(oidc):
    app, _ = oidc
    assert app.local is None
    c = browser(app)
    assert get(c, "/setup").status_code == 404
    r = c.post("/auth/local", base_url="http://127.0.0.1:8899",
               data={"name": "admin", "password": "x" * 20})
    assert r.status_code == 404


def test_a_deprovisioned_person_is_let_in_by_nothing(tmp_path, monkeypatch,
                                                      issuer):
    """With provisioning on, a person the provider signs in but never
    provisioned is refused at the door, and a session of someone since
    deactivated opens nothing."""
    place(tmp_path, monkeypatch)
    token = "scim-token-" + "x" * 30
    app = view(tmp_path, MIGKIT_WEB_OIDC_ISSUER=issuer.url,
               MIGKIT_WEB_OIDC_CLIENT_ID=issuer.client_id,
               MIGKIT_WEB_OIDC_CLIENT_SECRET=issuer.secret,
               MIGKIT_WEB_SCIM_TOKEN=token)
    try:
        c = browser(app)
        code, state = issuer.approve(_start(c))
        r = _finish(c, code, state)
        assert r.status_code == 403 and b"not among the people" in r.data
        # provisioned, the same person signs in
        scim = {"Authorization": f"Bearer {token}"}
        got = c.post("/scim/v2/Users", base_url="http://127.0.0.1:8899",
                     headers=scim, json={"userName": "ann@example.com"})
        assert got.status_code == 201
        uid = got.get_json()["id"]
        c = browser(app)
        code, state = issuer.approve(_start(c))
        assert _finish(c, code, state).status_code == 303
        assert _me(c)[0] == 200
        # deactivated: the session is gone at once
        got = c.patch(f"/scim/v2/Users/{uid}", base_url="http://127.0.0.1:8899",
                      headers=scim, json={
                          "schemas": ["urn:ietf:params:scim:api:messages:"
                                      "2.0:PatchOp"],
                          "Operations": [{"op": "Replace", "path": "active",
                                          "value": "False"}]})
        assert got.status_code == 200 and got.get_json()["active"] is False
        assert _me(c)[0] == 401
    finally:
        app.store.close()


def test_a_session_opened_for_someone_is_theirs_alone(oidc):
    """The session cookie is the only thing that says who someone is: a
    browser without it is nobody, whatever it sends."""
    app, _ = oidc
    c = browser(app)
    signed_in(app, c, {"id": "ann@example.com", "via": "oidc"})
    assert _me(c)[1]["id"] == "ann@example.com"
    other = browser(app)
    r = get(other, "/api/v1/me", headers={"X-Auth-Request-Email":
                                          "root@example.com"})
    assert r.status_code == 401
