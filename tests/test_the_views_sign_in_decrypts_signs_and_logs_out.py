"""The view's sign-in through an identity provider: an encrypted assertion
opened with the view's own key (and an unencrypted one refused where
encryption is demanded), the sign-in request signed where the provider's
metadata says it takes only signed ones, single logout both ways - the
view's sign-out reaching the provider, the provider's reaching the
view - and every call to the provider asking the egress gate first
(backlog W2's identity clause, DEC1001.gui).

The providers are stand-ins (`web_stand_ins.SamlIdp`, `Issuer`) with keys
of their own: the encryption is real RSA-OAEP key transport and AES, the
signatures real, so each refusal below is for its own reason and the same
answer untouched signs the person in.
"""
import base64
import re
import zlib
from urllib.parse import parse_qs, urlsplit

import pytest

from migkit import decide, egress
from tests.web_stand_ins import (
    URL, Issuer, SamlIdp, browser, csrf_in, get, place, post, view)


def _saml(tmp_path, monkeypatch, wants_signed=False, slo=False, **env):
    place(tmp_path, monkeypatch)
    idp = SamlIdp(tmp_path, wants_signed=wants_signed, slo=slo)
    app = view(tmp_path, MIGKIT_WEB_SAML_METADATA=str(idp.metadata), **env)
    return app, idp


@pytest.fixture
def saml(tmp_path, monkeypatch):
    app, idp = _saml(tmp_path, monkeypatch)
    yield app, idp
    app.store.close()


@pytest.fixture
def logout(tmp_path, monkeypatch):
    app, idp = _saml(tmp_path, monkeypatch, slo=True)
    yield app, idp
    app.store.close()


def _start(c):
    r = get(c, "/auth/saml/start?next=/hops/h")
    assert r.status_code == 303
    return r.headers["Location"]


def _post(c, response, relay):
    return post(c, "/auth/saml/acs",
                data={"SAMLResponse": response, "RelayState": relay})


def _me(c):
    r = get(c, "/api/v1/me")
    return r.status_code, (r.get_json() if r.status_code == 200 else None)


def _sign_in(app, idp, **kw):
    c = browser(app)
    where = _start(c)
    r = _post(c, *idp.answer(where, **kw))
    assert r.status_code == 303, r.data[:200]
    return c


# ---- an encrypted assertion ------------------------------------------------

@pytest.mark.parametrize("how,cipher,whole", [
    ("oaep-sha1", "gcm", False),      # signed inside the ciphertext
    ("oaep-sha256", "cbc", False),
    ("oaep-sha1", "gcm", True),       # the response signed over the cipher
])
def test_an_encrypted_assertion_is_opened_with_the_views_key(
        saml, how, cipher, whole):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    # a plain answer first: the same person, the same groups
    answer = idp.answer(where, encrypt_to=app.saml.cert_pem, how=how,
                        cipher=cipher, sign=not whole, sign_response=whole)
    assert b"ann@example.com" not in base64.b64decode(answer[0])
    r = _post(c, *answer)
    assert r.status_code == 303 and r.headers["Location"] == "/hops/h"
    status, me = _me(c)
    assert status == 200 and me["id"] == "ann@example.com"
    assert me["groups"] == ["change-board"] and me["via"] == "saml"


@pytest.mark.parametrize("kw,words", [
    # encrypted to a key that is not the view's
    ({"encrypt_to": "other"}, b"could not be opened"),
    # key transport by RSA 1.5, refused whatever the key
    ({"how": "rsa-1_5"}, b"could not be opened"),
    # opened, but never signed: a forged assertion encrypted to the view
    ({"sign": False}, b"signature"),
])
def test_an_encrypted_assertion_that_does_not_hold_signs_nobody_in(
        saml, kw, words):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    kw = dict(kw)
    kw.setdefault("encrypt_to", app.saml.cert_pem)
    if kw["encrypt_to"] == "other":
        kw["encrypt_to"] = idp.cert_pem
    r = _post(c, *idp.answer(where, **kw))
    assert r.status_code == 403 and words in r.data
    assert _me(c)[0] == 401


def test_a_tampered_ciphertext_is_refused(saml):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    response, relay = idp.answer(where, encrypt_to=app.saml.cert_pem)
    xml = base64.b64decode(response).decode()
    value = re.findall(r"<xenc:CipherValue>([^<]+)<", xml)[-1]
    raw = bytearray(base64.b64decode(value))
    raw[20] ^= 1
    xml = xml.replace(value, base64.b64encode(bytes(raw)).decode())
    r = _post(c, base64.b64encode(xml.encode()).decode(), relay)
    assert r.status_code == 403 and b"could not be opened" in r.data
    assert _me(c)[0] == 401


def test_a_plain_assertion_is_refused_where_encryption_is_demanded(
        tmp_path, monkeypatch):
    app, idp = _saml(tmp_path, monkeypatch, MIGKIT_WEB_SAML_ENCRYPTED="1")
    try:
        assert app.saml.asks["assertion"].path == "encrypted"
        c = browser(app)
        where = _start(c)
        r = _post(c, *idp.answer(where))                  # signed, plain
        assert r.status_code == 403 and b"encryption is demanded" in r.data
        assert _me(c)[0] == 401
        # a plain one beside the encrypted: still refused, not the signed
        c = browser(app)
        where = _start(c)
        r = _post(c, *idp.answer(where, encrypt_to=app.saml.cert_pem,
                                 beside="mallory@example.com"))
        assert r.status_code == 403
        assert _me(c)[0] == 401
        # the encrypted one signs in
        c = browser(app)
        where = _start(c)
        r = _post(c, *idp.answer(where, encrypt_to=app.saml.cert_pem))
        assert r.status_code == 303 and _me(c)[1]["id"] == "ann@example.com"
    finally:
        app.store.close()


def test_where_encryption_is_not_demanded_a_plain_assertion_stands(saml):
    app, idp = saml
    assert app.saml.asks["assertion"].path == "either"
    _sign_in(app, idp)


def test_the_views_key_is_kept_and_published(tmp_path, monkeypatch):
    app, idp = _saml(tmp_path, monkeypatch)
    try:
        body = get(browser(app), "/auth/saml/metadata").data.decode()
        assert body.count("<md:KeyDescriptor") == 2
        assert 'use="encryption"' in body and 'use="signing"' in body
        assert app.saml.cert_der in body
        key = tmp_path / "state" / "saml-sp.key"
        assert key.stat().st_mode & 0o777 == 0o600
        again = view(tmp_path, MIGKIT_WEB_SAML_METADATA=str(idp.metadata))
        try:
            assert again.saml.cert_pem == app.saml.cert_pem
        finally:
            again.store.close()
    finally:
        app.store.close()


# ---- a signed authentication request ------------------------------------------

def test_the_request_is_signed_where_the_providers_metadata_asks(
        tmp_path, monkeypatch):
    app, idp = _saml(tmp_path, monkeypatch, wants_signed=True)
    try:
        assert app.saml.asks["request"].path == "signed"
        assert 'AuthnRequestsSigned="true"' in get(
            browser(app), "/auth/saml/metadata").data.decode()
        c = browser(app)
        where = _start(c)
        assert idp.signed_by(where, app.saml.cert_pem)
        # a request altered after signing no longer carries the signature
        assert not idp.signed_by(where.replace("RelayState=", "RelayState=x"),
                                 app.saml.cert_pem)
        assert not idp.signed_by(where, idp.cert_pem)
        r = _post(c, *idp.answer(where))
        assert r.status_code == 303
    finally:
        app.store.close()


def test_the_request_is_not_signed_where_the_provider_does_not_ask(saml):
    app, idp = saml
    assert app.saml.asks["request"].path == "plain"
    where = _start(browser(app))
    assert "Signature" not in parse_qs(urlsplit(where).query)
    assert 'AuthnRequestsSigned="false"' in get(
        browser(app), "/auth/saml/metadata").data.decode()


# ---- single logout, from the view -----------------------------------------

def _tail(url):
    """What follows the view's logout address in a provider's redirect."""
    return url.split("/auth/saml/slo", 1)[1]


def _csrf(c):
    return csrf_in(get(c, "/").data.decode())


def _signout(c):
    return post(c, "/signout", data={"csrf": _csrf(c)},
                headers={"Origin": URL, "Sec-Fetch-Site": "same-origin"})


def test_signing_out_ends_the_providers_session_as_well(logout):
    app, idp = logout
    assert app.saml.asks["logout"].path == "redirect"
    c = _sign_in(app, idp)
    assert _me(c)[0] == 200
    r = _signout(c)
    to = r.headers["Location"]
    assert r.status_code == 303 and to.startswith(SamlIdp.SLO + "?")
    # the view's own session ended before the provider was asked
    assert _me(c)[0] == 401
    # the request names the person and the sign-in, and is signed
    rid, _, xml = idp.request_of(to)
    assert "<saml:NameID>ann@example.com</saml:NameID>" in xml
    assert "<samlp:SessionIndex>_s1</samlp:SessionIndex>" in xml
    assert idp.signed_by(to, app.saml.cert_pem)
    # the provider answers: confirmed, once
    back = idp.logout_response(to)
    path = _tail(back)
    r = get(c, "/auth/saml/slo" + path)
    assert r.status_code == 303 and r.headers["Location"] == "/signin"
    assert get(c, "/auth/saml/slo" + path).status_code == 400


def test_a_logout_answer_that_does_not_hold_is_refused(logout):
    app, idp = logout
    c = _sign_in(app, idp)
    to = _signout(c).headers["Location"]
    good = _tail(idp.logout_response(to))
    # not signed: the query without its signature
    unsigned = re.sub(r"&Signature=[^&]*", "", good)
    assert get(c, "/auth/saml/slo" + unsigned).status_code == 400
    # the provider says its session stands
    failed = _tail(idp.logout_response(to, status="PartialLogout"))
    r = get(c, "/auth/saml/slo" + failed)
    assert r.status_code == 400 and b"did not end its session" in r.data
    # an answer to a request this view never made
    other = _tail(idp.logout_response(to.replace(
        re.search(r'SAMLRequest=[^&]+', to).group(0), "SAMLRequest=" + (
            lambda x: __import__("urllib.parse").parse.quote(x))(
            base64.b64encode(zlib.compress(
                b'<samlp:LogoutRequest xmlns:samlp="urn:oasis:names:tc:SAML:'
                b'2.0:protocol" ID="_nope"/>')[2:-4]).decode()))))
    assert get(c, "/auth/saml/slo" + other).status_code == 400


def test_a_provider_with_no_single_logout_is_not_asked_for_one(saml):
    app, idp = saml
    assert app.saml.asks["logout"].path == "local"
    c = _sign_in(app, idp)
    r = _signout(c)
    assert r.status_code == 303 and r.headers["Location"] == "/signin"
    assert _me(c)[0] == 401
    # a logout the provider starts anyway is not taken: none was agreed
    r = get(c, "/auth/saml/slo" + idp.logout_request(
        "ann@example.com")[len(URL + "/auth/saml/slo"):])
    assert r.status_code == 400


# ---- single logout, from the provider -------------------------------------

def test_a_logout_the_provider_starts_ends_the_sessions_it_names(logout):
    app, idp = logout
    one = _sign_in(app, idp, index="_s1")
    two = _sign_in(app, idp, index="_s2")
    other = _sign_in(app, idp, index="_s3", name="bob@example.com")
    url = idp.logout_request("ann@example.com", index="_s1")
    r = get(one, url[len(URL):])
    assert r.status_code == 303
    back = r.headers["Location"]
    assert back.startswith(SamlIdp.SLO + "?") and "SAMLResponse=" in back
    assert idp.signed_by(back, app.saml.cert_pem)
    # answered to the request it came with
    q = parse_qs(urlsplit(back).query)["SAMLResponse"][0]
    xml = zlib.decompress(base64.b64decode(q), -15).decode()
    assert re.search(r'InResponseTo="_l[0-9a-f]+"', xml)
    assert "status:Success" in xml
    # the named sign-in ended; the same person's other one, and another
    # person's, stand
    assert _me(one)[0] == 401
    assert _me(two)[0] == 200 and _me(other)[0] == 200
    # with no sign-in named, every sign-in of the person ends
    get(two, idp.logout_request("ann@example.com")[len(URL):])
    assert _me(two)[0] == 401 and _me(other)[0] == 200


@pytest.mark.parametrize("kw", [
    {"sign": False},                                  # not signed
    {"issuer": "https://other.example.com/saml"},     # another provider
])
def test_a_logout_request_that_does_not_hold_ends_nothing(logout, kw):
    app, idp = logout
    c = _sign_in(app, idp)
    url = idp.logout_request("ann@example.com", **kw)
    r = get(c, url[len(URL):])
    assert r.status_code == 400
    assert _me(c)[0] == 200


def test_a_logout_request_counts_once(logout):
    app, idp = logout
    c = _sign_in(app, idp)
    url = idp.logout_request("ann@example.com", rid="_once")[len(URL):]
    assert get(c, url).status_code == 303
    again = _sign_in(app, idp)
    assert get(again, url).status_code == 400
    assert _me(again)[0] == 200


def test_a_logout_request_for_a_signature_taken_off_a_signed_one_fails(logout):
    app, idp = logout
    c = _sign_in(app, idp)
    url = idp.logout_request("ann@example.com")[len(URL):]
    # the same message with another name identifier: the signature, which
    # covers the message, no longer holds
    q = parse_qs(urlsplit(url).query)
    xml = zlib.decompress(base64.b64decode(q["SAMLRequest"][0]), -15)
    forged = zlib.compressobj(9, zlib.DEFLATED, -15)
    body = forged.compress(xml.replace(b"ann@example.com", b"bob@example.com")
                           ) + forged.flush()
    from urllib.parse import quote
    swapped = re.sub(r"SAMLRequest=[^&]+", "SAMLRequest=" + quote(
        base64.b64encode(body).decode(), safe=""), url)
    assert get(c, swapped).status_code == 400
    assert _me(c)[0] == 200


# ---- the egress gate ----------------------------------------------------------

@pytest.fixture
def issuer():
    got = Issuer()
    yield got
    got.close()


def _oidc(tmp_path, monkeypatch, issuer):
    place(tmp_path, monkeypatch)
    return view(tmp_path, MIGKIT_WEB_OIDC_ISSUER=issuer.url,
                MIGKIT_WEB_OIDC_CLIENT_ID=issuer.client_id,
                MIGKIT_WEB_OIDC_CLIENT_SECRET=issuer.secret)


def _oidc_sign_in(app, issuer):
    c = browser(app)
    r = get(c, "/auth/oidc/start?next=/hops/h")
    assert r.status_code == 303, r.data[:200]
    code, state = issuer.approve(r.headers["Location"])
    return c, get(c, f"/auth/oidc/callback?code={code}&state={state}")


def test_every_call_to_the_provider_asks_the_gate_first(
        tmp_path, monkeypatch, issuer):
    asked = []
    real = egress.permit

    def counted(hop, kind, address=None, host=None, log=None):
        # asked before the call: nothing has reached the provider for it
        asked.append((kind, address, len(issuer.calls)))
        return real(hop, kind, address, host, log)

    monkeypatch.setattr(egress, "permit", counted)
    app = _oidc(tmp_path, monkeypatch, issuer)
    try:
        c, r = _oidc_sign_in(app, issuer)
        assert r.status_code == 303
        assert "/.well-known/openid-configuration" in issuer.calls
        assert "/jwks" in issuer.calls and "/token" in issuer.calls
        # one question for each call that reached the provider
        assert len(asked) == len(issuer.calls)
        assert {k for k, _, _ in asked} == {"provider"}
        assert [n for _, _, n in asked] == list(range(len(asked)))
        assert all(a.startswith(issuer.url) for _, a, _ in asked)
    finally:
        app.store.close()


@pytest.mark.parametrize("env", [
    {"MIGKIT_EGRESS": "none"},
    {"MIGKIT_EGRESS": "notify,evidence"},             # the kind not let out
    {"MIGKIT_EGRESS_TO": "idp.corp.example.com"},     # the host not named
])
def test_a_refused_call_opens_no_connection(tmp_path, monkeypatch, issuer,
                                            env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    app = _oidc(tmp_path, monkeypatch, issuer)
    try:
        r = get(browser(app), "/auth/oidc/start?next=/hops/h")
        assert r.status_code == 403 and b"MIGKIT_EGRESS" in r.data
        assert issuer.calls == []
    finally:
        app.store.close()


def test_a_provider_the_egress_names_is_asked(tmp_path, monkeypatch, issuer):
    monkeypatch.setenv("MIGKIT_EGRESS", "provider")
    monkeypatch.setenv("MIGKIT_EGRESS_TO", "127.0.0.1")
    app = _oidc(tmp_path, monkeypatch, issuer)
    try:
        c, r = _oidc_sign_in(app, issuer)
        assert r.status_code == 303 and _me(c)[0] == 200
    finally:
        app.store.close()


# ---- what the provider is asked for is a rung on what it published ---------

def test_what_the_provider_is_asked_for_follows_its_metadata():
    got = decide.saml_asks({"key": True, "wants_signed": True,
                            "logout_to": "https://idp.example.com/slo",
                            "demanded": True})
    assert (got["request"].path, got["logout"].path,
            got["assertion"].path) == ("signed", "redirect", "encrypted")
    got = decide.saml_asks({"key": True})
    assert (got["request"].path, got["logout"].path,
            got["assertion"].path) == ("plain", "local", "either")
    # the provider demands signed requests and the view has no key: no
    # quiet unsigned request, a blocker said
    got = decide.saml_asks({"key": False, "wants_signed": True,
                            "logout_to": "https://idp.example.com/slo",
                            "demanded": True})
    assert got["request"].rung is None and "demands signed" in \
        got["request"].reason
    assert got["assertion"].rung is None
    assert got["logout"].path == "local"
