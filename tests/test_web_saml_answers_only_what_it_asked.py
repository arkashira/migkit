"""People sign in through a SAML identity provider: the view reads only a
signed answer to a request it made, once, out of the element the
signature covers (backlog W2).

The provider is a stand-in (`web_stand_ins.SamlIdp`) with its own key and
certificate, signing assertions the way providers do. Each attack a SAML
service provider has to withstand is tried and refused - an assertion
changed after signing, one not signed, one for another service, one sent
twice, one the provider made up without a request, an attacker's
assertion put beside a signed one, an expired one, one encrypted, one
finished in another browser - and the same answer untouched signs the
person in, so each refusal is for its own reason.
"""
import base64

import pytest

from tests.web_stand_ins import SamlIdp, browser, get, place, post, view


@pytest.fixture
def saml(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    idp = SamlIdp(tmp_path)
    app = view(tmp_path, MIGKIT_WEB_SAML_METADATA=str(idp.metadata))
    yield app, idp
    app.store.close()


def _start(c):
    r = get(c, "/auth/saml/start?next=/hops/h")
    assert r.status_code == 303
    where = r.headers["Location"]
    assert where.startswith(SamlIdp.SSO + "?")
    return where


def _post(c, response, relay):
    return post(c, "/auth/saml/acs",
                data={"SAMLResponse": response, "RelayState": relay})


def _me(c):
    r = get(c, "/api/v1/me")
    return r.status_code, (r.get_json() if r.status_code == 200 else None)


def test_a_signed_answer_signs_the_person_in(saml):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    rid, relay, xml = idp.request_of(where)
    # the request names this view and where to answer
    assert 'AssertionConsumerServiceURL="http://127.0.0.1:8899/auth/saml/acs"' \
        in xml
    assert "<saml:Issuer>http://127.0.0.1:8899/auth/saml/metadata" in xml
    r = _post(c, *idp.answer(where))
    assert r.status_code == 303 and r.headers["Location"] == "/hops/h"
    status, me = _me(c)
    assert status == 200 and me["id"] == "ann@example.com"
    assert me["via"] == "saml" and me["groups"] == ["change-board"]


def test_the_views_metadata_is_what_a_provider_reads(saml):
    app, _ = saml
    r = get(browser(app), "/auth/saml/metadata")
    assert r.status_code == 200
    body = r.data.decode()
    assert 'entityID="http://127.0.0.1:8899/auth/saml/metadata"' in body
    assert 'Location="http://127.0.0.1:8899/auth/saml/acs"' in body
    assert 'WantAssertionsSigned="true"' in body


def _tampered(response):
    raw = base64.b64decode(response).replace(b"ann@example.com",
                                             b"root@example.com")
    return base64.b64encode(raw).decode()


@pytest.mark.parametrize("attack,words", [
    ("changed after signing", b"signature does not hold"),
    ("not signed", b"signature does not hold"),
    ("for another service", b"issued for another service"),
    ("for another recipient", b"not confirmed for this service"),
    ("expired", b"has expired"),
    ("encrypted", b"encrypted assertion"),
    ("an assertion beside the signed one", b"more than one assertion"),
    ("failed at the provider", b"did not sign the person in"),
])
def test_an_answer_that_does_not_hold_signs_nobody_in(saml, attack, words):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    if attack == "changed after signing":
        response, relay = idp.answer(where)
        response = _tampered(response)
    elif attack == "not signed":
        response, relay = idp.answer(where, sign=False)
    elif attack == "for another service":
        response, relay = idp.answer(where,
                                     audience="https://other.example.com")
    elif attack == "for another recipient":
        response, relay = idp.answer(
            where, recipient="https://other.example.com/acs")
    elif attack == "expired":
        response, relay = idp.answer(where, life=-600)
    elif attack == "encrypted":
        response, relay = idp.answer(where, encrypted=True)
    elif attack == "an assertion beside the signed one":
        response, relay = idp.answer(where, beside="root@example.com")
    else:
        response, relay = idp.answer(where, status="Responder")
    r = _post(c, response, relay)
    assert r.status_code == 403, attack
    assert words in r.data, (attack, r.data[:500])
    assert _me(c)[0] == 401, attack


def test_the_attacks_fail_for_their_own_reason(saml):
    """The same steps with nothing changed sign in: a view refusing every
    answer would pass the test above."""
    app, idp = saml
    c = browser(app)
    where = _start(c)
    assert _post(c, *idp.answer(where)).status_code == 303
    assert _me(c)[0] == 200


def test_an_answer_counts_once(saml):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    answer = idp.answer(where)
    assert _post(c, *answer).status_code == 303
    again = browser(app)
    again.set_cookie(app.signin_cookie, "whatever", domain="127.0.0.1")
    r = _post(again, *answer)
    assert r.status_code == 403
    assert b"not to a sign-in this view started" in r.data
    assert _me(again)[0] == 401


def test_a_sign_in_the_provider_started_by_itself_is_not_taken(saml):
    app, idp = saml
    c = browser(app)
    where = _start(c)
    r = _post(c, *idp.answer(where, in_response_to="_never-asked"))
    assert r.status_code == 403
    assert b"not to a sign-in this view started" in r.data


def test_an_answer_posted_from_another_browser_signs_nobody_in(saml):
    app, idp = saml
    started, other = browser(app), browser(app)
    where = _start(started)
    r = _post(other, *idp.answer(where))
    assert r.status_code == 403 and b"another browser" in r.data
    assert _me(other)[0] == 401
