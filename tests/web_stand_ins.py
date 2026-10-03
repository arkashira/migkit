"""What the web view's tests share: an OpenID Connect provider and a SAML
identity provider standing in on this machine - real keys, real
signatures, the provider's side of each protocol - a hops file, and a
browser (werkzeug's test client) that speaks to the view the way one
does. No real identity provider and no database is reached.
"""
import base64
import datetime
import hashlib
import json
import re
import secrets
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

#: the address the view is opened at in the tests
URL = "http://127.0.0.1:8899"

HOPS = """\
access:
  admin: [root@example.com]
  groups:
    shop:
      hops: ["h*"]
      viewer: ["*@example.com"]
      operator: [ann@example.com, "group:dba"]
      approver: [bob@example.com, ann@example.com]
hops:
  h:
    engine: postgres
    source: {host: 10.0.0.1, port: 5432, user: app, password: s3cret-pass-word}
    target: {host: 10.0.0.2, port: 5432, user: app, password: s3cret-pass-word}
    databases: [app]
    options:
      approvals: {steps: [cutover], count: 1}
  other:
    engine: postgres
    source: {host: 10.0.0.3, port: 5432, user: app}
    target: {host: 10.0.0.4, port: 5432, user: app}
    options:
      access: {viewer: [dan@partner.example]}
"""


def person(pid, via="oidc", groups=()):
    return {"id": pid, "name": pid, "issuer": f"{via}:stand-in",
            "subject": "sub-" + pid, "groups": list(groups), "via": via}


def place(tmp_path, monkeypatch, hops=HOPS):
    """The hops file and the reports where the view and a run read them,
    as the environment of a run started from the view says them too."""
    import migkit.config as cfg
    from migkit import ui
    from migkit.web import views
    conf = tmp_path / "hops.yaml"
    conf.write_text(hops)
    reports = tmp_path / "reports"
    reports.mkdir(exist_ok=True)
    state = tmp_path / "state"
    monkeypatch.setattr(cfg, "CONF", conf)
    monkeypatch.setattr(cfg, "REPORTS", reports)
    monkeypatch.setattr(ui, "REPORTS", reports)
    monkeypatch.setenv("MIGKIT_CONF", str(conf))
    monkeypatch.setenv("MIGKIT_REPORTS", str(reports))
    monkeypatch.setenv("MIGKIT_WEB_STATE", str(state))
    monkeypatch.delenv("MIGKIT_ACTOR", raising=False)
    monkeypatch.delenv("MIGKIT_RUN_ID", raising=False)
    # a request is asked under no key of this machine's own user
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "no-such-key"))
    views._LOADED["key"] = None
    return conf, reports, state


def view(tmp_path, **env):
    """The view's application with these settings."""
    from migkit.web import settings
    from migkit.web.app import App
    environ = {"MIGKIT_WEB_STATE": str(tmp_path / "state"), **env}
    return App(settings.load(8899, environ=environ))


@pytest.fixture
def web(tmp_path, monkeypatch):
    place(tmp_path, monkeypatch)
    app = view(tmp_path)
    yield app
    app.store.close()


def browser(app):
    from werkzeug.test import Client
    return Client(app)


def get(c, path, **kw):
    kw.setdefault("base_url", URL)
    return c.get(path, **kw)


def post(c, path, **kw):
    kw.setdefault("base_url", URL)
    return c.post(path, **kw)


def signed_in(app, c, who):
    """A session for `who` (a principal's mapping), its cookie in the
    browser; the page's CSRF token."""
    value, csrf = app.store.open_session(who, 12)
    c.set_cookie(app.cookie, value, domain="127.0.0.1")
    return csrf


def call(c, method, path, csrf=None, body=None, **headers):
    """The API as the view's own page calls it."""
    h = {"Origin": URL, "Sec-Fetch-Site": "same-origin"}
    if csrf:
        h["X-CSRF-Token"] = csrf
    h.update(headers)
    kw = {"headers": h, "base_url": URL}
    if body is not None:
        kw["json"] = body
    return c.open(path, method=method, **kw)


def csrf_in(html):
    got = re.search(r'<meta name="csrf-token" content="([^"]+)"', html)
    return got.group(1) if got else None


# ---- an OpenID Connect provider ---------------------------------------------

def _b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class Issuer:
    """A provider on 127.0.0.1: discovery, keys, the token endpoint (the
    client's secret, the code once, the redirect and PKCE checked), user
    information. The browser's trip to its authorization endpoint is
    `approve`: the person has signed in there."""

    def __init__(self, client_id="migkit", secret="client-secret-0123456789"):
        from joserfc.jwk import RSAKey
        self.key = RSAKey.generate_key(2048, parameters={
            "kid": "k1", "alg": "RS256", "use": "sig"})
        self.client_id, self.secret = client_id, secret
        self.codes = {}
        self.claims = {}          #: put over the next ID tokens' claims
        self.sign_with = None     #: another key: a forged token
        self.alg = "RS256"
        self.says_issuer = None   #: a discovery document naming another
        self.token_calls = 0
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()

    def discovery(self):
        return {"issuer": self.says_issuer or self.url,
                "authorization_endpoint": f"{self.url}/authorize",
                "token_endpoint": f"{self.url}/token",
                "jwks_uri": f"{self.url}/jwks",
                "userinfo_endpoint": f"{self.url}/userinfo",
                "response_types_supported": ["code"],
                "subject_types_supported": ["public"],
                "id_token_signing_alg_values_supported": ["RS256"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported":
                    ["client_secret_basic"]}

    def approve(self, authorize_url, email="ann@example.com", sub="u-ann",
                groups=("dba",)):
        """The code the provider sends the browser back with, once the
        person signed in at its authorization endpoint."""
        q = {k: v[0] for k, v in parse_qs(urlsplit(authorize_url).query
                                          ).items()}
        assert authorize_url.startswith(f"{self.url}/authorize?")
        assert q["response_type"] == "code"
        assert q["client_id"] == self.client_id
        assert q["code_challenge_method"] == "S256"
        assert "openid" in q["scope"].split()
        code = secrets.token_urlsafe(16)
        self.codes[code] = {"nonce": q["nonce"], "sub": sub, "email": email,
                            "groups": list(groups),
                            "challenge": q["code_challenge"],
                            "redirect_uri": q["redirect_uri"]}
        return code, q["state"]

    def id_token(self, rec):
        from joserfc import jwt
        now = int(time.time())
        claims = {"iss": self.url, "sub": rec["sub"], "aud": self.client_id,
                  "exp": now + 300, "iat": now, "nonce": rec["nonce"],
                  "email": rec["email"], "email_verified": True,
                  "groups": rec["groups"], **self.claims}
        if self.alg == "none":
            head = _b64(json.dumps({"alg": "none", "kid": "k1"}).encode())
            return f"{head}.{_b64(json.dumps(claims).encode())}."
        if self.alg == "HS256":
            from joserfc.jwk import OctKey
            key = OctKey.import_key(b"a-shared-secret-anyone-could-guess")
            return jwt.encode({"alg": "HS256", "kid": "k1"}, claims, key,
                              algorithms=["HS256"])
        return jwt.encode({"alg": "RS256", "kid": "k1"}, claims,
                          self.sign_with or self.key)

    def _handler(self):
        issuer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _json(self, code, body):
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                path = urlsplit(self.path).path
                if path == "/.well-known/openid-configuration":
                    return self._json(200, issuer.discovery())
                if path == "/jwks":
                    return self._json(200, {"keys": [
                        issuer.key.as_dict(private=False)]})
                if path == "/userinfo":
                    return self._json(200, {"sub": "u-ann",
                                            "email": "ann@example.com"})
                return self._json(404, {"error": "not_found"})

            def do_POST(self):
                size = int(self.headers.get("Content-Length") or 0)
                form = {k: v[0] for k, v in parse_qs(
                    self.rfile.read(size).decode()).items()}
                if urlsplit(self.path).path != "/token":
                    return self._json(404, {"error": "not_found"})
                issuer.token_calls += 1
                auth = self.headers.get("Authorization", "")
                want = "Basic " + base64.b64encode(
                    f"{issuer.client_id}:{issuer.secret}".encode()).decode()
                if auth != want:
                    return self._json(401, {"error": "invalid_client"})
                rec = issuer.codes.pop(form.get("code"), None)
                if rec is None or form.get("grant_type") != \
                        "authorization_code":
                    return self._json(400, {"error": "invalid_grant"})
                if form.get("redirect_uri") != rec["redirect_uri"]:
                    return self._json(400, {"error": "invalid_grant"})
                made = _b64(hashlib.sha256(
                    form.get("code_verifier", "").encode()).digest())
                if made != rec["challenge"]:
                    return self._json(400, {"error": "invalid_grant"})
                return self._json(200, {
                    "access_token": "at-" + secrets.token_hex(8),
                    "token_type": "Bearer", "expires_in": 300,
                    "id_token": issuer.id_token(rec)})
        return Handler


# ---- a SAML identity provider ------------------------------------------------

SAML_P = "urn:oasis:names:tc:SAML:2.0:protocol"
SAML_A = "urn:oasis:names:tc:SAML:2.0:assertion"
EXC_C14N = "http://www.w3.org/2001/10/xml-exc-c14n#"


def _instant(offset=0):
    return datetime.datetime.fromtimestamp(
        time.time() + offset, datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


class SamlIdp:
    """A SAML identity provider: its metadata written where the view reads
    it, and signed answers to the view's requests - or answers tampered
    with the ways an attacker would."""

    ENTITY = "https://idp.example.com/saml"
    SSO = "https://idp.example.com/saml/sso"

    def __init__(self, where):
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        self.key = rsa.generate_private_key(public_exponent=65537,
                                            key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                             "idp.example.com")])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name)
                .issuer_name(name).public_key(self.key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=30))
                .sign(self.key, hashes.SHA256()))
        self.cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
        self.key_pem = self.key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
        der = base64.b64encode(cert.public_bytes(
            serialization.Encoding.DER)).decode()
        self.metadata = where / "idp-metadata.xml"
        self.metadata.write_text(
            '<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:'
            f'metadata" entityID="{self.ENTITY}"><md:IDPSSODescriptor'
            ' protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:'
            'protocol"><md:KeyDescriptor use="signing"><ds:KeyInfo'
            ' xmlns:ds="http://www.w3.org/2000/09/xmldsig#"><ds:X509Data>'
            f"<ds:X509Certificate>{der}</ds:X509Certificate></ds:X509Data>"
            "</ds:KeyInfo></md:KeyDescriptor><md:SingleSignOnService"
            ' Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"'
            f' Location="{self.SSO}"/></md:IDPSSODescriptor>'
            "</md:EntityDescriptor>")

    @staticmethod
    def request_of(redirect_url):
        """(the request's ID, its RelayState, the request's XML) of the
        view's redirect to the provider."""
        q = {k: v[0] for k, v in parse_qs(urlsplit(redirect_url).query
                                          ).items()}
        xml = zlib.decompress(base64.b64decode(q["SAMLRequest"]), -15)
        rid = re.search(rb'ID="([^"]+)"', xml).group(1).decode()
        return rid, q.get("RelayState", ""), xml.decode()

    def _assertion(self, rid, name, groups, audience, recipient, life,
                   aid=None):
        aid = aid or "_a" + secrets.token_hex(8)
        attrs = "".join(f"<saml:AttributeValue>{g}</saml:AttributeValue>"
                        for g in groups)
        return (
            f'<saml:Assertion xmlns:saml="{SAML_A}" ID="{aid}" Version="2.0"'
            f' IssueInstant="{_instant()}"><saml:Issuer>{self.ENTITY}'
            "</saml:Issuer><saml:Subject><saml:NameID Format="
            '"urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress">'
            f"{name}</saml:NameID><saml:SubjectConfirmation Method="
            '"urn:oasis:names:tc:SAML:2.0:cm:bearer">'
            f'<saml:SubjectConfirmationData InResponseTo="{rid}"'
            f' Recipient="{recipient}" NotOnOrAfter="{_instant(life)}"/>'
            "</saml:SubjectConfirmation></saml:Subject><saml:Conditions"
            f' NotBefore="{_instant(-60)}" NotOnOrAfter="{_instant(life)}">'
            f"<saml:AudienceRestriction><saml:Audience>{audience}"
            "</saml:Audience></saml:AudienceRestriction></saml:Conditions>"
            f'<saml:AuthnStatement AuthnInstant="{_instant()}"'
            f' SessionIndex="_s1"/><saml:AttributeStatement><saml:Attribute'
            f' Name="groups">{attrs}</saml:Attribute>'
            "</saml:AttributeStatement></saml:Assertion>")

    def sign(self, xml):
        from lxml import etree
        from signxml import XMLSigner
        el = etree.fromstring(xml.encode())
        signed = XMLSigner(c14n_algorithm=EXC_C14N).sign(
            el, key=self.key_pem, cert=self.cert_pem,
            reference_uri=el.get("ID"))
        return etree.tostring(signed).decode()

    def answer(self, redirect_url, name="ann@example.com",
               groups=("change-board",), sp=URL, sign=True, audience=None,
               recipient=None, in_response_to=None, life=300,
               encrypted=False, beside=None, status="Success"):
        """(SAMLResponse, RelayState) as the provider posts them back."""
        rid, relay, _ = self.request_of(redirect_url)
        acs = f"{sp}/auth/saml/acs"
        answered = in_response_to or rid
        a = self._assertion(answered, name, groups,
                            audience or f"{sp}/auth/saml/metadata",
                            recipient or acs, life)
        if sign:
            a = self.sign(a)
        if encrypted:
            a = (f'<saml:EncryptedAssertion xmlns:saml="{SAML_A}">'
                 "<xenc:EncryptedData xmlns:xenc="
                 '"http://www.w3.org/2001/04/xmlenc#"/>'
                 "</saml:EncryptedAssertion>")
        if beside is not None:
            # an attacker's own assertion put beside the signed one
            a = self._assertion(answered, beside, ("migkit-admins",),
                                f"{sp}/auth/saml/metadata", acs, life) + a
        xml = (f'<samlp:Response xmlns:samlp="{SAML_P}"'
               f' xmlns:saml="{SAML_A}" ID="_r{secrets.token_hex(8)}"'
               f' Version="2.0" IssueInstant="{_instant()}"'
               f' Destination="{acs}" InResponseTo="{answered}">'
               f"<saml:Issuer>{self.ENTITY}</saml:Issuer><samlp:Status>"
               '<samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:'
               f'status:{status}"/></samlp:Status>{a}</samlp:Response>')
        return base64.b64encode(xml.encode()).decode(), relay
