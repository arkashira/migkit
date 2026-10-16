"""Who is asking: signed in by the organisation's identity provider -
OpenID Connect or SAML - or, on an install one operator runs without
one, by the single local account (backlog W2).

Multi-factor sign-in, password rules and account recovery are the
provider's, where it is one: the view asks it who someone is and reads
the answer, and holds no password of theirs.

**OpenID Connect** (`Oidc`): the authorization-code flow with PKCE
(S256), a `state` and a `nonce` of its own each time, both kept on the
server and bound to the browser that started - a code handed to someone
else's browser signs nobody in. The provider is found from its issuer's
discovery document, which must name the same issuer (a provider mixed up
for another is refused), and its answer must name it too where it says
one (RFC 9207). The ID token is checked against the provider's published
keys - asymmetric algorithms only, never `none` or a shared secret - for
issuer, audience (and the authorised party where it names several),
expiry, issue time and the nonce this sign-in sent. A provider whose
e-mail claim says it is not verified is refused.

**SAML 2.0** (`Saml`): the view asks by the redirect binding and reads
the answer the provider posts back. Only an answer to a request this view
made is read (no sign-in the provider starts on its own), once; its
signature is checked against the certificate in the provider's metadata,
and everything read afterwards is read out of the element the signature
covers - never out of the document around it, which is where a forged
assertion beside a signed one would sit. Issuer, audience, recipient,
the times and the request it answers are checked; an assertion is taken
once. The view holds a key and certificate of its own (in its state
directory, published in its metadata): an encrypted assertion is opened
with it (RSA-OAEP key transport; AES-GCM or AES-CBC content - never
RSA-1.5), and where the operator demands encryption
(`MIGKIT_WEB_SAML_ENCRYPTED`) an assertion that is not encrypted is
refused. What the provider is asked for is a rung on its published
metadata (`decide.saml_asks`): its sign-in request is signed with that key
where the metadata says the provider takes only signed ones; a sign-out
goes to the provider's single logout by the redirect binding, signed, and
a logout the provider starts is taken only signed, ends the sessions it
names here and is answered. Every call to the provider goes through the
egress gate first (`provider` kind): a refusal is a sign-in that cannot be
made, said, with no connection opened.

**The local account** (`Local`): one, for the install one operator runs
without a provider; its password hashed with Argon2id; five wrong
passwords in a row and each next one doubles the wait (`store`). It is
set, and set again, only through a one-time address the server prints
when it starts (`MIGKIT_WEB_RESET` to print one where an account
exists). With a provider there is no password sign-in at all.

**Provisioning** (`scim`): where the provider provisions people, only
someone it provisioned and has not deactivated signs in, their groups
are the ones it gave them as well as the ones its tokens carry, and a
person deactivated or deleted there is signed out everywhere at once.
"""
import base64
import datetime
import json
import os
import secrets
import time
import zlib
from dataclasses import dataclass, field, replace
from urllib.parse import urlencode

#: seconds a sign-in may take from the redirect to the answer
PENDING = 600
#: seconds of clock difference accepted between the provider and here
SKEW = 120


class Refused(Exception):
    """A sign-in refused, with the reason the page shows."""


@dataclass(frozen=True)
class Principal:
    """A person as the view knows them: `id` their name as the provider
    gives it (an e-mail address where it gives one), `issuer` who said so,
    `subject` the provider's own identifier, `groups` theirs, `via` how
    they signed in (oidc, saml, local, or token for the API), `session`
    the provider's own reference to that sign-in (a SAML SessionIndex),
    which a sign-out from the provider names."""
    id: str
    name: str = ""
    issuer: str = ""
    subject: str = ""
    groups: tuple = field(default_factory=tuple)
    via: str = ""
    session: str = ""

    def as_dict(self):
        return {"id": self.id, "name": self.name, "issuer": self.issuer,
                "subject": self.subject, "groups": list(self.groups),
                "via": self.via, "session": self.session}

    @classmethod
    def from_dict(cls, d):
        d = dict(d or {})
        return cls(id=str(d.get("id") or ""), name=str(d.get("name") or ""),
                   issuer=str(d.get("issuer") or ""),
                   subject=str(d.get("subject") or ""),
                   groups=tuple(str(g) for g in d.get("groups") or ()),
                   via=str(d.get("via") or ""),
                   session=str(d.get("session") or ""))


def admitted(principal, store, settings):
    """The person as they may act now: where the provider provisions
    people, only one it provisioned and has not deactivated, with the
    groups it gave them added; Refused otherwise."""
    if not settings.scim or principal.via == "local":
        return principal
    person = store.person(principal.id)
    if person is None:
        raise Refused(f"{principal.id} is not among the people the identity"
                      " provider provisioned to this view - ask for access"
                      " there")
    if not person["active"]:
        raise Refused(f"{principal.id} has been deactivated by the identity"
                      " provider")
    groups = tuple(dict.fromkeys(tuple(principal.groups)
                                 + tuple(person["groups"])))
    return replace(principal, groups=groups)


def _groups(value):
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(v) for v in value if v is not None)


# ---- OpenID Connect ---------------------------------------------------------

#: the ID token algorithms taken: asymmetric only
ALGORITHMS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512",
              "ES256", "ES384", "ES512", "EdDSA")
#: seconds the discovery document and the keys are kept before asking again
KEPT = 3600


def _gate(url):
    """Every call to an identity provider asks the egress gate first (the
    `provider` kind, the machine's setting alone: no hop is signing
    anyone in). Refused, no address is read and no connection opened."""
    from .. import egress
    if egress.permit(None, "provider", address=url):
        return
    why = egress.decide(None, "provider", address=url)[1]
    raise Refused(f"{why}; let them out (MIGKIT_EGRESS, MIGKIT_EGRESS_TO)"
                  " to sign in through the identity provider")


class Oidc:
    """The view as a client of one OpenID Connect provider."""

    def __init__(self, settings, store, http=None):
        import requests
        self.s, self.store = settings, store
        self.http = http or requests.Session()
        self._meta, self._meta_at = None, 0.0
        self._keys, self._keys_at = None, 0.0
        self.redirect = f"{settings.url}/auth/oidc/callback"

    def _get(self, url):
        _gate(url)
        got = self.http.get(url, timeout=10)
        got.raise_for_status()
        return got.json()

    def metadata(self):
        """The provider's discovery document, checked: it names the issuer
        configured, and the endpoints and keys the flow needs."""
        from authlib.oidc.discovery import OpenIDProviderMetadata
        if self._meta is not None and time.time() - self._meta_at < KEPT:
            return self._meta
        issuer = self.s.oidc_issuer
        try:
            got = self._get(f"{issuer}/.well-known/openid-configuration")
        except Refused:
            raise
        except Exception as e:  # noqa: BLE001 - said, by its kind
            raise Refused("the identity provider's discovery document could"
                          f" not be read ({type(e).__name__})")
        if str(got.get("issuer", "")).rstrip("/") != issuer:
            raise Refused("the identity provider's discovery document names"
                          f" another issuer ({got.get('issuer')!r}), not"
                          f" {issuer} - refused as a provider mixed up for"
                          " another")
        meta = OpenIDProviderMetadata(got)
        try:
            for check in (meta.validate_authorization_endpoint,
                          meta.validate_token_endpoint,
                          meta.validate_jwks_uri,
                          meta.validate_response_types_supported,
                          meta.validate_id_token_signing_alg_values_supported,
                          meta.validate_code_challenge_methods_supported):
                check()
        except ValueError as e:
            raise Refused(f"the identity provider's discovery document is"
                          f" not usable: {e}")
        methods = got.get("code_challenge_methods_supported")
        if methods is not None and "S256" not in methods:
            raise Refused("the identity provider does not take PKCE with"
                          " S256, which this view needs")
        self._meta, self._meta_at = dict(got), time.time()
        return self._meta

    def keys(self, again=False):
        from joserfc.jwk import KeySet
        if self._keys is not None and not again \
                and time.time() - self._keys_at < KEPT:
            return self._keys
        try:
            got = self._get(self.metadata()["jwks_uri"])
            self._keys = KeySet.import_key_set(got)
        except Refused:
            raise
        except Exception as e:  # noqa: BLE001 - said, by its kind
            raise Refused("the identity provider's keys could not be read"
                          f" ({type(e).__name__})")
        self._keys_at = time.time()
        return self._keys

    def _session(self):
        from authlib.integrations.requests_client import OAuth2Session
        return OAuth2Session(
            self.s.oidc_client, self.s.oidc_secret,
            scope=" ".join(self.s.oidc_scopes), redirect_uri=self.redirect,
            code_challenge_method="S256",
            token_endpoint_auth_method="client_secret_basic",
            default_timeout=10)

    def begin(self, return_to):
        """(the provider's address to send the browser to, the value the
        browser keeps to finish it)."""
        from authlib.common.security import generate_token
        meta = self.metadata()
        state, nonce = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
        verifier, browser = generate_token(64), secrets.token_urlsafe(24)
        url, _ = self._session().create_authorization_url(
            meta["authorization_endpoint"], state=state,
            code_verifier=verifier, nonce=nonce)
        from .store import digest
        self.store.put_pending("oidc", state, {
            "nonce": nonce, "verifier": verifier, "browser": digest(browser),
            "return_to": return_to}, PENDING)
        return url, browser

    def finish(self, args, browser):
        """(the person the provider's answer names, where to go on, None);
        Refused where it is not an answer to a sign-in this browser started
        here, or does not hold."""
        from .store import digest
        if args.get("error"):
            raise Refused("the identity provider refused the sign-in: "
                          + str(args.get("error_description")
                                or args.get("error"))[:200])
        got = self.store.take_pending("oidc", args.get("state"))
        if got is None:
            raise Refused("this answer is not to a sign-in this view"
                          " started, or it was used already or took too"
                          " long - sign in again")
        if not browser or digest(browser) != got["browser"]:
            raise Refused("this sign-in was started in another browser -"
                          " sign in again from this one")
        iss = args.get("iss")
        if iss is not None and str(iss).rstrip("/") != self.s.oidc_issuer:
            raise Refused("the answer came from another issuer than the one"
                          " asked")
        code = args.get("code")
        if not code:
            raise Refused("the identity provider's answer holds no code")
        meta = self.metadata()
        _gate(meta["token_endpoint"])
        try:
            token = self._session().fetch_token(
                meta["token_endpoint"], grant_type="authorization_code",
                code=code, code_verifier=got["verifier"],
                redirect_uri=self.redirect)
        except Exception as e:  # noqa: BLE001 - said, by its kind
            raise Refused("the code could not be exchanged at the identity"
                          f" provider ({type(e).__name__})")
        idt = token.get("id_token")
        if not idt:
            raise Refused("the identity provider returned no ID token")
        claims = self.verify(idt, got["nonce"])
        info = {}
        if (not claims.get("email") or self.s.oidc_groups not in claims) \
                and meta.get("userinfo_endpoint") \
                and token.get("access_token"):
            info = self._userinfo(meta["userinfo_endpoint"],
                                  token["access_token"], claims["sub"])
        merged = {**info, **claims}
        if merged.get("email_verified") is False:
            raise Refused("the identity provider says this e-mail address is"
                          " not verified")
        who = str(merged.get("email") or merged.get("preferred_username")
                  or claims["sub"])
        return Principal(
            id=who.lower() if "@" in who else who,
            name=str(merged.get("name") or who),
            issuer=f"oidc:{self.s.oidc_issuer}",
            subject=str(claims["sub"]),
            groups=_groups(merged.get(self.s.oidc_groups)), via="oidc"), \
            got["return_to"], None

    def _userinfo(self, url, access_token, sub):
        try:
            _gate(url)
            got = self.http.get(url, timeout=10, headers={
                "Authorization": f"Bearer {access_token}"})
            got.raise_for_status()
            info = got.json()
        except Refused:
            # the egress gate refused: signing in without the groups the
            # provider would have given would grant less, silently
            raise
        except Exception:  # noqa: BLE001 - the ID token's claims stand
            return {}
        if str(info.get("sub")) != str(sub):
            raise Refused("the identity provider's user information names"
                          " another person than its ID token")
        return info

    def algorithms(self):
        offered = self.metadata().get(
            "id_token_signing_alg_values_supported") or ALGORITHMS
        return [a for a in offered if a in ALGORITHMS] or ["RS256"]

    def verify(self, id_token, nonce):
        """The ID token's claims, checked."""
        from joserfc import jwt
        from joserfc.errors import JoseError
        try:
            try:
                tok = jwt.decode(id_token, self.keys(),
                                 algorithms=self.algorithms())
            except JoseError:
                # a provider that rolled its keys: asked once more
                tok = jwt.decode(id_token, self.keys(again=True),
                                 algorithms=self.algorithms())
            claims = dict(tok.claims)
            jwt.JWTClaimsRegistry(
                leeway=SKEW,
                iss={"essential": True, "value": self.s.oidc_issuer},
                sub={"essential": True},
                aud={"essential": True, "value": self.s.oidc_client},
                exp={"essential": True}, iat={"essential": True},
                nonce={"essential": True, "value": nonce},
            ).validate(claims)
        except Refused:
            raise
        except JoseError as e:
            raise Refused(f"the ID token does not hold: {e}")
        except (ValueError, TypeError, KeyError) as e:
            # a token that is not one at all: no part of it is believed
            raise Refused("the ID token does not hold: it could not be read"
                          f" ({type(e).__name__})")
        aud = claims.get("aud")
        if isinstance(aud, list) and len(aud) > 1 \
                and claims.get("azp") != self.s.oidc_client:
            raise Refused("the ID token was issued to another party")
        return claims


# ---- SAML 2.0 ---------------------------------------------------------------

NS = {"md": "urn:oasis:names:tc:SAML:2.0:metadata",
      "ds": "http://www.w3.org/2000/09/xmldsig#",
      "saml": "urn:oasis:names:tc:SAML:2.0:assertion",
      "samlp": "urn:oasis:names:tc:SAML:2.0:protocol"}
REDIRECT = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
POST = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
BEARER = "urn:oasis:names:tc:SAML:2.0:cm:bearer"
SUCCESS = "urn:oasis:names:tc:SAML:2.0:status:Success"
#: the most of an answer read
SAML_MOST = 512 * 1024


def _parser():
    from lxml import etree
    return etree.XMLParser(resolve_entities=False, no_network=True,
                           huge_tree=False, load_dtd=False)


def _pem(b64):
    body = "".join(str(b64).split())
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    return ("-----BEGIN CERTIFICATE-----\n" + "\n".join(lines)
            + "\n-----END CERTIFICATE-----\n")


def _when(text):
    """A SAML time as seconds since the epoch."""
    t = str(text).strip().replace("Z", "+00:00")
    if "." in t:
        head, _, rest = t.partition(".")
        digits = "".join(c for c in rest if c.isdigit())
        zone = rest[len(digits):]
        t = f"{head}.{digits[:6]}{zone}"
    got = datetime.datetime.fromisoformat(t)
    if got.tzinfo is None:
        got = got.replace(tzinfo=datetime.timezone.utc)
    return got.timestamp()


def _instant(epoch=None):
    return datetime.datetime.fromtimestamp(
        epoch or time.time(), datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _text(el, path):
    got = el.find(path, NS)
    return (got.text or "").strip() if got is not None else ""


#: the signature algorithms a redirect-bound message may carry: RSA over
#: SHA-2, never SHA-1
RSA_SIGS = {"http://www.w3.org/2001/04/xmldsig-more#rsa-sha256": "SHA256",
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha384": "SHA384",
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512": "SHA512"}
RSA_SHA256 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
XENC = "http://www.w3.org/2001/04/xmlenc#"
XENC11 = "http://www.w3.org/2009/xmlenc11#"
NS["xenc"] = XENC
NS["xenc11"] = XENC11
#: the content ciphers an encrypted assertion may use, by key bytes
GCM = {XENC11 + "aes128-gcm": 16, XENC11 + "aes192-gcm": 24,
       XENC11 + "aes256-gcm": 32}
def _cipher(enc):
    """The content cipher an EncryptedAssertion names, or None."""
    method = enc.find("xenc:EncryptedData/xenc:EncryptionMethod", NS)
    return method.get("Algorithm") if method is not None else None


CBC = {XENC + "aes128-cbc": 16, XENC + "aes192-cbc": 24,
       XENC + "aes256-cbc": 32}
DIGESTS = {"http://www.w3.org/2000/09/xmldsig#sha1": "SHA1",
           XENC + "sha256": "SHA256", XENC + "sha512": "SHA512",
           "http://www.w3.org/2001/04/xmldsig-more#sha384": "SHA384"}
MGFS = {XENC11 + "mgf1sha1": "SHA1", XENC11 + "mgf1sha256": "SHA256",
        XENC11 + "mgf1sha384": "SHA384", XENC11 + "mgf1sha512": "SHA512"}
#: the longest a redirect-bound message is read, inflated
DEFLATED_MOST = 256 * 1024


def _hash(name):
    from cryptography.hazmat.primitives import hashes
    return getattr(hashes, name)()


def _own_key(state):
    """(the view's private key, its certificate as PEM and as base64 DER):
    made once in the state directory (0600) and read back after; the
    certificate is what its metadata publishes, for the provider to
    encrypt an assertion to and to check what the view signs with."""
    from pathlib import Path
    from urllib.parse import urlsplit

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    home = Path(state)
    home.mkdir(parents=True, exist_ok=True)
    kf, cf = home / "saml-sp.key", home / "saml-sp.crt"
    if kf.is_file() and cf.is_file():
        key = serialization.load_pem_private_key(kf.read_bytes(), None)
        cert = x509.load_pem_x509_certificate(cf.read_bytes())
    else:
        key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                             "migkit-web")])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder().subject_name(name)
                .issuer_name(name).public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=3650))
                .sign(key, hashes.SHA256()))
        fd = os.open(kf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as out:
            out.write(key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption()))
        cf.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    der = base64.b64encode(cert.public_bytes(
        serialization.Encoding.DER)).decode()
    return key, cert.public_bytes(serialization.Encoding.PEM).decode(), der


class Saml:
    """The view as a service provider of one SAML identity provider."""

    def __init__(self, settings, store):
        from pathlib import Path

        from lxml import etree

        from .. import decide
        self.s, self.store = settings, store
        self.entity = f"{settings.url}/auth/saml/metadata"
        self.acs = f"{settings.url}/auth/saml/acs"
        self.slo = f"{settings.url}/auth/saml/slo"
        raw = Path(settings.saml_metadata).expanduser().read_bytes()
        root = etree.fromstring(raw, parser=_parser())
        found = [root] if root.tag == f"{{{NS['md']}}}EntityDescriptor" \
            else root.findall(".//md:EntityDescriptor", NS)
        idp = None
        for ed in found:
            idp = ed.find("md:IDPSSODescriptor", NS)
            if idp is not None:
                self.idp_entity = ed.get("entityID")
                break
        if idp is None:
            raise SystemExit(f"{settings.saml_metadata} describes no"
                             " identity provider (no IDPSSODescriptor)")
        sso = [s.get("Location") for s in idp.findall(
            "md:SingleSignOnService", NS) if s.get("Binding") == REDIRECT]
        if not sso:
            raise SystemExit(f"{settings.saml_metadata}: the identity"
                             " provider offers no sign-in by redirect")
        self.sso = sso[0]
        self.certs = [_pem(c.text) for kd in idp.findall(
            "md:KeyDescriptor", NS) if kd.get("use") in (None, "signing")
            for c in kd.findall("ds:KeyInfo/ds:X509Data/ds:X509Certificate",
                                NS) if c.text]
        if not self.certs:
            raise SystemExit(f"{settings.saml_metadata}: no signing"
                             " certificate - an answer could not be proved")
        out = [(s.get("ResponseLocation") or s.get("Location"),
                s.get("Location")) for s in idp.findall(
            "md:SingleLogoutService", NS) if s.get("Binding") == REDIRECT]
        #: where a logout request, and where its answer, goes at the provider
        self.idp_slo = out[0][1] if out else ""
        self.idp_slo_answer = out[0][0] if out else ""
        self.key, self.cert_pem, self.cert_der = _own_key(settings.state)
        #: what the provider is asked for, a rung on what it published
        self.asks = decide.saml_asks({
            "key": True, "logout_to": self.idp_slo,
            "wants_signed": idp.get("WantAuthnRequestsSigned") == "true",
            "demanded": bool(settings.saml_encrypted)})
        for part, got in self.asks.items():
            if got.rung is None:
                raise SystemExit(f"{settings.saml_metadata}: {got.work}:"
                                 f" {got.reason}")

    @property
    def logs_out(self):
        """Whether a sign-out reaches the provider (a rung on its metadata)."""
        return self.asks["logout"].path == "redirect"

    def metadata_xml(self):
        """This view as a service provider, for the identity provider."""
        from xml.sax.saxutils import quoteattr
        signed = "true" if self.asks["request"].path == "signed" else "false"
        keys = "".join(
            f'<md:KeyDescriptor use="{use}"><ds:KeyInfo xmlns:ds='
            f'"{NS["ds"]}"><ds:X509Data><ds:X509Certificate>'
            f"{self.cert_der}</ds:X509Certificate></ds:X509Data>"
            "</ds:KeyInfo></md:KeyDescriptor>"
            for use in ("signing", "encryption"))
        slo = (f'<md:SingleLogoutService Binding="{REDIRECT}"'
               f" Location={quoteattr(self.slo)}/>") if self.logs_out else ""
        return (
            f'<?xml version="1.0"?>\n<md:EntityDescriptor xmlns:md='
            f'"{NS["md"]}" entityID={quoteattr(self.entity)}>'
            f'<md:SPSSODescriptor AuthnRequestsSigned="{signed}"'
            ' WantAssertionsSigned="true" protocolSupportEnumeration='
            f'"urn:oasis:names:tc:SAML:2.0:protocol">{keys}{slo}'
            "<md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:"
            "emailAddress</md:NameIDFormat>"
            f'<md:AssertionConsumerService Binding="{POST}"'
            f" Location={quoteattr(self.acs)} index=\"0\""
            ' isDefault="true"/></md:SPSSODescriptor></md:EntityDescriptor>'
            "\n")

    # ---- the redirect binding, signed and checked ----------------------

    def _redirect_to(self, dest, param, xml, relay=None, sign=False):
        """The provider's address `dest` with `xml` deflated on it, and
        the view's signature over it where `sign`."""
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        packer = zlib.compressobj(9, zlib.DEFLATED, -15)
        packed = packer.compress(xml.encode()) + packer.flush()
        pairs = [(param, base64.b64encode(packed).decode())]
        if relay:
            pairs.append(("RelayState", relay))
        if sign:
            pairs.append(("SigAlg", RSA_SHA256))
        query = urlencode(pairs)
        if sign:
            sig = self.key.sign(query.encode(), padding.PKCS1v15(),
                                hashes.SHA256())
            query += "&" + urlencode(
                {"Signature": base64.b64encode(sig).decode()})
        return dest + ("&" if "?" in dest else "?") + query

    def _redirect_ok(self, raw_query, param):
        """Whether the message in `param` of this query, as it was
        received, carries the provider's signature (RSA over SHA-2)."""
        from urllib.parse import unquote

        from cryptography import x509
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric import padding, rsa
        pieces = {}
        for part in str(raw_query or "").split("&"):
            k, _, _v = part.partition("=")
            if k in pieces:
                return False
            pieces[k] = part
        alg = unquote(pieces.get("SigAlg", "=").partition("=")[2])
        if param not in pieces or "Signature" not in pieces \
                or alg not in RSA_SIGS:
            return False
        data = "&".join(pieces[k] for k in (param, "RelayState", "SigAlg")
                        if k in pieces)
        try:
            sig = base64.b64decode(unquote(
                pieces["Signature"].partition("=")[2]), validate=True)
        except ValueError:
            return False
        for cert in self.certs:
            pub = x509.load_pem_x509_certificate(cert.encode()).public_key()
            if not isinstance(pub, rsa.RSAPublicKey):
                continue
            try:
                pub.verify(sig, data.encode(), padding.PKCS1v15(),
                           _hash(RSA_SIGS[alg]))
                return True
            except InvalidSignature:
                continue
        return False

    @staticmethod
    def _inflate(text):
        """The XML a redirect-bound message carries, or Refused."""
        try:
            packed = base64.b64decode(str(text), validate=True)
            gone = zlib.decompressobj(-15)
            raw = gone.decompress(packed, DEFLATED_MOST)
            if gone.unconsumed_tail:
                raise ValueError("too large")
            from lxml import etree
            return etree.fromstring(raw, parser=_parser())
        except Exception:  # noqa: BLE001 - not a message at all
            raise Refused("the identity provider's message could not be"
                          " read")

    # ---- sign-in ---------------------------------------------------------

    def begin(self, return_to):
        """(the provider's address with the request, the browser's
        value)."""
        from xml.sax.saxutils import escape, quoteattr
        rid = "_" + secrets.token_hex(16)
        xml = (f'<samlp:AuthnRequest xmlns:samlp="{NS["samlp"]}"'
               f' xmlns:saml="{NS["saml"]}" ID="{rid}" Version="2.0"'
               f' IssueInstant="{_instant()}"'
               f" Destination={quoteattr(self.sso)}"
               f" AssertionConsumerServiceURL={quoteattr(self.acs)}"
               f' ProtocolBinding="{POST}">'
               f"<saml:Issuer>{escape(self.entity)}</saml:Issuer>"
               '<samlp:NameIDPolicy AllowCreate="true"/>'
               "</samlp:AuthnRequest>")
        relay = secrets.token_urlsafe(16)
        browser = secrets.token_urlsafe(24)
        from .store import digest
        self.store.put_pending("saml", rid, {
            "browser": digest(browser), "relay": relay,
            "return_to": return_to}, PENDING)
        return self._redirect_to(
            self.sso, "SAMLRequest", xml, relay,
            sign=self.asks["request"].path == "signed"), browser

    def _open(self, enc):
        """The assertion inside an EncryptedAssertion, opened with the
        view's key; Refused where it cannot be: RSA-OAEP key transport
        only (RSA-1.5 is refused), AES-GCM or AES-CBC for the content."""
        import base64 as b64

        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.ciphers import (
            Cipher, algorithms, modes)
        from lxml import etree
        bad = Refused("the encrypted assertion could not be opened with"
                      " this view's key")
        try:
            data = enc.find("xenc:EncryptedData", NS)
            method = data.find("xenc:EncryptionMethod", NS).get("Algorithm")
            if method not in GCM and method not in CBC:
                raise Refused("the assertion is encrypted with a cipher this"
                              " view does not take")
            key = None
            for ek in enc.iter(f"{{{XENC}}}EncryptedKey"):
                how = ek.find("xenc:EncryptionMethod", NS)
                alg = how.get("Algorithm")
                if alg == XENC + "rsa-oaep-mgf1p":
                    dig, mgf = "SHA1", "SHA1"
                elif alg == XENC11 + "rsa-oaep":
                    d = how.find("ds:DigestMethod", NS)
                    m = how.find("xenc11:MGF", NS)
                    dig = DIGESTS.get(d.get("Algorithm")) \
                        if d is not None else "SHA1"
                    mgf = MGFS.get(m.get("Algorithm")) \
                        if m is not None else "SHA1"
                    if not dig or not mgf:
                        continue
                else:
                    continue
                wrapped = b64.b64decode("".join(ek.find(
                    "xenc:CipherData/xenc:CipherValue", NS).text.split()))
                key = self.key.decrypt(wrapped, padding.OAEP(
                    mgf=padding.MGF1(_hash(mgf)), algorithm=_hash(dig),
                    label=None))
                break
            size = GCM.get(method) or CBC.get(method)
            if key is None or len(key) != size:
                raise bad
            blob = b64.b64decode("".join(data.find(
                "xenc:CipherData/xenc:CipherValue", NS).text.split()))
            if len(blob) > SAML_MOST:
                raise bad
            if method in GCM:
                plain = Cipher(algorithms.AES(key), modes.GCM(
                    blob[:12], blob[-16:])).decryptor()
                body = plain.update(blob[12:-16]) + plain.finalize()
            else:
                plain = Cipher(algorithms.AES(key),
                               modes.CBC(blob[:16])).decryptor()
                body = plain.update(blob[16:]) + plain.finalize()
                pad = body[-1]
                if not 1 <= pad <= 16:
                    raise bad
                body = body[:-pad]
            got = etree.fromstring(body, parser=_parser())
        except Refused:
            raise
        except Exception:  # noqa: BLE001 - one answer for every failure
            raise bad
        if got.tag != f"{{{NS['saml']}}}Assertion":
            raise bad
        return got

    def finish(self, response_b64, relay, browser):
        """(the person the provider's answer names, where to go on, when
        the provider says the sign-in ends or None); Refused where it does
        not hold."""
        from lxml import etree
        from signxml import SignatureMethod, XMLVerifier
        from signxml.exceptions import SignXMLException
        from signxml.verifier import SignatureConfiguration

        from .store import digest
        if not response_b64 or len(response_b64) > SAML_MOST * 2:
            raise Refused("no answer from the identity provider, or one too"
                          " large to read")
        try:
            raw = base64.b64decode(response_b64, validate=True)
            doc = etree.fromstring(raw, parser=_parser())
        except Exception:  # noqa: BLE001 - not an answer at all
            raise Refused("the identity provider's answer could not be read")
        if doc.tag != f"{{{NS['samlp']}}}Response":
            raise Refused("the identity provider's answer is not a SAML"
                          " response")
        asked = doc.get("InResponseTo")
        got = self.store.take_pending("saml", asked) if asked else None
        if got is None:
            raise Refused("this answer is not to a sign-in this view"
                          " started, or it was used already or took too long"
                          " - sign in again")
        if not browser or digest(browser) != got["browser"]:
            raise Refused("this sign-in was started in another browser -"
                          " sign in again from this one")
        if relay != got["relay"]:
            raise Refused("the answer does not carry this sign-in's state")
        dest = doc.get("Destination")
        if dest and dest != self.acs:
            raise Refused("the answer was addressed to another service")
        code = doc.find("samlp:Status/samlp:StatusCode", NS)
        if code is None or code.get("Value") != SUCCESS:
            raise Refused("the identity provider did not sign the person in"
                          f" ({code.get('Value') if code is not None else '?'})")
        enc = doc.findall("saml:EncryptedAssertion", NS)
        if len(doc.findall(".//saml:EncryptedAssertion", NS)) != len(enc) \
                or len(enc) > 1:
            raise Refused("the answer holds an encrypted assertion where"
                          " none is read")
        if self.asks["assertion"].path == "encrypted" and (
                not enc or doc.find(".//saml:Assertion", NS) is not None):
            raise Refused("the identity provider sent an assertion that is"
                          " not encrypted, and encryption is demanded"
                          " (MIGKIT_WEB_SAML_ENCRYPTED) - have it encrypt"
                          " the assertion to this view's certificate")
        # a signature on the response covers the ciphertext; one on the
        # assertion is inside it, and is read once the assertion is open
        if enc and doc.find("ds:Signature", NS) is None:
            # CBC content is not authenticated: opened before any signature
            # held, its padding errors answer whoever sent it (a padding
            # oracle), so it is opened only under a signed response
            if _cipher(enc[0]) in CBC:
                raise Refused("the answer is not signed and its assertion is"
                              " encrypted with AES-CBC, which is opened only"
                              " under a signature that held first - have the"
                              " identity provider sign the response, or"
                              " encrypt with AES-GCM")
            enc[0].getparent().replace(enc[0], self._open(enc[0]))
            raw = etree.tostring(doc)
        taken = frozenset(m for m in SignatureMethod
                          if "HMAC" not in m.name and "SHA1" not in m.name)
        proved = None
        for cert in self.certs:
            try:
                proved = XMLVerifier().verify(
                    raw, x509_cert=cert,
                    expect_config=SignatureConfiguration(
                        expect_references=1, signature_methods=taken)
                ).signed_xml
                break
            except SignXMLException:
                continue
            except Exception:  # noqa: BLE001 - an unreadable signature
                continue
        if proved is None:
            raise Refused("the answer's signature does not hold against the"
                          " identity provider's certificate")
        if proved.tag == f"{{{NS['samlp']}}}Response":
            found = proved.findall("saml:Assertion", NS)
            found += [self._open(e) for e in proved.findall(
                "saml:EncryptedAssertion", NS)]
        elif proved.tag == f"{{{NS['saml']}}}Assertion":
            found = [proved]
        else:
            raise Refused("the signature covers neither the response nor"
                          " its assertion")
        if len(found) != 1 or len(doc.findall(".//saml:Assertion", NS)) \
                + len(doc.findall(".//saml:EncryptedAssertion", NS)) != 1:
            raise Refused("the answer holds more than one assertion")
        try:
            person, until = self._read(found[0], asked)
        except ValueError:
            raise Refused("the assertion's times could not be read")
        return person, got["return_to"], until

    # ---- single logout -----------------------------------------------------

    def logout_begin(self, principal):
        """The provider's address a sign-out of this person is sent to (a
        signed logout request by the redirect binding), or None where the
        provider's metadata offers none to use: their session here is
        ended by the caller either way. The request is kept to be matched
        with the provider's answer."""
        from xml.sax.saxutils import escape, quoteattr
        if not self.logs_out or principal.via != "saml" \
                or principal.issuer != f"saml:{self.idp_entity}":
            return None
        rid = "_" + secrets.token_hex(16)
        index = (f"<samlp:SessionIndex>{escape(principal.session)}"
                 "</samlp:SessionIndex>") if principal.session else ""
        xml = (f'<samlp:LogoutRequest xmlns:samlp="{NS["samlp"]}"'
               f' xmlns:saml="{NS["saml"]}" ID="{rid}" Version="2.0"'
               f' IssueInstant="{_instant()}"'
               f" Destination={quoteattr(self.idp_slo)}"
               f' NotOnOrAfter="{_instant(time.time() + 300)}">'
               f"<saml:Issuer>{escape(self.entity)}</saml:Issuer>"
               f"<saml:NameID>{escape(principal.subject)}</saml:NameID>"
               f"{index}</samlp:LogoutRequest>")
        self.store.put_pending("saml-logout", rid, {
            "who": principal.id}, PENDING)
        return self._redirect_to(self.idp_slo, "SAMLRequest", xml,
                                 sign=True)

    def logout_done(self, raw_query, args):
        """True when the provider answers (signed) that it ended its
        session for a request this view made; Refused where the answer
        does not hold or says the provider's session stands."""
        doc = self._inflate(args.get("SAMLResponse"))
        if doc.tag != f"{{{NS['samlp']}}}LogoutResponse" \
                or not self._redirect_ok(raw_query, "SAMLResponse"):
            raise Refused("the identity provider's logout answer is not"
                          " signed by it")
        if _text(doc, "saml:Issuer") != self.idp_entity:
            raise Refused("the logout answer came from another provider")
        if self.store.take_pending("saml-logout",
                                   doc.get("InResponseTo")) is None:
            raise Refused("this logout answer is not to a request this"
                          " view made, or it was used already")
        code = doc.find("samlp:Status/samlp:StatusCode", NS)
        if code is None or code.get("Value") != SUCCESS:
            raise Refused("the identity provider did not end its session"
                          f" ({code.get('Value') if code is not None else '?'})")
        return True

    def logout_asked(self, raw_query, args):
        """(how many sessions here the provider's logout request ended,
        the signed answer's address): a request taken only signed by the
        provider, once, for this view, naming the person and, where it
        says so, the sign-in; Refused otherwise."""
        from xml.sax.saxutils import escape, quoteattr
        if not self.logs_out:
            raise Refused("single logout is not set up with this identity"
                          " provider")
        doc = self._inflate(args.get("SAMLRequest"))
        if doc.tag != f"{{{NS['samlp']}}}LogoutRequest" \
                or not self._redirect_ok(raw_query, "SAMLRequest"):
            raise Refused("the identity provider's logout request is not"
                          " signed by it")
        if _text(doc, "saml:Issuer") != self.idp_entity:
            raise Refused("the logout request came from another provider")
        if doc.get("Destination") not in (None, self.slo):
            raise Refused("the logout request was addressed to another"
                          " service")
        try:
            if doc.get("NotOnOrAfter") \
                    and _when(doc.get("NotOnOrAfter")) <= time.time() - SKEW:
                raise Refused("the logout request has expired")
        except ValueError:
            raise Refused("the logout request's time could not be read")
        rid = doc.get("ID") or ""
        name = _text(doc, "saml:NameID")
        if not rid or not name:
            raise Refused("the logout request names nobody")
        if not self.store.first_time("saml-logout", rid, time.time() + 600):
            raise Refused("this logout request was presented before")
        ended = self._end(name, [(x.text or "").strip() for x in
                                 doc.findall("samlp:SessionIndex", NS)])
        xml = (f'<samlp:LogoutResponse xmlns:samlp="{NS["samlp"]}"'
               f' xmlns:saml="{NS["saml"]}" ID="_{secrets.token_hex(16)}"'
               f' Version="2.0" IssueInstant="{_instant()}"'
               f" Destination={quoteattr(self.idp_slo_answer)}"
               f" InResponseTo={quoteattr(rid)}>"
               f"<saml:Issuer>{escape(self.entity)}</saml:Issuer>"
               f'<samlp:Status><samlp:StatusCode Value="{SUCCESS}"/>'
               "</samlp:Status></samlp:LogoutResponse>")
        return ended, self._redirect_to(
            self.idp_slo_answer, "SAMLResponse", xml,
            args.get("RelayState"), sign=True)

    def _end(self, name_id, indexes):
        """The sessions this provider signed `name_id` in with (the ones
        of `indexes` where it names them), ended here; how many. API
        tokens are not sessions of the provider's and stand."""
        n = 0
        with self.store.tx() as db:
            for r in db.execute("select id_hash, principal from sessions"
                                ).fetchall():
                p = json.loads(r["principal"])
                if p.get("via") == "saml" and p.get("subject") == name_id \
                        and p.get("issuer") == f"saml:{self.idp_entity}" \
                        and (not indexes or p.get("session") in indexes):
                    db.execute("delete from sessions where id_hash = ?",
                               (r["id_hash"],))
                    n += 1
        return n

    def _read(self, a, asked):
        """(the person an assertion - the signed one - names, when the
        sign-in ends or None), its conditions checked."""
        now = time.time()
        if _text(a, "saml:Issuer") != self.idp_entity:
            raise Refused("the assertion was issued by another provider")
        cond = a.find("saml:Conditions", NS)
        if cond is None:
            raise Refused("the assertion states no conditions")
        if cond.get("NotBefore") and _when(cond.get("NotBefore")) > now + SKEW:
            raise Refused("the assertion is not valid yet")
        if not cond.get("NotOnOrAfter") \
                or _when(cond.get("NotOnOrAfter")) <= now - SKEW:
            raise Refused("the assertion has expired")
        audiences = [(x.text or "").strip() for x in cond.findall(
            "saml:AudienceRestriction/saml:Audience", NS)]
        if self.entity not in audiences:
            raise Refused("the assertion was issued for another service")
        subject = a.find("saml:Subject", NS)
        if subject is None:
            raise Refused("the assertion names nobody")
        ok = False
        for sc in subject.findall("saml:SubjectConfirmation", NS):
            data = sc.find("saml:SubjectConfirmationData", NS)
            if sc.get("Method") != BEARER or data is None:
                continue
            if data.get("Recipient") != self.acs:
                continue
            if data.get("InResponseTo") != asked:
                continue
            if not data.get("NotOnOrAfter") \
                    or _when(data.get("NotOnOrAfter")) <= now - SKEW:
                continue
            ok = True
        if not ok:
            raise Refused("the assertion is not confirmed for this service's"
                          " request")
        until = _when(cond.get("NotOnOrAfter"))
        if not self.store.first_time("saml", a.get("ID") or "", until):
            raise Refused("this assertion was presented before")
        name_id = _text(subject, "saml:NameID")
        attrs = {}
        for at in a.findall("saml:AttributeStatement/saml:Attribute", NS):
            attrs[at.get("Name")] = [(v.text or "").strip() for v in
                                     at.findall("saml:AttributeValue", NS)]
        who = (attrs.get(self.s.saml_name) or [None])[0] \
            if self.s.saml_name else None
        who = who or name_id
        if not who:
            raise Refused("the assertion names nobody")
        stmt = a.find("saml:AuthnStatement", NS)
        ends = stmt.get("SessionNotOnOrAfter") if stmt is not None else None
        p = Principal(id=who.lower() if "@" in who else who, name=who,
                      issuer=f"saml:{self.idp_entity}", subject=name_id,
                      groups=_groups(attrs.get(self.s.saml_groups)),
                      via="saml",
                      session=(stmt.get("SessionIndex") or "")
                      if stmt is not None else "")
        return p, (_when(ends) if ends else None)




# ---- the local account ------------------------------------------------------

#: the least a local password may be
PASSWORD_LEAST = 12
#: seconds a printed set-up address stands
SETUP_SECONDS = 1800


class Local:
    """The single account of an install without an identity provider."""

    def __init__(self, store):
        from argon2 import PasswordHasher
        self.store = store
        self.hasher = PasswordHasher()

    def exists(self):
        return self.store.local_account() is not None

    def setup_code(self):
        """A one-time code for the address the server prints: it sets the
        account's password, once, within `SETUP_SECONDS`."""
        code = secrets.token_urlsafe(24)
        self.store.put_pending("setup", code, {"at": time.time()},
                               SETUP_SECONDS)
        return code

    def set_up(self, code, name, password, again):
        """The account set with the printed code; Refused otherwise."""
        name = str(name or "").strip()
        if not name or len(name) > 64 or not name.replace(".", "").replace(
                "-", "").replace("_", "").replace("@", "").isalnum():
            raise Refused("a name of letters, digits, '.', '-', '_' or '@'")
        if len(password or "") < PASSWORD_LEAST:
            raise Refused(f"a password of {PASSWORD_LEAST} characters at"
                          " least")
        if password.lower() == name.lower() or password != again:
            raise Refused("the two passwords differ, or the password is the"
                          " name")
        if self.store.take_pending("setup", code) is None:
            raise Refused("that set-up address has been used, or ran out -"
                          " restart the view with MIGKIT_WEB_RESET=1 for a"
                          " new one")
        self.store.set_local(name, self.hasher.hash(password))
        return Principal(id=name, name=name, issuer="local", via="local")

    def sign_in(self, name, password, client):
        """The operator, for their name and password; Refused otherwise,
        and after five wrong ones in a row, a wait that doubles."""
        from argon2.exceptions import Argon2Error
        key = f"local:{client}"
        wait = self.store.wait(key)
        if wait > 0:
            raise Refused(f"too many wrong passwords - wait {int(wait) + 1}"
                          " seconds")
        got = self.store.local_account()
        ok = False
        if got is not None and str(name or "").strip() == got[0]:
            try:
                ok = self.hasher.verify(got[1], password or "")
            except Argon2Error:
                ok = False
        else:
            # the same work for a name that is not the account's, so the
            # time taken does not say which it was
            try:
                self.hasher.verify(self.hasher.hash("x"), "y")
            except Argon2Error:
                pass
        if not ok:
            self.store.failed(key)
            raise Refused("that name and password do not sign in here")
        self.store.succeeded(key)
        if self.hasher.check_needs_rehash(got[1]):
            self.store.set_local(got[0], self.hasher.hash(password))
        return Principal(id=got[0], name=got[0], issuer="local", via="local")
