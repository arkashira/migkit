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
once. An encrypted assertion is refused, said: the view holds no key to
read one, and TLS carries it.

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
import secrets
import time
import zlib
from dataclasses import dataclass, field
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
    they signed in (oidc, saml, local, or token for the API)."""
    id: str
    name: str = ""
    issuer: str = ""
    subject: str = ""
    groups: tuple = field(default_factory=tuple)
    via: str = ""

    def as_dict(self):
        return {"id": self.id, "name": self.name, "issuer": self.issuer,
                "subject": self.subject, "groups": list(self.groups),
                "via": self.via}

    @classmethod
    def from_dict(cls, d):
        d = dict(d or {})
        return cls(id=str(d.get("id") or ""), name=str(d.get("name") or ""),
                   issuer=str(d.get("issuer") or ""),
                   subject=str(d.get("subject") or ""),
                   groups=tuple(str(g) for g in d.get("groups") or ()),
                   via=str(d.get("via") or ""))


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
    return Principal(principal.id, principal.name, principal.issuer,
                     principal.subject, groups, principal.via)


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
            got = self.http.get(url, timeout=10, headers={
                "Authorization": f"Bearer {access_token}"})
            got.raise_for_status()
            info = got.json()
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


class Saml:
    """The view as a service provider of one SAML identity provider."""

    def __init__(self, settings, store):
        from pathlib import Path

        from lxml import etree
        self.s, self.store = settings, store
        self.entity = f"{settings.url}/auth/saml/metadata"
        self.acs = f"{settings.url}/auth/saml/acs"
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

    def metadata_xml(self):
        """This view as a service provider, for the identity provider."""
        from xml.sax.saxutils import quoteattr
        return (
            f'<?xml version="1.0"?>\n<md:EntityDescriptor xmlns:md='
            f'"{NS["md"]}" entityID={quoteattr(self.entity)}>'
            '<md:SPSSODescriptor AuthnRequestsSigned="false"'
            ' WantAssertionsSigned="true" protocolSupportEnumeration='
            '"urn:oasis:names:tc:SAML:2.0:protocol">'
            "<md:NameIDFormat>urn:oasis:names:tc:SAML:1.1:nameid-format:"
            "emailAddress</md:NameIDFormat>"
            f'<md:AssertionConsumerService Binding="{POST}"'
            f" Location={quoteattr(self.acs)} index=\"0\""
            ' isDefault="true"/></md:SPSSODescriptor></md:EntityDescriptor>'
            "\n")

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
        packer = zlib.compressobj(9, zlib.DEFLATED, -15)
        packed = packer.compress(xml.encode()) + packer.flush()
        relay = secrets.token_urlsafe(16)
        browser = secrets.token_urlsafe(24)
        from .store import digest
        self.store.put_pending("saml", rid, {
            "browser": digest(browser), "relay": relay,
            "return_to": return_to}, PENDING)
        sep = "&" if "?" in self.sso else "?"
        return (self.sso + sep + urlencode({
            "SAMLRequest": base64.b64encode(packed).decode(),
            "RelayState": relay}), browser)

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
        if doc.find(".//saml:EncryptedAssertion", NS) is not None:
            raise Refused("the identity provider sent an encrypted assertion,"
                          " which this view does not read - have it sign the"
                          " assertion and leave it unencrypted; TLS carries"
                          " it")
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
        elif proved.tag == f"{{{NS['saml']}}}Assertion":
            found = [proved]
        else:
            raise Refused("the signature covers neither the response nor"
                          " its assertion")
        if len(found) != 1 or len(doc.findall(".//saml:Assertion", NS)) != 1:
            raise Refused("the answer holds more than one assertion")
        try:
            person, until = self._read(found[0], asked)
        except ValueError:
            raise Refused("the assertion's times could not be read")
        return person, got["return_to"], until

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
                      via="saml")
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
