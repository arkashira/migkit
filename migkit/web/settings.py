"""The view's settings, from the environment (`envkeys`), checked once at
start: an address people reach it by, how it is served, and who signs
people in.

Secure by default, and refused rather than weakened:

* Listening anywhere but this machine's own loopback needs TLS - a
  certificate and key of its own, or the address people open being
  `https://` with the reverse proxies in front named
  (`MIGKIT_WEB_PROXIES`), whose forwarded headers are the only ones read
  and whose connections are the only ones answered - or the operator
  saying the network to it is their own (`MIGKIT_WEB_PLAIN`: a container
  published on the host's loopback only), said again at every start.
* An identity provider is reached and returned to over `https://` only,
  but for an address on this machine's loopback, which the providers
  allow for development.
* With a provider, nobody signs in with a password: the single local
  account is for an install one operator runs without one.
"""
import ipaddress
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from .. import envkeys

#: where the view keeps what it must keep (sessions, the provisioned
#: directory, its signing key), where `MIGKIT_WEB_STATE` does not say
STATE = "~/.local/share/migkit/web"

LOOPBACK_NAMES = ("localhost",)


def state_dir(environ=None):
    """The view's own directory, private to this user."""
    return Path(envkeys.text("MIGKIT_WEB_STATE", environ, STATE)
                ).expanduser()


def loopback(host):
    host = str(host or "").strip("[]").lower()
    if host in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _secret(value):
    """A secret variable's value, or what the reference it holds names
    (env:, file:, vault:, keyring:, a cloud's secret store) - read the
    way a hop's passwords are."""
    from .. import addresses
    from ..config import _secret as read
    if value and addresses.is_reference(value):
        return str(read(value))
    return value


@dataclass(frozen=True)
class Settings:
    url: str                      #: the address people open, no path
    bind: str
    port: int
    cert: str = ""
    key: str = ""
    state: Path = None
    proxies: tuple = ()
    hosts: tuple = ()
    oidc_issuer: str = ""
    oidc_client: str = ""
    oidc_secret: str = ""
    oidc_scopes: tuple = ("openid", "email", "profile")
    oidc_groups: str = "groups"
    saml_metadata: str = ""
    saml_groups: str = "groups"
    saml_name: str = ""
    scim_token: str = ""
    session_hours: float = 12.0
    idle_minutes: float = 60.0
    token_days: float = 90.0
    metrics_token: str = ""
    reset: bool = False
    explain: bool = False
    #: plain HTTP beyond the loopback, the operator's own network said so
    plain: bool = False
    extra: dict = field(default_factory=dict)

    @property
    def https(self):
        return self.url.startswith("https://")

    @property
    def origin(self):
        return self.url

    @property
    def host(self):
        return urlsplit(self.url).netloc

    @property
    def sso(self):
        """Whether people sign in through an identity provider."""
        return bool(self.oidc_issuer or self.saml_metadata)

    @property
    def scim(self):
        return bool(self.scim_token)

    @property
    def bound_locally(self):
        return loopback(self.bind)

    def allowed_hosts(self):
        """The Host headers answered: the address people open, this
        machine's loopback names at the port where it listens there, and
        `MIGKIT_UI_HOSTS` - a page elsewhere rebinding a name it controls
        to this machine gets nothing."""
        got = {self.host.lower()}
        if self.bound_locally:
            got |= {f"127.0.0.1:{self.port}", f"localhost:{self.port}",
                    f"[::1]:{self.port}"}
        got |= {h.lower() for h in self.hosts}
        return got


def load(port, environ=None):
    """The settings for a view listening on `port`, checked; SystemExit
    saying what to set where they cannot be served safely."""
    env = os.environ if environ is None else environ
    bind = envkeys.text("MIGKIT_UI_BIND", env, "127.0.0.1")
    cert = envkeys.text("MIGKIT_WEB_TLS_CERT", env)
    key = envkeys.text("MIGKIT_WEB_TLS_KEY", env)
    scheme = "https" if cert else "http"
    shown = "127.0.0.1" if loopback(bind) or bind in ("", "0.0.0.0", "::") \
        else bind
    url = envkeys.text("MIGKIT_WEB_URL", env,
                       f"{scheme}://{shown}:{port}").rstrip("/")
    s = Settings(
        url=url, bind=bind, port=int(port), cert=cert, key=key,
        state=state_dir(env),
        proxies=tuple(envkeys.items("MIGKIT_WEB_PROXIES", env)),
        hosts=tuple(envkeys.items("MIGKIT_UI_HOSTS", env)),
        oidc_issuer=envkeys.text("MIGKIT_WEB_OIDC_ISSUER", env).rstrip("/"),
        oidc_client=envkeys.text("MIGKIT_WEB_OIDC_CLIENT_ID", env),
        oidc_secret=_secret(envkeys.text("MIGKIT_WEB_OIDC_CLIENT_SECRET",
                                         env)),
        oidc_scopes=tuple(envkeys.items("MIGKIT_WEB_OIDC_SCOPES", env)
                          or ("openid", "email", "profile")),
        oidc_groups=envkeys.text("MIGKIT_WEB_OIDC_GROUPS", env, "groups"),
        saml_metadata=envkeys.text("MIGKIT_WEB_SAML_METADATA", env),
        saml_groups=envkeys.text("MIGKIT_WEB_SAML_GROUPS", env, "groups"),
        saml_name=envkeys.text("MIGKIT_WEB_SAML_NAME", env),
        scim_token=_secret(envkeys.text("MIGKIT_WEB_SCIM_TOKEN", env)),
        session_hours=envkeys.number("MIGKIT_WEB_SESSION_HOURS", env),
        idle_minutes=envkeys.number("MIGKIT_WEB_IDLE_MINUTES", env),
        token_days=envkeys.number("MIGKIT_WEB_TOKEN_DAYS", env),
        metrics_token=_secret(envkeys.text("MIGKIT_METRICS_TOKEN",
                                           env)),
        reset=envkeys.flag("MIGKIT_WEB_RESET", env),
        explain=envkeys.flag("MIGKIT_EXPLAIN", env),
        plain=envkeys.flag("MIGKIT_WEB_PLAIN", env))
    check(s)
    return s


def check(s):
    """SystemExit, saying what to set, where the settings cannot be
    served safely."""
    parts = urlsplit(s.url)
    if parts.scheme not in ("http", "https") or not parts.netloc \
            or parts.path not in ("", "/") or parts.query:
        raise SystemExit(f"MIGKIT_WEB_URL={s.url} is not an address people"
                         " open: https://host[:port], no path")
    if bool(s.cert) != bool(s.key):
        raise SystemExit("MIGKIT_WEB_TLS_CERT and MIGKIT_WEB_TLS_KEY go"
                         " together: the certificate and its private key")
    for p in (s.cert, s.key):
        if p and not Path(p).expanduser().is_file():
            raise SystemExit(f"{p} is not a file this machine can read")
    if not s.bound_locally and not s.cert and not s.plain:
        if not (s.https and s.proxies):
            raise SystemExit(
                f"the view would listen on {s.bind} without TLS: give it a"
                " certificate (MIGKIT_WEB_TLS_CERT, MIGKIT_WEB_TLS_KEY), or"
                " serve it behind a reverse proxy that does TLS -"
                " MIGKIT_WEB_URL=https://... and MIGKIT_WEB_PROXIES naming"
                " the proxy's address, the only one then answered; or, in"
                " a container published on the host's loopback only,"
                " MIGKIT_WEB_PLAIN=on")
    if s.sso and not s.https and not loopback(parts.hostname):
        raise SystemExit("signing in through an identity provider needs"
                         " the view at an https:// address (MIGKIT_WEB_URL)"
                         " - a code or an assertion would cross the network"
                         " in the clear otherwise")
    if s.oidc_issuer:
        if not (s.oidc_client and s.oidc_secret):
            raise SystemExit("MIGKIT_WEB_OIDC_ISSUER needs"
                             " MIGKIT_WEB_OIDC_CLIENT_ID and"
                             " MIGKIT_WEB_OIDC_CLIENT_SECRET - the client"
                             " the provider registered for this view")
        iss = urlsplit(s.oidc_issuer)
        if iss.scheme != "https" and not loopback(iss.hostname):
            raise SystemExit(f"the identity provider {s.oidc_issuer} is"
                             " not an https:// address")
    if s.saml_metadata and not Path(s.saml_metadata).expanduser().is_file():
        raise SystemExit(f"MIGKIT_WEB_SAML_METADATA={s.saml_metadata} is not"
                         " a file: the identity provider's metadata, as it"
                         " publishes it")
    if s.scim_token and len(s.scim_token) < 24:
        raise SystemExit("MIGKIT_WEB_SCIM_TOKEN is shorter than 24"
                         " characters - anyone who guesses it provisions"
                         " people; draw a long random one")
    if s.metrics_token and len(s.metrics_token) < 24:
        raise SystemExit("MIGKIT_METRICS_TOKEN is shorter than 24"
                         " characters; draw a long random one")
