"""One way of hiding secrets in what migkit writes or says (audit
2026-10-01, SEC12).

There were five, each missing something - measured: an address with an
empty user (`redis://:pw@cache`) kept its password; `password=` in a
connection string or a JDBC query was left; a password holding `/` or
`@` broke the address pattern (`mysql://app:pa/ss@db` unchanged,
`postgres://u:p@ss@h` read as `<removed>@ss@h`); a password URL-encoded
into a notification's text went out to the channel as it was.

A `Redactor` hides a secret two ways, and both are needed:

* **by value** - every secret the hop holds right now (both sides'
  passwords and tokens, the secret-named options, the notification
  addresses, the secret variables of the environment), each in every form
  it reaches text in: as it is, URL-encoded, shell-quoted, SQL-escaped
  both ways, and JSON-escaped. A password in an argument of its own is
  found only this way.
* **by shape** - what is a secret wherever it stands: the userinfo of an
  address up to its last `@`, `password=` / `PWD=` and every `...password`
  / `...secret` / `...token` key set to a value, `IDENTIFIED BY '...'`,
  `PASSWORD '...'`, `masterauth x`. A secret the hop does not hold -
  another account's, in a statement read back - is found only this way.

`SECRET_KEY` is the one pattern for a key whose value is not shown, in a
configuration or a record carried out (`value`).
"""
import json
import re
import shlex
from urllib.parse import quote, quote_plus

#: a key whose value is never shown, in a configuration or a record: the
#: diagnostics bundle's, now everyone's
SECRET_KEY = re.compile(r"pass|pwd|secret|token|key|credential|notify|auth",
                        re.I)

#: an option key that holds a secret itself, whose value is hidden
#: wherever it appears - narrower than `SECRET_KEY`, which also names keys
#: that hold a file's path (`ssl_key`), a way of signing in (`auth`) or a
#: table's key column (`key`): hidden by value, `id` would be taken out
#: of every line
HOLDS_SECRET = re.compile(r"pass(?:word|wd|phrase)?|pwd|secret|token", re.I)

#: a quoted value, which ends where its closing quote is followed by the
#: end of a word: `password=' publication p with (slot_name = 'x')`, an
#: empty password closing its connection string, is not a quoted value
_END = r"(?=[\s;&,)}\]]|$)"
_Q1 = r"'(?:[^'\\]|''|\\.)*'" + _END
_Q2 = r'"(?:[^"\\]|\\.)*"' + _END

#: `scheme://user:password@host`, the password up to the last `@` of the
#: address: a password may hold `/`, `:` and `@`
_USERINFO = re.compile(r"(?P<s>\b[A-Za-z][\w+.-]*://)(?P<u>[^\s:/@'\"]*)"
                       r":(?P<p>[^\s'\"]*)@")
#: the same, the whole userinfo: for what is handed to someone else
_USERINFO_ALL = re.compile(r"(?P<s>\b[A-Za-z][\w+.-]*://)(?P<p>[^\s'\"]+)@")
#: `password=x`, `PWD={x}`, `MASTER_PASSWORD = 'x'`, `"api_token": "x"`
_ASSIGN = re.compile(
    r"(?i)(?P<k>(?<![\w.-])[\w.-]*(?:pass(?:word|wd|phrase)?|pwd|secret"
    # `:` only after a quoted key (JSON): in prose, "token: expired" is
    # words, not a secret
    r"|token))(?P<kq>[\"']?)(?P<op>\s*=\s*|(?<=[\"'])\s*:\s*)"
    rf"(?P<v>{_Q1}|{_Q2}|\{{[^}}]*\}}|[^\s;&,'\"}}\]]+)")
#: `IDENTIFIED BY 'x'`, `IDENTIFIED WITH plugin BY 'x'`, and Oracle's
#: `IDENTIFIED BY x`, unquoted
_IDENTIFIED = re.compile(
    r"(?i)(?P<k>\bidentified\s+(?:with\s+[\w'\"`]+\s+)?by\s+"
    r"(?:password\s+)?)"
    rf"(?P<v>{_Q1}|{_Q2}|[^\s;'\"]+)")
#: `PASSWORD 'x'`, `ENCRYPTED PASSWORD 'x'`, a user mapping's option
_PASSWORD = re.compile(rf"(?i)(?P<k>\bpassword\s+)(?P<v>{_Q1}|{_Q2})")
#: a Redis configuration's `masterauth x`, `requirepass x`
_REDIS = re.compile(r"(?i)(?P<k>\b(?:masterauth|requirepass)\s+)"
                    r"(?P<v>\"[^\"]*\"|'[^']*'|\S+)")


def _masked(value, mask):
    """`value` with what it holds replaced, its quotes kept; an empty one
    left as it is - an empty password hides nothing."""
    if len(value) >= 2 and value[0] in "'\"{" and \
            value[-1] == {"'": "'", '"': '"', "{": "}"}[value[0]]:
        inner = value[1:-1]
        return value if not inner else value[0] + mask + value[-1]
    return value if not value else mask


def shapes(text, mask="***", userinfo="password"):
    """`text` with every secret found by its shape hidden. `userinfo`
    "password" keeps an address's user name; "all" takes the whole
    userinfo out, for what is handed to someone else."""
    text = str(text)
    if userinfo == "all":
        text = _USERINFO_ALL.sub(lambda m: m["s"] + mask + "@", text)
    else:
        text = _USERINFO.sub(
            lambda m: f"{m['s']}{m['u']}:"
                      f"{mask if m['p'] else ''}@", text)
    text = _ASSIGN.sub(lambda m: m["k"] + m["kq"] + m["op"]
                       + _masked(m["v"], mask), text)
    text = _IDENTIFIED.sub(lambda m: m["k"] + _masked(m["v"], mask), text)
    text = _PASSWORD.sub(lambda m: m["k"] + _masked(m["v"], mask), text)
    text = _REDIS.sub(lambda m: m["k"] + _masked(m["v"], mask), text)
    return text


def forms(secret):
    """Every form `secret` reaches text in, longest first."""
    s = str(secret)
    if not s:
        return []
    got = {s, quote(s, safe=""), quote_plus(s), shlex.quote(s),
           s.replace("'", "''"),
           s.replace("\\", "\\\\").replace("'", "\\'"),
           json.dumps(s)[1:-1]}
    return sorted((f for f in got if f), key=len, reverse=True)


def _option_secrets(value, key="", out=None):
    out = [] if out is None else out
    if isinstance(value, dict):
        for k, v in value.items():
            _option_secrets(v, str(k), out)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _option_secrets(v, key, out)
    elif isinstance(value, str) and value and HOLDS_SECRET.search(key):
        out.append(value)
    return out


def _receivers(raw):
    """The notification addresses of a hop's `notify`: they carry the
    channel's own token."""
    if isinstance(raw, dict):
        raw = raw.get("to")
    if isinstance(raw, str):
        raw = [raw]
    return [str(r) for r in raw or [] if isinstance(r, str) and r]


def _held(ep):
    if ep is None:
        return []
    held = getattr(ep, "held_secrets", None)
    got = list(held()) if held else [getattr(ep, "password", "")]
    got += _option_secrets(getattr(ep, "options", None) or {})
    return got


class Redactor:
    """What a hop, the environment and `secrets` hold, hidden in any text.

    The secrets are read at each call, not when it is made: a token a
    cloud signs later in the run is hidden once it exists. Nothing is
    asked to find them - a password kept in the keychain that has not
    been read yet is not read for this."""

    def __init__(self, hop=None, secrets=(), mask="***",
                 userinfo="password", environ=None):
        self.hop = hop
        self.extra = [str(s) for s in secrets or () if s]
        self.mask = mask
        self.userinfo = userinfo
        self.environ = environ

    def secrets(self):
        from . import envkeys
        hop = self.hop
        got = list(self.extra)
        if hop is not None:
            for ep in (getattr(hop, "source", None),
                       getattr(hop, "target", None)):
                got += _held(ep)
            opts = getattr(hop, "options", None) or {}
            got += _option_secrets(opts)
            got += _receivers(opts.get("notify"))
        got += envkeys.secrets(self.environ)
        return [s for s in dict.fromkeys(str(s) for s in got) if s]

    def __call__(self, text):
        if text is None:
            return text
        text = str(text)
        every = sorted({f for s in self.secrets() for f in forms(s)},
                       key=len, reverse=True)
        for f in every:
            text = text.replace(f, self.mask)
        return shapes(text, self.mask, self.userinfo)

    def argv(self, argv):
        """A command line as one line, hidden."""
        return self(" ".join(str(a) for a in argv))

    def value(self, value, key=""):
        """A configuration or a record, at any depth, with the value of
        every key `SECRET_KEY` names replaced and every text hidden."""
        if isinstance(value, dict):
            return {k: self.value(v, str(k)) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            if SECRET_KEY.search(key):
                return [self.mask for _ in value]
            return [self.value(v, key) for v in value]
        if SECRET_KEY.search(key) and value not in (None, ""):
            return self.mask
        if isinstance(value, str):
            return self(value)
        return value


def scrub(value, mask="<removed>", key=""):
    """A configuration as it may be handed to someone else: secret keys'
    values replaced and whole address credentials taken out, at any
    depth (the diagnostics bundle). `key` is the one `value` is under."""
    return Redactor(mask=mask, userinfo="all").value(value, key)
