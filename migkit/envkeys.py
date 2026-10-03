"""Every environment variable migkit reads, in one place, and one way of
reading each kind (audit 2026-10-01, K13).

Measured on the code the day this was written: 71 `MIGKIT_*` names, about
25 of them written down nowhere, and "on" said five ways - `1/true/yes/
on` for two variables, anything but `""` and `"0"` for two more (so
`false` turned them on), any text at all for one (`0` turned it on),
only `"1"` for one (`true` left it off), anything but `"false"` for the
last. A number that did not read was a clean error for two variables, a
silent default for three, a traceback for five, and pasted raw into a
connection string for one. A typo in a name was read by nothing and said
nothing.

So each variable is declared here with its kind, its default and what it
does, and read through `flag`, `number` or `text`: one rule for "on"
(`1/true/yes/on`, and `0/false/no/off` for off - anything else is an
error naming the variable), one error for a number that is not one.
`unknown` finds a `MIGKIT_*` in the environment nothing reads, with the
closest name that is read; `reference` is the table the manual prints.
`tests/test_every_env_var_is_declared.py` reads the code and fails on a
name it reads that is not declared here, so the list cannot fall behind
the code.
"""
import difflib
import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Var:
    """One variable: what it does, its kind - `flag`, `number` (seconds
    and the like), `whole` (a whole number), `text`, `list` (separated
    by commas), `path` or `secret` - its default, and the least a number
    may be (a smaller one is raised to it). `internal`: set by migkit for
    a process it starts, never by the operator. `tests`: read by the test
    suite only. `named_for_a_program`: its own name is a program's, so
    it is neither offered back as a "did you mean" nor printed in the
    reference (programs are not named in what migkit says)."""
    what: str
    kind: str = "text"
    default: object = None
    least: float = None
    internal: bool = False
    tests: bool = False
    named_for_a_program: bool = False

    @property
    def secret(self):
        return self.kind == "secret"


KINDS = ("flag", "number", "whole", "text", "list", "path", "secret")

VARS = {
    # ---- where things are ---------------------------------------------
    "MIGKIT_CONF": Var("the hops file, where it is not ./conf/hops.yaml or"
                       " the user's config directory", "path"),
    "MIGKIT_REPORTS": Var("where reports and the state of runs are kept",
                          "path"),
    "MIGKIT_TOOLS": Var("where migkit keeps the programs it builds for"
                        " itself", "path", "~/.migkit/tools"),
    "MIGKIT_INSTANCES": Var("the machine catalogue refreshed from a"
                            " provider's API", "path",
                            "~/.cache/migkit/instances.json"),
    "MIGKIT_CATALOG_REFRESH": Var("refresh the machine catalogue from the"
                                  " provider's API where this machine has"
                                  " its credentials", "flag", False),
    "MIGKIT_KNOWLEDGE": Var("where what migkit has measured about this"
                            " machine is kept; never sent anywhere", "path",
                            "~/.local/share/migkit"),
    "MIGKIT_EXCLUDE": Var("databases or tables left alone for this run, on"
                          " top of the hop's `exclude`", "list"),
    "MIGKIT_COMPARE_WITH": Var("another tool's own check report to hold"
                               " this run's answers against, in place of"
                               " the hop's `compare_with`", "path"),
    # ---- `migkit init` without a terminal -------------------------------
    "MIGKIT_SOURCE": Var("the source's address for `init`, and for a"
                         " step that writes its hop", "text"),
    "MIGKIT_TARGET": Var("the target's address for `init`", "text"),
    "MIGKIT_SOURCE_PASSWORD": Var("the source's password for `init`, and"
                                  " for a copy migkit runs in a process of"
                                  " its own", "secret"),
    "MIGKIT_TARGET_PASSWORD": Var("the target's password, the same way",
                                  "secret"),
    "MIGKIT_SECRETS": Var("where `init` keeps the passwords it is given:"
                          " keychain or env", "text"),
    "MIGKIT_GOAL": Var("the goal `init` writes into the hop", "text"),
    "MIGKIT_HOP": Var("the hop's name for `init` and for a CI step; set"
                      " for a cutover's own commands", "text"),
    # ---- a CI step or a workflow ----------------------------------------
    "MIGKIT_COMMAND": Var("the command a CI step runs", "text", "check"),
    "MIGKIT_ARGS": Var("its arguments, split as a shell would", "text"),
    "MIGKIT_OUTPUTS": Var("the directory a workflow step's outputs and"
                          " evidence are written to", "path"),
    "MIGKIT_FAIL_ON_DIFFERENCE": Var("whether a CI step fails on a"
                                     " difference", "flag", True),
    # ---- how a run goes -------------------------------------------------
    "MIGKIT_MOVER": Var("one way of copying forced, to diagnose - migkit"
                        " decides it otherwise", "text"),
    "MIGKIT_TABLE_COPY": Var("one way every table of a copy between two"
                             " engines takes where it holds, forced for"
                             " measuring", "text"),
    "MIGKIT_CDC": Var("how changes are carried: native (the servers' own"
                      " replication) or follow (driven from here)", "text"),
    "MIGKIT_CUTOVER": Var("unproven: tear the stream down whatever the"
                          " last check found", "text"),
    "MIGKIT_PREFLIGHT": Var("warn: say what would stop a run and go on",
                            "text"),
    "MIGKIT_EXPLAIN": Var("print why each way was chosen", "flag", False),
    "MIGKIT_CROSSCHECK": Var("read the data a second way beside the"
                             " first", "flag", False),
    "MIGKIT_REPAIR_TEXT": Var("include the statements of a repair in its"
                              " report", "flag", False),
    "GRANTS_IGNORE_ROLES": Var("accounts and roles the grant checks leave"
                               " out, beside the servers' and providers'"
                               " own", "list"),
    "MIGKIT_TAIL_APPLY_ALL": Var("apply every change, for measuring what"
                                 " the tail leaves out", "flag", False),
    "MIGKIT_ORACLE_READER": Var("one way of reading Oracle's changes"
                                " forced: logminer or redo-files", "text"),
    "MIGKIT_ORACLE_WRITE": Var("one way of writing into Oracle put first,"
                               " to diagnose", "text"),
    "MIGKIT_GENERATIONS": Var("how many times the proof reads again only"
                              " the parts that still differ", "whole",
                              least=0),
    "MIGKIT_PROOF_COLD_SECONDS": Var("the age after which a kept proof's"
                                     " parts are read again", "number",
                                     300),
    "MIGKIT_PROOF_COLD_LEAVES": Var("how many of those are read in one"
                                    " check", "whole", 256),
    "MIGKIT_PROOF_OLDEST_SECONDS": Var("the age after which every part is"
                                       " read again", "number", 86400),
    "MIGKIT_PROBE_SECONDS": Var("how long a copy's first stretch is timed"
                                " for", "number", 1.5),
    "MIGKIT_PROBE_FROM": Var("the table size from which a copy's first"
                             " stretch is timed", "whole", 256 * 2 ** 20),
    # ---- waits and timeouts ---------------------------------------------
    "MIGKIT_STALL_SECONDS": Var("seconds without progress before a copy"
                                " is ended as stalled", "number", 300,
                                least=1),
    "MIGKIT_LEASE_SECONDS": Var("the term of the hop's lease", "number", 60,
                                least=1),
    "MIGKIT_STANDBY_SECONDS": Var("the term of the lease a hop with a"
                                  " standby is moved under, and of a shared"
                                  " range's claim: a machine not heard"
                                  " from for as long is taken over",
                                  "number", 15, least=3),
    "MIGKIT_FOLLOW_TIMEOUT": Var("seconds a follow of changes is given to"
                                 " catch up", "number", 1800),
    "MIGKIT_SUBSCRIBE_TIMEOUT": Var("seconds a subscription is given to"
                                    " copy and catch up", "whole"),
    "MIGKIT_READ_TIMEOUT": Var("seconds one MySQL read may take", "whole",
                               3600),
    "MIGKIT_KEEPALIVE_IDLE": Var("seconds a quiet connection waits before"
                                 " it is probed", "whole", 60, least=1),
    "MIGKIT_SEED_START_WAIT": Var("seconds a copy of the pages is given to"
                                  " start", "number"),
    "MIGKIT_CLOUD_POLL": Var("seconds between looks at a cloud's restore"
                             " or clone", "number"),
    "MIGKIT_CLOUD_WAIT": Var("seconds a cloud's restore or clone is given",
                             "number"),
    # ---- schedule -------------------------------------------------------
    "MIGKIT_SCHEDULED": Var("the command runs in its hop's schedule",
                            "flag", False),
    "MIGKIT_SCHEDULE_RESUME": Var("go on with a schedule its last slots"
                                  " stopped", "flag", False),
    "MIGKIT_SLOT_ENDS": Var("when the slot a scheduled run is in ends,"
                            " seconds since the epoch", "number",
                            internal=True),
    # ---- keys, identities and what is told ------------------------------
    "MIGKIT_STATE_KEY": Var("the passphrase restore points and state kept"
                            " away from the machine are sealed with",
                            "secret"),
    "MIGKIT_EVIDENCE_KEY": Var("the private SSH key the evidence is signed"
                               " with", "path"),
    "MIGKIT_IDENTITY": Var("the private key files held at rest are read"
                           " back with", "path", "~/.ssh/id_ed25519"),
    "MIGKIT_NOTIFY": Var("addresses told about every hop", "list"),
    "MIGKIT_DIAGNOSE": Var("`doctor` writes a problem-report bundle to"
                           " this file", "path"),
    "MIGKIT_ACCEPT_TERMS": Var("the programs whose terms are accepted for"
                               " an unattended install, or all", "list"),
    "MIGKIT_BUNDLE": Var("an offline bundle `doctor --install` installs"
                         " from, with no network", "path"),
    "MIGKIT_BUNDLE_BUILD": Var("the directory `doctor --install` builds an"
                               " offline bundle in, installing nothing",
                               "path"),
    "MIGKIT_BUNDLE_PLATFORM": Var("the machine an offline bundle is built"
                                  " for: linux-x86_64, darwin-arm64, or a"
                                  " Linux named after them"
                                  " (linux-x86_64-ubuntu-22.04)", "text"),
    "MIGKIT_BUNDLE_KEY": Var("the private SSH key an offline bundle's sums"
                             " are signed with", "path"),
    "MIGKIT_BUNDLE_SIGNERS": Var("the allowed signers file an offline"
                                 " bundle's signature is checked against",
                                 "path"),
    "MIGKIT_DB2_CONNECT_LICENSE": Var("the Db2 Connect licence file",
                                      "path"),
    "MIGKIT_UI_HOSTS": Var("more host names the web view and the dashboard"
                           " answer to", "list"),
    "MIGKIT_UI_USER_HEADER": Var("the header a signing-in proxy names the"
                                 " dashboard's user in", "text"),
    "MIGKIT_UI_PROXY_SECRET": Var("the secret a signing-in proxy sends in"
                                  " X-Migkit-Proxy: without it the user"
                                  " header is not believed", "secret"),
    "MIGKIT_UI_BIND": Var("the address the web view (and the dashboard)"
                          " listens on; for the web view, anything but this"
                          " machine's loopback needs TLS", "text",
                          "127.0.0.1"),
    "MIGKIT_METRICS_TOKEN": Var("the bearer a scraper reads /metrics with;"
                                " without it only someone signed in reads"
                                " it, their own hops", "secret"),
    # ---- what leaves the machine ----------------------------------------
    "MIGKIT_EGRESS": Var("what may leave the machine, for every hop: none,"
                         " or the kinds that may (notify, traces, assist,"
                         " evidence); all that is configured where unset",
                         "list"),
    "MIGKIT_EGRESS_TO": Var("the hosts anything that leaves may go to:"
                            " names, shell patterns, addresses and"
                            " networks", "list"),
    # ---- the web view (`migkit report --serve`) -------------------------
    "MIGKIT_WEB_URL": Var("the address people open the web view at,"
                          " https:// behind a proxy or with a certificate",
                          "text"),
    "MIGKIT_WEB_TLS_CERT": Var("the web view's certificate (PEM, its chain"
                               " after it)", "path"),
    "MIGKIT_WEB_TLS_KEY": Var("the certificate's private key", "path"),
    "MIGKIT_WEB_PROXIES": Var("the addresses of the reverse proxies in front"
                              " of the web view: the only ones answered,"
                              " and whose forwarded headers are read",
                              "list"),
    "MIGKIT_WEB_STATE": Var("where the web view keeps its sessions, the"
                            " people provisioned and its signing key",
                            "path", "~/.local/share/migkit/web"),
    "MIGKIT_WEB_OIDC_ISSUER": Var("the OpenID Connect provider people sign"
                                  " in through, its issuer's address",
                                  "text"),
    "MIGKIT_WEB_OIDC_CLIENT_ID": Var("the client the provider registered"
                                     " for the web view", "text"),
    "MIGKIT_WEB_OIDC_CLIENT_SECRET": Var("its secret, or a reference to"
                                         " where it is kept", "secret"),
    "MIGKIT_WEB_OIDC_SCOPES": Var("the scopes asked for", "list",
                                  "openid,email,profile"),
    "MIGKIT_WEB_OIDC_GROUPS": Var("the claim a person's groups are in",
                                  "text", "groups"),
    "MIGKIT_WEB_SAML_METADATA": Var("the SAML identity provider's metadata,"
                                    " as it publishes it", "path"),
    "MIGKIT_WEB_SAML_GROUPS": Var("the attribute a person's groups are in",
                                  "text", "groups"),
    "MIGKIT_WEB_SAML_NAME": Var("the attribute naming the person, where"
                                " not the name identifier", "text"),
    "MIGKIT_WEB_SCIM_TOKEN": Var("the bearer token the provider provisions"
                                 " people and groups with; set, only people"
                                 " it provisioned sign in", "secret"),
    "MIGKIT_WEB_SESSION_HOURS": Var("the longest a sign-in lasts", "number",
                                    12, least=0.25),
    "MIGKIT_WEB_IDLE_MINUTES": Var("minutes without a request before a"
                                   " sign-in ends", "number", 60, least=5),
    "MIGKIT_WEB_TOKEN_DAYS": Var("the longest an API token lasts",
                                 "number", 90, least=1),
    "MIGKIT_WEB_RESET": Var("print an address that sets the local"
                            " operator's password again", "flag", False),
    "MIGKIT_WEB_PLAIN": Var("serve the web view without TLS beyond this"
                            " machine's loopback - only where the network"
                            " to it is the operator's own: a container"
                            " published on the host's loopback", "flag",
                            False),
    # ---- the assistant ---------------------------------------------------
    "MIGKIT_AI": Var("the model provider asked, where one is: openai,"
                     " anthropic or google", "text"),
    "MIGKIT_AI_URL": Var("the provider's address", "text"),
    "MIGKIT_AI_KEY": Var("the provider's key", "secret"),
    "MIGKIT_AI_MODEL": Var("the model", "text"),
    "MIGKIT_AI_SHARE": Var("what may be sent: code, detail", "list"),
    # ---- set by migkit for what it starts --------------------------------
    "MIGKIT_SPEC": Var("what a copy in a process of its own does",
                       internal=True),
    "MIGKIT_SCHEMA_FROM": Var("one side's address for the schema"
                              " comparison", "secret", internal=True),
    "MIGKIT_SCHEMA_TO": Var("the other side's", "secret", internal=True),
    "MIGKIT_MIRROR_PROPERTIES": Var("a topic mirror's settings, for the"
                                    " container it runs in", "secret",
                                    internal=True),
    "MIGKIT_MIRROR_NETWORK": Var("the network a topic mirror's container"
                                 " runs on", "text", "host"),
    "MIGKIT_PGCOPYDB_NETWORK": Var("the network the streaming copy's"
                                   " container runs on", "text", "host",
                                   named_for_a_program=True),
    "MIGKIT_SECOND_READER_PYTHON": Var("the interpreter the second reader"
                                       " runs in", "path"),
    "MIGKIT_ACTOR": Var("the person who started a run from the web view",
                        internal=True),
    "MIGKIT_RUN_ID": Var("the id the web view drew for a run it started",
                         internal=True),
    "MIGKIT_HUAWEI_SDK_PYTHON": Var("the interpreter Huawei Cloud's API is"
                                    " driven from, in its environment of"
                                    " its own", "path"),
    # ---- the tests -------------------------------------------------------
    "MIGKIT_FAILPOINT": Var("stop a move where it keeps what it has done,"
                            " the way a crash would", "list", tests=True),
    "MIGKIT_TEST_KEYCHAIN": Var("the keychain the tests use", tests=True),
    "MIGKIT_TEST_ROUTES": Var("the cutover harness's routes file",
                              "path", tests=True),
    "MIGKIT_TEST_KEEP_ORACLE": Var("leave the Oracle container up", "flag",
                                   False, tests=True),
    "MIGKIT_LIVE_E2E": Var("run the end-to-end stream tests", "flag", False,
                           tests=True),
    "MIGKIT_MSSQL_IMAGE": Var("the SQL Server image the tests start",
                              tests=True),
    "MIGKIT_TERADATA_HOST": Var("the Teradata system the tests against a"
                                " server reach", tests=True),
    "MIGKIT_TERADATA_USER": Var("the user they sign in as", tests=True),
}

#: names read by a pattern rather than one by one: a hop's password
#: references (`env:MIGKIT_<HOP>_<SIDE>_PASSWORD`, as `init` writes them)
FAMILIES = ((re.compile(r"^MIGKIT_[A-Z0-9_]+_PASSWORD$"),
             Var("a password a hop's reference names", "secret")),)

_ON = ("1", "true", "yes", "on")
_OFF = ("0", "false", "no", "off")


class Bad(SystemExit):
    """A variable set to what its kind cannot be."""


def declared(name):
    """The declaration of `name`, or None."""
    got = VARS.get(name)
    if got is not None:
        return got
    for pattern, var in FAMILIES:
        if pattern.match(name):
            return var
    return None


def _var(name):
    got = declared(name)
    if got is None:
        raise KeyError(f"{name} is not declared in migkit.envkeys")
    return got


def _raw(name, environ):
    env = os.environ if environ is None else environ
    return str(env.get(name) or "").strip()


def flag(name, environ=None):
    """On or off: `1/true/yes/on` or `0/false/no/off`, any case; unset or
    empty is the declared default."""
    var = _var(name)
    raw = _raw(name, environ).lower()
    if not raw:
        return bool(var.default)
    if raw in _ON:
        return True
    if raw in _OFF:
        return False
    raise Bad(f"{name}={raw} is neither on ({', '.join(_ON)}) nor off"
              f" ({', '.join(_OFF)})")


def number(name, environ=None, default=None):
    """A number (`whole` ones whole); unset or empty is `default`, else
    the declared one. Below the declared least it is raised to it."""
    var = _var(name)
    raw = _raw(name, environ)
    if not raw:
        got = var.default if default is None else default
        return got
    try:
        got = int(raw) if var.kind == "whole" else float(raw)
    except ValueError:
        raise Bad(f"{name}={raw} is not a"
                  f" {'whole number' if var.kind == 'whole' else 'number'}")
    if var.least is not None and got < var.least:
        got = type(got)(var.least)
    return got


def text(name, environ=None, default=None):
    """The text, stripped; unset or empty is `default`, else the declared
    one ("" where there is none)."""
    var = _var(name)
    raw = _raw(name, environ)
    if raw:
        return raw
    got = var.default if default is None else default
    return "" if got is None else str(got)


def items(name, environ=None):
    """A `list` variable's items, separated by commas, blanks left out."""
    _var(name)
    return [p.strip() for p in _raw(name, environ).split(",") if p.strip()]


def secrets(environ=None):
    """The values of the secret variables set in the environment: what
    `redact` hides wherever they appear."""
    env = os.environ if environ is None else environ
    out = []
    for name, value in env.items():
        if not name.startswith("MIGKIT_") or not value:
            continue
        var = declared(name)
        if var is not None and var.secret:
            out.append(str(value))
    return out


def unknown(environ=None):
    """[(name, closest declared name or "")] for each `MIGKIT_*` in the
    environment that nothing reads - a typo reads as nothing and says
    nothing otherwise."""
    env = os.environ if environ is None else environ
    offered = [n for n, v in VARS.items()
               if not v.named_for_a_program and not v.tests
               and not v.internal]
    out = []
    for name in sorted(env):
        if not name.startswith("MIGKIT_") or declared(name) is not None:
            continue
        near = difflib.get_close_matches(name, offered, n=1, cutoff=0.8)
        out.append((name, near[0] if near else ""))
    return out


def said_unknown(environ=None):
    """`unknown` in words, one line each, for start-up."""
    return [f"{name} is not a variable migkit reads"
            + (f" - did you mean {near}?" if near else "")
            for name, near in unknown(environ)]


def reference():
    """The variables an operator may set, as a Markdown table: name,
    kind, default, what it does."""
    rows = ["| Variable | Kind | Default | What it does |",
            "|---|---|---|---|"]
    for name in sorted(VARS):
        var = VARS[name]
        if var.internal or var.tests or var.named_for_a_program:
            continue
        default = "" if var.default is None else (
            ("on" if var.default else "off") if var.kind == "flag"
            else str(var.default))
        rows.append(f"| `{name}` | {var.kind} | {default} | {var.what} |")
    return "\n".join(rows)
