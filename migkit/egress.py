"""What leaves the machine, and the one gate it goes through (audit
2026-10-01, SEC12, SEC20, W8).

migkit sends five things off the machine of its own accord, each only
where the operator configured it: the hop's notifications (`notify`), the
run's spans to an OpenTelemetry collector (`traces`), findings and code
to a language model (`assist`), the evidence's digests into the run
state's bucket (`evidence`), and the audit record's checkpoints into an
object-locked bucket (`anchor`). Each asks this gate first, with where it
would go, and sends nothing when the gate says no - no address read from
where it is kept, no name looked up, no connection opened. Measured
before the gate: each of the first four decided alone, with its own idea of
what to take out, and nothing let a machine in a closed network say
"nothing leaves" once.

Who says what may leave, the stricter of the two winning:

* the environment, for every hop: `MIGKIT_EGRESS=none` (nothing), or
  the kinds that may (`MIGKIT_EGRESS=notify,evidence`); and
  `MIGKIT_EGRESS_TO`, the hosts anything may go to
* the hop's option

      options:
        egress: none                      # nothing leaves for this hop
        egress: [notify]                  # only its notifications
        egress:
          allow: [notify, traces]
          to: ["*.corp.example.com", 10.0.0.0/8, localhost]

  `to` names the hosts - shell patterns, addresses and networks - a
  send may go to; one whose host is not among them, or whose host
  migkit cannot tell (a channel reached by a token, whose service is
  its vendor's), is refused.

Left unset, everything the hop configures goes as it did. A refusal is
said once a run in the run log and to the command's own output.

Whatever passes is cleaned here, one way for all four (`words`,
`facts`): every secret the hop holds, in every form, and every secret by
its shape (`redact.Redactor`), and no program migkit drives by name
(`export.words`). What a hop masks is the sender's to leave out: only it
knows which words are a row's.
"""
import fnmatch
import ipaddress
import threading
from urllib.parse import urlsplit

#: what may leave, in the words a refusal says it with
KINDS = {
    "notify": "notifications to the hop's receivers",
    "traces": "the run's spans to the OpenTelemetry collector",
    "assist": "findings and code to a language model",
    "evidence": "the evidence's digests in the run state's bucket",
    "anchor": "the audit record's checkpoints in the anchor bucket",
}

NONE = ("none", "off", "false", "no", "0")

_SAID = set()
_LOCK = threading.Lock()


def _listed(raw, where):
    """The kinds a setting lets out: a list, a comma-separated text, or
    none of them; an unknown kind is an error naming the setting."""
    if raw is None:
        return None
    if raw is False or (isinstance(raw, str) and raw.strip().lower()
                        in NONE):
        return set()
    items = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
    got = {str(k).strip().lower() for k in items if str(k).strip()}
    bad = sorted(got - set(KINDS))
    if bad:
        raise SystemExit(f"{where}: {', '.join(bad)} is not something"
                         " migkit sends; one of " + ", ".join(KINDS)
                         + ", or none")
    return got


def _hosts(raw):
    if raw is None:
        return None
    items = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
    return [str(h).strip().lower() for h in items if str(h).strip()]


def settings(hop):
    """{"allow": the kinds that may leave, or None for all; "to": the
    lists of hosts a send's host must be in, each of them - the
    environment's and the hop's, the stricter winning}."""
    from . import envkeys
    allow = _listed(envkeys.text("MIGKIT_EGRESS") or None, "MIGKIT_EGRESS")
    to = [x for x in [_hosts(envkeys.text("MIGKIT_EGRESS_TO") or None)]
          if x is not None]
    got = ((getattr(hop, "options", None) or {}).get("egress")
           if hop is not None else None)
    where = f"hops.{getattr(hop, 'name', '?')}.options.egress"
    if isinstance(got, dict):
        mine = _listed(got.get("allow"), where + ".allow")
        there = _hosts(got.get("to"))
    else:
        mine, there = _listed(got, where), None
    if mine is not None:
        allow = mine if allow is None else allow & mine
    if there is not None:
        to.append(there)
    return {"allow": allow, "to": to}


def host_of(address):
    """The host a send to `address` reaches, or None where it cannot be
    told from the address."""
    text = str(address or "").strip()
    if not text:
        return None
    if "://" not in text:
        text = "//" + text
    try:
        return (urlsplit(text).hostname or "").lower() or None
    except ValueError:
        return None


def _matches(host, patterns):
    if host is None:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    for p in patterns:
        if ip is not None and "/" in p:
            try:
                if ip in ipaddress.ip_network(p, strict=False):
                    return True
            except ValueError:
                pass
        if fnmatch.fnmatch(host, p):
            return True
    return False


def decide(hop, kind, address=None, host=None):
    """(True, "") where `kind` may go to `address` (or to `host`, where
    the sender knows it and the address does not say), else (False, why)
    in words: the setting that said no, never the address."""
    if kind not in KINDS:
        raise ValueError(f"{kind!r} is not a kind of send")
    conf = settings(hop)
    said = KINDS[kind]
    if conf["allow"] is not None and kind not in conf["allow"]:
        who = ("the hop's egress" if _hop_says(hop, kind) else
               "MIGKIT_EGRESS")
        return False, f"{said} are not sent: {who} does not let them out"
    if conf["to"]:
        where = host or host_of(address)
        if where is None:
            return False, (f"{said} are not sent: where this one goes"
                           " cannot be told from its address, and the"
                           " egress names the hosts sends may go to")
        if not all(_matches(where, hosts) for hosts in conf["to"]):
            return False, (f"{said} are not sent to a host the egress"
                           " does not name")
    return True, ""


def _hop_says(hop, kind):
    got = (getattr(hop, "options", None) or {}).get("egress") \
        if hop is not None else None
    if isinstance(got, dict):
        got = got.get("allow")
    try:
        mine = _listed(got, "")
    except SystemExit:
        return True
    return mine is not None and kind not in mine


def allows(hop, kind, log=None):
    """Whether `kind` may leave at all, wherever it would go: asked before
    anything is read or looked up for a send."""
    conf = settings(hop)
    if conf["allow"] is None or kind in conf["allow"]:
        return True
    return _refused(hop, kind, decide(hop, kind)[1], log)


def permit(hop, kind, address=None, host=None, log=None):
    """Whether `kind` may go to `address`: a refusal said once a run, in
    the run log and through `log`."""
    ok, why = decide(hop, kind, address, host)
    if ok:
        return True
    return _refused(hop, kind, why, log)


def _refused(hop, kind, why, log):
    from . import runlog, session
    key = (session.current(), getattr(hop, "name", None), kind, why)
    with _LOCK:
        fresh = key not in _SAID
        _SAID.add(key)
    if fresh:
        runlog.event("egress-refused", why, level="warn", kind=kind)
        if log is not None:
            log(why)
    return False


def nothing_leaves(hop=None):
    """Whether nothing at all may leave for this hop (or, with none, for
    any): the closed network W8 describes."""
    allow = settings(hop)["allow"]
    return allow is not None and not allow


def _redactor(hop):
    from .redact import Redactor
    return Redactor(hop)


def words(hop, text):
    """`text` as it may leave the machine: no secret the hop holds in any
    form, no secret by its shape, no program migkit drives named."""
    if text is None:
        return None
    from .export import words as plain
    return plain(_redactor(hop)(str(text)))


def facts(hop, value):
    """A mapping sent beside a message, as it may leave: a key that names
    a secret loses its value, every text goes through `words`."""
    from .export import words as plain
    got = _redactor(hop).value(value)

    def clean(v):
        if isinstance(v, dict):
            return {k: clean(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [clean(x) for x in v]
        if isinstance(v, str):
            return plain(v)
        return v
    return clean(got)
