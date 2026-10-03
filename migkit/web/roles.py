"""Who may do what on which hop, in the view and the API (backlog W2).

Four roles, each holding the ones before it:

* **viewer** - reads the hop: what it does to each side, the prediction
  with its spread, the progress, the evidence, the approvals and offers
  standing, the decision record when asked.
* **operator** - also starts runs (the plan, a check, the move, a repair,
  the cutover, a rollback, the evidence export), holds or lets go a
  change tail, and answers a change on the source that a faster way
  needs - what someone at the terminal under `--go` answers.
* **approver** - also approves a step the hop's `approvals` hold back,
  never one they asked for themselves (`approvals`).
* **admin** - also sends the hop's receivers a test message; a global
  admin also reads the users and groups provisioned, every session and
  the view's own record.

Where the roles are written - the hops file, never a second one, and
never by the view (the API writes no configuration):

    access:                          # the file's own, beside `hops`
      admin: [ops@example.com, "group:migkit-admins"]
      groups:                        # a group of hops, by name pattern
        payments:
          hops: ["pay-*", orders]
          viewer: ["*@example.com"]
          operator: ["group:dba"]
          approver: ["group:change-board"]
    hops:
      orders:
        options:
          access:                    # this hop's own, added to the above
            operator: [ann@example.com]

A person is matched by their name as the identity provider gives it (an
e-mail address, shell patterns allowed, any case) or by a group they are
in (`group:NAME`) - from the provider's own claim or attribute, or as it
provisions them (`scim`). Their role on a hop is the highest any of the
three places gives them.

Nobody signed in through an identity provider has any role the file does
not give: a view shared with a company's whole directory shows nothing
to someone nobody named. The one exception is the single operator of an
install without a provider (`identity.Local`), who is the admin of every
hop - there is nobody else to give a role to.
"""
import fnmatch
import threading
from pathlib import Path

#: the roles, weakest first; each holds every one before it
ROLES = ("viewer", "operator", "approver", "admin")
RANK = {name: n for n, name in enumerate(ROLES, 1)}

#: what each role may do, as the pages and the manual say it
WHAT = {
    "viewer": "reads the hop: what it does to each side, the prediction"
              " with its spread, the progress, the evidence, the approvals"
              " and offers standing, and the decision record when asked",
    "operator": "also starts runs, holds or lets go a change tail, and"
                " answers a change on the source a faster way needs",
    "approver": "also approves a step the hop's approvals hold back - never"
                " one they asked for",
    "admin": "also sends the hop's receivers a test message; a global admin"
             " also reads the users, groups, sessions and the view's record",
}

#: the prefix of a pattern that names a group rather than a person
GROUP = "group:"

_CACHE = {"key": None, "access": {}}
_LOCK = threading.Lock()


def _conf_path(path=None):
    from .. import config
    return Path(path or config.CONF)


def access_config(path=None):
    """The hops file's own `access` mapping ({} where it has none), read
    again only when the file changes."""
    import yaml
    p = _conf_path(path)
    try:
        st = p.stat()
    except OSError:
        return {}
    key = (str(p), st.st_mtime_ns, st.st_size)
    with _LOCK:
        if _CACHE["key"] == key:
            return _CACHE["access"]
    try:
        data = yaml.safe_load(p.read_text()) or {}
    except (OSError, yaml.YAMLError):
        data = {}
    got = data.get("access") if isinstance(data, dict) else None
    got = got if isinstance(got, dict) else {}
    with _LOCK:
        _CACHE.update(key=key, access=got)
    return got


def _names(value):
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value or [] if v is not None]


def matches(principal, pattern):
    """Whether `pattern` names the person: `group:NAME` any group they
    are in, anything else their name as a shell pattern - both in any
    case, as providers differ in how they write them."""
    pattern = str(pattern).strip()
    if not pattern:
        return False
    if pattern.lower().startswith(GROUP):
        want = pattern[len(GROUP):].strip().lower()
        return any(str(g).strip().lower() == want
                   for g in principal.groups or ())
    return fnmatch.fnmatchcase(str(principal.id).lower(), pattern.lower())


def _best(principal, grants):
    """The highest role a mapping of {role: [pattern]} gives."""
    best = None
    if not isinstance(grants, dict):
        return None
    for name in ROLES:
        if any(matches(principal, p) for p in _names(grants.get(name))):
            best = name
    return best


def _higher(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if RANK[a] >= RANK[b] else b


def is_admin(principal, conf=None):
    """A global admin: named in the file's `access.admin`, or the single
    operator of an install without an identity provider."""
    if principal is None:
        return False
    if principal.via == "local":
        return True
    conf = access_config() if conf is None else conf
    return any(matches(principal, p) for p in _names(conf.get("admin")))


def groups_of(hop_name, conf=None):
    """The names of the hop groups `hop_name` is in."""
    conf = access_config() if conf is None else conf
    out = []
    for name, g in (conf.get("groups") or {}).items():
        if isinstance(g, dict) and any(
                fnmatch.fnmatchcase(str(hop_name), str(p))
                for p in _names(g.get("hops"))):
            out.append(str(name))
    return out


def role(principal, hop, conf=None):
    """The person's role on `hop` - one of `ROLES` - or None, where the
    hop is not theirs to see."""
    if principal is None:
        return None
    if is_admin(principal, conf):
        return "admin"
    conf = access_config() if conf is None else conf
    got = None
    for name in groups_of(hop.name, conf):
        got = _higher(got, _best(principal, conf["groups"][name]))
    own = (hop.options or {}).get("access")
    got = _higher(got, _best(principal, own))
    return got


def may(principal, hop, need, conf=None):
    """Whether the person holds `need` (or a role above it) on `hop`."""
    got = role(principal, hop, conf)
    return got is not None and RANK[got] >= RANK[need]


def anyone_approves(hop, conf=None):
    """Whether any pattern gives anyone the approver role (or admin) on
    `hop` - so an approval given in the view can count for it. A hop
    that names nobody behaves as it did before the view existed."""
    conf = access_config() if conf is None else conf
    if _names(conf.get("admin")):
        return True
    places = [conf["groups"][n] for n in groups_of(hop.name, conf)]
    places.append((hop.options or {}).get("access"))
    for grants in places:
        if isinstance(grants, dict) and (_names(grants.get("approver"))
                                         or _names(grants.get("admin"))):
            return True
    return False


def hop_problems(access):
    """What is wrong with a hop's own `options.access`, as (where,
    words): a key that is not a role - a typo would give nobody the role
    the file meant to give - or a role not written as names."""
    out = []
    if not isinstance(access, dict):
        return out
    for k, v in access.items():
        if k not in ROLES:
            out.append((f"access.{k}", f"is not a role - one of"
                                       f" {', '.join(ROLES)}"))
        elif not isinstance(v, (list, str)):
            out.append((f"access.{k}", "is not a list of names"))
    return out


def problems(access):
    """What is wrong with a file's `access` mapping, as (where, words):
    an unknown key, a role written as something other than a list of
    names, a group with no hops."""
    out = []
    if access is None:
        return out
    if not isinstance(access, dict):
        return [("access", "is not a mapping of roles and groups")]
    for k, v in access.items():
        if k == "groups":
            if not isinstance(v, dict):
                out.append(("access.groups", "is not a mapping of groups"))
                continue
            for name, g in v.items():
                where = f"access.groups.{name}"
                if not isinstance(g, dict):
                    out.append((where, "is not a mapping"))
                    continue
                if not _names(g.get("hops")):
                    out.append((where, "names no hops (`hops:` patterns)"))
                for gk, gv in g.items():
                    if gk == "hops":
                        continue
                    if gk not in ROLES:
                        out.append((f"{where}.{gk}",
                                    f"is not a role - one of"
                                    f" {', '.join(ROLES)}"))
                    elif not isinstance(gv, (list, str)):
                        out.append((f"{where}.{gk}",
                                    "is not a list of names"))
        elif k == "admin":
            if not isinstance(v, (list, str)):
                out.append(("access.admin", "is not a list of names"))
        else:
            out.append((f"access.{k}", "read by nothing - `admin` and"
                                       " `groups` are"))
    return out
