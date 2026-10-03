"""A step the hop says needs approval waits for it: approvers sign the
step's request with their own SSH keys, and migkit checks each signature
against the hop's list of who may approve.

    options:
      approvals:
        steps: [cutover, repair, rollback, source-setting,
                point-of-no-return]
        # source-setting: a change on the source a faster way needs, that
        # the operator answered yes to (`sourcechange.making`)
        # point-of-no-return: the cutover taken past going back - the
        # stream back dropped, the old source given back (`cutover`)
        count: 2                     # distinct approvers (1 by default)
        signers: /etc/migkit/allowed_signers   # ssh-keygen's format
        expires: 3600                # seconds a request stands (1 hour)

No new command: the request is a file in the run's reports, and an
approver signs it with the machine's own `ssh-keygen`, as migkit prints -
or approves it in the web view, signed in as themselves, where the hop's
`access` names who approves (`web.approve`; then `signers` may be left
out). A signature is counted only for this request - the hop, the step,
the database and when it expires, all signed - so one given for another
step, another run, or a request since changed, counts for nothing.

The one who asked does not count as an approver, told apart by key: the
request carries the fingerprint of the requester's own key
(`MIGKIT_IDENTITY`, or `~/.ssh/id_ed25519`) and is signed with it, and a
signature made with that key - or by the principal the signers list
names for it - is not counted. It was told apart by the login name, which
never matches a signers list's principals (e-mail addresses, usually), so
a requester's own signature counted; and `count: 0` approved with none.
In the web view the one who asked is the person a run started from the
view was started by (`MIGKIT_ACTOR`), and their approval there does not
count either. Every approval is written to the run's record.
"""
import getpass
import json
import os
import re
import secrets
import subprocess
import time

NAMESPACE = "migkit-approve"
#: what the requester signs its own request under
ASKED = "migkit-request"
#: every step a hop may hold for approval; a name not here is a mistake -
#: `[Cutover]` would have held nothing
STEPS = ("cutover", "repair", "rollback", "source-setting",
         "point-of-no-return")


def rules(hop):
    got = (hop.options or {}).get("approvals") or {}
    return got if isinstance(got, dict) else {}


def _requester():
    """Who asks: the person a run started from the web view was started
    by, the machine's user otherwise."""
    actor = os.environ.get("MIGKIT_ACTOR", "").strip()
    if actor:
        return actor
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - nobody to name
        return ""


def _identity():
    """The requester's own private key: `MIGKIT_IDENTITY`, as the
    evidence's is, or the default SSH key; None where there is none."""
    path = os.environ.get("MIGKIT_IDENTITY") or os.path.expanduser(
        "~/.ssh/id_ed25519")
    return path if os.path.isfile(path) else None


_FP = re.compile(r"SHA256:[A-Za-z0-9+/=]+")


def _keygen(argv, data=None):
    """`ssh-keygen` with no terminal and no window to ask a passphrase
    at: a key that needs one fails here rather than waiting."""
    kw = {} if data is not None else {"stdin": subprocess.DEVNULL}
    return subprocess.run(["ssh-keygen", *argv], input=data,
                          capture_output=True, start_new_session=True,
                          env={**os.environ, "SSH_ASKPASS_REQUIRE": "never"},
                          **kw)


def _fingerprint(key):
    """The fingerprint of `key` (a private key, its `.pub` beside it read
    where there is one), or None."""
    if not key:
        return None
    pub = key + ".pub" if os.path.isfile(key + ".pub") else key
    got = _keygen(["-l", "-f", pub])
    found = _FP.search((got.stdout or b"").decode("utf-8", "replace"))
    return found.group(0) if got.returncode == 0 and found else None


def _asked(path):
    """Where the requester's own signature of the request is kept: not a
    name the approvers' signatures are looked for under."""
    return path.with_name(path.name[:-len(".json")] + ".asked")


def _sign_request(path, key):
    """The request signed with the requester's key, beside it; False where
    the key cannot sign here (one with a passphrase and no agent)."""
    if not key:
        return False
    got = _keygen(["-Y", "sign", "-n", ASKED, "-f", key], path.read_bytes())
    if got.returncode != 0 or not got.stdout:
        return False
    _asked(path).write_bytes(got.stdout)
    return True


def _request_signed_by(path):
    """The fingerprint of the key the request itself is signed with, where
    the signature holds over the file as it is now; "" where it is not
    signed; None where it is and does not hold - the file was changed."""
    sig = _asked(path)
    if not sig.exists():
        return ""
    got = _keygen(["-Y", "check-novalidate", "-n", ASKED, "-s", str(sig)],
                  path.read_bytes())
    found = _FP.search((got.stdout or b"").decode("utf-8", "replace")
                       + (got.stderr or b"").decode("utf-8", "replace"))
    return found.group(0) if got.returncode == 0 and found else None


def _request(hop, step, db, where, rule, detail=None, key=None):
    """The standing request for this step, made where there is none, the
    one there has expired, what it asks approval of has changed since, it
    was asked by another key than this one, or the file was changed after
    its requester signed it: a signature is for the plan it was shown, not
    for whatever runs under the same step's name."""
    path = where / f"{step}-{db or 'all'}.request.json"
    mine = _fingerprint(key)
    try:
        req = json.loads(path.read_text())
        by = _request_signed_by(path)
        if req.get("expires", 0) > time.time() \
                and req.get("detail") == detail \
                and req.get("asked_by_key") == mine \
                and by is not None and (not by or by == mine):
            return path, req
    except (OSError, ValueError):
        pass
    req = {"hop": hop.name, "step": step, "db": db,
           "asked_by": _requester(), "nonce": secrets.token_hex(8),
           "expires": int(time.time()) + int(rule.get("expires", 3600))}
    if mine:
        req["asked_by_key"] = mine
    if detail is not None:
        req["detail"] = detail
    where.mkdir(parents=True, exist_ok=True)
    for old in where.glob(f"{step}-{db or 'all'}.*.sig"):
        old.unlink()
    _asked(path).unlink(missing_ok=True)
    path.write_text(json.dumps(req, sort_keys=True))
    _sign_request(path, key)
    return path, req


def _signer(sig, signers, data):
    """(the principal whose key made `sig` over `data`, the key's
    fingerprint), or (None, None)."""
    found = subprocess.run(["ssh-keygen", "-Y", "find-principals", "-s",
                            str(sig), "-f", str(signers)],
                           capture_output=True, text=True)
    for principal in found.stdout.split():
        ok = subprocess.run(["ssh-keygen", "-Y", "verify", "-f",
                             str(signers), "-I", principal, "-n",
                             NAMESPACE, "-s", str(sig)], input=data,
                            capture_output=True)
        if ok.returncode == 0:
            said = (ok.stdout or b"").decode("utf-8", "replace") + \
                (ok.stderr or b"").decode("utf-8", "replace")
            fp = _FP.search(said)
            return principal, fp.group(0) if fp else None
    return None, None


def _principals_of(key, signers):
    """The principals the signers list names for the requester's key -
    the requester, whichever of their keys signs."""
    if not key:
        return set()
    pub = key + ".pub" if os.path.isfile(key + ".pub") else None
    if pub is None:
        return set()
    try:
        mine = open(pub).read().split()[:2]
        listed = open(signers).read().splitlines()
    except OSError:
        return set()
    out = set()
    for line in listed:
        words = line.split()
        if len(words) >= 3 and mine and all(w in words for w in mine):
            out |= set(words[0].split(","))
    return out


def check_steps(rule):
    """The hop's steps, each one migkit holds; a name it does not is said
    - it would hold nothing."""
    from .verdict import Usage
    steps = rule.get("steps") or []
    if not isinstance(steps, (list, tuple)):
        raise Usage("approvals.steps: a list of steps -"
                    f" {', '.join(STEPS)}")
    odd = [str(s) for s in steps if str(s) not in STEPS]
    if odd:
        raise Usage(f"approvals.steps: {', '.join(odd)} - not a step"
                    f" migkit holds; the steps are {', '.join(STEPS)}")
    return list(steps)


def needed(hop, step):
    """Whether the hop holds `step` for approval - asked by a plan, which
    says so without making a request."""
    return step in check_steps(rules(hop))


def require(hop, step, db, log=None, detail=None):
    """Returns the approvers where the step may go ahead; raises SystemExit
    saying what to do where the hop asks for approval and it is not all
    there yet. A hop that asks for none goes ahead at once. `detail` is
    what the step will do (the cutover's plan), signed with the rest."""
    rule = rules(hop)
    if step not in check_steps(rule):
        return []
    from .web import approve as view
    if _signers(rule) is None and not view.accepts(hop):
        raise SystemExit(f"{step} needs approval, and the hop's"
                         " `approvals.signers` - the list of who may"
                         " approve - is not there, and no one approves it"
                         " in the web view either (the hop's `access`"
                         " names no approver)")
    where = hop.report_dir(db) / "approvals"
    key = _identity()
    path, req = _request(hop, step, db, where, rule, detail, key)
    need = max(1, int(rule.get("count", 1)))
    who = _approvers(hop, rule, path, req, key)
    from . import notify
    if len(who) >= need:
        from . import audit
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "approved", "step": step, "db": db,
                      "approvers": sorted(who), "request": req["nonce"]})
        if log:
            log(f"{step} approved by {', '.join(sorted(who))}")
        notify.approved(hop, step, db, sorted(who), log)
        return sorted(who)
    stop = (
        f"{step} needs {need} approval{'s' if need > 1 else ''} and has"
        f" {len(who)}"
        + (f" ({', '.join(sorted(who))})" if who else "")
        + f". Nothing was done. An approver runs, until"
        f" {time.strftime('%F %T', time.localtime(req['expires']))}:\n"
        f"    ssh-keygen -Y sign -n {NAMESPACE} -f ~/.ssh/id_ed25519"
        f" < {path} > {path.parent / (path.name[:-len('.request.json')])}"
        ".$(whoami).sig\n"
        + ("or approves it in the web view, on the hop's page;\n"
           if view.accepts(hop) else "")
        + "then this is run again.")
    # the approvers are told where the team is, once for this request
    notify.approval_asked(hop, step, db, need, len(who), req["expires"],
                          req["nonce"], log, stop=stop)
    raise SystemExit(stop)


def _signers(rule):
    """The hop's list of who signs with their own key, where it is there."""
    signers = rule.get("signers")
    if not signers:
        return None
    signers = os.path.expanduser(str(signers))
    if not os.path.exists(signers):
        return None
    # whoever may change the list may approve: refused where anyone but
    # this user or root may (audit SEC21)
    from .config import trusted_file
    trusted_file(signers, "the approvals' signers list",
                 owners=(os.getuid(), 0))
    return signers


def _approvers(hop, rule, path, req, key=None):
    """{who} approved the request at `path`: each signature made with an
    approver's own key over it, and each approval given in the web view
    that holds (`web.approve.counted`) - never the one who asked, by name
    or by key (`key`: the requester's own, where this is the requester's
    run; the request names its fingerprint either way)."""
    from .web import approve as view
    data = path.read_bytes()
    who = set()
    signers = _signers(rule)
    if signers is not None:
        asker = {req.get("asked_by")} | _principals_of(key, signers)
        theirs = {req.get("asked_by_key"), _fingerprint(key)} - {None}
        for sig in sorted(path.parent.glob(
                f"{path.name[:-len('.request.json')]}.*.sig")):
            principal, fp = _signer(sig, signers, data)
            if principal and principal not in asker and fp not in theirs:
                who.add(principal)
    who |= view.counted(hop, path, data, req)
    return who


#: what approving each step lets happen, as the web view says it
LETS = {
    "cutover": "the cutover goes ahead: the source's writes are held, the"
               " last changes applied, the application moved to the target",
    "repair": "the repair writes the target's rows, its undo kept",
    "rollback": "the saved state is put back on the target",
    "source-setting": "migkit makes the change on the source it was answered"
                      " yes to, records it and takes it off again after",
    "point-of-no-return": "the cutover goes past going back: the stream back"
                          " is dropped and the old source given back",
}


def standing(hop):
    """[(request path, request, approvals needed, {who approved})] of
    every request of the hop standing now - not expired - for the web
    view, counted the way `require` counts them."""
    rule = rules(hop)
    need = max(1, int(rule.get("count", 1)))
    root = hop.report_dir()
    out = []
    for path in sorted(list(root.glob("approvals/*.request.json"))
                       + list(root.glob("*/approvals/*.request.json"))):
        try:
            req = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(req, dict) or req.get("hop") != hop.name \
                or int(req.get("expires", 0)) <= time.time():
            continue
        out.append((path, req, need, _approvers(hop, rule, path, req)))
    return out
