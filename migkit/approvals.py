"""A step the hop says needs approval waits for it: approvers sign the
step's request with their own SSH keys, and migkit checks each signature
against the hop's list of who may approve.

    options:
      approvals:
        steps: [cutover, repair, rollback]
        count: 2                     # distinct approvers (1 by default)
        signers: /etc/migkit/allowed_signers   # ssh-keygen's format
        expires: 3600                # seconds a request stands (1 hour)

No server and no new command: the request is a file in the run's reports,
and an approver signs it with the machine's own `ssh-keygen`, as migkit
prints. A signature is counted only for this request - the hop, the step,
the database and when it expires, all signed - so one given for another
step, another run, or a request since changed, counts for nothing. The one
who asked does not count as an approver, and every approval is written to
the run's record.
"""
import getpass
import json
import os
import secrets
import subprocess
import time

NAMESPACE = "migkit-approve"


def rules(hop):
    got = (hop.options or {}).get("approvals") or {}
    return got if isinstance(got, dict) else {}


def _requester():
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - nobody to name
        return ""


def _request(hop, step, db, where, rule):
    """The standing request for this step, made where there is none or
    the one there has expired."""
    path = where / f"{step}-{db or 'all'}.request.json"
    try:
        req = json.loads(path.read_text())
        if req.get("expires", 0) > time.time():
            return path, req
    except (OSError, ValueError):
        pass
    req = {"hop": hop.name, "step": step, "db": db,
           "asked_by": _requester(), "nonce": secrets.token_hex(8),
           "expires": int(time.time()) + int(rule.get("expires", 3600))}
    where.mkdir(parents=True, exist_ok=True)
    for old in where.glob(f"{step}-{db or 'all'}.*.sig"):
        old.unlink()
    path.write_text(json.dumps(req, sort_keys=True))
    return path, req


def _signer(sig, signers, data):
    """The principal whose key made `sig` over `data`, or None."""
    found = subprocess.run(["ssh-keygen", "-Y", "find-principals", "-s",
                            str(sig), "-f", str(signers)],
                           capture_output=True, text=True)
    for principal in found.stdout.split():
        ok = subprocess.run(["ssh-keygen", "-Y", "verify", "-f",
                             str(signers), "-I", principal, "-n",
                             NAMESPACE, "-s", str(sig)], input=data,
                            capture_output=True)
        if ok.returncode == 0:
            return principal
    return None


def require(hop, step, db, log=None):
    """Returns the approvers where the step may go ahead; raises SystemExit
    saying what to do where the hop asks for approval and it is not all
    there yet. A hop that asks for none goes ahead at once."""
    rule = rules(hop)
    if step not in (rule.get("steps") or []):
        return []
    signers = rule.get("signers")
    if not signers or not os.path.exists(os.path.expanduser(str(signers))):
        raise SystemExit(f"{step} needs approval, and the hop's"
                         " `approvals.signers` - the list of who may"
                         " approve - is not there")
    signers = os.path.expanduser(str(signers))
    where = hop.report_dir(db) / "approvals"
    path, req = _request(hop, step, db, where, rule)
    data = path.read_bytes()
    need = int(rule.get("count", 1))
    who = set()
    for sig in sorted(where.glob(f"{path.name[:-len('.request.json')]}"
                                 ".*.sig")):
        principal = _signer(sig, signers, data)
        if principal and principal != req.get("asked_by"):
            who.add(principal)
    if len(who) >= need:
        from . import audit
        audit.append(hop.report_dir() / "changelog.jsonl",
                     {"op": "approved", "step": step, "db": db,
                      "approvers": sorted(who), "request": req["nonce"]})
        if log:
            log(f"{step} approved by {', '.join(sorted(who))}")
        return sorted(who)
    raise SystemExit(
        f"{step} needs {need} approval{'s' if need > 1 else ''} and has"
        f" {len(who)}"
        + (f" ({', '.join(sorted(who))})" if who else "")
        + f". Nothing was done. An approver runs, until"
        f" {time.strftime('%F %T', time.localtime(req['expires']))}:\n"
        f"    ssh-keygen -Y sign -n {NAMESPACE} -f ~/.ssh/id_ed25519"
        f" < {path} > {path.parent / (path.name[:-len('.request.json')])}"
        ".$(whoami).sig\n"
        "then this is run again.")
