"""An approval given in the view, counted where the hop's approvals are
counted (`approvals.require`) - beside the signatures approvers make
with their own SSH keys, never instead of the rules.

The request is the same file (`<step>-<db>.request.json`); an approver
signed in to the view approves it, and the view writes, beside it, what
was approved - the request's SHA-256 and nonce, the hop, the step and the
database - who approved (their name, their provider, the provider's own
identifier for them, their groups), their role on the hop then and when,
signed with the view's own Ed25519 key (its private half 0600 in the
view's directory). It counts, when a run asks, only:

* for this request - a request made again (expired, or its plan changed)
  has another digest, and an approval of the old one counts for nothing;
* under a signature the view's key made - a file written by hand beside
  the request, or one changed, counts for nothing;
* where the person approving holds the approver role on the hop now, not
  only when they approved (`roles`) - and, where the provider provisions
  people, is still provisioned and active (`scim`);
* where they are not the one who asked (`approvals` names the one who
  asked; a run started from the view asks under the name of the person
  who started it).

A hop whose `access` (or the file's) names no approver and no admin
counts no approval from the view, and behaves as it did before the view.
"""
import base64
import hashlib
import json
import time

from . import settings as _settings

#: the view's signing key and its public half, in the view's directory
KEY, PUBLIC = "approvals.key", "approvals.pub"
#: what an approval given in the view is written as, beside the request
SUFFIX = ".view.json"


def _dir(state=None):
    from .. import durable
    return durable.private_dir(state or _settings.state_dir())


def _private(state=None):
    """The view's signing key, made the first time it is asked for."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey)

    from .. import durable
    where = _dir(state)
    path = where / KEY
    try:
        return serialization.load_pem_private_key(path.read_bytes(), None)
    except FileNotFoundError:
        pass
    key = Ed25519PrivateKey.generate()
    durable.write_atomic(path, key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()), mode=0o600)
    durable.write_atomic(where / PUBLIC, key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo), mode=0o644)
    return key


def _public(state=None):
    from cryptography.hazmat.primitives import serialization
    where = state or _settings.state_dir()
    try:
        return serialization.load_pem_public_key(
            (where / PUBLIC).read_bytes())
    except (OSError, ValueError):
        return None


def _canonical(body):
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def _file(request_path, principal_id):
    stem = request_path.name[:-len(".request.json")]
    who = hashlib.sha256(str(principal_id).lower().encode()).hexdigest()[:16]
    return request_path.parent / f"{stem}.{who}{SUFFIX}"


def accepts(hop):
    """Whether an approval given in the view can count for this hop."""
    from . import roles
    try:
        return roles.anyone_approves(hop)
    except Exception:  # noqa: BLE001 - an unreadable file names nobody
        return False


def give(hop, request_path, principal, role, state=None):
    """The approval of `principal` written beside the request; its path.
    ValueError where the request is not there or is theirs."""
    from .. import durable
    data = request_path.read_bytes()
    req = json.loads(data)
    if str(req.get("asked_by", "")).lower() == principal.id.lower():
        raise ValueError("the one who asked does not approve their own"
                         " request")
    if int(req.get("expires", 0)) <= time.time():
        raise ValueError("the request has expired; the run asks again")
    body = {"v": 1, "hop": hop.name, "step": req.get("step"),
            "db": req.get("db"), "nonce": req.get("nonce"),
            "request_sha256": hashlib.sha256(data).hexdigest(),
            "by": principal.id, "name": principal.name,
            "issuer": principal.issuer, "subject": principal.subject,
            "groups": list(principal.groups), "role": role,
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    sig = _private(state).sign(_canonical(body))
    path = _file(request_path, principal.id)
    durable.write_json(path, {**body, "signature": base64.b64encode(
        sig).decode()}, mode=0o600, indent=1)
    return path


def given(request_path):
    """[the approvals written beside a request], unchecked - for showing
    who approved; `counted` is what counts."""
    stem = request_path.name[:-len(".request.json")]
    out = []
    for p in sorted(request_path.parent.glob(f"{stem}.*{SUFFIX}")):
        try:
            out.append(json.loads(p.read_text()))
        except (OSError, ValueError):
            continue
    return out


def counted(hop, request_path, data, req, state=None, store=None):
    """{the names of the people} whose approval given in the view counts
    for this request now - see the module's docstring for when one does."""
    if not accepts(hop):
        return set()
    key = _public(state)
    if key is None:
        return set()
    sha = hashlib.sha256(data).hexdigest()
    asked = str(req.get("asked_by", "")).lower()
    directory = store if store is not None else _directory(state)
    try:
        return _counted(hop, request_path, req, key, sha, asked, directory)
    finally:
        if store is None and directory is not None:
            directory.close()


def _counted(hop, request_path, req, key, sha, asked, directory):
    from cryptography.exceptions import InvalidSignature

    from . import roles
    from .identity import Principal
    # where the provider provisions people, only the people it provisioned
    kept = bool(directory is not None
                and directory.rows("select 1 from users limit 1"))
    out = set()
    for body in given(request_path):
        sig = body.pop("signature", "")
        try:
            key.verify(base64.b64decode(sig), _canonical(body))
        except (InvalidSignature, ValueError, TypeError):
            continue
        if body.get("request_sha256") != sha \
                or body.get("nonce") != req.get("nonce") \
                or body.get("hop") != hop.name \
                or body.get("step") != req.get("step") \
                or body.get("db") != req.get("db"):
            continue
        who = str(body.get("by") or "")
        if not who or who.lower() == asked:
            continue
        person = Principal(id=who, issuer=str(body.get("issuer") or ""),
                           subject=str(body.get("subject") or ""),
                           groups=tuple(body.get("groups") or ()),
                           via=str(body.get("issuer") or "").split(":")[0])
        got = directory.person(who) if directory is not None else None
        if got is not None:
            if not got["active"]:
                continue
            person = Principal(person.id, person.name, person.issuer,
                               person.subject,
                               tuple(dict.fromkeys(tuple(person.groups)
                                                   + tuple(got["groups"]))),
                               person.via)
        elif kept and person.via != "local":
            continue
        if roles.may(person, hop, "approver"):
            out.add(who)
    return out


def _directory(state=None):
    """The view's store, where there is one on this machine, read by the
    run that counts; None otherwise."""
    where = (state or _settings.state_dir()) / "web.db"
    if not where.exists():
        return None
    from .store import Store
    try:
        return Store(where)
    except Exception:  # noqa: BLE001 - no directory to read
        return None
