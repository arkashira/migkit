"""Signed checkpoints of the audit record, kept in a bucket that cannot be
rewritten (backlog R5.anchor).

The record's hash chain (`audit`) shows an entry changed, taken out or
moved - but not a record rewritten whole from its start by someone who can
write the file: the new chain is whole. So a checkpoint - the number of
entries the record stands at and the hash of the last - is put, as it is
made, in a bucket whose Object Lock holds each object unchangeable and
undeletable until its retention ends. `verify` reads the objects back and
holds the local record to them: entry N must still hash to what was
anchored for N.

    options:
      anchor:
        bucket: audit-anchors          # must have Object Lock enabled
        prefix: migkit-anchors/        # default migkit-anchors/
        region: eu-west-1              # optional
        endpoint_url: https://store.example.com   # an S3-compatible store
        retention: {mode: GOVERNANCE, days: 365}  # or COMPLIANCE
        required: false                # true: see "Failure" below
        allowed_signers: ~/.ssh/allowed_signers   # to check signatures

* **Credentials** are never in the hop: the store's own chain (the
  environment, a profile, the machine's role) is used, and a key in this
  option is refused by the configuration's check. Nothing printed or kept
  here carries one: every reason is put through `redact.Redactor`.
* **What is sent** is `{format, kind, seq, head, signature?}` - the digest
  of the record's last entry, how many entries stand to it, and where the
  hop names a key (`evidence: {sign: key}` or `MIGKIT_EVIDENCE_KEY`) an
  SSH signature over those two (`ssh-keygen -Y`, namespace
  `migkit-audit`). No row, table, schema, SQL, hostname or value-derived
  digest: the head hashes migkit's own entries, which hold what migkit
  did, and the signature is over the head. The object's key is
  `<prefix><hop>/audit/<seq>-<head>.json`; the hop's name is the
  operator's label for it. The egress gate (`egress`, kind `anchor`)
  decides first, like every other thing that leaves the machine.
* **Idempotency**: the key is the checkpoint, so the same checkpoint
  written twice is one object - the second write is a conditional one
  (`If-None-Match: *`) and a key already there is read back and held to
  the checkpoint: the same head and seq is "already anchored" (a write
  whose answer was lost is thereby settled, never doubled), a different
  one is a refusal naming the disagreement. A new version of a locked
  key is never made; `verify` still reads every version of every key and
  says a second one, since a write over a key hides the first from a
  plain read.
* **The bucket is checked first**: one without Object Lock is refused
  with its reason (an object there could be deleted or overwritten, so it
  would anchor nothing), and after each write the object is read back for
  the lock it was given - a copy the store did not hold is not an anchor.
* **When** a checkpoint is made: at the end of every run that left
  anything, with the evidence export (`export.write`). Not every
  entry - the head covers all before it, so a later checkpoint holds
  what an earlier missed.
* **Failure**: an anchor that cannot be written is said (through the
  run's output), kept in `anchors.json` beside the record as pending with
  its reason and tries, and written at the next run; inside one call a
  transient error (a timeout, the store's throttling or 5xx) is retried a
  few times with a growing wait, a refusal (no Object Lock, access
  denied) is not, since the same request would be refused again. It
  never stops the migration - unless the hop says `required: true`:
  then a writing command is refused before it begins when the bucket
  cannot hold an anchor (`require_ready`), and a checkpoint that could not
  be written ends the command with the reason (`Blocked`), the work it
  did stands. A record that does not verify is not anchored - the break
  would be locked in as the truth - and is said.
* **Verification** (`verify`, shown by `migkit history`): every
  checkpoint in the bucket read back and compared with the record
  (rewritten entry, entries taken out), with its own name (an object
  that is not what its key says), with the lock it must still hold
  (retention ended, no lock), with its signature where `allowed_signers`
  names who may have signed; and how many entries stand after the last
  anchor.

What this proves, and does not: a rewritten local record is shown, as
long as one checkpoint taken before the rewrite is in the bucket and its
retention has not ended. It does not show that the entries before an
anchor were true when written, entries since the last anchor, or a bucket
whose Object Lock was never real. Tested against a stand-in for the
store, not the store.
"""
import json
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

NAMESPACE = "migkit-audit"
FORMAT = 1
KIND = "migkit-audit-checkpoint"
MODES = ("GOVERNANCE", "COMPLIANCE")
PREFIX = "migkit-anchors/"
DAYS = 365
#: tries inside one call for a transient error, and the first wait
TRIES, WAIT = 3, 0.5
#: where the pending and last anchored checkpoint are kept, beside the record
STATE = "anchors.json"

#: answers that a repeat of the same request would meet again
REFUSALS = {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
            "NoSuchBucket", "InvalidBucketName", "AllAccessDisabled",
            "ObjectLockConfigurationNotFoundError", "InvalidRequest",
            "InvalidArgument", "AuthorizationHeaderMalformed",
            "PermanentRedirect", "MethodNotAllowed", "NotImplemented"}

_ALIEN = ("access_key", "secret_key", "secret_access_key", "session_token",
          "aws_access_key_id", "aws_secret_access_key", "password", "token")


class Refused(Exception):
    """A bucket or a setting that cannot hold an anchor, and why - the
    same request would be refused again."""


class Blocked(SystemExit):
    """The hop says `required: true`, and the anchor could not be written
    or cannot be: ends the command, with the reason."""


def _sleep(seconds):
    time.sleep(seconds)


def _now():
    return datetime.now(timezone.utc)


def _client(conf):
    """The store's client, from its own credentials chain."""
    try:
        import boto3
    except ImportError:
        raise Refused("the bucket cannot be reached: the library that"
                      " reaches it is not installed (pip install boto3)")
    kw = {}
    if conf.get("endpoint_url"):
        kw["endpoint_url"] = conf["endpoint_url"]
    if conf.get("region"):
        kw["region_name"] = conf["region"]
    return boto3.client("s3", **kw)


def settings(hop):
    """The hop's anchor setting read one way for everything that asks
    (also the configuration's check, `hopkeys`): None where the hop names
    none, else {bucket, prefix, region, endpoint_url, mode, days,
    required, allowed_signers}; a refusal naming the setting where it is
    wrong."""
    got = (getattr(hop, "options", None) or {}).get("anchor")
    if got is None or got is False:
        return None
    if not isinstance(got, dict):
        raise SystemExit("anchor: a mapping naming the bucket - bucket,"
                         " prefix, retention, required")
    for key in got:
        if str(key).lower() in _ALIEN:
            raise SystemExit(f"anchor: {key} is not kept in the hop - the"
                             " bucket is reached with the credentials the"
                             " machine already has (environment, profile"
                             " or role)")
    if not str(got.get("bucket") or "").strip():
        raise SystemExit("anchor: needs `bucket`, the object-locked bucket"
                         " the checkpoints go to")
    ret = got.get("retention") or {}
    if not isinstance(ret, dict):
        raise SystemExit("anchor: retention is {mode: GOVERNANCE or"
                         " COMPLIANCE, days: a whole number}")
    mode = str(ret.get("mode") or "GOVERNANCE").strip().upper()
    if mode not in MODES:
        raise SystemExit(f"anchor: retention mode {mode} is not one of"
                         f" {', '.join(MODES)}")
    try:
        days = int(DAYS if ret.get("days") is None else ret["days"])
    except (TypeError, ValueError):
        days = 0
    if days < 1:
        raise SystemExit("anchor: retention days is a whole number of at"
                         " least 1")
    req = got.get("required", False)
    if isinstance(req, str):
        req = req.strip().lower() in ("true", "yes", "on", "1", "required")
    prefix = str(got.get("prefix") or PREFIX).lstrip("/")
    return {"bucket": str(got["bucket"]).strip(),
            "prefix": prefix.rstrip("/") + "/",
            "region": got.get("region") or None,
            "endpoint_url": got.get("endpoint_url") or None,
            "mode": mode, "days": days, "required": bool(req),
            "allowed_signers": (os.path.expanduser(str(
                got["allowed_signers"])) if got.get("allowed_signers")
                else None)}


def key_of(conf, hop, seq, head):
    """The object a checkpoint is, named by what it is: written twice it
    is the one object."""
    return f"{conf['prefix']}{hop.name}/audit/{int(seq):012d}-{head}.json"


def _parse(conf, hop, key):
    """(seq, head) a key names, or None for an object that is not a
    checkpoint's."""
    base = f"{conf['prefix']}{hop.name}/audit/"
    if not key.startswith(base) or not key.endswith(".json"):
        return None
    seq, _, head = key[len(base):-5].partition("-")
    if not seq.isdigit() or len(head) != 64 \
            or any(c not in "0123456789abcdef" for c in head):
        return None
    return int(seq), head


def _code(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code", "")


def _why(hop, e):
    """A failure in words, with no credential in it: what the store
    answered, or the kind of failure."""
    from .redact import Redactor
    clean = Redactor(hop)
    if isinstance(e, (Refused, Blocked)):
        return clean(str(e))[:300]
    code = _code(e)
    if code:
        said = getattr(e, "response", {}).get("Error", {}).get("Message", "")
        return clean(f"the store answered {code}"
                     + (f" ({said})" if said else ""))[:300]
    return clean(f"the store could not be reached ({type(e).__name__})")


def _transient(e):
    if isinstance(e, Refused):
        return False
    code = _code(e)
    if code:
        return code not in REFUSALS and not code.startswith("4")
    return True


def check_bucket(s3, conf):
    """Raises `Refused` with the reason where the bucket cannot hold an
    anchor: not there, not reachable, or without Object Lock."""
    try:
        got = s3.get_object_lock_configuration(Bucket=conf["bucket"])
    except Exception as e:  # noqa: BLE001 - every answer is read
        code = _code(e)
        if code == "ObjectLockConfigurationNotFoundError":
            raise Refused(
                f"bucket {conf['bucket']} has no Object Lock, so a "
                "checkpoint in it could be deleted or overwritten and would"
                " anchor nothing; use a bucket made with Object Lock"
                " enabled (or enable it, with versioning, on this one)")
        if code in ("NoSuchBucket", "404"):
            raise Refused(f"bucket {conf['bucket']} does not exist")
        raise
    on = (got or {}).get("ObjectLockConfiguration", {}).get(
        "ObjectLockEnabled")
    if on != "Enabled":
        raise Refused(f"bucket {conf['bucket']} does not have Object Lock"
                      " enabled, so a checkpoint in it could be deleted or"
                      " overwritten and would anchor nothing")


def _payload(seq, head):
    """The text a signature is over."""
    return f"{KIND}\nseq={int(seq)}\nhead={head}\n"


def _signing_key(hop):
    from . import export
    return export._signing_key(hop)


def _signed(hop, seq, head):
    """The armoured SSH signature over the checkpoint, or None where the
    hop names no key; raises where it names one and cannot sign."""
    key = _signing_key(hop)
    if not key:
        return None
    from . import export
    with tempfile.TemporaryDirectory(prefix="migkit-anchor-") as tmp:
        path = Path(tmp) / "checkpoint"
        path.write_text(_payload(seq, head))
        try:
            export._sign(path, key, NAMESPACE)
        except Exception as e:  # noqa: BLE001 - the reason is the answer
            raise Refused("the checkpoint could not be signed with the key"
                          f" the hop names ({str(e)[:120]}); an unsigned"
                          " one is not written in its place, since a locked"
                          " object is never improved")
        return Path(str(path) + ".sig").read_text()


def _body(seq, head, signature):
    doc = {"format": FORMAT, "kind": KIND, "seq": int(seq), "head": head}
    if signature:
        doc["signature"] = signature
    return json.dumps(doc, sort_keys=True, separators=(",", ":"))


def put(hop, conf, seq, head, s3=None):
    """The checkpoint (`seq`, `head`) in the bucket, held by the lock the
    setting names; "written", or "already" where the key was there with
    this checkpoint. Raises `Refused` where the bucket cannot hold it or
    the key holds another checkpoint; any other error is the store's."""
    s3 = s3 or _client(conf)
    check_bucket(s3, conf)
    key = key_of(conf, hop, seq, head)
    until = _now() + timedelta(days=conf["days"])
    state = "already"
    if not _there(s3, conf, key, seq, head):
        signature = _signed(hop, seq, head)
        try:
            s3.put_object(Bucket=conf["bucket"], Key=key,
                          Body=_body(seq, head, signature).encode(),
                          ContentType="application/json",
                          IfNoneMatch="*",
                          ObjectLockMode=conf["mode"],
                          ObjectLockRetainUntilDate=until)
            state = "written"
        except Exception as e:  # noqa: BLE001 - one already there is read
            if _code(e) not in ("PreconditionFailed", "412",
                                "ConditionalRequestConflict", "409"):
                raise
            if not _there(s3, conf, key, seq, head):
                raise
    held = s3.head_object(Bucket=conf["bucket"], Key=key)
    if held.get("ObjectLockMode") not in MODES or not \
            held.get("ObjectLockRetainUntilDate"):
        raise Refused(f"the object written for checkpoint {seq} is not"
                      " held by Object Lock - the store did not apply the"
                      " retention asked for, so it anchors nothing")
    return state


def _there(s3, conf, key, seq, head):
    """Whether `key` holds this checkpoint: False where it is not there;
    `Refused` where it holds another."""
    try:
        got = s3.get_object(Bucket=conf["bucket"], Key=key)
    except Exception as e:  # noqa: BLE001 - only absence is an answer
        if _code(e) in ("NoSuchKey", "404", "NotFound"):
            return False
        raise
    try:
        doc = json.loads(got["Body"].read())
        same = doc.get("seq") == int(seq) and doc.get("head") == head
    except (ValueError, AttributeError):
        same = False
    if not same:
        raise Refused(f"the object for checkpoint {seq} is already in the"
                      " bucket and holds another checkpoint than its name"
                      " says - the bucket was written to by something else")
    return True


# ---- what is kept locally: pending, and the last that went --------------

def _state_path(hop):
    return Path(hop.report_dir()) / STATE


def _read(hop):
    from . import durable
    got = durable.read_state(_state_path(hop), "cache")
    return got if isinstance(got, dict) else {}


def _position(path):
    """(entries, head) of the record's last hashed entry, and what breaks
    it where it does (else "")."""
    from . import audit
    n, chained, broken = audit.verify(path)
    if broken:
        return 0, "", broken
    hashes = audit.hashes(path)
    for i in range(len(hashes), 0, -1):
        if hashes[i - 1]:
            return i, hashes[i - 1], ""
    return 0, "", ""


def checkpoint(hop, say=None, s3=None):
    """The record's head anchored, with any checkpoint still pending from
    an earlier run: None where the hop names no anchor, else
    {"anchored": [seq...], "already": [seq...], "pending": [{seq, why}]}.
    Said, never silent, when one could not be written; raises `Blocked`
    where the hop says `required: true` and one could not."""
    from . import egress
    conf = settings(hop)
    if conf is None:
        return None
    say = say or (lambda m: None)
    path = Path(hop.report_dir()) / "changelog.jsonl"
    seq, head, broken = _position(path)
    out = {"anchored": [], "already": [], "pending": []}
    if broken:
        why = ("the record does not verify, so it is not anchored (the"
               f" break would be locked in as the truth): {broken}")
        out["pending"].append({"seq": None, "why": why})
        return _settled(hop, conf, out, why, say, path)
    if not head:
        return out
    past = _read(hop).get("pending") or []
    wanted = {seq: head}
    local = None
    for one in past:
        if one.get("seq") in wanted or not one.get("head"):
            continue
        local = local if local is not None else _hashes(path)
        s = one["seq"]
        if isinstance(s, int) and 0 < s <= len(local) \
                and local[s - 1] == one["head"]:
            wanted[s] = one["head"]
        else:
            why = (f"checkpoint {s}, pending from an earlier run, is not"
                   " in the record any more - the record was changed since")
            out["pending"].append({"seq": s, "why": why})
            say(why)
    if not egress.allows(hop, "anchor", say) or not egress.permit(
            hop, "anchor", conf["endpoint_url"] or "https://s3.amazonaws.com",
            log=say):
        why = ("the audit checkpoint is not sent: the hop's egress does not"
               " let it out")
        for s in sorted(wanted):
            out["pending"].append({"seq": s, "why": why})
        return _settled(hop, conf, out, why, say, path, wanted)
    for s in sorted(wanted):
        h = wanted[s]
        why = None
        try:
            if s3 is None:
                s3 = _client(conf)
            state = _retry(hop, lambda: put(hop, conf, s, h, s3))
            out["anchored" if state == "written" else "already"].append(s)
        except Exception as e:  # noqa: BLE001 - said, kept, tried again
            why = _why(hop, e)
            out["pending"].append({"seq": s, "why": why})
            say(f"audit checkpoint {s} could not be anchored in bucket"
                f" {conf['bucket']} ({why}); it is kept and tried again at"
                " the next run" + ("" if not conf["required"] else
                                   " - this hop requires the anchor"))
    return _settled(hop, conf, out, None, say, path, wanted)


def _hashes(path):
    from . import audit
    return audit.hashes(path)


def _retry(hop, call):
    """`call` tried a few times while its failure is a transient one,
    with a growing wait; the last failure raised."""
    for n in range(TRIES):
        try:
            return call()
        except Exception as e:  # noqa: BLE001 - classified
            if n == TRIES - 1 or not _transient(e):
                raise
            _sleep(WAIT * (2 ** n))


def _settled(hop, conf, out, why, say, path, wanted=None):
    """Kept for the next run, and refused where the hop requires it."""
    from . import durable
    if why:
        say(f"audit checkpoint not anchored: {why}")
    wanted = wanted or {}
    now = time.strftime("%F %T")

    def change(doc):
        old = {p.get("seq"): p for p in doc.get("pending") or []}
        keep = []
        for p in out["pending"]:
            s = p.get("seq")
            prior = old.get(s, {})
            keep.append({"seq": s, "head": wanted.get(s) or prior.get("head"),
                         "why": p["why"], "tries": prior.get("tries", 0) + 1,
                         "since": prior.get("since", now)})
        doc["pending"] = keep
        if out["anchored"] or out["already"]:
            s = max(out["anchored"] + out["already"])
            doc["anchored"] = {"seq": s, "head": wanted.get(s), "at": now,
                               "bucket": conf["bucket"]}
    try:
        durable.change_json(_state_path(hop), change, kind="cache")
    except OSError as e:
        say(f"the anchors' state could not be kept ({type(e).__name__})")
    if out["pending"] and conf["required"]:
        reasons = "; ".join(sorted({p["why"] for p in out["pending"]}))
        raise Blocked(f"this hop requires its audit checkpoints anchored"
                      f" and one was not: {reasons}")
    return out


def require_ready(hop):
    """Before a command that writes: where the hop says `required: true`,
    the bucket must be able to hold an anchor now - reachable, with
    Object Lock, and let out by the egress. Raises `Blocked` with the
    reason; does nothing otherwise (an anchor not required never stops a
    migration)."""
    from . import egress
    conf = settings(hop)
    if conf is None or not conf["required"]:
        return
    if not egress.allows(hop, "anchor") or not egress.permit(
            hop, "anchor", conf["endpoint_url"] or "https://s3.amazonaws.com"):
        raise Blocked("this hop requires its audit checkpoints anchored and"
                      " the hop's egress does not let them out")
    try:
        check_bucket(_client(conf), conf)
    except Exception as e:  # noqa: BLE001 - the reason is the answer
        raise Blocked("this hop requires its audit checkpoints anchored"
                      f" and the bucket cannot hold them: {_why(hop, e)}")


# ---- the record held to the bucket ---------------------------------------

def _versions(s3, conf, hop):
    """{key: [version...]} and the delete markers' keys, for every
    object under the hop's prefix."""
    base = f"{conf['prefix']}{hop.name}/audit/"
    found, marked = {}, set()
    kw = {"Bucket": conf["bucket"], "Prefix": base}
    while True:
        got = s3.list_object_versions(**kw)
        for v in got.get("Versions") or []:
            found.setdefault(v["Key"], []).append(v)
        for m in got.get("DeleteMarkers") or []:
            marked.add(m["Key"])
        if not got.get("IsTruncated"):
            return found, marked
        kw["KeyMarker"] = got.get("NextKeyMarker")
        if got.get("NextVersionIdMarker"):
            kw["VersionIdMarker"] = got["NextVersionIdMarker"]


def _signature_holds(conf, doc, seq, head):
    """"" where the signature holds (or cannot be asked: no allowed
    signers named), else why not."""
    if not conf.get("allowed_signers"):
        return ""
    sig = doc.get("signature")
    if not sig:
        return "carries no signature, and the hop names who may have signed"
    from . import export
    with tempfile.TemporaryDirectory(prefix="migkit-anchor-") as tmp:
        path = Path(tmp) / "checkpoint"
        path.write_text(_payload(seq, head))
        sigpath = Path(tmp) / "checkpoint.sig"
        sigpath.write_text(sig)
        try:
            export._verify(path, sigpath, conf["allowed_signers"], NAMESPACE)
        except Exception as e:  # noqa: BLE001 - the reason is the answer
            return f"its signature does not hold ({str(e)[:120]})"
    return ""


def verify(hop, say=None, s3=None):
    """The record held to every checkpoint in the bucket: None where the
    hop names no anchor, else {"checked", "held", "problems": [..],
    "latest", "since"} - `problems` empty and `held` equal to `checked`
    where every checkpoint reads back, is what it is named, is still
    locked and is the entry now in the record; `since`, the entries after
    the latest anchor, which no anchor covers. A bucket that cannot be
    read is a problem, not a pass."""
    conf = settings(hop)
    if conf is None:
        return None
    from . import audit, egress
    path = Path(hop.report_dir()) / "changelog.jsonl"
    out = {"checked": 0, "held": 0, "problems": [], "latest": 0, "since": 0}
    problems = out["problems"]
    if not egress.allows(hop, "anchor", say) or not egress.permit(
            hop, "anchor", conf["endpoint_url"] or "https://s3.amazonaws.com",
            log=say):
        problems.append("the anchors were not read: the hop's egress does"
                        " not let migkit reach the bucket")
        return out
    _, _, broken = audit.verify(path)
    if broken:
        problems.append(f"the record itself does not verify: {broken}")
    local = audit.hashes(path)
    try:
        s3 = s3 or _client(conf)
        found, marked = _versions(s3, conf, hop)
    except Exception as e:  # noqa: BLE001 - not read is not a pass
        problems.append(f"the anchors could not be read ({_why(hop, e)})")
        return out
    now = _now()
    for key in sorted(found):
        named = _parse(conf, hop, key)
        if named is None:
            continue
        seq, head = named
        out["checked"] += 1
        bad = []
        if len(found[key]) > 1:
            bad.append(f"has {len(found[key])} versions - a later write over"
                       " the key hides the one anchored")
        if key in marked:
            bad.append("has a delete marker over it")
        for ver in found[key]:
            try:
                got = s3.get_object(Bucket=conf["bucket"], Key=key,
                                    VersionId=ver["VersionId"])
                doc = json.loads(got["Body"].read())
                if doc.get("seq") != seq or doc.get("head") != head:
                    bad.append("holds another checkpoint than its name says")
                    continue
                held = s3.head_object(Bucket=conf["bucket"], Key=key,
                                      VersionId=ver["VersionId"])
            except Exception as e:  # noqa: BLE001
                bad.append(f"could not be read back ({_why(hop, e)})")
                continue
            until = held.get("ObjectLockRetainUntilDate")
            if held.get("ObjectLockMode") not in MODES or not until:
                bad.append("is not held by Object Lock")
            elif until < now:
                bad.append(f"its retention ended {until:%F}; nothing holds"
                           " it unchangeable any more")
            why = _signature_holds(conf, doc, seq, head)
            if why:
                bad.append(why)
        if seq > len(local):
            bad.append(f"stands at entry {seq} and the record has"
                       f" {len(local)} - entries were taken out")
        elif local[seq - 1] != head:
            bad.append(f"is for entry {seq}, which is not the entry now"
                       " there - the record was rewritten")
        if bad:
            problems.extend(f"checkpoint {seq} {b}" for b in bad)
        else:
            out["held"] += 1
        out["latest"] = max(out["latest"], seq)
    out["since"] = max(0, len(local) - out["latest"])
    return out


def lines(result):
    """The result of `verify` in an operator's words."""
    if result is None:
        return []
    if not result["checked"] and not result["problems"]:
        return ["audit anchors: none in the bucket yet"]
    head = (f"audit anchors: {result['held']} of {result['checked']}"
            f" checkpoints read back from the bucket and held to the record"
            f" (latest at entry {result['latest']}, {result['since']}"
            " entries since it that no anchor covers)")
    return [head, *(f"  {p}" for p in result["problems"])]


# ---- the library's account (`tooluse`, backlog 0f) -----------------------
# what of the object store's client the anchor uses, read off this module

_S3 = ("put_object", "get_object", "head_object", "list_object_versions",
       "get_object_lock_configuration", "IfNoneMatch", "ObjectLockMode",
       "ObjectLockRetainUntilDate", "VersionId", "ContentType")


def _account():
    from . import tooluse
    tooluse.register(
        "boto3-s3-anchor", licence="Apache-2.0", release="1.40",
        declared={"s3": frozenset(_S3)}, options={},
        reads=(("anchor.py",), {("s3", o): o for o in _S3}))


_account()
