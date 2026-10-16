"""Values from the rows, masked where the hop asks, wherever a person or a
report sees them (G2).

A drilldown is meant to be read, pasted into a ticket, attached to a change
request. It showed keys and values as they were, and a key can be an email
address and a value a card number. The hop option `mask` names what is
masked:
* `all` (or `true`): every value shown from a row, keys included
* a list of columns, `column` or `table.column`, matched like every other
  name in the hop: those columns' values; a table with any of them has its
  keys shown masked too, because a key shown whole can be the column itself

A masked value is shown as `masked:` and ten hex digits, a keyed HMAC-SHA256
of the value (backlog R17b.hmac). Equal values show equal, so a comparison
over masked values still lines rows up and still says which differ. The
key is made of two parts, and the report holds only one:
* the hop's salt (`mask.salt`, in the hop's report directory, readable by
  this user only) - a domain separator, so another hop shows other tokens
* a secret kept local to the user, outside every report directory
  (`mask.key` in the state directory, `config.state_home`, 0600)

A report that is shared with its salt still cannot be tested against a list
of guessed emails or card numbers: without the secret, a token confirms
nothing. A salted hash with the salt in the report could be.

`keyed` is the one function every hash of a value a report shows goes
through; `token` is its masked-value form. Every place that shows one
(the drilldown, the repair plan, the second reading, the evidence export,
the two-way conflicts) already calls `token`.

Limits: the secret lives in the user's state directory, so a report taken
to another machine cannot be re-tokenised there and tokens differ between
users and machines (they only ever needed to agree within one report). Where
the state directory cannot be written the secret is drawn for the process
alone: tokens then agree within that run and not across runs, never
weaker. Tokens made before this (a salted SHA-256) do not match the new
ones. Not covered here: the integrity digests of files that hold values
(`applied_sha256`, `plain_sha256`, a conflict record's `sha256`) - those
are plain hashes of a whole file, checked by recomputing them, and stay so.
What migkit keeps on disk for its own repairs (the drilldown key files) is
not masked: those are its working state, not a report.

The secret can be replaced (`rotate`, `migkit secrets rotate-mask`): every
token made under the old one stops matching, and says so with its reason
(`verify`) - a report records the key's id beside its salt (`mask.keyid`),
an id that is a digest of the key under a fixed label, so the old key need
not be kept to tell a token of the old key from a wrong value. `export`
describes the key by its shape (length, alphabet, id, when it was made),
never by its value.
"""
import hashlib
import hmac
import os


def _rules(hop):
    got = (getattr(hop, "options", None) or {}).get("mask")
    if got is None or got is False or got in ("", [], ()):
        return None
    if got is True or str(got).strip().lower() in ("all", "true", "yes"):
        return True
    if isinstance(got, (list, tuple, set)):
        return [str(x) for x in got]
    return [p.strip() for p in str(got).split(",") if p.strip()]


def active(hop):
    return _rules(hop) is not None


def _leaf(table):
    return str(table or "").rpartition(".")[2]


def column(hop, table, name):
    """Whether this column's values are shown masked."""
    r = _rules(hop)
    if r is None:
        return False
    if r is True:
        return True
    name = str(name)
    return any(p.lower() == name.lower()
               or p.lower() in (f"{_leaf(table)}.{name}".lower(),
                                f"{table}.{name}".lower())
               for p in r)


def keys(hop, table):
    """Whether this table's keys are shown masked: any of its columns is,
    or the table is not known and anything is."""
    r = _rules(hop)
    if r is None:
        return False
    if r is True or table is None:
        return True
    return any("." not in p or p.rpartition(".")[0].lower()
               in (_leaf(table).lower(), str(table).lower()) for p in r)


def _salt(hop):
    path = hop.report_dir() / "mask.salt"
    try:
        return path.read_bytes()
    except OSError:
        pass
    salt = os.urandom(16)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(salt)
        return salt
    except FileExistsError:
        return path.read_bytes()


#: {state directory: the user's local secret}, read once per process; the
#: other local keys (`state_key`) are kept as "<directory>|<file>"
_SECRETS = {}


def state_key(name="mask.key"):
    """A local key: 32 random bytes in file `name` under the state
    directory, 0600, made once. Never inside a report directory. The one
    place a key of the user's own is made and read (`mask.key` here,
    `transform.key` for `transforms`' encryption and tokens)."""
    from . import config
    where = config.state_home()
    slot = str(where) if name == "mask.key" else f"{where}|{name}"
    got = _SECRETS.get(slot)
    if got is not None:
        return got
    path = where / name
    try:
        got = path.read_bytes()
    except OSError:
        got = b""
    if len(got) < 32:
        fresh = os.urandom(32)
        got = fresh
        try:
            from . import durable
            durable.private_dir(where)
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(fresh)
        except FileExistsError:
            try:
                got = path.read_bytes()
            except OSError:
                pass
        except OSError:
            pass  # the process's own: it agrees within this run only
        if len(got) < 32:
            got = fresh
    _SECRETS[slot] = got
    return got


def _secret():
    """The user's local masking secret (`state_key`)."""
    return state_key("mask.key")


def keyed(hop, data):
    """The HMAC-SHA256 hex digest of `data` (bytes or text) under this hop's
    key: the local secret and the hop's salt. The one way a hash of a value
    is made for a report that may be shared (R17b.hmac)."""
    if not isinstance(data, bytes):
        data = str(data).encode("utf-8", "surrogatepass")
    salt = _salt(hop)
    _stamp(hop)
    key = hmac.new(_secret(), b"migkit-mask-v1|" + salt,
                   hashlib.sha256).digest()
    return hmac.new(key, data, hashlib.sha256).hexdigest()


def token(hop, value):
    """The value as shown masked; NULL stays NULL, which is not a value."""
    if value is None:
        return None
    return "masked:" + keyed(hop, str(value))[:10]


def shown(hop, table, name, value):
    """A value as a person may see it."""
    return token(hop, value) if column(hop, table, name) else value


def frame(hop, table, df):
    """The rows with their masked columns tokenised; equal stays equal."""
    if not active(hop):
        return df
    df = df.copy()
    for c in df.columns:
        if column(hop, table, c):
            df[c] = df[c].map(lambda v: token(hop, v))
    return df


# --- the key's life: its id, its rotation, its description -------------------

def key_id():
    """A short id of the current key: a digest of it under a fixed label,
    which says which key made a token and tells nothing of the key."""
    return hmac.new(_secret(), b"migkit-mask-id-v1",
                    hashlib.sha256).hexdigest()[:12]


def _history_path():
    from . import config
    return config.state_home() / "mask.history"


def _history():
    """[{id, retired}] of the keys replaced, oldest first; the keys
    themselves are not kept."""
    import json
    try:
        got = json.loads(_history_path().read_text())
    except (OSError, ValueError):
        return []
    return [h for h in got if isinstance(h, dict)] \
        if isinstance(got, list) else []


def rotate(now=None):
    """Replace the masking secret by a new one. Returns (old id, new id).
    The old key is not kept: a token it made can no longer be made, and
    `verify` says so for a report that records its id. A state directory
    that cannot be written refuses (a key drawn for this process alone
    would be the same loss with nothing to show for it)."""
    import json
    import time
    from . import config, durable
    where = config.state_home()
    old = key_id()
    fresh = os.urandom(32)
    try:
        durable.private_dir(where)
        durable.write_atomic(where / "mask.key", fresh, mode=0o600)
        durable.write_atomic(_history_path(), json.dumps(
            _history() + [{"id": old, "retired": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(now))}]))
    except OSError as e:
        raise SystemExit(f"the masking key was not replaced: {where} cannot"
                         f" be written ({e})") from None
    _SECRETS[str(where)] = fresh
    return old, key_id()


def export():
    """The key described by its shape, never its value: {id, bytes, bits,
    alphabet, length_of_text, made, replaced, where}. `alphabet` and
    `length_of_text` are those of the key written as hex digits, the form
    a person would be shown it in; the value itself is not in the result."""
    import time
    from . import config
    path = config.state_home() / "mask.key"
    key = _secret()
    try:
        made = time.strftime("%Y-%m-%d %H:%M:%S",
                             time.localtime(path.stat().st_mtime))
    except OSError:
        made = None
    return {"id": key_id(), "bytes": len(key), "bits": 8 * len(key),
            "alphabet": "hex digits 0-9a-f", "length_of_text": 2 * len(key),
            "made": made, "replaced": len(_history()), "where": str(path)}


def _stamp(hop):
    """Record, beside the salt, which key the tokens of this report are
    made under: the id of each key that has made one, oldest first, added
    once. A report made before a rotation keeps the id of the key that
    made it until a run makes tokens under the new one."""
    path = hop.report_dir() / "mask.keyid"
    now = key_id()
    try:
        if now in path.read_text().split():
            return
    except OSError:
        pass
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a") as f:
            f.write(now + "\n")
    except OSError:
        pass


def made_under(hop):
    """[id] of the keys this hop's report records its tokens under, oldest
    first; empty where it records none (a report made before keys were
    recorded)."""
    try:
        return (hop.report_dir() / "mask.keyid").read_text().split()
    except OSError:
        return []


def verify(hop, shown, value, under=None):
    """(whether `shown` is the token of `value` now, why not). A token made
    under a key since replaced is said to be so, with when - it no longer
    matches whatever the value is; any other mismatch is a different
    value. `under` is the key id the token was made under (a saved report
    carries it); where not given, the report's record is used, which names
    the old key only while no run has made tokens under the new one."""
    ids = [under] if under else made_under(hop)
    if ids and key_id() not in ids:
        when = next((h.get("retired") for h in _history()
                     if h.get("id") == ids[0]), None)
        return False, (f"made under masking key {ids[0]}, replaced"
                       + (f" on {when}" if when else "")
                       + f" by {key_id()}: a token of the old key matches no"
                       " value now; run the report again to make them anew")
    if shown == token(hop, value):
        return True, ""
    return False, "a different value than the one shown"
