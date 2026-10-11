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


#: {state directory: the user's local secret}, read once per process
_SECRETS = {}


def _secret():
    """The user's local secret: 32 random bytes in `mask.key` under the
    state directory, 0600, made once. Never inside a report directory."""
    from . import config
    where = config.state_home()
    got = _SECRETS.get(str(where))
    if got is not None:
        return got
    path = where / "mask.key"
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
    _SECRETS[str(where)] = got
    return got


def keyed(hop, data):
    """The HMAC-SHA256 hex digest of `data` (bytes or text) under this hop's
    key: the local secret and the hop's salt. The one way a hash of a value
    is made for a report that may be shared (R17b.hmac)."""
    if not isinstance(data, bytes):
        data = str(data).encode("utf-8", "surrogatepass")
    key = hmac.new(_secret(), b"migkit-mask-v1|" + _salt(hop),
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
