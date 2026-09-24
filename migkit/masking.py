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

A masked value is shown as `masked:` and ten hex digits, a hash of the value
with a salt kept for this hop. Equal values show equal, so a comparison
over masked values still lines rows up and still says which differ. The
salt stays in the hop's report directory, readable by this user only. What
migkit keeps on disk for its own repairs (the drilldown key files) is not
masked: those are its working state, not a report.
"""
import hashlib
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


def token(hop, value):
    """The value as shown masked; NULL stays NULL, which is not a value."""
    if value is None:
        return None
    digest = hashlib.sha256(_salt(hop) + str(value).encode("utf-8",
                                                            "surrogatepass"))
    return "masked:" + digest.hexdigest()[:10]


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
